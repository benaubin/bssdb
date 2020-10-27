
use std::{
    io,
    path::Path,
    fs::{OpenOptions, File},
    os::unix::{
        fs::OpenOptionsExt,
        io::AsRawFd
    },
    convert::TryInto,
    sync::{Arc, atomic::AtomicU64, atomic}
};
use crc32fast::Hasher;
use bytes::{Bytes, Buf, BufMut};
use thiserror::Error;

#[cfg(target_os = "linux")]
use rio::Rio;

#[cfg(target_family = "unix")]
use libc::{LOCK_NB, LOCK_EX};

use super::{PageIndex};

#[repr(align(4096))]
#[repr(C)]
#[derive(Clone)]
struct Page([u8; 4096 as usize]);

impl Into<[u8; PAGE_SIZE]> for Page { fn into(self) -> [u8; PAGE_SIZE] { self.0 } }
pub const PAGE_SIZE: usize = std::mem::size_of::<Page>();

const CHUNK_LEN_SIZE: usize = 4;
const CHUNK_CHECKSUM_SIZE: usize = 4;
pub const CHUNK_HEADER_SIZE: usize = 8;

pub struct FileStore {
    file: File,

    page_len: AtomicU64,

    #[cfg(target_os = "linux")]
    ring: Rio,
}

pub struct FileStoreWriter(Arc<FileStore>);

pub struct FileStoreTransaction<'w> {
    writer: &'w mut FileStoreWriter,
    uncommitted_pages: Vec<Page>,
    prior_page_n: u64
}

#[derive(Error, Debug, Clone)]
pub enum RetrieveError {
    #[error("{0}")]
    Io(#[source] #[from] Arc<io::Error>),
    #[error("Bad checksum")]
    BadChecksum,
    #[error("Ran out of pages to read")]
    OutOfPages
}

#[derive(Error, Debug)]
pub enum PageWriteError {
    #[error("{0}")]
    Io(#[source] #[from] io::Error)

}

trait PageVec {
    fn borrow_bytes<'s>(&'s self) -> &[u8];
}

impl PageVec for Vec<Page> {
    fn borrow_bytes<'s>(&'s self) -> &[u8] {
        let byte_ptr = self.as_ptr() as *const u8;
        let byte_len = self.len() * PAGE_SIZE;
        unsafe { std::slice::from_raw_parts(byte_ptr, byte_len) }
    }
}

impl FileStoreWriter {
    pub fn tx<'s>(&'s mut self) -> FileStoreTransaction<'s> {
        FileStoreTransaction {
            writer: self,
            uncommitted_pages: vec![],
            prior_page_n: self.0.page_len.load(atomic::Ordering::SeqCst)
        }
    }
}

impl<'w> FileStoreTransaction<'w> {
    /// Get the file store we're attached to
    #[inline(always)] pub fn store(&self) -> &FileStore { &self.writer.0 }
    
    /// Append a chunk to uncommited writes
    ///
    /// Returns page index and number of overflow pages
    pub fn append_chunk<B: Buf>(&mut self, chunk: &mut B) -> (PageIndex, u32) {
        let n_pages = {
            let len = chunk.remaining();
            assert!((len + CHUNK_HEADER_SIZE) % PAGE_SIZE == 0, "Chunk length must be (N * PAGE_SIZE) - CHUNK_HEADER_SIZE");
            len + CHUNK_HEADER_SIZE / PAGE_SIZE
        };

        let overflow_pages = (n_pages - 1).try_into().expect("chunk may only have 2^32 pages");

        let first_page_index = self.prior_page_n + (self.uncommitted_pages.len() as u64);

        self.uncommitted_pages.reserve(n_pages);

        let pages = std::iter::repeat(Page([0; PAGE_SIZE])).take(n_pages).map(|page| {
            self.uncommitted_pages.push(page);
            &mut self.uncommitted_pages.last_mut().unwrap().0
        });

        let first_page = pages.next().unwrap();

        let (first_page_header, first_page_content) = first_page.split_at_mut(CHUNK_HEADER_SIZE);

        let mut hasher = crc32fast::Hasher::new();
        chunk.copy_to_slice(first_page_content);
        hasher.update(first_page_content);

        for page in pages {
            chunk.copy_to_slice(page);
            hasher.update(page);
        }

        first_page_header.put_u32_le(overflow_pages);
        first_page_header.put_u32_le(hasher.finalize());

        (first_page_index, overflow_pages)
    }
    
    /// Commit pages
    pub async fn commit(self) -> Result<(), PageWriteError> {
        let n_pages = self.uncommitted_pages.len();
        assert!(n_pages > 0);

        let bytes = self.uncommitted_pages.borrow_bytes();

        let existing_len = self.store().page_len.load(atomic::Ordering::SeqCst);
        
        let starting_pos = existing_len * (PAGE_SIZE as u64);
        let mut total_written = 0;

        while bytes.len() > total_written {
            let written = if cfg!(target_os = "linux") {
                // TODO check if this write is safe and aligned correctly
                self.store().ring.write_at(&self.store().file, &&bytes[total_written..], starting_pos + (total_written as u64)).await?
            } else {
                #[cfg(not(target_os = "linux"))] compile_error!("only linux writing is supported");
                unreachable!();
            };
            
            total_written += written;
        }

        self.store().ring.fsync_ordered(&self.store().file, rio::Ordering::Drain).await?;
        self.store().page_len.store(existing_len + (n_pages as u64), atomic::Ordering::SeqCst);

        Ok(())
    }
}

#[cfg(target_family="unix")]
fn lock_file_for_writing(file: &File) -> io::Result<()> {
    let status = unsafe { libc::flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    match status {
        0 => Ok(()),
        1 => Err(io::Error::last_os_error())
    }
}

impl FileStore {
    pub async fn open<'a, P: AsRef<Path>>(path: P) -> io::Result<(Arc<FileStore>, FileStoreWriter)> {
        let mut file = OpenOptions::new();

        file.read(true)
            .write(true)
            .create(true);

        #[cfg(target_os = "linux")]
        file.custom_flags(libc::O_DIRECT);
        
        let file = file.open(path)?;
        
        lock_file_for_writing(&file)?;

        let len = file.metadata()?.len();
        let page_len = len / (PAGE_SIZE as u64);
        file.set_len(page_len * (PAGE_SIZE as u64));

        let store = Arc::new(FileStore {
            file,
            #[cfg(target_os = "linux")]
            ring: rio::new()?,
            page_len: AtomicU64::new(page_len)
        });

        let writer = FileStoreWriter(store.clone());

        Ok((store, writer))
    }

    /// Read pages into the uninitialized capacity of the given vector.
    ///
    /// Returns the number of pages that were read.
    async fn read_into_pages_vec(&self, starting_idx: PageIndex, pages: &mut Vec<Page>) -> Result<usize, RetrieveError> {
        let remaining = pages.capacity() - pages.len();

        let read_bytes = {
            // Reinterpret the vector's remaining capacity as a u8 buffer
            let buf = unsafe {
                std::slice::from_raw_parts_mut(
                    pages.as_mut_ptr().add(pages.len()) as *mut u8, 
                    remaining * PAGE_SIZE
                )
            };

            if cfg!(target_os = "linux") {
                // Read the pages into the buffer, returning the number of bytes read
                self.ring.read_at(&self.file, &buf, (starting_idx + (pages.len() as u64)) * (PAGE_SIZE as u64)).await?
            } else {
                #[cfg(not(target_os = "linux"))]
                compile_error!("Reading is only implemented for linux");
                unreachable!()
            }
        };

        // Calculate the number of full pages read
        let read_pages = read_bytes / PAGE_SIZE;

        if read_pages == 0 {
            return Err(RetrieveError::OutOfPages);
        }

        // Increase the length of the pages array
        unsafe { pages.set_len(pages.len() + read_pages) };

        // Return the number of pages read
        Ok(read_pages)
    }

    pub async fn get_chunk(&self, idx: PageIndex, overflow_size_hint: u32) -> Result<Bytes, RetrieveError> {
        let mut pages: Vec<Page> = Vec::with_capacity((overflow_size_hint as usize) + 1);

        self.read_into_pages_vec(idx, &mut pages).await?;

        let (overflow_size, checksum) = {
            let first_page = pages.first().unwrap();
            let mut header = &first_page.0[..CHUNK_HEADER_SIZE];
            let overflow_size = header.get_u32_le();
            let checksum = header.get_u32_le();
            (overflow_size, checksum)
        };

        let total_pages = overflow_size as usize + 1;
        if pages.len() > total_pages {
            println!("Chunk starting at index {} was overread (hint: {}, true: {})", idx, overflow_size_hint, overflow_size); // TODO: use a warn! macro
            pages.truncate(total_pages);
        } else if pages.len() < total_pages {
            pages.reserve_exact(total_pages - pages.len());

            while total_pages > pages.len() {
                self.read_into_pages_vec(idx, &mut pages).await?;
            }
        }

        let bytes = Bytes::from(pages.into_iter().map(|a| a.0).collect::<Vec<_>>().concat());
        let content = bytes.slice(CHUNK_HEADER_SIZE..);

        let mut hasher = Hasher::new();
        hasher.update(&content);

        if checksum == hasher.finalize() {
            return Err(RetrieveError::BadChecksum)
        }

        Ok(content)
    }
}
