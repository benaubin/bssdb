
use std::{
    io,
    path::Path,
    fs::{OpenOptions, File},
    os::unix::{
        fs::OpenOptionsExt,
        io::AsRawFd
    },
    sync::{Arc}
};
use thiserror::Error;

#[cfg(target_os = "linux")]
use rio::Rio;

#[cfg(target_family = "unix")]
use libc::{LOCK_NB, LOCK_EX};

use super::page::{PageContent, PAGE_SIZE, PageIndex};

pub struct FileStore {
    file: File,

    #[cfg(target_os = "linux")]
    ring: Rio,
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

fn lock_file_for_writing(file: &File) -> io::Result<()> {
    #[cfg(target_family="unix")]
    return {
        let status = unsafe { libc::flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        match status {
            0 => Ok(()),
            _ => Err(io::Error::last_os_error())
        }
    };

    #[cfg(not(target_family="unix"))]
    compile_error!("locking files is not supported on non-unix")
}

impl FileStore {
    pub async fn open<'a, P: AsRef<Path>>(path: P) -> io::Result<Arc<FileStore>> {
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
            ring: rio::new()?
        });

        Ok(store)
    }

    /// Read a page
    pub(super) async fn read_page(&self, idx: PageIndex) -> Result<PageContent, RetrieveError> {
        let mut page = PageContent::uninit();
       
        let file_pos = idx * (PAGE_SIZE as u64);
        let mut read_bytes = 0;

        {
            let buf = page.as_page_bytes(read_bytes);
            loop {
                read_bytes += if cfg!(target_os = "linux") {
                    self.ring.read_at(&self.file, &buf, file_pos + (read_bytes as u64)).await.map_err(Arc::new)?
                } else {
                    #[cfg(not(target_os = "linux"))]
                    compile_error!("not supported not on linux");
                    unreachable!();
                };

                if read_bytes >= PAGE_SIZE { break; }
            }
        }

        return Ok(unsafe { page.assume_init() })
    }

    pub(super) fn write_page<'a>(&'a self, page_idx: u64, page: &'a PageContent) -> PageWrite<'a> {
        let pos = page_idx * (PAGE_SIZE as u64);

        let content = page.as_slice();

        let ring = &self.ring;
        let file = &self.file;

        #[cfg(target_os = "linux")]
        return {
            // immediately start writing
            let completion = ring.write_at(file, &content, pos);

            PageWrite {
                file,
                pos,
                content,

                ring,
                completion
            }
        };
        
        #[cfg(not(target_os = "linux"))]
        compile_error!("writing is not supported for this os")
    }
}

pub struct PageWrite<'a> {
    file: &'a File,
    pos: u64,
    content: &'a [u8],

    #[cfg(target_os = "linux")]
    ring: &'a rio::Rio,
    #[cfg(target_os = "linux")]
    completion: rio::Completion<'a, usize>
}

impl<'a> PageWrite<'a> {
    pub(crate) async fn finish(self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        return {
            let PageWrite {ring, file, pos, content, completion } = self;

            let mut total_written = completion.await?;

            while PAGE_SIZE > total_written {
                let res = ring.write_at(
                    file,
                    &&content[total_written..],
                    pos + (total_written as u64)
                ).await;
                total_written += res?;
            }

            Ok(())
        };
        
        #[cfg(not(target_os = "linux"))]
        compile_error!("writing is not supported for this os")
    }
}
