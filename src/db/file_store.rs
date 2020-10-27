
use std::{
    io,
    path::Path,
    fs::{OpenOptions, File},
    os::unix::fs::OpenOptionsExt,
    convert::TryInto
};

use crc32fast::Hasher;
use bytes::{Bytes};
use thiserror::Error;

#[cfg(target_os = "linux")]
use rio::Rio;

// Each key-value pair is up-to 256 bytes in the main page cache
//
// First byte = entry length
// Second byte = key length
// Second byte = value length
// 1-256 bytes = key

use super::{PageIndex};


#[repr(align(4096))]
struct Page([u8; 4096 as usize]);

impl Into<[u8; PAGE_SIZE]> for Page { fn into(self) -> [u8; PAGE_SIZE] { self.0 } }
const PAGE_SIZE: usize = std::mem::size_of::<Page>();
const PAGE_CHECKSUM_SIZE: usize = 4;
const PAGE_CONTENT_SIZE: usize = PAGE_SIZE - PAGE_CHECKSUM_SIZE;

impl Page {
    fn from_content(content: &Bytes) -> Page {
        let mut page = Page([0; PAGE_SIZE as usize]);

        {
            let (content_slice, checksum) = page.0.split_at_mut(PAGE_CONTENT_SIZE);
            content_slice.copy_from_slice(content);
            checksum.copy_from_slice({
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&content_slice);
                &hasher.finalize().to_le_bytes()
            });
        }

        page
    }
}

pub struct FileStore {
    file: File,

    #[cfg(target_os = "linux")]
    ring: Rio,
}

#[derive(Error, Debug)]
pub enum PageRetrieveError {
    #[error("{0}")]
    Io(#[source] #[from] io::Error),
    #[error("Short read")]
    ShortRead,
    #[error("Bad checksum")]
    BadChecksum
}

#[derive(Error, Debug)]
pub enum PageWriteError {
    #[error("{0}")]
    Io(#[source] #[from] io::Error),
    #[error("Write did not finish")]
    ShortWrite

}

impl FileStore {
    #[cfg(target_os = "linux")]
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<FileStore> {
        let ring = rio::new()?;
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        Ok(FileStore { file, ring })
    }

    pub async fn get_page(&self, idx: PageIndex) -> Result<Bytes, PageRetrieveError> {
        let (content, checksum) = {
            let mut page = Page([0; PAGE_SIZE as usize]);

            #[cfg(target_os = "linux")]
            {
                let read = self.ring.read_at(&self.file, &mut page.0, idx * (PAGE_SIZE as u64)).await?;
                if read != PAGE_SIZE {return Err(PageRetrieveError::ShortRead) };
            }
        
            let page: [u8; PAGE_SIZE] = page.into();

            let page = Bytes::from(page.to_vec());

            let content = page.slice(..PAGE_CONTENT_SIZE);
            let checksum = u32::from_le_bytes(page[PAGE_CONTENT_SIZE..].try_into().unwrap());

            (content, checksum)
        };


        {
            let mut hasher = Hasher::new();
            hasher.update(&content);

            if checksum == hasher.finalize() {
                return Err(PageRetrieveError::BadChecksum)
            }
        };

        Ok(content)
    } 

    pub async fn set_page(&mut self, idx: PageIndex, content: &Bytes) -> Result<(), PageWriteError> {
        let page = Page::from_content(content);
        let written = self.ring.write_at(&self.file, &page.0, idx * (PAGE_SIZE as u64)).await?;
        if written == content.len() {
            Ok(())
        } else {
            Err(PageWriteError::ShortWrite)
        }
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.ring.fsync_ordered(&self.file, rio::Ordering::Drain).await
    }
}
