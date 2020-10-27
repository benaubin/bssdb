
use bytes::{Bytes};

mod file_store;
mod value_log;
mod chunk_cache;

pub use file_store::{FileStore, RetrieveError};

pub type PageIndex = u64;

pub struct DB {
    chunk_cache: chunk_cache::ChunkCache
}

impl DB {
    pub(crate) async fn get_chunk(&mut self, idx: PageIndex, overflow_size_hint: u32) -> Result<Bytes, file_store::RetrieveError> {
        self.chunk_cache.get(idx, overflow_size_hint).await
    }
}

