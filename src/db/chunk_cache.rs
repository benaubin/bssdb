use std::collections::{HashMap};
use std::sync::{Mutex, Arc};
use bytes::Bytes;
use futures::future::{Shared, BoxFuture};
use futures::FutureExt;

use super::{PageIndex, FileStore, RetrieveError};

const CACHE_SHARDS: usize = 64;

type SharedLoad = Shared<BoxFuture<'static, Result<Bytes, RetrieveError>>>;

pub struct ChunkCache {
    store: Arc<FileStore>,
    shards: [Mutex<HashMap<PageIndex, SharedLoad>>; CACHE_SHARDS]
}

impl ChunkCache {
    pub fn get(&self, idx: PageIndex, overflow_size_hint: u32) -> SharedLoad {
        let cache_shard = unsafe { self.shards.get_unchecked(idx as usize % CACHE_SHARDS) };
        let mut cache_shard = cache_shard.lock().unwrap();

        match cache_shard.get(&idx) {
            Some(future) if use_cached_load(future) => {
                future.clone()
            },
            _ => {
                let store = self.store.clone();
                let future = async move {
                    store.get_chunk(idx, overflow_size_hint).await
                }.boxed().shared();
                cache_shard.insert(idx, future.clone());
                future
            }
        }
    }
}

/// Only use a cached load if it's still pending or if the result is okay.
fn use_cached_load<'f>(load: &SharedLoad) -> bool {
    match load.peek() {
        Some(a) => {
            let b: Result<Bytes, RetrieveError> = a.clone();
            b.is_ok()
        },
        None => {
            true
        }
    }
}