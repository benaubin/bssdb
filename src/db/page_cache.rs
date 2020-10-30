use std::collections::{HashMap};
use std::sync::Arc;
use bytes::Bytes;
use futures::future::{Shared, BoxFuture};
use futures::FutureExt;
use lru::LruCache;
use parking_lot::{Mutex};

use super::{PageIndex, FileStore, RetrieveError};

const CACHE_SHARDS: usize = 64;

type SharedLoad<'l> = Shared<BoxFuture<'l, Result<Bytes, RetrieveError>>>;

struct CacheShard<'l> {
    cache: Mutex<LruCache<PageIndex, Bytes>>,
    loads: Mutex<HashMap<PageIndex, SharedLoad<'l>>>
}

impl<'l> CacheShard<'l> {
    fn new(cache_shard_size: usize) -> CacheShard<'l> {
        CacheShard {
            cache: Mutex::new(LruCache::new(cache_shard_size)),
            loads: Mutex::new(HashMap::new())
        }
    }

    pub async fn get(&'l self, store: Arc<FileStore>, idx: PageIndex, overflow_size_hint: u32) -> Result<Bytes, RetrieveError> {
        if let Some(cached) = self.cache.lock().get(&idx) { return Ok(cached.clone()) };

        let mut loads = self.loads.lock();

        if let Some(in_progress) = loads.get(&idx) {
            let in_progress: SharedLoad = in_progress.clone().clone();

            std::mem::drop(loads);

            return in_progress.await;
        }

        let future = async move {
            let res = store.get_chunk(idx, overflow_size_hint).await;

            if let Ok(data) = res.clone() { self.cache.lock().put(idx, data); };
            self.loads.lock().remove(&idx);

            res
        }.boxed().shared();

        loads.insert(idx, future.clone());

        std::mem::drop(loads);

        future.await
    }
}

pub struct PageCache<'l> {
    store: Arc<FileStore>,
    shards: [CacheShard<'l>; CACHE_SHARDS]
}

impl<'l> PageCache<'l> {
    pub fn new(store: FileStore, cache_shard_size: usize) -> &'l PageCache<'l> {
        &PageCache {
            store: Arc::new(store),
            shards: [CacheShard::new(cache_shard_size); CACHE_SHARDS]
        }
    }

    pub async fn get(&'l self, idx: PageIndex, overflow_size_hint: u32) -> Result<Bytes, RetrieveError> {
        let cache_shard = unsafe { self.shards.get_unchecked(idx as usize % CACHE_SHARDS) };
        cache_shard.get(self.store.clone(), idx, overflow_size_hint).await
    }
}
