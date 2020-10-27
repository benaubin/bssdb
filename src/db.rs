use lru::LruCache;
use bytes::{Bytes};
use futures::stream::StreamExt;

mod file_store;
pub use file_store::PageRetrieveError;


pub type PageIndex = u64;

pub struct DB {
    file_store: file_store::FileStore,
    cache: LruCache<PageIndex, Bytes>
}

impl DB {
    pub(crate) async fn get_pages(&mut self, idx: &[PageIndex]) -> Result<Vec<Bytes>, (PageIndex, PageRetrieveError)> {
        let mut pages = vec![Bytes::new(); idx.len()];

        let iter: Vec<_> = idx
            .iter()
            .enumerate()
            .filter(|(i, idx)| {
                if let Some(page) = self.cache.get(idx.clone()) { 
                    pages[i.clone()] = page.clone();
                    false
                } else {
                    true
                }
            }).collect();
            
        {
            let mut futures = futures::stream::FuturesUnordered::new();

            let file_store = &self.file_store;

            for (i, idx) in iter {
                let idx = idx.clone();
                futures.push(async move {
                    (i, idx, file_store.get_page(idx).await)
                })
            }

            while let Some((i, idx, res)) = futures.next().await {
                let bytes = res.map_err(|err| (idx, err))?;
                pages[i] = bytes.clone();
                self.cache.put(idx, bytes);
            }
        }

        Ok(pages)
    }
}

