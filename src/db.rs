
use bytes::{Bytes};

mod file_store;
mod page;
mod transaction;

pub use file_store::{FileStore, RetrieveError};
pub use page::{Page, PageContent, PageIndex};

pub struct DB {
    version: Bytes
}

