use super::{PageContent, page::Page};
use std::ops::{DerefMut, Deref};
use std::sync::Arc;

pub type TransactionIdx = u64;

struct DirtyPage<'t> {
    page: Page,
    txn: &'t Transaction,
}

pub struct DirtyPageGuard<'a, 't> (&'a mut DirtyPage<'t>);

pub enum TxPage<'t> {
    Shared {
        shared: Arc<Page>,
        txn: &'t Transaction
    },
    Dirty(DirtyPage<'t>)
}

pub(crate) struct Transaction {
    idx: TransactionIdx
}

impl Transaction {
    fn alloc_page(&self, content: PageContent) -> Page {
        let index = todo!();
        Page::new(content, index)
    }

    pub(crate) fn tx_page<'t>(&'t self, page: Arc<Page>) -> TxPage<'t> {
        TxPage::Shared {
            shared: page,
            txn: self
        }
    }
}

impl<'t> TxPage<'t> {
    fn dirty(&mut self) -> &mut DirtyPage<'t> {
        if let TxPage::Shared { shared, txn } = *self {
            let content = match Arc::try_unwrap(shared) {
                Ok(page) => page.content,
                Err(arc) => arc.content.clone()
            };
            *self = TxPage::Dirty(DirtyPage {
                page: txn.alloc_page(content),
                txn
            });
        }

        match self {
            TxPage::Dirty(dirty) => dirty,
            _ => unreachable!()
        }
    }

    pub fn write<'s>(&'s mut self) -> DirtyPageGuard<'s, 't> {
        DirtyPageGuard(self.dirty())
    }
}

impl<'t> Deref for TxPage<'t> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        match self {
            TxPage::Shared {shared, ..} => shared,
            TxPage::Dirty(dirty) => &dirty.page
        }
    }
}


impl<'a, 't> Deref for DirtyPageGuard<'a, 't> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.0.page
    }
}

impl<'a, 't> DerefMut for DirtyPageGuard<'a, 't> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.page
    }
}

impl<'a, 't> Drop for DirtyPageGuard<'a, 't> {
    fn drop(&mut self) {
        todo!("As an optimization, consider queueing a write-to-disk here.")
    }
}