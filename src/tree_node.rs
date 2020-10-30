use std::sync::Arc;
use crate::db::Page;
use bytes::Bytes;

struct TreeNode {
    page_idx: u64,
    page: Arc<Page>
}

struct Leaf {
    node: TreeNode,
    value: ValuePointer
}

struct SearchPosition {
    node: TreeNode,
    unprefixed_key: Bytes
}

pub(crate) struct ValuePointer {

}

impl TreeNode {
    pub fn prefix<'a>(&'a self) -> &'a [u8] {todo!()}

    pub fn prev_node(&self) -> Arc<Option<TreeNode>> {todo!()}
    pub fn next_node(&self) -> Arc<Option<TreeNode>> {todo!()}

    pub fn search_node(&self, key: Bytes) -> TreeNode {todo!()}

    pub(crate) fn insert_here(&mut self, value: ValuePointer) -> Option<Leaf> {
        todo!()
    }
}
