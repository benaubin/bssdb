use bytes::Bytes;

use crate::RootNode;

pub struct WriteTransaction {
    root: RootNode,
    kv_pairs: Vec<KVPair>
}

struct KVPair {
    key: Bytes,
    value: Bytes,

    /// The index of the node to insert too (found once to speed up searches)
    node_idx: u64
}



impl WriteTransaction {
    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.root.

        self.kv_pairs.push(KVPair {
            key,
            value,
            
        });
    }
}