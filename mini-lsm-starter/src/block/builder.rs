use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: &KeyVec, key: KeySlice) -> usize {
    let key_vec = key.to_key_vec();
    let mut len = 0;
    for i in 0..first_key.len() {
        if i >= key_vec.len() {
            break;
        }
        if first_key.raw_ref()[i] != key_vec.raw_ref()[i] {
            break;
        }
        len += 1;
    }

    len
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + 3 * SIZEOF_U16 /* key_len, value_len, offset_len */ > self.block_size
            && !self.is_empty()
        {
            // println!("block full {:?} {:?} {:?}", self.block_size, self.estimated_size(), key.len() + value.len() + 3 * SIZEOF_U16);
            return false;
        }
        self.offsets.push(self.data.len() as u16);

        let overlap_len = compute_overlap(&self.first_key, key);
        self.data.put_u16(overlap_len as u16);
        let rest_key_len = key.len() - overlap_len;
        self.data.put_u16(rest_key_len as u16);
        let x = &key.raw_ref()[overlap_len..];
        assert!(x.len() == key.len() - overlap_len);
        self.data.put(x);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(!self.is_empty(), "block is empty");
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
