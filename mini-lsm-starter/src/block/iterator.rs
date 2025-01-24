use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut entry = block.data.as_slice();
        let key_overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key_slice = &entry[..key_len];
        entry.advance(key_len);
        let ts = entry.get_u64();
        let key = KeyVec::from_vec_with_ts(key_slice.to_vec(), ts);
        let first_key = KeyVec::from_vec_with_ts(key_slice.to_vec(), ts);
        let value_len = (&block.data[(2 + 2 + key_len + 8)..]).get_u16() as usize;
        let value_range = (2 + 2 + key_len + 8 + 2, 2 + 2 + key_len + 8 + 2 + value_len);
        Self {
            block,
            key,
            value_range,
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::create_and_seek_to_first(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        let key_len = self.block.offsets.len();
        self.idx < key_len
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek_to_idx(self.idx);
    }

    pub fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            return;
        }
        self.idx = idx;
        let offset = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset..];
        let key_overlap_len = entry.get_u16() as usize;
        let rest_key = entry.get_u16();
        let rest_key_len = rest_key as usize;
        let mut key = KeyVec::new();
        key.append(&self.first_key.key_ref()[..key_overlap_len]);
        key.append(&entry[..rest_key_len]);
        entry.advance(rest_key_len);
        let ts = entry.get_u64();
        key.set_ts(ts);
        self.key = key;
        let value_len = entry.get_u16() as usize;
        self.value_range = (
            offset + 2 + 2 + rest_key_len + 8 + 2,
            offset + 2 + 2 + rest_key_len + 8 + 2 + value_len,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let keys = self.block.offsets.len();
        for i in 0..keys {
            self.seek_to_idx(i);
            if self.key.as_key_slice() >= key {
                return;
            }
        }
        self.idx += 1;
    }

    pub fn conclude(&self) {
        println!(
            "{:?} {:?} {:?}",
            self.block.data.len(),
            self.block.offsets.len(),
            self.idx
        )
    }
}
