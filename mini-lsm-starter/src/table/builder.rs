#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, SsTable};
use crate::key::KeyVec;
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::hash32(key.key_ref()));

        if !self.builder.add(key, value) {
            let mut builder = BlockBuilder::new(self.block_size);
            std::mem::swap(&mut self.builder, &mut builder);
            assert!(
                self.builder.add(key, value),
                "blockbuilder add key-value error"
            );
            let block = builder.build();
            let meta = BlockMeta {
                offset: self.data.len(),
                first_key: self.first_key.clone().into_key_bytes(),
                last_key: self.last_key.clone().into_key_bytes(),
            };
            self.meta.push(meta);
            self.data.append(&mut block.encode().to_vec());
            self.first_key = key.to_key_vec();
            self.last_key = key.to_key_vec();
        }
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        self.last_key = key.to_key_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut meta_vec = self.meta;
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: self.first_key.clone().into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
        };
        meta_vec.push(meta);
        let block = self.builder.build();
        let mut buf = self.data;
        buf.append(&mut block.encode().to_vec());
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&meta_vec, &mut buf);
        buf.put_u32(meta_offset as u32);
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: meta_vec.first().unwrap().first_key.clone(),
            last_key: meta_vec.last().unwrap().last_key.clone(),
            block_meta: meta_vec,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0, // will be changed to latest ts in week 2
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
