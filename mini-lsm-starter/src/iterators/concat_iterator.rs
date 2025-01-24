#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for table in sstables {
            assert!(table.first_key() <= table.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                if sstables[i].last_key() >= sstables[i + 1].first_key() {
                    println!(
                        "check_sst_valid {:?} {:?} {:?}",
                        i,
                        std::str::from_utf8(sstables[i].last_key().key_ref()),
                        std::str::from_utf8(sstables[i + 1].first_key().key_ref())
                    );
                }
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }

    pub fn move_until_valid(&mut self) -> Result<()> {
        while let Some(current) = self.current.as_ref() {
            if current.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                break;
            }
            let iter = SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?;
            self.current = Some(iter);
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(sstables.as_slice());
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(sstables.as_slice());
        let idx: usize = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        assert!(self.current.is_some());
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.current.is_some());
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            assert!(current.is_valid());
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
