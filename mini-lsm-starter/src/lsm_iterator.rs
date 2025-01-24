#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type MemTableAndLevel0Iter =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;
type LsmIteratorInner = TwoMergeIterator<MemTableAndLevel0Iter, MergeIterator<SstConcatIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            end_bound,
            prev_key: Vec::new(),
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    pub fn move_to_non_delete(&mut self) -> Result<()> {
        loop {
            while self.inner.is_valid() && self.inner.key().to_key_vec().key_ref() == self.prev_key
            {
                self.inner.next()?;
            }
            if !self.inner.is_valid() {
                break;
            }

            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            if !self.inner.value().is_empty() {
                break;
            }
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if self.inner.is_valid() {
            match &self.end_bound {
                Bound::Included(key) => return self.key() <= &key[..],
                Bound::Excluded(key) => return self.key() < &key[..],
                Bound::Unbounded => return true,
            }
        }
        false
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_non_delete()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("has errored"))?;
        }
        if !self.is_valid() {
            return Ok(());
        }
        let nx = self.iter.next();
        if nx.is_err() {
            self.has_errored = true;
        }
        nx
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
