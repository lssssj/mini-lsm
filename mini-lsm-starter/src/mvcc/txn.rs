#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        let entry = self.local_storage.get(key);
        if let Some(entry) = entry {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        let iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut txn_local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let _ = txn_local_iter.next();
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(txn_local_iter, iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(&[]));
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        self.inner.write_batch(&batch)?;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::from_static(&[])))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = (entry.0.clone(), entry.1.clone()));
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
