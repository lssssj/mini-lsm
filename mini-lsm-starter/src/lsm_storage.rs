#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{self, Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // create memtable and skip updating manifest
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

fn range_overlap(
    first_key: &KeyBytes,
    last_key: &KeyBytes,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> bool {
    match lower {
        Bound::Excluded(key) if last_key.raw_ref() <= key => {
            return false;
        }
        Bound::Included(key) if last_key.raw_ref() < key => {
            return false;
        }
        _ => {}
    }

    match upper {
        Bound::Excluded(key) if first_key.raw_ref() >= key => {
            return false;
        }
        Bound::Included(key) if first_key.raw_ref() > key => {
            return false;
        }
        _ => {}
    }
    true
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        // println!("call {:?}", self.next_sst_id);
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;
        let manifest;
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(manifest_path)?;
            let mut mem_ids = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        assert!(mem_ids.remove(&id), "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                        next_sst_id = next_sst_id.max(id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (snapshot, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = snapshot;
                        assert!(!output.is_empty(), "output is empty?");
                        next_sst_id = next_sst_id.max(output.iter().max().copied().unwrap());
                    }
                    ManifestRecord::NewMemtable(id) => {
                        mem_ids.insert(id);
                        next_sst_id = next_sst_id.max(id);
                    }
                }
            }

            let mut sst_cnt = 0;
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table = SsTable::open(
                    *table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *table_id))?,
                )?;
                state.sstables.insert(*table_id, Arc::new(table));
                sst_cnt += 1;
            }

            println!("sst_cnt {}", sst_cnt);
            next_sst_id += 1;

            // Sort SSTs on each level (only for leveled compaction)
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    })
                }
            }

            // recover memtable
            if options.enable_wal {
                let mut wal_cnt = 0;
                for mem_id in mem_ids.iter() {
                    let mem_table = MemTable::recover_from_wal(
                        *mem_id,
                        Self::path_of_wal_static(path, *mem_id),
                    )?;
                    if !mem_table.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(mem_table));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            next_sst_id += 1;
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(_key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }
        for mem in snapshot.imm_memtables.iter() {
            if let Some(value) = mem.get(_key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut sst_iters = Vec::new();
        let key_hash = farmhash::hash32(_key);
        for id in &snapshot.l0_sstables {
            let table = snapshot.sstables.get(id).unwrap();
            if let Some(bloom) = &table.bloom {
                if !bloom.may_contain(key_hash) {
                    continue;
                }
            }
            sst_iters.push(Box::new(
                SsTableIterator::create_and_seek_to_key(table.clone(), KeySlice::from_slice(_key))
                    .unwrap(),
            ));
        }
        let sst_merge_iter = MergeIterator::create(sst_iters);

        let mut iters = Vec::new();
        for level in &snapshot.levels {
            let mut ssts = Vec::new();
            for sst_id in &level.1 {
                let table = snapshot.sstables.get(sst_id);
                if let Some(table) = table {
                    ssts.push(table.clone());
                }
            }
            let sst_iter =
                SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(_key))?;
            iters.push(Box::new(sst_iter));
        }
        // let sst_iter = SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(_key))?;
        let iter = MergeIterator::create(iters);
        let two_iter = TwoMergeIterator::create(sst_merge_iter, iter)?;

        if two_iter.is_valid()
            && two_iter.key() == KeySlice::from_slice(_key)
            && !two_iter.value().is_empty()
        {
            return Ok(Some(Bytes::copy_from_slice(two_iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let size = {
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
            guard.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let size = {
            let guard = self.state.read();
            guard.memtable.put(_key, &[])?;
            guard.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    pub fn try_freeze(&self, size: usize) -> Result<()> {
        if size > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() > self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        // println!("here call id {:?}", id);
        let memtable = {
            if self.options.enable_wal {
                Arc::new(MemTable::create_with_wal(
                    memtable_id,
                    self.path_of_wal(memtable_id),
                )?)
            } else {
                Arc::new(MemTable::create(memtable_id))
            }
        };

        let mut guard = self.state.write();
        let mut state = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut state.memtable, memtable);

        state.imm_memtables.insert(0, old_memtable.clone());
        *guard = Arc::new(state);
        drop(guard);

        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;

        old_memtable.sync_wal()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let memtable_to_flush = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .expect("no imm_memtable to flush")
                .clone()
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let id = memtable_to_flush.id();
        let sst = Arc::new(sst_builder.build(
            id,
            Some(self.block_cache.clone()),
            self.path_of_sst(id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert!(mem.id() == id);
            snapshot.sstables.insert(id, sst);
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, id);
            } else {
                snapshot.levels.insert(0, (id, vec![id]));
            }
            *guard = Arc::new(snapshot);
        };

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_state_lock, manifest::ManifestRecord::Flush(id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; //

        let mut iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());
        let mem_iter = snapshot.memtable.scan(_lower, _upper);
        iters.push(Box::new(mem_iter));

        for table in &snapshot.imm_memtables {
            iters.push(Box::new(table.scan(_lower, _upper)));
        }
        let merge_iter = MergeIterator::create(iters);
        let mut sst_iters = Vec::new();

        for id in &snapshot.l0_sstables {
            let table = snapshot.sstables.get(id).unwrap();
            if !range_overlap(table.first_key(), table.last_key(), _lower, _upper) {
                continue;
            }

            match _lower {
                Bound::Included(key) => {
                    sst_iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_key(
                            table.clone(),
                            KeySlice::from_slice(key),
                        )
                        .unwrap(),
                    ));
                }
                Bound::Excluded(key) => {
                    let mut iter = Box::new(
                        SsTableIterator::create_and_seek_to_key(
                            table.clone(),
                            KeySlice::from_slice(key),
                        )
                        .unwrap(),
                    );
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                        let _ = iter.next();
                    }
                    sst_iters.push(iter);
                }
                Bound::Unbounded => {
                    sst_iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(table.clone()).unwrap(),
                    ));
                }
            };
        }
        let sst_merge_iter = MergeIterator::create(sst_iters);

        let inner = TwoMergeIterator::create(merge_iter, sst_merge_iter)?;

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if range_overlap(table.first_key(), table.last_key(), _lower, _upper) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }
        let iter = TwoMergeIterator::create(inner, MergeIterator::create(level_iters))?;
        let lsm_iter = LsmIterator::new(iter, map_bound(_upper))?;
        Ok(FusedIterator::new(lsm_iter))
    }
}
