#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut result = Vec::new();
        let mut builder: Option<SsTableBuilder> = None;

        while iter.is_valid() {
            if iter.value().is_empty() && compact_to_bottom_level {
                let _ = iter.next();
                continue;
            }
            if builder.is_some() {
                builder.as_mut().unwrap().add(iter.key(), iter.value());
            } else {
                builder = Some(SsTableBuilder::new(self.options.block_size));
                builder.as_mut().unwrap().add(iter.key(), iter.value());
            }

            if builder.as_mut().unwrap().estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let sst = Arc::new(builder.unwrap().build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?);
                result.push(sst);
                builder = None;
            }
            iter.next()?;
        }

        if let Some(builder) = builder {
            let id = self.next_sst_id();
            let sst = Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?);
            result.push(sst);
        }
        Ok(result)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match &_task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let state = self.state.read();

                let mut l0_sst_iters = Vec::new();
                for id in l0_sstables {
                    let table = state.sstables.get(id).unwrap();
                    l0_sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        table.clone(),
                    )?));
                }

                let mut l1_sst_iters = Vec::new();
                for id in l1_sstables {
                    let table = state.sstables.get(id).unwrap();
                    l1_sst_iters.push(table.clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_sst_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_sst_iters)?,
                )?;
                self.compact_sst_from_iter(iter, true)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                let state = self.state.read();

                if let Some(_) = upper_level {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        let table = state.sstables.get(id).unwrap();
                        upper_ssts.push(table.clone());
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;

                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        let table = state.sstables.get(id).unwrap();
                        lower_ssts.push(table.clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_sst_from_iter(iter, *is_lower_level_bottom_level)
                } else {
                    let mut l0_sst_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        let table = state.sstables.get(id).unwrap();
                        l0_sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            table.clone(),
                        )?));
                    }
                    let upper_iter = MergeIterator::create(l0_sst_iters);

                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        let table = state.sstables.get(id).unwrap();
                        lower_ssts.push(table.clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_sst_from_iter(iter, *is_lower_level_bottom_level)
                }
            }
            CompactionTask::Tiered(tiered) => {
                let state = self.state.read();
                let mut iters = Vec::with_capacity(tiered.tiers.len());
                for level in &tiered.tiers {
                    let mut ssts = Vec::with_capacity(level.1.len());
                    for id in &level.1 {
                        ssts.push(state.sstables.get(id).unwrap().clone());
                    }
                    let iter = SstConcatIterator::create_and_seek_to_first(ssts)?;
                    iters.push(Box::new(iter));
                }
                self.compact_sst_from_iter(
                    MergeIterator::create(iters),
                    tiered.bottom_tier_included,
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let state = self.state.read();

        let mut l1_sstables = Vec::new();
        for level in &state.levels {
            for sst_id in &level.1 {
                l1_sstables.push(*sst_id);
            }
        }
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: state.l0_sstables.clone(),
            l1_sstables,
        };
        let result = self.compact(&task)?;
        drop(state);

        let _state_lock = self.state_lock.lock();
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        snapshot.l0_sstables.clear();
        snapshot.levels.clear();
        snapshot.sstables.clear();

        let mut sst_ids = Vec::new();
        for sst in result {
            sst_ids.push(sst.sst_id());
            snapshot.sstables.insert(sst.sst_id(), sst.clone());
        }
        snapshot.levels.push((1, sst_ids));
        *guard = Arc::new(snapshot);
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        if let Some(task) = task {
            println!("compaction start");
            println!("mem_id {:?}", snapshot.memtable.id());
            print!("imm: ");
            for i in &snapshot.imm_memtables {
                print!("{:?} ", i.id());
            }
            println!();
            if !snapshot.l0_sstables.is_empty() {
                println!(
                    "L0 ({}): {:?}",
                    snapshot.l0_sstables.len(),
                    snapshot.l0_sstables,
                );
            }
            for (level, files) in &snapshot.levels {
                println!("L{level} ({}): {:?}", files.len(), files);
            }
            let result = self.compact(&task)?;
            let output: Vec<_> = result.iter().map(|sst| sst.sst_id()).collect();
            println!();

            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in result {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            let (mut new_snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, output.as_slice(), false);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &_state_lock,
                    crate::manifest::ManifestRecord::Compaction(task, output),
                )?;
            }

            for file in &files_to_remove {
                let res = new_snapshot.sstables.remove(file);
                assert!(res.is_some(), "error");
            }

            let mut guard = self.state.write();
            *guard = Arc::new(new_snapshot);
            drop(guard);
            self.dump_structure();
            println!("=======Compact=======\n");
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };

        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
