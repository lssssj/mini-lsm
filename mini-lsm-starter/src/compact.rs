#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::os::macos::raw::stat;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
use clap::builder;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{self, SsTable, SsTableBuilder, SsTableIterator};

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
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match &_task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sst_iters = Vec::new();
                let state = self.state.read();
                for id in l0_sstables {
                    let table = state.sstables.get(id).unwrap();
                    sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        table.clone(),
                    )?));
                }
                for id in l1_sstables {
                    let table = state.sstables.get(id).unwrap();
                    sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        table.clone(),
                    )?));
                }
                let mut iter = MergeIterator::create(sst_iters);
                let mut result = Vec::new();
                let mut builder: Option<SsTableBuilder> = None;
                while iter.is_valid() {
                    if iter.value().is_empty() {
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
                    let _ = iter.next();
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
            _ => Ok(Vec::new()),
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

        let state_lock = self.state_lock.lock();
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
        unimplemented!()
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
