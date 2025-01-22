use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Read};

use anyhow::{Context, Result};
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest")?;

        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let mut buf = content.as_slice();
        let mut records = Vec::new();

        while buf.remaining() > 0 {
            let len = buf.get_u64() as usize;
            let hash = buf.get_u32();
            let slice = &buf[..len];
            assert!(hash == crc32fast::hash(slice), "Manifest hash error");
            let record: ManifestRecord = serde_json::from_slice(slice)?;
            records.push(record);
            buf.advance(len);
        }
        let manifest = Self {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let buf = serde_json::to_vec(&record)?;
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        let hash = crc32fast::hash(&buf);
        file.write_all(&hash.to_be_bytes())?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
