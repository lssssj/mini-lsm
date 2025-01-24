#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut slice = buf.as_slice();
        while slice.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = slice.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key_bytes = Bytes::copy_from_slice(&slice[..key_len]);
            hasher.write(&slice[..key_len]);
            slice.advance(key_len);
            let ts = slice.get_u64();
            hasher.write_u64(ts);
            let val_len = slice.get_u16() as usize;
            hasher.write_u16(val_len as u16);
            let val = Bytes::copy_from_slice(&slice[..val_len]);
            hasher.write(&slice[..val_len]);
            let hash = hasher.finalize();
            slice.advance(val_len);
            assert!(slice.get_u32() == hash, "wal hash error");
            let key = KeyBytes::from_bytes_with_ts(key_bytes, ts);
            skiplist.insert(key, val);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.raw_len() + value.len() + std::mem::size_of::<u16>());
        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.key_len() as u16);
        buf.put_u16(key.key_len() as u16);
        hasher.write(&key.key_ref());
        buf.put_slice(&key.key_ref());
        hasher.write_u64(key.ts());
        buf.put_u64(key.ts());
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        hasher.write(value);
        // add checksum: week 2 day 7
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
