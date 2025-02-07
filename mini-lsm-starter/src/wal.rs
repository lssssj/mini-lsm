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
            let batch_size = slice.get_u32() as usize;
            if batch_size > slice.remaining() {
                panic!("incomplete WAL");
            }
            let mut batch_buf = &slice[..batch_size];

            let mut kv_pairs = Vec::new();

            let single_checksum = crc32fast::hash(batch_buf);
            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16() as usize;
                let key_bytes = Bytes::copy_from_slice(&batch_buf[..key_len]);
                batch_buf.advance(key_len);
                let ts = batch_buf.get_u64();
                let val_len = batch_buf.get_u16() as usize;
                let val = Bytes::copy_from_slice(&batch_buf[..val_len]);
                batch_buf.advance(val_len);
                kv_pairs.push((KeyBytes::from_bytes_with_ts(key_bytes, ts), val));
            }
            slice.advance(batch_size);
            if single_checksum != slice.get_u32() {
                panic!("wal checksum error");
            }
            for (key, value) in kv_pairs {
                skiplist.insert(key, value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }

        // add checksum: week 2 day 7
        file.write_all(&(buf.len() as u32).to_be_bytes())?;
        file.write_all(&buf)?;
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
