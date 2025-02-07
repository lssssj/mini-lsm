#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        let num = self.readers.get(&ts);
        if let Some(num) = num {
            self.readers.insert(ts, num + 1);
        } else {
            self.readers.insert(ts, 1);
        }
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let num = self.readers.get(&ts);
        if let Some(num) = num {
            if *num > 1 {
                self.readers.insert(ts, num - 1);
            } else {
                self.readers.remove(&ts);
            }
        } else {
            panic!();
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|entry| *entry.0)
    }
}
