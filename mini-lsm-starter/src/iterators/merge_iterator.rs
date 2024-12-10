#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None
            };
        }

        let mut binary_heap = BinaryHeap::new();
        if iters.iter().all(|b| !b.is_valid()) {
            let mut iters = iters;
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, iters.pop().unwrap()))
            };
        }

        let mut idx = 0;
        for elem in iters {
            if elem.is_valid() {
                binary_heap.push(HeapWrapper(idx, elem));
                idx += 1;
            }
        }
    
        let cur = binary_heap.pop().unwrap();
        MergeIterator {
            iters: binary_heap,
            current: Some(cur)
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let current = self.current.as_ref().unwrap();
        current.1.key()
    }

    fn value(&self) -> &[u8] {
        let current = self.current.as_ref().unwrap();
        current.1.value()
    }

    fn is_valid(&self) -> bool {
        let current = self.current.as_ref();
        match current {
            Some(wrapper) => wrapper.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        while let Some(mut heap ) = self.iters.peek_mut() {
            if heap.1.key() < current.1.key() {
                println!("bug {:?} {:?}, {:?} {:?}", std::str::from_utf8(heap.1.key().raw_ref()), std::str::from_utf8(heap.1.value()), std::str::from_utf8(current.1.key().raw_ref()), std::str::from_utf8(current.1.value()));
                assert!(false);
            }
            if heap.1.key() == current.1.key() {
                if let e @ Err(_) =  heap.1.next() {
                    PeekMut::pop(heap);
                    return e;
                }
                if !heap.1.is_valid() {
                    PeekMut::pop(heap);
                }
            } else {
                break;
            }
        }
        current.1.next()?;
        if !current.1.is_valid() {
            if let Some(cur) = self.iters.pop() {
                *current = cur;
            }
            return Ok(());
        }
        if let Some(mut heap) = self.iters.peek_mut() {
            if *heap > *current {
                std::mem::swap(&mut *heap, current);
            }
        }
        Ok(())
    }
}
