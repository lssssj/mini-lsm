#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes_mut = BytesMut::new();
        bytes_mut.extend_from_slice(&self.data);
        for u in &self.offsets {
            bytes_mut.put_u16(*u);
        }
        bytes_mut.put_u16(self.offsets.len() as u16);
        bytes_mut.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        if data.len() < 2 {
            return Block {
                data: vec![],
                offsets: vec![],
            };
        }
        let num_of_entries = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - num_of_entries * SIZEOF_U16 - SIZEOF_U16;
        let offsets = data[data_end..data_end + num_of_entries * SIZEOF_U16]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();

        Self { data, offsets }
    }
}
