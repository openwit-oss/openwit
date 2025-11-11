//! Wrapper for bloomfilter crate to add serialization support

use anyhow::Result;
use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};

/// Serializable bloom filter wrapper
#[derive(Clone)]
pub struct SerializableBloom {
    inner: Bloom<Vec<u8>>,
    bitmap_bits: u64,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
}

impl SerializableBloom {
    pub fn new_for_fp_rate(items_count: usize, fp_p: f64) -> Self {
        let inner = Bloom::new_for_fp_rate(items_count, fp_p);
        
        // Extract internal state for serialization
        // Note: This is a workaround - ideally bloomfilter would implement Serialize
        let bitmap_bits = inner.number_of_bits();
        let k_num = inner.number_of_hash_functions();
        
        // Default SIP keys - in real implementation, these should be extracted
        let sip_keys = [(0, 0), (0, 0)];
        
        Self {
            inner,
            bitmap_bits,
            k_num,
            sip_keys,
        }
    }
    
    pub fn set(&mut self, item: &[u8]) {
        self.inner.set(&item.to_vec());
    }
    
    pub fn check(&self, item: &[u8]) -> bool {
        self.inner.check(&item.to_vec())
    }
}

impl std::fmt::Debug for SerializableBloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializableBloom")
            .field("bitmap_bits", &self.bitmap_bits)
            .field("k_num", &self.k_num)
            .field("sip_keys", &self.sip_keys)
            .finish()
    }
}

// Custom serialization since bloomfilter doesn't implement it
impl Serialize for SerializableBloom {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize the bloom filter's bitmap
        let bitmap = self.inner.bitmap();
        
        #[derive(Serialize)]
        struct BloomData<'a> {
            bitmap: &'a [u8],
            bitmap_bits: u64,
            k_num: u32,
            sip_keys: [(u64, u64); 2],
        }
        
        let data = BloomData {
            bitmap: &bitmap,
            bitmap_bits: self.bitmap_bits,
            k_num: self.k_num,
            sip_keys: self.sip_keys,
        };
        
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerializableBloom {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct BloomData {
            bitmap: Vec<u8>,
            bitmap_bits: u64,
            k_num: u32,
            sip_keys: [(u64, u64); 2],
        }
        
        let data = BloomData::deserialize(deserializer)?;
        
        // Reconstruct bloom filter from bitmap
        let inner = Bloom::from_existing(
            &data.bitmap,
            data.bitmap_bits,
            data.k_num,
            data.sip_keys,
        );
        
        Ok(Self {
            inner,
            bitmap_bits: data.bitmap_bits,
            k_num: data.k_num,
            sip_keys: data.sip_keys,
        })
    }
}