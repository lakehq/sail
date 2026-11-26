// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

const K: usize = 4096;
const SEED: u32 = 9001;
const MAX_THETA: u64 = i64::MAX as u64;

// [CREDIT]: https://github.com/apache/datasketches-java/blob/6.2.0/src/main/java/org/apache/datasketches/theta/HeapAlphaSketch.java
// Puffin requires the Alpha update family. The serialized blob is an ordered compact
// Theta sketch, which can be read and unioned by other DataSketches implementations.
pub(super) struct AlphaSketch {
    hashes: HashSet<u64>,
    theta: u64,
}

impl Default for AlphaSketch {
    fn default() -> Self {
        Self {
            hashes: HashSet::new(),
            theta: MAX_THETA,
        }
    }
}

impl AlphaSketch {
    pub(super) fn update(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let hash = (murmur3::murmur3_x64_128(&mut bytes, SEED)? as u64) >> 1;
        if hash != 0 && hash < self.theta && self.hashes.insert(hash) {
            if self.theta != MAX_THETA || self.hashes.len() > K {
                const ALPHA: f64 = K as f64 / (K + 1) as f64;
                self.theta = (self.theta as f64 * ALPHA) as u64;
            }
            // Over-theta entries can never be accepted again. Prune them in batches.
            if self.hashes.len() >= 2 * K {
                self.hashes.retain(|&h| h < self.theta);
            }
        }
        Ok(())
    }

    pub(super) fn compact(self) -> (Vec<u8>, i64) {
        let mut hashes: Vec<_> = self
            .hashes
            .into_iter()
            .filter(|h| *h < self.theta)
            .collect();
        hashes.sort_unstable();
        let estimate = (hashes.len() as f64 / (self.theta as f64 / MAX_THETA as f64)) as i64;
        let estimating = self.theta != MAX_THETA;
        if hashes.is_empty() && !estimating {
            return (vec![1, 3, 3, 0, 0, 0x1e, 0, 0], 0);
        }
        // Compact serialization v3: preamble words, version, compact family, reserved,
        // read-only | compact | ordered flags, and the little-endian hash of seed 9001.
        let words = if estimating {
            3
        } else if hashes.len() == 1 {
            1
        } else {
            2
        };
        let flags = if words == 1 { 0x3a } else { 0x1a };
        let mut bytes = vec![words, 3, 3, 0, 0, flags, 0xcc, 0x93];
        if words > 1 {
            bytes.extend((hashes.len() as u32).to_le_bytes());
            bytes.extend(1_f32.to_le_bytes());
        }
        if estimating {
            bytes.extend(self.theta.to_le_bytes());
        }
        for hash in hashes {
            bytes.extend(hash.to_le_bytes());
        }
        (bytes, estimate)
    }
}
