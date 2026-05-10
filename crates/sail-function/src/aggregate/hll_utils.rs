//! Shared HyperLogLog (HLL) sketch implementation used by Spark-compatible HLL
//! aggregate and scalar functions.
//!
//! This module implements a self-contained HLL sketch with a binary
//! serialization format internal to Sail. The format is **not** byte-compatible
//! with Apache DataSketches' Java HLL serialization, but it provides the same
//! observable semantics for the Spark `hll_*` functions: cardinality
//! estimation, sketch merging (union), and configurable `lgConfigK`.
//!
//! Binary format:
//!
//! ```text
//! offset 0..4   : magic bytes "SHLL"
//! offset 4      : lgConfigK (u8, range [4, 21])
//! offset 5..end : 2^lgConfigK register bytes (each holds the leading-zero
//!                 count for a hash bucket; one byte per register).
//! ```

use std::hash::Hasher;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use twox_hash::XxHash64;

/// Magic bytes prefix for Sail HLL sketches.
pub const HLL_MAGIC: &[u8; 4] = b"SHLL";

/// The default `lgConfigK` value used by Spark's HLL sketch functions.
pub const DEFAULT_LG_CONFIG_K: u8 = 12;

/// Minimum supported `lgConfigK`.
pub const MIN_LG_CONFIG_K: u8 = 4;

/// Maximum supported `lgConfigK`.
pub const MAX_LG_CONFIG_K: u8 = 21;

/// Fixed seed used for deterministic Sail HLL value hashing. The seed has no
/// compatibility significance because Sail's sketch format is internal.
pub const HLL_HASH_SEED: u64 = 9001;

/// Returns `true` if the given [`DataType`] is a binary variant
/// (`Binary`, `LargeBinary`, `BinaryView`, or `FixedSizeBinary`).
pub fn is_binary_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Returns `true` if the given [`DataType`] can be coerced to `Binary`.
/// This includes all binary variants, all string variants (Spark allows
/// implicit `String -> Binary` cast), and `Null`.
pub fn is_coercible_to_binary(dt: &DataType) -> bool {
    is_binary_type(dt)
        || matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        )
}

/// A HyperLogLog sketch.
#[derive(Debug, Clone)]
pub struct HllSketch {
    lg_config_k: u8,
    /// Length is `1 << lg_config_k`. Each entry holds the maximum observed
    /// leading-zero count + 1 for a register.
    registers: Vec<u8>,
}

impl HllSketch {
    /// Creates a new empty sketch with the given `lgConfigK`.
    pub fn new(lg_config_k: u8) -> Result<Self> {
        validate_lg_config_k(lg_config_k)?;
        let m = 1usize << lg_config_k;
        Ok(Self {
            lg_config_k,
            registers: vec![0u8; m],
        })
    }

    pub fn lg_config_k(&self) -> u8 {
        self.lg_config_k
    }

    pub fn num_registers(&self) -> usize {
        self.registers.len()
    }

    /// Updates the sketch with a hashed representation of the given bytes.
    pub fn update_bytes(&mut self, value: &[u8]) {
        let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
        hasher.write(value);
        self.update_hash(hasher.finish());
    }

    /// Updates the sketch with the given pre-computed 64-bit hash.
    pub fn update_hash(&mut self, hash: u64) {
        let bucket_bits = self.lg_config_k as u32;
        let m = self.registers.len();
        let bucket = (hash & ((m as u64) - 1)) as usize;
        let remaining = hash >> bucket_bits;
        // Compute the leading-zero count + 1 over the useful (non-bucket)
        // bits of the hash. We have `useful_bits = 64 - bucket_bits` of
        // useful information. Because we right-shift `hash` by
        // `bucket_bits`, the top `bucket_bits` of `remaining` are always
        // zero; those are *not* real leading zeros of the useful region,
        // so we subtract `bucket_bits` from `remaining.leading_zeros()`
        // to get the leading-zero count within the useful bits, then
        // add 1.
        let useful_bits = 64 - bucket_bits;
        let lz = if remaining == 0 {
            useful_bits + 1
        } else {
            remaining.leading_zeros().saturating_sub(bucket_bits) + 1
        };
        let lz = u8::try_from(lz).unwrap_or(u8::MAX);
        if lz > self.registers[bucket] {
            self.registers[bucket] = lz;
        }
    }

    /// Merges another sketch into `self`. The other sketch must have the same
    /// `lgConfigK` value.
    pub fn merge(&mut self, other: &HllSketch) -> Result<()> {
        if self.lg_config_k != other.lg_config_k {
            return exec_err!(
                "HLL sketches have different lgConfigK values ({} vs {}); set allowDifferentLgConfigK to true to allow merging",
                self.lg_config_k,
                other.lg_config_k
            );
        }
        for (dst, src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *src > *dst {
                *dst = *src;
            }
        }
        Ok(())
    }

    /// Merges another sketch into `self`, downgrading `lgConfigK` to the
    /// smaller of the two when they differ. This implements the
    /// `allowDifferentLgConfigK = true` behavior.
    pub fn merge_lossy(&mut self, other: &HllSketch) -> Result<()> {
        if self.lg_config_k == other.lg_config_k {
            return self.merge(other);
        }
        let target_lg = self.lg_config_k.min(other.lg_config_k);
        if self.lg_config_k != target_lg {
            *self = self.downsample(target_lg)?;
        }
        let other_ds: HllSketch;
        let other_ref = if other.lg_config_k != target_lg {
            other_ds = other.downsample(target_lg)?;
            &other_ds
        } else {
            other
        };
        for (dst, src) in self.registers.iter_mut().zip(other_ref.registers.iter()) {
            if *src > *dst {
                *dst = *src;
            }
        }
        Ok(())
    }

    /// Returns a sketch downsampled to the given (smaller or equal)
    /// `lgConfigK`. In HLL, each hash's bucket index is the *low*
    /// `lgConfigK` bits of the hash. When reducing the bucket width from
    /// `K = lg_config_k` to `K' = target_lg < K`, every old bucket index
    /// `idx` collapses into the new bucket `idx & ((1 << K') - 1)`, i.e.
    /// `2^(K - K')` old buckets feed into each new bucket. We take the
    /// maximum register value of each such group as the new register
    /// value. The leading-zero rank stored in each register is computed
    /// from the most significant bits of the hash and is unchanged by
    /// reducing the bucket width, so taking the max preserves the
    /// cardinality estimate.
    fn downsample(&self, target_lg: u8) -> Result<HllSketch> {
        if target_lg > self.lg_config_k {
            return exec_err!(
                "cannot downsample HLL sketch from lgConfigK={} to {}",
                self.lg_config_k,
                target_lg
            );
        }
        if target_lg == self.lg_config_k {
            return Ok(self.clone());
        }
        let new_size = 1usize << target_lg;
        let mask = new_size - 1;
        let mut out = HllSketch::new(target_lg)?;
        for (old_idx, &old_val) in self.registers.iter().enumerate() {
            if old_val == 0 {
                continue;
            }
            let new_idx = old_idx & mask;
            if old_val > out.registers[new_idx] {
                out.registers[new_idx] = old_val;
            }
        }
        Ok(out)
    }

    /// Returns the estimated cardinality.
    pub fn estimate(&self) -> u64 {
        let m = self.registers.len() as f64;
        let alpha_m_sq = alpha_m_squared(self.registers.len());
        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum += 1.0f64 / ((1u64 << r) as f64);
            if r == 0 {
                zeros += 1;
            }
        }
        let raw_estimate = alpha_m_sq / sum;
        // Linear-counting correction for small range.
        let estimate = if raw_estimate <= 2.5 * m && zeros != 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw_estimate
        };
        // The floating-point boundary is not exact; this is only a defensive
        // saturation guard for non-finite or extreme estimates.
        estimate.round().min(u64::MAX as f64) as u64
    }

    /// Serializes the sketch to the Sail HLL binary format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + self.registers.len());
        out.extend_from_slice(HLL_MAGIC);
        out.push(self.lg_config_k);
        out.extend_from_slice(&self.registers);
        out
    }

    /// Deserializes a sketch from the Sail HLL binary format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
            return Err(DataFusionError::Execution(
                "invalid HLL sketch: missing magic header".to_string(),
            ));
        }
        let lg_config_k = bytes[4];
        validate_lg_config_k(lg_config_k)?;
        let m = 1usize << lg_config_k;
        if bytes.len() != 5 + m {
            return Err(DataFusionError::Execution(format!(
                "invalid HLL sketch: expected {} bytes for lgConfigK={}, got {}",
                5 + m,
                lg_config_k,
                bytes.len()
            )));
        }
        Ok(Self {
            lg_config_k,
            registers: bytes[5..].to_vec(),
        })
    }

    /// Returns the heap-allocated size of the sketch in bytes (excludes the
    /// inline struct size so that callers can combine it with
    /// `std::mem::size_of_val` without double-counting).
    pub fn heap_size(&self) -> usize {
        self.registers.capacity()
    }
}

fn validate_lg_config_k(lg_config_k: u8) -> Result<()> {
    if !(MIN_LG_CONFIG_K..=MAX_LG_CONFIG_K).contains(&lg_config_k) {
        return exec_err!(
            "lgConfigK must be in [{}, {}], got {}",
            MIN_LG_CONFIG_K,
            MAX_LG_CONFIG_K,
            lg_config_k
        );
    }
    Ok(())
}

fn alpha_m_squared(m: usize) -> f64 {
    let m_f = m as f64;
    let alpha = match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m_f),
    };
    alpha * m_f * m_f
}

/// Extracts a positive integer literal from a [`ScalarValue`], used to parse
/// the `lgConfigK` argument.
pub fn scalar_to_lg_config_k(scalar: &ScalarValue) -> Result<u8> {
    let value: i64 = match scalar {
        ScalarValue::Int8(Some(v)) => *v as i64,
        ScalarValue::Int16(Some(v)) => *v as i64,
        ScalarValue::Int32(Some(v)) => *v as i64,
        ScalarValue::Int64(Some(v)) => *v,
        ScalarValue::UInt8(Some(v)) => *v as i64,
        ScalarValue::UInt16(Some(v)) => *v as i64,
        ScalarValue::UInt32(Some(v)) => *v as i64,
        ScalarValue::UInt64(Some(v)) => *v as i64,
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => {
            return exec_err!("lgConfigK must be a non-null integer literal");
        }
        other => {
            return exec_err!(
                "lgConfigK must be an integer literal, got {}",
                other.data_type()
            );
        }
    };
    if !(MIN_LG_CONFIG_K as i64..=MAX_LG_CONFIG_K as i64).contains(&value) {
        return exec_err!(
            "lgConfigK must be in [{}, {}], got {}",
            MIN_LG_CONFIG_K,
            MAX_LG_CONFIG_K,
            value
        );
    }
    Ok(value as u8)
}

/// Extracts a boolean literal from a [`ScalarValue`], used to parse the
/// `allowDifferentLgConfigK` argument. Returns `false` when the literal is
/// null.
pub fn scalar_to_allow_diff(scalar: &ScalarValue) -> Result<bool> {
    match scalar {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        ScalarValue::Boolean(None) => Ok(false),
        other => exec_err!(
            "allowDifferentLgConfigK must be a boolean literal, got {}",
            other.data_type()
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn round_trip_estimate() {
        let mut sketch = HllSketch::new(DEFAULT_LG_CONFIG_K).expect("create sketch");
        for i in 0..1000i64 {
            sketch.update_bytes(&i.to_le_bytes());
        }
        let estimate = sketch.estimate();
        assert!(
            estimate as i64 - 1000 > -100 && estimate as i64 - 1000 < 100,
            "estimate {} too far from 1000",
            estimate
        );
    }

    #[test]
    fn small_distinct_count_exact() {
        let mut sketch = HllSketch::new(DEFAULT_LG_CONFIG_K).expect("create sketch");
        for i in [1i64, 2, 2, 3] {
            sketch.update_bytes(&i.to_le_bytes());
        }
        assert_eq!(sketch.estimate(), 3);
    }

    #[test]
    fn serde_round_trip() {
        let mut sketch = HllSketch::new(10).expect("create sketch");
        for i in 0..50i64 {
            sketch.update_bytes(&i.to_le_bytes());
        }
        let bytes = sketch.to_bytes();
        let restored = HllSketch::from_bytes(&bytes).expect("from_bytes");
        assert_eq!(sketch.estimate(), restored.estimate());
        assert_eq!(sketch.lg_config_k(), restored.lg_config_k());
    }

    #[test]
    fn merge_same_k() {
        let mut a = HllSketch::new(DEFAULT_LG_CONFIG_K).expect("a");
        let mut b = HllSketch::new(DEFAULT_LG_CONFIG_K).expect("b");
        for i in 0..3i64 {
            a.update_bytes(&i.to_le_bytes());
        }
        for i in 3..6i64 {
            b.update_bytes(&i.to_le_bytes());
        }
        a.merge(&b).expect("merge");
        assert_eq!(a.estimate(), 6);
    }

    #[test]
    fn merge_different_k_errors() {
        let mut a = HllSketch::new(10).expect("a");
        let b = HllSketch::new(11).expect("b");
        let err = a.merge(&b);
        assert!(err.is_err());
    }
}
