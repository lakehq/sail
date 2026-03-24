//! Port of Spark's XORShiftRandom
//!
//! This is a Rust implementation of Apache Spark's XORShiftRandom class:
//! https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala
//!
//! The implementation includes:
//! - MurmurHash3 seed hashing (matching Scala's scala.util.hashing.MurmurHash3)
//! - XORShift algorithm with shifts 21, 35, 4
//! - Java's Random.nextDouble() compatible output

/// A Spark-compatible XORShift random number generator.
///
/// This produces the same sequence of random numbers as Spark's
/// `org.apache.spark.util.random.XORShiftRandom` for a given seed.
#[derive(Debug, Clone)]
pub struct SparkXorShiftRandom {
    seed: i64,
}

impl SparkXorShiftRandom {
    /// Create a new RNG with the given seed.
    ///
    /// The seed is hashed using MurmurHash3, matching Spark's behavior.
    pub fn new(init: i64) -> Self {
        Self {
            seed: Self::hash_seed(init),
        }
    }

    /// Port of XORShiftRandom.hashSeed using MurmurHash3.
    ///
    /// This matches Scala's `MurmurHash3.bytesHash` with `MurmurHash3.arraySeed` (0x3c074a61).
    fn hash_seed(seed: i64) -> i64 {
        // Convert seed to big-endian bytes (like Java's ByteBuffer.putLong)
        let bytes = seed.to_be_bytes();

        // MurmurHash3.arraySeed = 0x3c074a61
        let array_seed: u32 = 0x3c074a61;

        // Hash twice like Spark does
        let low_bits = Self::murmur3_bytes_hash(&bytes, array_seed);
        let high_bits = Self::murmur3_bytes_hash(&bytes, low_bits);

        // Combine: (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
        ((high_bits as i64) << 32) | ((low_bits as i64) & 0xFFFFFFFF)
    }

    /// Port of Scala's MurmurHash3.bytesHash.
    ///
    /// This matches `scala.util.hashing.MurmurHash3.bytesHash`.
    fn murmur3_bytes_hash(data: &[u8], seed: u32) -> u32 {
        let mut h1 = seed;
        let len = data.len();

        // Process 4-byte chunks
        let n_blocks = len / 4;
        for i in 0..n_blocks {
            let i4 = i * 4;
            // Little-endian read for the block
            let k1 = u32::from_le_bytes([data[i4], data[i4 + 1], data[i4 + 2], data[i4 + 3]]);
            h1 = Self::mix(h1, k1);
        }

        // Process remaining bytes
        let tail_start = n_blocks * 4;
        let mut k1: u32 = 0;
        let tail_len = len - tail_start;

        if tail_len >= 3 {
            k1 ^= (data[tail_start + 2] as u32) << 16;
        }
        if tail_len >= 2 {
            k1 ^= (data[tail_start + 1] as u32) << 8;
        }
        if tail_len >= 1 {
            k1 ^= data[tail_start] as u32;
            k1 = k1.wrapping_mul(0xcc9e2d51);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(0x1b873593);
            h1 ^= k1;
        }

        // Finalization
        h1 ^= len as u32;
        Self::fmix32(h1)
    }

    fn mix(h1: u32, k1: u32) -> u32 {
        let mut k = k1;
        k = k.wrapping_mul(0xcc9e2d51);
        k = k.rotate_left(15);
        k = k.wrapping_mul(0x1b873593);

        let mut h = h1 ^ k;
        h = h.rotate_left(13);
        h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
        h
    }

    fn fmix32(mut h: u32) -> u32 {
        h ^= h >> 16;
        h = h.wrapping_mul(0x85ebca6b);
        h ^= h >> 13;
        h = h.wrapping_mul(0xc2b2ae35);
        h ^= h >> 16;
        h
    }

    /// Generate the next n bits of randomness.
    ///
    /// This is equivalent to Java's `Random.next(bits)`.
    fn next(&mut self, bits: i32) -> i32 {
        let mut next_seed = self.seed ^ (self.seed << 21);
        // >>> in Java/Scala is unsigned right shift
        next_seed ^= ((next_seed as u64) >> 35) as i64;
        next_seed ^= next_seed << 4;
        self.seed = next_seed;

        (next_seed & ((1i64 << bits) - 1)) as i32
    }

    /// Generate the next random double in [0.0, 1.0).
    ///
    /// This is equivalent to Java's `Random.nextDouble()`.
    pub fn next_double(&mut self) -> f64 {
        let high = (self.next(26) as i64) << 27;
        let low = self.next(27) as i64;
        ((high + low) as f64) / ((1i64 << 53) as f64)
    }

    /// Generate the next random 32-bit integer.
    ///
    /// This is equivalent to Java's `Random.nextInt()`.
    pub fn next_int(&mut self) -> i32 {
        self.next(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values from Spark's XORShiftRandom (verified via spark-shell).
    const SPARK_XORSHIFT_SEED_1: [f64; 5] = [
        0.6363787615254752,
        0.5993846534021868,
        0.134842710012538,
        0.07684163905460906,
        0.8539211111755448,
    ];

    const SPARK_XORSHIFT_SEED_24: [f64; 5] = [
        0.3943255396952755,
        0.48619924381941027,
        0.2923951640552428,
        0.33335316633280176,
        0.3981939745854918,
    ];

    #[test]
    fn test_spark_xorshift_seed_1() {
        let mut rng = SparkXorShiftRandom::new(1);
        for expected in SPARK_XORSHIFT_SEED_1 {
            let actual = rng.next_double();
            assert!(
                (actual - expected).abs() < 1e-15,
                "Expected {}, got {}",
                expected,
                actual
            );
        }
    }

    #[test]
    fn test_spark_xorshift_seed_24() {
        let mut rng = SparkXorShiftRandom::new(24);
        for expected in SPARK_XORSHIFT_SEED_24 {
            let actual = rng.next_double();
            assert!(
                (actual - expected).abs() < 1e-15,
                "Expected {}, got {}",
                expected,
                actual
            );
        }
    }
}
