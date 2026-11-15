//! Port of the Mersenne Twister of Apache Commons Math, which Spark uses for `uuid` and
//! `shuffle` (`RandomUUIDGenerator` and `RandomIndicesGenerator`).
//!
//! [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/random/MersenneTwister.java
//! [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/random/BitsStreamGenerator.java

const N: usize = 624;
const M: usize = 397;
const MAG01: [u32; 2] = [0x0, 0x9908_b0df];

/// A random number generator that produces the same sequence as
/// `org.apache.commons.math3.random.MersenneTwister` for a given seed.
#[derive(Debug, Clone)]
pub struct MersenneTwister {
    mt: Box<[u32; N]>,
    mti: usize,
}

impl MersenneTwister {
    /// Creates a generator as `new MersenneTwister(seed)`.
    pub fn new(seed: i64) -> Self {
        let mut generator = Self {
            mt: Box::new([0; N]),
            mti: N,
        };
        generator.set_seed_array(&[((seed as u64) >> 32) as u32, seed as u32]);
        generator
    }

    fn set_seed_int(&mut self, seed: u32) {
        self.mt[0] = seed;
        for i in 1..N {
            let prev = self.mt[i - 1];
            self.mt[i] = 1_812_433_253u32
                .wrapping_mul(prev ^ (prev >> 30))
                .wrapping_add(i as u32);
        }
        self.mti = N;
    }

    fn set_seed_array(&mut self, seed: &[u32]) {
        self.set_seed_int(19_650_218);
        let mut i = 1;
        let mut j = 0;
        for _ in 0..N.max(seed.len()) {
            let prev = self.mt[i - 1];
            self.mt[i] = (self.mt[i] ^ (prev ^ (prev >> 30)).wrapping_mul(1_664_525))
                .wrapping_add(seed[j])
                .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
            if j >= seed.len() {
                j = 0;
            }
        }
        for _ in 0..N - 1 {
            let prev = self.mt[i - 1];
            self.mt[i] = (self.mt[i] ^ (prev ^ (prev >> 30)).wrapping_mul(1_566_083_941))
                .wrapping_sub(i as u32);
            i += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
        }
        self.mt[0] = 0x8000_0000;
    }

    /// Returns the next `bits` random bits, as `next(bits)`.
    fn next(&mut self, bits: u32) -> u32 {
        if self.mti >= N {
            let mt = &mut self.mt;
            for k in 0..N - M {
                let y = (mt[k] & 0x8000_0000) | (mt[k + 1] & 0x7fff_ffff);
                mt[k] = mt[k + M] ^ (y >> 1) ^ MAG01[(y & 0x1) as usize];
            }
            for k in N - M..N - 1 {
                let y = (mt[k] & 0x8000_0000) | (mt[k + 1] & 0x7fff_ffff);
                mt[k] = mt[k + M - N] ^ (y >> 1) ^ MAG01[(y & 0x1) as usize];
            }
            let y = (mt[N - 1] & 0x8000_0000) | (mt[0] & 0x7fff_ffff);
            mt[N - 1] = mt[M - 1] ^ (y >> 1) ^ MAG01[(y & 0x1) as usize];
            self.mti = 0;
        }
        let mut y = self.mt[self.mti];
        self.mti += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y >> (32 - bits)
    }

    /// Returns the next random 64-bit integer, as `nextLong()`.
    pub fn next_i64(&mut self) -> i64 {
        let high = (self.next(32) as u64) << 32;
        let low = self.next(32) as u64;
        (high | low) as i64
    }

    /// Returns a random integer in `[0, n)` for a positive `n`, as `nextInt(n)`.
    pub fn next_int_bounded(&mut self, n: i32) -> i32 {
        debug_assert!(n > 0);
        if (n as u32).is_power_of_two() {
            return ((n as i64 * self.next(31) as i64) >> 31) as i32;
        }
        loop {
            let bits = self.next(31) as i32;
            let value = bits % n;
            if bits.wrapping_sub(value).wrapping_add(n - 1) >= 0 {
                return value;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_commons_math() {
        // `new MersenneTwister(seed)` of Apache Commons Math 3.6.1, as bundled with Spark 4.2.
        let mut rng = MersenneTwister::new(42);
        assert_eq!(
            [rng.next_i64(), rng.next_i64(), rng.next_i64()],
            [
                8004395815503774571,
                -6657214946198437198,
                -7864936058809080539
            ]
        );
        let mut rng = MersenneTwister::new(-7);
        assert_eq!(
            [
                rng.next_int_bounded(7),
                rng.next_int_bounded(7),
                rng.next_int_bounded(7)
            ],
            [0, 3, 5]
        );
        let mut rng = MersenneTwister::new(i64::MAX);
        assert_eq!(
            [
                rng.next_int_bounded(1),
                rng.next_int_bounded(8),
                rng.next_int_bounded(1_000_000_007)
            ],
            [0, 6, 850257208]
        );
        // Beyond the first block of 624 values.
        let mut rng = MersenneTwister::new(1);
        let value = (0..2000).fold(0i64, |acc, _| acc ^ rng.next_i64());
        assert_eq!(value, 7127784837572445277);
    }
}
