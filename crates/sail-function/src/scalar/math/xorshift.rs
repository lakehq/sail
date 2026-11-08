//! Port of Spark's XORShiftRandom
//!
//! This is a Rust implementation of Apache Spark's XORShiftRandom class:
//! https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala
//!
//! The implementation includes:
//! - MurmurHash3 seed hashing (matching Scala's scala.util.hashing.MurmurHash3)
//! - XORShift algorithm with shifts 21, 35, 4
//! - Java's Random.nextDouble() compatible output
//! - Java's Random.nextGaussian() polar Box-Muller with cached spare value

/// A Spark-compatible XORShift random number generator.
///
/// This produces the same sequence of random numbers as Spark's
/// `org.apache.spark.util.random.XORShiftRandom` for a given seed.
#[derive(Debug, Clone)]
pub struct SparkXorShiftRandom {
    seed: i64,
    next_next_gaussian: f64,
    have_next_next_gaussian: bool,
}

impl SparkXorShiftRandom {
    /// Create a new RNG with the given seed.
    ///
    /// The seed is hashed using MurmurHash3, matching Spark's behavior.
    pub fn new(init: i64) -> Self {
        Self {
            seed: Self::hash_seed(init),
            next_next_gaussian: 0.0,
            have_next_next_gaussian: false,
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

    /// Generate the next pseudorandom Gaussian value with mean 0.0 and std dev 1.0.
    ///
    /// This is equivalent to Java's `Random.nextGaussian()`: polar Box-Muller using
    /// `next_double()` as the source of randomness, with the spare value from each
    /// pair cached for the following call.
    /// Ref: https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/Random.java#L695-L762
    pub fn next_gaussian(&mut self) -> f64 {
        if self.have_next_next_gaussian {
            self.have_next_next_gaussian = false;
            return self.next_next_gaussian;
        }
        let (v1, v2, s) = loop {
            let v1 = 2.0 * self.next_double() - 1.0;
            let v2 = 2.0 * self.next_double() - 1.0;
            let s = v1 * v1 + v2 * v2;
            if s < 1.0 && s != 0.0 {
                break (v1, v2, s);
            }
        };
        let multiplier = (-2.0 * strict_log(s) / s).sqrt();
        self.next_next_gaussian = v2 * multiplier;
        self.have_next_next_gaussian = true;
        v1 * multiplier
    }
}

// [CREDIT]: https://netlib.org/fdlibm/e_log.c (fdlibm 5.3, "@(#)e_log.c 1.3 95/01/18")
// ====================================================
// Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
// Developed at SunSoft, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this
// software is freely granted, provided that this notice
// is preserved.
// ====================================================
//
// __ieee754_log(x)
// Return the logrithm of x
//
// Method :
//   1. Argument Reduction: find k and f such that
//            x = 2^k * (1+f),
//       where  sqrt(2)/2 < 1+f < sqrt(2) .
//
//   2. Approximation of log(1+f).
//    Let s = f/(2+f) ; based on log(1+f) = log(1+s) - log(1-s)
//         = 2s + 2/3 s**3 + 2/5 s**5 + .....,
//              = 2s + s*R
//      We use a special Reme algorithm on [0,0.1716] to generate
//     a polynomial of degree 14 to approximate R The maximum error
//    of this polynomial approximation is bounded by 2**-58.45. In
//    other words,
//                2      4      6      8      10      12      14
//        R(z) ~ Lg1*s +Lg2*s +Lg3*s +Lg4*s +Lg5*s  +Lg6*s  +Lg7*s
//      (the values of Lg1 to Lg7 are listed in the program)
//    and
//        |      2          14          |     -58.45
//        | Lg1*s +...+Lg7*s    -  R(z) | <= 2
//        |                             |
//    Note that 2s = f - s*f = f - hfsq + s*hfsq, where hfsq = f*f/2.
//    In order to guarantee error in log below 1ulp, we compute log
//    by
//        log(1+f) = f - s*(f - R)    (if f is not too large)
//        log(1+f) = f - (hfsq - s*(hfsq+R)).    (better accuracy)
//
//    3. Finally,  log(x) = k*ln2 + log(1+f).
//                = k*ln2_hi+(f-(hfsq-(s*(hfsq+R)+k*ln2_lo)))
//       Here ln2 is split into two floating point number:
//            ln2_hi + ln2_lo,
//       where n*ln2_hi is always exact for |n| < 2000.
//
// Special cases:
//    log(x) is NaN with signal if x < 0 (including -INF) ;
//    log(+INF) is +INF; log(0) is -INF with signal;
//    log(NaN) is that NaN with no signal.
//
// Accuracy:
//    according to an error analysis, the error is always less than
//    1 ulp (unit in the last place).
//
// Constants:
// The hexadecimal values are the intended ones for the following
// constants. The decimal values may be used, provided that the
// compiler will convert from decimal to binary accurately enough
// to produce the hexadecimal values shown.
//
/// Returns the natural logarithm of `x`, bit for bit as Java's `StrictMath.log`,
/// which `Random.nextGaussian()` uses.
fn strict_log(x: f64) -> f64 {
    const LN2_HI: f64 = f64::from_bits(0x3fe6_2e42_fee0_0000);
    const LN2_LO: f64 = f64::from_bits(0x3dea_39ef_3579_3c76);
    const TWO54: f64 = f64::from_bits(0x4350_0000_0000_0000);
    const LG1: f64 = f64::from_bits(0x3fe5_5555_5555_5593);
    const LG2: f64 = f64::from_bits(0x3fd9_9999_9997_fa04);
    const LG3: f64 = f64::from_bits(0x3fd2_4924_9422_9359);
    const LG4: f64 = f64::from_bits(0x3fcc_71c5_1d8e_78af);
    const LG5: f64 = f64::from_bits(0x3fc7_4664_96cb_03de);
    const LG6: f64 = f64::from_bits(0x3fc3_9a09_d078_c69f);
    const LG7: f64 = f64::from_bits(0x3fc2_f112_df3e_5244);
    const THIRD: f64 = f64::from_bits(0x3fd5_5555_5555_5555);

    let mut x = x;
    let mut hx = (x.to_bits() >> 32) as i32;
    let lx = x.to_bits() as u32;
    let mut k: i32 = 0;
    if hx < 0x0010_0000 {
        if ((hx & 0x7fff_ffff) as u32 | lx) == 0 {
            return f64::NEG_INFINITY;
        }
        if hx < 0 {
            return f64::NAN;
        }
        k -= 54;
        x *= TWO54;
        hx = (x.to_bits() >> 32) as i32;
    }
    if hx >= 0x7ff0_0000 {
        return x + x;
    }
    k += (hx >> 20) - 1023;
    hx &= 0x000f_ffff;
    let i = (hx + 0x95f64) & 0x10_0000;
    x = f64::from_bits(
        (((hx | (i ^ 0x3ff0_0000)) as u32 as u64) << 32) | (x.to_bits() & 0xffff_ffff),
    );
    k += i >> 20;
    let f = x - 1.0;
    if (0x000f_ffff & (2 + hx)) < 3 {
        if f == 0.0 {
            if k == 0 {
                return 0.0;
            }
            let dk = k as f64;
            return dk * LN2_HI + dk * LN2_LO;
        }
        let r = f * f * (0.5 - THIRD * f);
        if k == 0 {
            return f - r;
        }
        let dk = k as f64;
        return dk * LN2_HI - ((r - dk * LN2_LO) - f);
    }
    let s = f / (2.0 + f);
    let dk = k as f64;
    let z = s * s;
    let mut i = hx - 0x6147a;
    let w = z * z;
    let j = 0x6b851 - hx;
    let t1 = w * (LG2 + w * (LG4 + w * LG6));
    let t2 = z * (LG1 + w * (LG3 + w * (LG5 + w * LG7)));
    i |= j;
    let r = t2 + t1;
    if i > 0 {
        let hfsq = 0.5 * f * f;
        if k == 0 {
            f - (hfsq - s * (hfsq + r))
        } else {
            dk * LN2_HI - ((hfsq - (s * (hfsq + r) + dk * LN2_LO)) - f)
        }
    } else if k == 0 {
        f - s * (f - r)
    } else {
        dk * LN2_HI - ((s * (f - r) - dk * LN2_LO) - f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strict_log_matches_java() {
        // `StrictMath.log(x)` in Java, where `f64::ln` differs in the last bit.
        for (x, expected) in [
            (0.3012676595157123, -1.199756175096359),
            (0.7141294836112025, -0.33669098348888105),
            (0.3949634040007439, -0.9289621664724979),
            (0.5842517929701989, -0.5374232367271445),
        ] {
            assert_eq!(strict_log(x), expected);
        }
        assert_eq!(strict_log(1.0), 0.0);
        assert_eq!(strict_log(0.0), f64::NEG_INFINITY);
        assert!(strict_log(-1.0).is_nan());
    }

    #[test]
    fn test_next_gaussian_matches_spark() {
        // `new XORShiftRandom(42).nextGaussian()` in Spark 4.2.
        let expected = [
            2.384479054241165,
            0.1920934041293524,
            0.7337336533286575,
            -0.5224480195716871,
            2.060084179317831,
            0.20963383826633825,
            -0.548526047771831,
            1.907968332171668,
            -0.41705865828987015,
            1.0872260173387922,
            0.952245128862656,
            0.8510993210731433,
            -0.14118066384892058,
            1.7477132048926163,
            0.10149926853578273,
            0.9645306446729486,
            1.0996250111363042,
            -0.878471078118558,
            -0.6581329650674823,
            -0.5268540568138337,
            0.5749488892067608,
            0.8799444982255561,
            0.6553338995859576,
        ];
        let mut rng = SparkXorShiftRandom::new(42);
        assert_eq!(expected.map(|_| rng.next_gaussian()), expected);
    }

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

    const SPARK_XORSHIFT_GAUSSIAN_SEED_42: [f64; 5] = [
        2.384479054241165,
        0.1920934041293524,
        0.7337336533286575,
        -0.5224480195716871,
        2.060084179317831,
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

    #[test]
    fn test_spark_xorshift_gaussian_seed_42() {
        let mut rng = SparkXorShiftRandom::new(42);
        for expected in SPARK_XORSHIFT_GAUSSIAN_SEED_42 {
            let actual = rng.next_gaussian();
            assert!(
                (actual - expected).abs() < 1e-15,
                "Expected {}, got {}",
                expected,
                actual
            );
        }
    }
}
