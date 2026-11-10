//! Port of the Poisson sampler that Spark uses for sampling with replacement.
//!
//! Spark's `PoissonSampler` draws from Apache Commons Math 3.6.1 `PoissonDistribution`,
//! which is backed by a `Well19937c` generator, so the same seed gives the same counts.
//!
//! The code is a Rust translation of the following Commons Math 3.6.1 sources
//! (`commons-math3-3.6.1-sources.jar`, SHA-256
//! `e2ff85a3c360d56c51a7021614a194f3fbaf224054642ac535016f118322934d`):
//!
//! - `AbstractWell` (seeding) and `Well19937c` (`next`) as [`Well19937c`]
//! - `BitsStreamGenerator` (`nextDouble`, `nextGaussian`) as [`Well19937c`]
//! - `ExponentialDistribution.sample` (mean 1) as [`SparkPoisson::exponential`]
//! - `CombinatoricsUtils.factorialLog` as [`factorial_log`]
//! - `PoissonDistribution.sample` and `nextPoisson` as [`SparkPoisson`]
//!
//! Commons Math uses `FastMath` for `exp`, `log`, `sqrt`, `sin` and `cos`, while this port
//! uses the Rust standard library. `FastMath` is accurate to about one ulp, so a count may
//! differ from Spark only when a random value falls within an ulp of a decision threshold.

// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/random/AbstractWell.java
// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/random/Well19937c.java
// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/random/BitsStreamGenerator.java
// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/distribution/ExponentialDistribution.java
// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/util/CombinatoricsUtils.java
// [CREDIT]: https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/distribution/PoissonDistribution.java

use std::f64::consts::PI;

const K: usize = 19937;
const M1: usize = 70;
const M2: usize = 179;
const M3: usize = 449;
const R: usize = K.div_ceil(32);

/// Returns `index % R` for an index below `2 * R`.
#[inline(always)]
fn wrap_index(index: usize) -> usize {
    if index >= R { index - R } else { index }
}

/// The number of `nextDouble` values that are generated at a time.
const BUF: usize = 256;

/// The `Well19937c` generator of Commons Math, with the `BitsStreamGenerator` methods.
///
/// The `nextDouble` values are generated ahead into a buffer, which gives the same sequence
/// since all other methods are based on `nextDouble`.
#[derive(Debug, Clone)]
pub struct Well19937c {
    v: Box<[u32; R]>,
    index: usize,
    next_gaussian: f64,
    buffer: Box<[f64; BUF]>,
    pos: usize,
}

impl Well19937c {
    /// Creates a generator in the state of `Well19937c.setSeed(long)`.
    pub fn new(seed: i64) -> Self {
        let mut v = Box::new([0u32; R]);
        v[0] = ((seed as u64) >> 32) as u32;
        v[1] = seed as u32;
        for i in 2..R {
            // `l` is the signed `int` widened to `long`, so `>>` is an arithmetic shift.
            let l = v[i - 2] as i32 as i64;
            v[i] = (1812433253_i64
                .wrapping_mul(l ^ (l >> 30))
                .wrapping_add(i as i64)) as u32;
        }
        Self {
            v,
            index: 0,
            next_gaussian: f64::NAN,
            buffer: Box::new([0.0; BUF]),
            pos: BUF,
        }
    }

    #[inline(always)]
    fn next(&mut self, bits: u32) -> u32 {
        let index = self.index;
        // The indices replace the `iRm1`, `iRm2`, `i1`, `i2` and `i3` tables of `AbstractWell`.
        let index_rm1 = wrap_index(index + R - 1);
        let index_rm2 = wrap_index(index + R - 2);

        let v0 = self.v[index];
        let v_m1 = self.v[wrap_index(index + M1)];
        let v_m2 = self.v[wrap_index(index + M2)];
        let v_m3 = self.v[wrap_index(index + M3)];

        let z0 = (0x80000000 & self.v[index_rm1]) ^ (0x7FFFFFFF & self.v[index_rm2]);
        let z1 = (v0 ^ (v0 << 25)) ^ (v_m1 ^ (v_m1 >> 27));
        let z2 = (v_m2 >> 9) ^ (v_m3 ^ (v_m3 >> 1));
        let z3 = z1 ^ z2;
        let mut z4 = z0 ^ (z1 ^ (z1 << 9)) ^ (z2 ^ (z2 << 21)) ^ (z3 ^ (z3 >> 21));

        self.v[index] = z3;
        self.v[index_rm1] = z4;
        self.v[index_rm2] &= 0x80000000;
        self.index = index_rm1;

        // add Matsumoto-Kurita tempering
        // to get a maximally-equidistributed generator
        z4 ^= (z4 << 7) & 0xe46e1700;
        z4 ^= (z4 << 15) & 0x9b868000;

        z4 >> (32 - bits)
    }

    #[inline(always)]
    pub fn next_double(&mut self) -> f64 {
        if self.pos == BUF {
            self.refill();
        }
        let value = self.buffer[self.pos];
        self.pos += 1;
        value
    }

    #[inline(never)]
    fn refill(&mut self) {
        for i in 0..BUF {
            self.buffer[i] = self.raw_next_double();
        }
        self.pos = 0;
    }

    #[inline(always)]
    fn raw_next_double(&mut self) -> f64 {
        let high = (self.next(26) as u64) << 26;
        let low = self.next(26) as u64;
        (high | low) as f64 * f64::from_bits(0x3CB0000000000000) // 0x1.0p-52
    }

    pub fn next_gaussian(&mut self) -> f64 {
        if self.next_gaussian.is_nan() {
            // generate a new pair of gaussian numbers
            let x = self.next_double();
            let y = self.next_double();
            let alpha = 2.0 * PI * x;
            let r = (-2.0 * y.ln()).sqrt();
            self.next_gaussian = r * alpha.sin();
            r * alpha.cos()
        } else {
            // use the second element of the pair already generated
            let random = self.next_gaussian;
            self.next_gaussian = f64::NAN;
            random
        }
    }
}

/// `ExponentialDistribution.EXPONENTIAL_SA_QI`: `qi = sum(ln(2)^i / i!)` for `i >= 1`
/// while `qi < 1`.
fn exponential_sa_qi() -> Vec<f64> {
    let ln2 = 2.0_f64.ln();
    let mut qi = 0.0;
    let mut i = 1;
    let mut factorial = 1.0_f64;
    let mut result = Vec::with_capacity(20);
    while qi < 1.0 {
        factorial *= i as f64;
        qi += ln2.powi(i) / factorial;
        result.push(qi);
        i += 1;
    }
    result
}

/// The factorials up to 20, as in `CombinatoricsUtils.FACTORIALS`.
fn factorial(n: usize) -> f64 {
    (1..=n as u64).product::<u64>() as f64
}

/// `CombinatoricsUtils.factorialLog` for a nonnegative `n`.
fn factorial_log(n: i32) -> f64 {
    let n = n.max(0) as usize;
    if n < 21 {
        return factorial(n).ln();
    }
    let mut log_sum = 0.0;
    for i in 2..=n {
        log_sum += (i as f64).ln();
    }
    log_sum
}

/// The `PoissonDistribution` of Commons Math with its own `Well19937c` generator.
///
/// `nextPoisson` computes its constants from the mean on every draw. They are computed
/// once here instead, with the same operations, so the values are the same.
#[derive(Debug, Clone)]
pub struct SparkPoisson {
    rng: Well19937c,
    method: Method,
    exponential_sa_qi: Vec<f64>,
}

#[derive(Debug, Clone, Copy)]
enum Method {
    /// The multiplication method for a mean below 40.
    Small(SmallMean),
    /// The normal and exponential method for a mean of at least 40.
    Large(LargeMean),
}

#[derive(Debug, Clone, Copy)]
struct SmallMean {
    mean: f64,
    /// `exp(-mean)`
    p: f64,
}

impl SmallMean {
    fn new(mean: f64) -> Self {
        Self {
            mean,
            p: (-mean).exp(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LargeMean {
    lambda: f64,
    log_lambda: f64,
    log_lambda_factorial: f64,
    /// The fractional part of the mean, if it is not below `Double.MIN_VALUE`.
    fractional: Option<SmallMean>,
    delta: f64,
    half_delta: f64,
    twolpd: f64,
    p1: f64,
    p2: f64,
    c1: f64,
}

impl SparkPoisson {
    /// Creates the distribution after `reseedRandomGenerator(seed)`. The mean must be positive.
    pub fn new(mean: f64, seed: i64) -> Self {
        let pivot = 40.0;
        let method = if mean < pivot {
            Method::Small(SmallMean::new(mean))
        } else {
            let lambda = mean.floor();
            let lambda_fractional = mean - lambda;
            let log_lambda = lambda.ln();
            let log_lambda_factorial = factorial_log(lambda as i32);
            // The fractional part is below 1, so it always uses the multiplication method.
            let fractional =
                (lambda_fractional >= f64::from_bits(1)).then(|| SmallMean::new(lambda_fractional));
            let delta = (lambda * (32.0 * lambda / PI + 1.0).ln()).sqrt();
            let half_delta = delta / 2.0;
            let twolpd = 2.0 * lambda + delta;
            let a1 = (PI * twolpd).sqrt() * (1.0 / (8.0 * lambda)).exp();
            let a2 = (twolpd / delta) * (-delta * (1.0 + delta) / twolpd).exp();
            let a_sum = a1 + a2 + 1.0;
            let p1 = a1 / a_sum;
            let p2 = a2 / a_sum;
            let c1 = 1.0 / (8.0 * lambda);
            Method::Large(LargeMean {
                lambda,
                log_lambda,
                log_lambda_factorial,
                fractional,
                delta,
                half_delta,
                twolpd,
                p1,
                p2,
                c1,
            })
        };
        Self {
            rng: Well19937c::new(seed),
            method,
            exponential_sa_qi: exponential_sa_qi(),
        }
    }

    /// `PoissonDistribution.sample`.
    pub fn sample(&mut self) -> i64 {
        let n = match self.method {
            Method::Small(small) => Self::next_poisson_small(&mut self.rng, &small),
            Method::Large(large) => self.next_poisson_large(&large),
        };
        n.min(i32::MAX as i64)
    }

    /// `nextPoisson` for a mean below 40.
    fn next_poisson_small(rng: &mut Well19937c, small: &SmallMean) -> i64 {
        let SmallMean { mean, p } = *small;
        let mut n: i64 = 0;
        let mut r = 1.0;
        while (n as f64) < 1000.0 * mean {
            let rnd = rng.next_double();
            r *= rnd;
            if r >= p {
                n += 1;
            } else {
                return n;
            }
        }
        n
    }

    /// `nextPoisson` for a mean of at least 40.
    fn next_poisson_large(&mut self, large: &LargeMean) -> i64 {
        let LargeMean {
            lambda,
            log_lambda,
            log_lambda_factorial,
            fractional,
            delta,
            half_delta,
            twolpd,
            p1,
            p2,
            c1,
        } = *large;
        let y2 = match fractional {
            None => 0,
            Some(fractional) => Self::next_poisson_small(&mut self.rng, &fractional),
        };

        let mut x;
        let mut y;
        let mut v;
        loop {
            let u = self.rng.next_double();
            if u <= p1 {
                let n = self.rng.next_gaussian();
                x = n * (lambda + half_delta).sqrt() - 0.5;
                if x > delta || x < -lambda {
                    continue;
                }
                y = if x < 0.0 { x.floor() } else { x.ceil() };
                let e = self.exponential();
                v = -e - (n * n / 2.0) + c1;
            } else if u > p1 + p2 {
                y = lambda;
                break;
            } else {
                x = delta + (twolpd / delta) * self.exponential();
                y = x.ceil();
                v = -self.exponential() - delta * (x + 1.0) / twolpd;
            }
            let a = if x < 0.0 { 1.0 } else { 0.0 };
            let t = y * (y + 1.0) / (2.0 * lambda);
            if v < -t && a == 0.0 {
                y += lambda;
                break;
            }
            let qr = t * ((2.0 * y + 1.0) / (6.0 * lambda) - 1.0);
            let qa = qr - (t * t) / (3.0 * (lambda + a * (y + 1.0)));
            if v < qa {
                y += lambda;
                break;
            }
            if v > qr {
                continue;
            }
            if v < y * log_lambda - factorial_log((y + lambda) as i32) + log_lambda_factorial {
                y += lambda;
                break;
            }
        }
        y2 + y as i64
    }

    /// `ExponentialDistribution.sample` with mean 1.
    fn exponential(&mut self) -> f64 {
        let qi = &self.exponential_sa_qi;
        // Step 1:
        let mut a = 0.0;
        let mut u = self.rng.next_double();

        // Step 2 and 3:
        while u < 0.5 {
            a += qi[0];
            u *= 2.0;
        }

        // Step 4 (now u >= 0.5):
        u += u - 1.0;

        // Step 5:
        if u <= qi[0] {
            return a + u;
        }

        // Step 6:
        let mut i = 0;
        let mut u2 = self.rng.next_double();
        let mut umin = u2;

        // Step 7 and 8:
        loop {
            i += 1;
            u2 = self.rng.next_double();
            if u2 < umin {
                umin = u2;
            }
            // Step 8:
            if u <= qi[i] {
                break;
            }
        }

        a + umin * qi[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from Commons Math 3.6.1 on the JVM of Spark 4.2.0:
    /// `d = new PoissonDistribution(mean); d.reseedRandomGenerator(seed); d.sample()` x 12.
    #[test]
    fn test_samples_match_commons_math() {
        let cases: &[(f64, i64, [i64; 12])] = &[
            (0.1, 1, [0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0]),
            (0.5, 3, [2, 0, 0, 0, 3, 1, 1, 0, 0, 1, 0, 0]),
            (1.0, -7, [0, 2, 0, 1, 0, 1, 1, 1, 1, 0, 1, 3]),
            (2.5, 7, [0, 1, 3, 1, 1, 2, 1, 3, 1, 3, 3, 2]),
            (39.9, 42, [43, 41, 47, 48, 47, 36, 43, 47, 44, 36, 35, 38]),
            (40.0, 5, [45, 50, 44, 43, 48, 48, 43, 39, 36, 34, 32, 36]),
            (45.5, 7, [46, 53, 44, 51, 37, 44, 43, 36, 48, 43, 44, 45]),
            (
                1000.25,
                -123456789012345,
                [
                    996, 1028, 991, 972, 1005, 1002, 974, 1000, 1006, 1041, 1020, 966,
                ],
            ),
        ];
        for (mean, seed, expected) in cases {
            let mut poisson = SparkPoisson::new(*mean, *seed);
            let actual: Vec<i64> = (0..12).map(|_| poisson.sample()).collect();
            assert_eq!(actual, expected, "mean {mean}, seed {seed}");
        }
    }

    #[test]
    fn test_factorial_log() {
        assert_eq!(factorial_log(5), 120_f64.ln());
        assert_eq!(factorial_log(20), 2432902008176640000_f64.ln());
        let expected: f64 = (2..=100).map(|i| (i as f64).ln()).sum();
        assert_eq!(factorial_log(100), expected);
    }
}
