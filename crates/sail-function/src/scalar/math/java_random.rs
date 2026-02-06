/// Port of Java's `java.util.Random` linear congruential generator.
///
/// Produces the same sequence as `new java.util.Random(seed)` in Java/Spark.
/// Used by `histogram_numeric` for deterministic tie-breaking when merging bins.
#[derive(Debug, Clone)]
pub struct JavaRandom {
    state: u64,
}

impl JavaRandom {
    /// Creates a new RNG matching `new java.util.Random(seed)`.
    pub fn new(seed: u64) -> Self {
        let initial = (seed ^ 0x5DEECE66D) & ((1u64 << 48) - 1);
        Self { state: initial }
    }

    /// Returns the next random `f64` in [0.0, 1.0), matching `Random.nextDouble()`.
    pub fn next_f64(&mut self) -> f64 {
        let bits = self.next_bits(26) as u64;
        let frac = self.next_bits(27) as u64;
        (bits * (1u64 << 27) + frac) as f64 / ((1u64 << 53) as f64)
    }

    fn next_bits(&mut self, bits: u32) -> i32 {
        self.state = self.state.wrapping_mul(0x5DEECE66D).wrapping_add(0xB) & ((1u64 << 48) - 1);
        (self.state >> (48 - bits)) as i32
    }
}
