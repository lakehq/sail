//! Port of Spark's `QuantileSummaries`, the Greenwald-Khanna sketch backing
//! `percentile_approx` / `approx_percentile`.
//!
//! Ported from
//! `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/QuantileSummaries.scala`
//! (Apache Spark). The structure is reproduced faithfully — including the head
//! buffer, the integer division in `query`'s `targetError`, and the
//! `relativeError` short-circuits — because `percentile_approx` results are only
//! Spark-compatible if the sketch evolves identically.
//!
//! Reference: Greenwald & Khanna, "Space-efficient Online Computation of
//! Quantile Summaries" (<https://doi.org/10.1145/375663.375670>).

/// Default compression threshold: the sampled buffer is compressed once it
/// grows past this many entries.
pub const DEFAULT_COMPRESS_THRESHOLD: usize = 10_000;

/// Size of the head buffer of not-yet-inserted observations.
const DEFAULT_HEAD_SIZE: usize = 50_000;

/// Orders two observations the way Spark's sort does.
///
/// Spark sorts the head buffer with `headSampled.toArray.sorted`
/// (`QuantileSummaries.scala:92`), whose implicit `Ordering[Double]` delegates
/// to `java.lang.Double.compare`. That routes every NaN through
/// `doubleToLongBits`, canonicalizing it — so a sign-set NaN compares equal to a
/// positive one and sorts last.
///
/// IEEE's `totalOrder`, which `f64::total_cmp` implements, honors the sign bit
/// instead and sorts `-NaN` first, below `-Infinity`. The two orders agree
/// everywhere else, `-0.0 < 0.0` included, so canonicalizing the sign of a NaN
/// is the entire difference — and it is not cosmetic: `compress_immut` drops the
/// first sample when `curr_head.value <= head.value` is false, which any
/// comparison against NaN is, so the end the NaN lands on decides whether the
/// sketch keeps its minimum.
fn spark_compare(a: f64, b: f64) -> std::cmp::Ordering {
    fn canonical(x: f64) -> f64 {
        if x.is_nan() { f64::NAN } else { x }
    }
    canonical(a).total_cmp(&canonical(b))
}

/// A sampled observation.
///
/// `g` is the minimum rank jump from the previous sample's minimum rank, and
/// `delta` is the maximum span of the rank.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    pub value: f64,
    pub g: i64,
    pub delta: i64,
}

#[derive(Debug, Clone)]
pub struct QuantileSummaries {
    compress_threshold: usize,
    relative_error: f64,
    sampled: Vec<Stats>,
    /// Number of observations inserted into `sampled`, excluding `head_sampled`.
    count: i64,
    compressed: bool,
    head_sampled: Vec<f64>,
}

impl QuantileSummaries {
    pub fn new(relative_error: f64) -> Self {
        Self {
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
            relative_error,
            sampled: Vec::new(),
            count: 0,
            compressed: false,
            head_sampled: Vec::new(),
        }
    }

    /// Rebuilds a compressed summary from serialized aggregate state.
    pub fn from_parts(relative_error: f64, sampled: Vec<Stats>, count: i64) -> Self {
        Self {
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
            relative_error,
            sampled,
            count,
            compressed: true,
            head_sampled: Vec::new(),
        }
    }

    pub fn is_compressed(&self) -> bool {
        self.compressed
    }

    /// Spark exposes `relativeError` as a public field on the summary; the
    /// accumulator needs it to rebuild a peer summary when merging state.
    pub fn relative_error(&self) -> f64 {
        self.relative_error
    }

    /// Heap actually held by the sketch, for DataFusion's memory accounting.
    ///
    /// Both buffers are reported by capacity, and `head_sampled` matters most:
    /// it grows to [`DEFAULT_HEAD_SIZE`] observations before it flushes.
    pub fn allocated_size(&self) -> usize {
        self.sampled.capacity() * size_of::<Stats>()
            + self.head_sampled.capacity() * size_of::<f64>()
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn sampled(&self) -> &[Stats] {
        &self.sampled
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0 && self.head_sampled.is_empty()
    }

    /// Inserts one observation, flushing and compressing on the same schedule
    /// as Spark's `insert`.
    pub fn insert(&mut self, x: f64) {
        self.head_sampled.push(x);
        self.compressed = false;
        if self.head_sampled.len() >= DEFAULT_HEAD_SIZE {
            self.insert_head_buffer();
            if self.sampled.len() >= self.compress_threshold {
                self.compress();
            }
        }
    }

    /// Spark's `withHeadBufferInserted`: merges the sorted head buffer into
    /// `sampled` in a single pass.
    fn insert_head_buffer(&mut self) {
        if self.head_sampled.is_empty() {
            return;
        }
        let mut sorted = std::mem::take(&mut self.head_sampled);
        sorted.sort_unstable_by(|a, b| spark_compare(*a, *b));

        let mut current_count = self.count;
        let mut new_samples: Vec<Stats> = Vec::with_capacity(self.sampled.len() + sorted.len());
        let mut sample_idx = 0;

        for (ops_idx, &current_sample) in sorted.iter().enumerate() {
            while sample_idx < self.sampled.len()
                && self.sampled[sample_idx].value <= current_sample
            {
                new_samples.push(self.sampled[sample_idx]);
                sample_idx += 1;
            }

            current_count += 1;
            // The first and the last inserted observations bound the stream, so
            // they carry no rank uncertainty.
            let delta = if new_samples.is_empty()
                || (sample_idx == self.sampled.len() && ops_idx == sorted.len() - 1)
            {
                0
            } else {
                (2.0 * self.relative_error * current_count as f64).floor() as i64
            };

            new_samples.push(Stats {
                value: current_sample,
                g: 1,
                delta,
            });
        }

        while sample_idx < self.sampled.len() {
            new_samples.push(self.sampled[sample_idx]);
            sample_idx += 1;
        }

        self.sampled = new_samples;
        self.count = current_count;
    }

    /// Spark's `compress`: flushes the head buffer, then applies the GK
    /// COMPRESS step.
    pub fn compress(&mut self) {
        self.insert_head_buffer();
        let merge_threshold = 2.0 * self.relative_error * self.count as f64;
        self.sampled = compress_immut(&self.sampled, merge_threshold);
        self.compressed = true;
    }

    /// Spark's `merge`. Both summaries must already be compressed.
    pub fn merge_with(&mut self, other: &QuantileSummaries) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.compress_threshold = other.compress_threshold;
            self.relative_error = other.relative_error;
            self.sampled = other.sampled.clone();
            self.count = other.count;
            self.compressed = other.compressed;
            return;
        }

        // Samples from one side inherit the other side's lack of precision, but
        // only where they interleave: values below (or above) everything on the
        // other side keep their original delta.
        let merged_relative_error = self.relative_error.max(other.relative_error);
        let merged_count = self.count + other.count;
        let additional_self_delta =
            (2.0 * other.relative_error * other.count as f64).floor() as i64;
        let additional_other_delta = (2.0 * self.relative_error * self.count as f64).floor() as i64;

        let mut merged: Vec<Stats> = Vec::with_capacity(self.sampled.len() + other.sampled.len());
        let mut self_idx = 0;
        let mut other_idx = 0;
        while self_idx < self.sampled.len() && other_idx < other.sampled.len() {
            let self_sample = self.sampled[self_idx];
            let other_sample = other.sampled[other_idx];

            let (next_sample, additional_delta) = if self_sample.value < other_sample.value {
                self_idx += 1;
                (
                    self_sample,
                    if other_idx > 0 {
                        additional_self_delta
                    } else {
                        0
                    },
                )
            } else {
                other_idx += 1;
                (
                    other_sample,
                    if self_idx > 0 {
                        additional_other_delta
                    } else {
                        0
                    },
                )
            };

            merged.push(Stats {
                delta: next_sample.delta + additional_delta,
                ..next_sample
            });
        }
        // By construction at most one of these runs.
        merged.extend_from_slice(&self.sampled[self_idx..]);
        merged.extend_from_slice(&other.sampled[other_idx..]);

        let merge_threshold = 2.0 * merged_relative_error * merged_count as f64;
        self.compress_threshold = other.compress_threshold;
        self.relative_error = merged_relative_error;
        self.sampled = compress_immut(&merged, merge_threshold);
        self.count = merged_count;
        self.compressed = true;
    }

    /// Spark's `query`: returns the approximate quantiles in the order the
    /// percentiles were given, or `None` when the summary holds no samples.
    ///
    /// The caller must compress first; an uncompressed head buffer would be
    /// silently ignored.
    pub fn query(&self, percentiles: &[f64]) -> Option<Vec<f64>> {
        let (first, last) = match (self.sampled.first(), self.sampled.last()) {
            (Some(first), Some(last)) => (first, last),
            _ => return None,
        };

        // Integer division, as in Spark: the halved error is truncated before
        // it is compared against the ranks.
        let max_span = self
            .sampled
            .iter()
            .map(|s| s.delta + s.g)
            .max()
            .unwrap_or(i64::MIN);
        let target_error = (max_span / 2) as f64;

        let mut index = 0;
        let mut min_rank = first.g;

        let mut order: Vec<usize> = (0..percentiles.len()).collect();
        order.sort_by(|&a, &b| percentiles[a].total_cmp(&percentiles[b]));

        let mut result = vec![0.0; percentiles.len()];
        for pos in order {
            let percentile = percentiles[pos];
            if percentile <= self.relative_error {
                result[pos] = first.value;
            } else if percentile >= 1.0 - self.relative_error {
                result[pos] = last.value;
            } else {
                let (new_index, new_min_rank, approx) =
                    self.find_approx_quantile(index, min_rank, target_error, percentile);
                index = new_index;
                min_rank = new_min_rank;
                result[pos] = approx;
            }
        }
        Some(result)
    }

    /// Spark's `findApproxQuantile`: scans forward from `index` for the sample
    /// whose rank interval brackets `ceil(percentile * count)`.
    fn find_approx_quantile(
        &self,
        index: usize,
        min_rank_at_index: i64,
        target_error: f64,
        percentile: f64,
    ) -> (usize, i64, f64) {
        let fallback = match self.sampled.last() {
            Some(last) => (self.sampled.len().saturating_sub(1), 0, last.value),
            None => return (0, 0, f64::NAN),
        };
        let Some(mut cur_sample) = self.sampled.get(index).copied() else {
            return fallback;
        };

        let rank = (percentile * self.count as f64).ceil() as i64;
        let mut i = index;
        let mut min_rank = min_rank_at_index;
        while i < self.sampled.len() - 1 {
            let max_rank = min_rank + cur_sample.delta;
            if (max_rank as f64) - target_error <= rank as f64
                && (rank as f64) <= min_rank as f64 + target_error
            {
                return (i, min_rank, cur_sample.value);
            }
            i += 1;
            cur_sample = self.sampled[i];
            min_rank += cur_sample.g;
        }
        fallback
    }
}

/// Spark's `compressImmut`: merges adjacent samples whose combined rank span
/// stays under `merge_threshold`.
///
/// The first and last samples are never merged away, which is what makes
/// `query`'s `relativeError` short-circuits return the true minimum and maximum.
fn compress_immut(current_samples: &[Stats], merge_threshold: f64) -> Vec<Stats> {
    let Some(&last) = current_samples.last() else {
        return Vec::new();
    };
    // Built back-to-front, then reversed, so this is a push instead of Spark's
    // prepend into a linked list.
    let mut res: Vec<Stats> = Vec::with_capacity(current_samples.len());
    let mut head = last;
    let mut i = current_samples.len() as isize - 2;
    while i >= 1 {
        let sample1 = current_samples[i as usize];
        if ((sample1.g + head.g + head.delta) as f64) < merge_threshold {
            head.g += sample1.g;
        } else {
            res.push(head);
            head = sample1;
        }
        i -= 1;
    }
    res.push(head);

    if let Some(&curr_head) = current_samples.first() {
        // Skip when `current_samples` has a single element: `curr_head` and
        // `head` are then the same sample.
        if curr_head.value <= head.value && current_samples.len() > 1 {
            res.push(curr_head);
        }
    }
    res.reverse();
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(value: f64, g: i64, delta: i64) -> Stats {
        Stats { value, g, delta }
    }

    /// `relativeError` small enough that nothing is merged, so the sketch is
    /// exact and results can be reasoned about directly.
    fn exact_summary(values: &[f64]) -> QuantileSummaries {
        let mut s = QuantileSummaries::new(1.0 / 1_000_000.0);
        for &v in values {
            s.insert(v);
        }
        s.compress();
        s
    }

    #[test]
    fn head_buffer_flushes_past_the_threshold() {
        let mut s = QuantileSummaries::new(1.0 / 1_000_000.0);
        for i in 0..DEFAULT_HEAD_SIZE - 1 {
            s.insert(i as f64);
        }
        // Still buffered: nothing has been inserted into `sampled` yet.
        assert_eq!(s.count(), 0);
        assert!(s.sampled().is_empty());

        s.insert((DEFAULT_HEAD_SIZE - 1) as f64);
        assert_eq!(s.count(), DEFAULT_HEAD_SIZE as i64);
        assert!(!s.sampled().is_empty());
    }

    #[test]
    fn count_survives_compression_of_a_large_stream() {
        let n = 120_000;
        let mut s = QuantileSummaries::new(0.01);
        for i in 0..n {
            s.insert(i as f64);
        }
        s.compress();
        assert_eq!(s.count(), n as i64);
        // The whole point of the sketch: memory stays bounded well below `n`.
        assert!(s.sampled().len() < n / 10);

        // 1% relative error over a uniform 0..n stream. A missing quantile
        // surfaces as NaN, which fails the bound below.
        let median = s
            .query(&[0.5])
            .and_then(|v| v.first().copied())
            .unwrap_or(f64::NAN);
        assert!(
            (median - (n as f64) / 2.0).abs() <= 0.01 * n as f64,
            "median {median} outside the relative error bound"
        );
    }

    /// `compress_immut` drops the first sample when `first.value <= second.value`
    /// is false — which NaN makes false. The fast path in `compress` must not
    /// skip that, or a NaN-poisoned sketch keeps a sample Spark discards.
    #[test]
    fn compress_keeps_sparks_nan_head_drop() {
        // Sorting puts the NaNs last, so `sampled` is [1.0, NaN, NaN] and the
        // head-drop test `1.0 <= NaN` is false: Spark discards the 1.0, leaving
        // NaN as the minimum the `relativeError` short-circuit returns.
        let mut s = QuantileSummaries::new(1.0 / 10_000.0);
        for v in [f64::NAN, f64::NAN, 1.0] {
            s.insert(v);
        }
        s.compress();
        assert_eq!(s.sampled().len(), 2, "the 1.0 head must be dropped");
        assert!(
            s.sampled().first().is_some_and(|st| st.value.is_nan()),
            "the surviving head must be NaN: {:?}",
            s.sampled()
        );
        assert_eq!(s.query(&[0.0]).map(|v| v[0].is_nan()), Some(true));

        // Directly: an unordered head makes the comparison false and drops it.
        let poisoned = vec![stats(f64::NAN, 1, 0), stats(1.0, 1, 0), stats(2.0, 1, 0)];
        let compressed = compress_immut(&poisoned, 0.0006);
        assert_eq!(
            compressed.len(),
            2,
            "NaN head must be dropped: {compressed:?}"
        );
        assert_eq!(compressed.first().map(|s| s.value), Some(1.0));

        // A totally ordered sketch is untouched — the fast path stays valid.
        let ordered = vec![stats(1.0, 1, 0), stats(2.0, 1, 0), stats(3.0, 1, 0)];
        assert_eq!(compress_immut(&ordered, 0.0006), ordered);
    }

    #[test]
    fn merge_adjusts_delta_like_the_spark_example() {
        // The worked example from Spark's `merge` comment:
        //   a = [(0, 1, 0), (20, 99, 0)]  (100 values in [0, 20])
        //   b = [(10, 1, 0), (30, 49, 0)] (50 values in [10, 30])
        // Only interleaving samples pick up the other side's uncertainty.
        let a =
            QuantileSummaries::from_parts(0.01, vec![stats(0.0, 1, 0), stats(20.0, 99, 0)], 100);
        let b =
            QuantileSummaries::from_parts(0.01, vec![stats(10.0, 1, 0), stats(30.0, 49, 0)], 50);

        let mut merged = a.clone();
        merged.merge_with(&b);

        assert_eq!(merged.count(), 150);
        let values: Vec<f64> = merged.sampled().iter().map(|s| s.value).collect();
        assert_eq!(values.first().copied(), Some(0.0));
        assert_eq!(values.last().copied(), Some(30.0));
        assert!(values.windows(2).all(|w| w[0] <= w[1]), "{values:?}");
    }

    #[test]
    fn merge_with_an_empty_summary_is_identity() {
        let a = exact_summary(&[1.0, 2.0, 3.0]);
        let empty = {
            let mut s = QuantileSummaries::new(1.0 / 1_000_000.0);
            s.compress();
            s
        };

        let mut left = a.clone();
        left.merge_with(&empty);
        assert_eq!(left.count(), 3);
        assert_eq!(left.query(&[0.5]), Some(vec![2.0]));

        let mut right = empty.clone();
        right.merge_with(&a);
        assert_eq!(right.count(), 3);
        assert_eq!(right.query(&[0.5]), Some(vec![2.0]));
    }

    #[test]
    fn merge_matches_a_single_pass_over_the_same_values() {
        let left_values: Vec<f64> = (0..500).map(|i| i as f64).collect();
        let right_values: Vec<f64> = (500..1000).map(|i| i as f64).collect();

        let mut merged = exact_summary(&left_values);
        merged.merge_with(&exact_summary(&right_values));

        let all: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let single = exact_summary(&all);

        assert_eq!(merged.count(), single.count());
        for p in [0.1, 0.25, 0.5, 0.75, 0.9] {
            let m = merged
                .query(&[p])
                .and_then(|v| v.first().copied())
                .unwrap_or(f64::NAN);
            let s = single
                .query(&[p])
                .and_then(|v| v.first().copied())
                .unwrap_or(f64::NAN);
            assert!((m - s).abs() <= 1.0, "p={p}: merged {m} vs single {s}");
        }
    }
}
