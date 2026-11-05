use std::cmp::Ordering;
use std::mem::size_of_val;

use datafusion_common::ScalarValue;

// Greenwald-Khanna rank summary, following the buffered insertion and merge rules in
// [CREDIT]: https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/QuantileSummaries.scala
// Retain native numeric values to avoid Spark's lossy conversion through double.
const HEAD_SIZE: usize = 50000;

#[derive(Debug)]
pub(super) struct Sample {
    pub value: ScalarValue,
    pub g: u64,
    pub delta: u64,
}

#[derive(Debug)]
pub(super) struct QuantileSummary {
    pub sampled: Vec<Sample>,
    pub count: u64,
    head: Vec<ScalarValue>,
    relative_error: f64,
}

// A homogeneous numeric summary has a total order. Treat every NaN as greater
// than all finite values and infinities, consistently in insertion and merging.
fn compare(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    macro_rules! floats {
        ($a:expr, $b:expr) => {
            match ($a.is_nan(), $b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => $a.partial_cmp($b).unwrap_or(Ordering::Equal),
            }
        };
    }
    match (a, b) {
        (ScalarValue::Float16(Some(a)), ScalarValue::Float16(Some(b))) => floats!(a, b),
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => floats!(a, b),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => floats!(a, b),
        _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
    }
}

impl QuantileSummary {
    pub fn new(accuracy: usize) -> Self {
        Self {
            sampled: vec![],
            count: 0,
            head: vec![],
            relative_error: 1.0 / accuracy as f64,
        }
    }

    pub fn insert(&mut self, value: ScalarValue) {
        self.head.push(value);
        if self.head.len() == HEAD_SIZE {
            self.compress();
        }
    }

    pub fn compress(&mut self) {
        if !self.head.is_empty() {
            self.head.sort_by(compare);
            let mut samples = std::mem::take(&mut self.sampled).into_iter().peekable();
            let mut merged = Vec::with_capacity(samples.len() + self.head.len());
            let head_len = self.head.len();
            for (index, value) in self.head.drain(..).enumerate() {
                while samples
                    .peek()
                    .is_some_and(|s| compare(&s.value, &value) != Ordering::Greater)
                {
                    merged.extend(samples.next());
                }
                self.count += 1;
                let delta =
                    if merged.is_empty() || (samples.peek().is_none() && index + 1 == head_len) {
                        0
                    } else {
                        (2.0 * self.relative_error * self.count as f64).floor() as u64
                    };
                merged.push(Sample { value, g: 1, delta });
            }
            merged.extend(samples);
            self.sampled = merged;
        }
        self.compress_samples();
    }

    fn compress_samples(&mut self) {
        let threshold = 2.0 * self.relative_error * self.count as f64;
        let mut samples = std::mem::take(&mut self.sampled).into_iter();
        let Some(first) = samples.next() else {
            return;
        };
        let mut result = Vec::new();
        if let Some(mut head) = samples.next_back() {
            for sample in samples.rev() {
                if ((sample.g + head.g + head.delta) as f64) < threshold {
                    head.g += sample.g;
                } else {
                    result.push(head);
                    head = sample;
                }
            }
            result.push(head);
        }
        result.push(first);
        result.reverse();
        self.sampled = result;
    }

    pub fn merge(&mut self, sampled: Vec<Sample>, count: u64) {
        self.compress();
        if count == 0 {
            return;
        }
        if self.count == 0 {
            self.sampled = sampled;
            self.count = count;
            return;
        }
        let self_delta = (2.0 * self.relative_error * count as f64).floor() as u64;
        let other_delta = (2.0 * self.relative_error * self.count as f64).floor() as u64;
        let mut left = std::mem::take(&mut self.sampled).into_iter().peekable();
        let mut right = sampled.into_iter().peekable();
        let mut merged = Vec::with_capacity(left.len() + right.len());
        let mut left_seen = false;
        let mut right_seen = false;
        while let (Some(a), Some(b)) = (left.peek(), right.peek()) {
            let (sample, delta) = if compare(&a.value, &b.value) == Ordering::Less {
                left_seen = true;
                (left.next(), if right_seen { self_delta } else { 0 })
            } else {
                right_seen = true;
                (right.next(), if left_seen { other_delta } else { 0 })
            };
            if let Some(mut sample) = sample {
                sample.delta += delta;
                merged.push(sample);
            }
        }
        merged.extend(left);
        merged.extend(right);
        self.count += count;
        self.sampled = merged;
        self.compress_samples();
    }

    // Called only on a nonempty compressed summary and a validated percentage.
    pub fn quantile(&self, percentile: f64) -> ScalarValue {
        let first = &self.sampled[0];
        let last = &self.sampled[self.sampled.len() - 1];
        if percentile <= self.relative_error {
            return first.value.clone();
        }
        if percentile >= 1.0 - self.relative_error {
            return last.value.clone();
        }
        let target_error = self
            .sampled
            .iter()
            .map(|s| s.g + s.delta)
            .max()
            .unwrap_or(0)
            / 2;
        let rank = (percentile * self.count as f64).ceil() as u64;
        let mut min_rank = 0;
        for sample in &self.sampled {
            min_rank += sample.g;
            if min_rank + sample.delta <= rank + target_error && rank <= min_rank + target_error {
                return sample.value.clone();
            }
        }
        last.value.clone()
    }

    pub fn size(&self) -> usize {
        size_of_val(self)
            + self.head.capacity() * size_of_val(&ScalarValue::Null)
            + self.sampled.capacity() * std::mem::size_of::<Sample>()
    }
}
