use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use datafusion_common::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimestampMicros(pub i64);

/// A predicate function.
pub type Predicate<T> = Arc<dyn Fn(&T) -> Result<bool> + Send + Sync>;

/// A range of values accepted by a filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueRange<T> {
    pub lower: Bound<T>,
    pub upper: Bound<T>,
}

/// A normalized union of ranges accepted by a filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueDomain<T> {
    ranges: Vec<ValueRange<T>>,
}

impl<T> ValueDomain<T> {
    pub fn all() -> Self {
        Self {
            ranges: vec![ValueRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            }],
        }
    }

    pub fn empty() -> Self {
        Self { ranges: vec![] }
    }

    pub fn ranges(&self) -> &[ValueRange<T>] {
        &self.ranges
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn is_all(&self) -> bool {
        matches!(
            self.ranges.as_slice(),
            [ValueRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            }]
        )
    }
}

impl<T: Clone + Ord> ValueDomain<T> {
    pub fn point(value: T) -> Self {
        Self::from_points([value])
    }

    pub fn from_points(values: impl IntoIterator<Item = T>) -> Self {
        Self {
            ranges: values
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|value| ValueRange {
                    lower: Bound::Included(value.clone()),
                    upper: Bound::Included(value),
                })
                .collect(),
        }
    }

    /// Returns the values when every range is a point, or `None` otherwise.
    pub fn points(&self) -> Option<Vec<T>> {
        self.ranges
            .iter()
            .map(|range| match (&range.lower, &range.upper) {
                (Bound::Included(lower), Bound::Included(upper)) if lower == upper => {
                    Some(lower.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn range(lower: Bound<T>, upper: Bound<T>) -> Self {
        let range = ValueRange { lower, upper };
        if Self::is_non_empty_range(&range) {
            Self {
                ranges: vec![range],
            }
        } else {
            Self::empty()
        }
    }

    pub fn intersect(&self, other: &Self) -> Self {
        let mut ranges = vec![];
        for left in &self.ranges {
            for right in &other.ranges {
                let range = ValueRange {
                    lower: Self::max_lower(&left.lower, &right.lower),
                    upper: Self::min_upper(&left.upper, &right.upper),
                };
                if Self::is_non_empty_range(&range) {
                    ranges.push(range);
                }
            }
        }
        Self { ranges }
    }

    fn max_lower(left: &Bound<T>, right: &Bound<T>) -> Bound<T> {
        match (left, right) {
            (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
            (Bound::Included(left), Bound::Included(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Included(right.clone()),
                std::cmp::Ordering::Greater => Bound::Included(left.clone()),
                std::cmp::Ordering::Equal => Bound::Included(left.clone()),
            },
            (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Excluded(right.clone()),
                std::cmp::Ordering::Greater => Bound::Included(left.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
            (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Included(right.clone()),
                std::cmp::Ordering::Greater => Bound::Excluded(left.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
            (Bound::Excluded(left), Bound::Excluded(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Excluded(right.clone()),
                std::cmp::Ordering::Greater => Bound::Excluded(left.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
        }
    }

    fn min_upper(left: &Bound<T>, right: &Bound<T>) -> Bound<T> {
        match (left, right) {
            (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
            (Bound::Included(left), Bound::Included(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Included(left.clone()),
                std::cmp::Ordering::Greater => Bound::Included(right.clone()),
                std::cmp::Ordering::Equal => Bound::Included(left.clone()),
            },
            (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Included(left.clone()),
                std::cmp::Ordering::Greater => Bound::Excluded(right.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
            (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Excluded(left.clone()),
                std::cmp::Ordering::Greater => Bound::Included(right.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
            (Bound::Excluded(left), Bound::Excluded(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => Bound::Excluded(left.clone()),
                std::cmp::Ordering::Greater => Bound::Excluded(right.clone()),
                std::cmp::Ordering::Equal => Bound::Excluded(left.clone()),
            },
        }
    }

    fn is_non_empty_range(range: &ValueRange<T>) -> bool {
        match (&range.lower, &range.upper) {
            (Bound::Included(lower), Bound::Included(upper)) => lower <= upper,
            (Bound::Included(lower), Bound::Excluded(upper))
            | (Bound::Excluded(lower), Bound::Included(upper))
            | (Bound::Excluded(lower), Bound::Excluded(upper)) => lower < upper,
            _ => true,
        }
    }
}

/// A filter that carries both a value domain for access planning and an exact predicate.
pub struct ValueFilter<T> {
    pub domain: ValueDomain<T>,
    pub predicate: Predicate<T>,
}

/// A filter over the value associated with a specific map key.
pub struct MapValueFilter<K, V> {
    pub key: K,
    pub domain: ValueDomain<V>,
    pub predicate: Predicate<V>,
}

impl<K, V> MapValueFilter<K, V> {
    pub fn new(key: K, domain: ValueDomain<V>, predicate: Predicate<V>) -> Self {
        Self {
            key,
            domain,
            predicate,
        }
    }
}

impl<T> ValueFilter<T> {
    pub fn new(domain: ValueDomain<T>, predicate: Predicate<T>) -> Self {
        Self { domain, predicate }
    }

    pub fn all(predicate: Predicate<T>) -> Self {
        Self::new(ValueDomain::all(), predicate)
    }
}

/// A collection of common predicate helpers.
pub struct Predicates;

impl Predicates {
    pub fn always_true<T>() -> Predicate<T> {
        Arc::new(|_| Ok(true))
    }

    pub fn always_false<T>() -> Predicate<T> {
        Arc::new(|_| Ok(false))
    }
}

#[cfg(test)]
mod tests {
    use super::{Bound, ValueDomain};

    #[test]
    fn value_domain_intersects_points_and_ranges() {
        let points = ValueDomain::from_points([1_u64, 3, 5]);
        let range = ValueDomain::range(Bound::Excluded(1), Bound::Included(4));

        assert_eq!(points.intersect(&range).points(), Some(vec![3]));
    }
}
