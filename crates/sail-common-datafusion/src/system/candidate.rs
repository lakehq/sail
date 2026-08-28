use std::cmp::Ordering;
use std::collections::BTreeSet;

/// A value that can be compared with another related type of value.
pub trait ValueOrd<K> {
    fn cmp(&self, other: &K) -> Ordering;
}

/// A type that has a minimum value.
/// This can be used to pad the prefix of a composite value.
pub trait ValueMinimum {
    fn minimum() -> Self;
}

macro_rules! impl_value_minimum {
    ($($type:ty),+ $(,)?) => {
        $(
            impl ValueMinimum for $type {
                fn minimum() -> Self {
                    Self::MIN
                }
            }
        )+
    };
}

impl_value_minimum!(u8, u16, u32, u64, u128, usize);
impl_value_minimum!(i8, i16, i32, i64, i128, isize);

impl ValueMinimum for String {
    fn minimum() -> Self {
        Self::new()
    }
}

/// A synthetic cut in value ordering.
#[derive(Clone, Debug)]
pub enum ValuePosition<T> {
    BeforeAll,
    At(T),
    After(T),
}

impl<T: Ord> ValueOrd<T> for ValuePosition<T> {
    fn cmp(&self, other: &T) -> Ordering {
        match self {
            Self::BeforeAll => Ordering::Less,
            Self::At(value) => value.cmp(other),
            Self::After(value) => match value.cmp(other) {
                Ordering::Equal => Ordering::Greater,
                ordering => ordering,
            },
        }
    }
}

/// An ordered-map scan from a real start key to an exclusive synthetic end.
pub struct CandidateRange<K, B> {
    pub start: K,
    pub end: Option<B>,
}

/// Candidate entries to inspect in an ordered map.
pub enum CandidateSet<K, B> {
    Empty,
    All,
    Points(BTreeSet<K>),
    Ranges(Vec<CandidateRange<K, B>>),
}

#[macro_export]
macro_rules! candidate_key_bound {
    (
        $key:ident => $bound:ident {
            $( $field:ident : $field_type:ty ),+ $(,)?
        }
    ) => {
        #[derive(Clone, Debug)]
        struct $bound {
            $( $field: $crate::system::candidate::ValuePosition<$field_type>, )+
        }

        impl $crate::system::candidate::ValueOrd<$key> for $bound {
            fn cmp(&self, other: &$key) -> std::cmp::Ordering {
                let $key { $( $field, )+ } = other;
                std::cmp::Ordering::Equal
                    $(.then_with(|| $crate::system::candidate::ValueOrd::cmp(&self.$field, $field)))+
            }
        }
    };
}

#[macro_export]
macro_rules! candidate_set {
    (
        $key:ident => $bound:ident {
            $( $field:ident : $field_type:ty => $domain:expr ),+ $(,)?
        }
    ) => {{
        if $( $domain.is_empty() )||+ {
            $crate::system::candidate::CandidateSet::Empty
        } else {
            let mut prefixes = vec![($key {
                $( $field: <$field_type as $crate::system::candidate::ValueMinimum>::minimum(), )+
            }, $bound {
                $( $field: $crate::system::candidate::ValuePosition::BeforeAll, )+
            }, None)];
            let mut candidates = None;

            'plan: {
                $(
                    if let Some(points) = $domain.points() {
                        let capacity = prefixes.len().saturating_mul(points.len());
                        let mut expanded = Vec::with_capacity(capacity);
                        for (prefix, at, _) in std::mem::take(&mut prefixes) {
                            for value in &points {
                                let mut key = prefix.clone();
                                key.$field = value.clone();
                                let mut at = at.clone();
                                at.$field = $crate::system::candidate::ValuePosition::At(value.clone());
                                let mut after = at.clone();
                                after.$field = $crate::system::candidate::ValuePosition::After(value.clone());
                                expanded.push((key, at, Some(after)));
                            }
                        }
                        prefixes = expanded;
                    } else {
                        let capacity = prefixes.len().saturating_mul($domain.ranges().len());
                        let mut ranges = Vec::with_capacity(capacity);
                        for (prefix, at, after) in std::mem::take(&mut prefixes) {
                            for range in $domain.ranges() {
                                let mut start = prefix.clone();
                                start.$field = match &range.lower {
                                    std::ops::Bound::Included(value)
                                    | std::ops::Bound::Excluded(value) => value.clone(),
                                    std::ops::Bound::Unbounded => {
                                        <$field_type as $crate::system::candidate::ValueMinimum>::minimum()
                                    }
                                };
                                let end = match &range.upper {
                                    std::ops::Bound::Included(value) => {
                                        let mut end = at.clone();
                                        end.$field = $crate::system::candidate::ValuePosition::After(value.clone());
                                        Some(end)
                                    }
                                    std::ops::Bound::Excluded(value) => {
                                        let mut end = at.clone();
                                        end.$field = $crate::system::candidate::ValuePosition::At(value.clone());
                                        Some(end)
                                    }
                                    std::ops::Bound::Unbounded => after.clone(),
                                };
                                ranges.push($crate::system::candidate::CandidateRange { start, end });
                            }
                        }
                        candidates = Some($crate::system::candidate::CandidateSet::Ranges(ranges));
                        break 'plan;
                    }
                )+
            }

            candidates.unwrap_or_else(|| {
                $crate::system::candidate::CandidateSet::Points(
                    prefixes.into_iter().map(|(key, _, _)| key).collect(),
                )
            })
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound;

    use datafusion_common::Result;

    use super::CandidateSet;
    use crate::system::predicate::ValueDomain;
    use crate::system::reader::read_ordered_map;

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct TestKey {
        session_id: String,
        job_id: u64,
        label: String,
    }

    candidate_key_bound! {
        TestKey => TestKeyBound {
            session_id: String,
            job_id: u64,
            label: String,
        }
    }

    #[test]
    fn candidate_set_uses_an_exclusive_composite_end_bound() -> Result<()> {
        let session_id = ValueDomain::point("session".to_string());
        let job_id = ValueDomain::range(Bound::Excluded(7_u64), Bound::Unbounded);
        let label = ValueDomain::<String>::all();
        let candidates = candidate_set! {
            TestKey => TestKeyBound {
                session_id: String => &session_id,
                job_id: u64 => &job_id,
                label: String => &label,
            }
        };

        assert!(matches!(&candidates, CandidateSet::Ranges(_)));

        let map = BTreeMap::from([
            (
                TestKey {
                    session_id: "session".to_string(),
                    job_id: 7,
                    label: "a".to_string(),
                },
                7_u64,
            ),
            (
                TestKey {
                    session_id: "session".to_string(),
                    job_id: 8,
                    label: "z".to_string(),
                },
                8,
            ),
            (
                TestKey {
                    session_id: "other".to_string(),
                    job_id: 0,
                    label: "a".to_string(),
                },
                0,
            ),
        ]);
        let values = read_ordered_map(&map, candidates, |value| Ok(*value > 7), 10)?;

        assert_eq!(values, vec![8]);
        Ok(())
    }

    #[test]
    fn candidate_set_expands_point_prefixes() -> Result<()> {
        let session_id = ValueDomain::from_points(["one".to_string(), "two".to_string()]);
        let job_id = ValueDomain::from_points([1_u64, 2]);
        let label = ValueDomain::point("label".to_string());
        let candidates = candidate_set! {
            TestKey => TestKeyBound {
                session_id: String => &session_id,
                job_id: u64 => &job_id,
                label: String => &label,
            }
        };

        assert!(matches!(&candidates, CandidateSet::Points(points) if points.len() == 4));

        let map = BTreeMap::from([(
            TestKey {
                session_id: "two".to_string(),
                job_id: 2,
                label: "label".to_string(),
            },
            22_u64,
        )]);
        let values = read_ordered_map(&map, candidates, |_| Ok(true), 10)?;

        assert_eq!(values, vec![22]);
        Ok(())
    }
}
