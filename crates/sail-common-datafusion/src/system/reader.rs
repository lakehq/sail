use std::collections::BTreeMap;

use datafusion_common::Result;

use super::candidate::{CandidateSet, ValueOrd};

/// Reads matching values from an ordered map without materializing rejected rows.
pub fn read_ordered_map<K, B, V, F>(
    map: &BTreeMap<K, V>,
    candidates: CandidateSet<K, B>,
    predicate: F,
    fetch: usize,
) -> Result<Vec<V>>
where
    K: Ord,
    B: ValueOrd<K>,
    V: Clone,
    F: Fn(&V) -> Result<bool>,
{
    if fetch == 0 {
        return Ok(vec![]);
    }
    let mut values = vec![];
    let mut add_if_matching = |value: &V| -> Result<bool> {
        if predicate(value)? {
            values.push(value.clone());
        }
        Ok(values.len() == fetch)
    };
    match candidates {
        CandidateSet::Empty => {}
        CandidateSet::All => {
            for value in map.values() {
                if add_if_matching(value)? {
                    break;
                }
            }
        }
        CandidateSet::Points(points) => {
            for point in points {
                if let Some(value) = map.get(&point)
                    && add_if_matching(value)?
                {
                    break;
                }
            }
        }
        CandidateSet::Ranges(ranges) => {
            'ranges: for range in ranges {
                for (key, value) in map.range(range.start..) {
                    if range.end.as_ref().is_some_and(|end| end.cmp(key).is_le()) {
                        break;
                    }
                    if add_if_matching(value)? {
                        break 'ranges;
                    }
                }
            }
        }
    }
    Ok(values)
}
