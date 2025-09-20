use std::ops::BitOr;

/// Represents a set of relations using a u64 bitset, supporting up to 64 reorderable relations.
/// This is sufficient for most queries. For more relations, u128 or bitvec can be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct JoinSet(u64);

impl JoinSet {
    /// Creates a set containing only a single relation `relation_idx`.
    pub fn new_singleton(relation_idx: usize) -> Self {
        assert!(relation_idx < 64, "Relation index must be less than 64");
        Self(1 << relation_idx)
    }

    /// Creates a JoinSet from a bitset.
    pub fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Creates a JoinSet from an iterator of relation indices.
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        let mut bits = 0u64;
        for relation_idx in iter {
            assert!(relation_idx < 64, "Relation index must be less than 64");
            bits |= 1 << relation_idx;
        }
        Self(bits)
    }

    /// Gets the internal bitset.
    pub fn bits(&self) -> u64 {
        self.0
    }

    /// Checks if `other` is a subset of the current set.
    pub fn is_subset(&self, other: &Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Checks if two sets are disjoint.
    pub fn is_disjoint(&self, other: &Self) -> bool {
        (self.0 & other.0) == 0
    }

    /// Computes the union of two sets.
    pub fn union(&self, other: &Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns the number of relations in the set.
    pub fn cardinality(&self) -> u32 {
        self.0.count_ones()
    }

    /// Gets all relation indices in the set.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..64).filter(move |&i| (self.0 & (1 << i)) != 0)
    }
}

impl BitOr for JoinSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton() {
        let set = JoinSet::new_singleton(5);
        assert_eq!(set.cardinality(), 1);
        assert!(set.iter().collect::<Vec<_>>() == vec![5]);
    }

    #[test]
    fn test_union() {
        let set1 = JoinSet::new_singleton(1);
        let set2 = JoinSet::new_singleton(3);
        let union = set1.union(&set2);
        assert_eq!(union.cardinality(), 2);
        assert!(union.iter().collect::<Vec<_>>() == vec![1, 3]);
    }

    #[test]
    fn test_is_disjoint() {
        let set1 = JoinSet::new_singleton(1);
        let set2 = JoinSet::new_singleton(3);
        let set3 = JoinSet::new_singleton(1);

        assert!(set1.is_disjoint(&set2));
        assert!(!set1.is_disjoint(&set3));
    }

    #[test]
    fn test_is_subset() {
        let set1 = JoinSet::new_singleton(1);
        let set2 = JoinSet::new_singleton(3);
        let union = set1.union(&set2);

        assert!(union.is_subset(&set1));
        assert!(union.is_subset(&set2));
        assert!(!set1.is_subset(&union));
    }
}
