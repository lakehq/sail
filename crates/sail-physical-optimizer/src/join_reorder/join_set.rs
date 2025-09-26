use std::fmt;
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Sub, SubAssign};

use datafusion::error::{DataFusionError, Result};

/// The maximum number of reorderable relations supported by the bitset.
const MAX_RELATIONS: usize = 64;

/// Represents a set of relations using a u64 bitset.
/// This is sufficient for most queries. For more relations, u128 or bitvec can be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct JoinSet(u64);

impl JoinSet {
    /// Creates an empty set.
    pub fn new() -> Self {
        Self(0)
    }

    /// Creates a set containing only a single relation `relation_idx`.
    pub fn new_singleton(relation_idx: usize) -> Result<Self> {
        if relation_idx >= MAX_RELATIONS {
            return Err(DataFusionError::Internal(format!(
                "Relation index {} must be less than {}",
                relation_idx, MAX_RELATIONS
            )));
        }
        Ok(Self(1 << relation_idx))
    }

    /// Creates a JoinSet from a bitset.
    pub fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Gets the internal bitset.
    pub fn bits(&self) -> u64 {
        self.0
    }

    /// Checks if `other` is a subset of the current set.
    /// Returns true if all elements in `other` are also in `self`.
    pub fn is_superset(&self, other: &Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Checks if the current set is a subset of `other`.
    /// Returns true if all elements in `self` are also in `other`.
    pub fn is_subset(&self, other: &Self) -> bool {
        (self.0 & other.0) == self.0
    }

    /// Checks if two sets are disjoint (have no common elements).
    pub fn is_disjoint(&self, other: &Self) -> bool {
        (self.0 & other.0) == 0
    }

    /// Computes the union of two sets.
    pub fn union(&self, other: &Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Computes the intersection of two sets.
    pub fn intersection(&self, other: &Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Computes the difference of two sets (`self` - `other`).
    pub fn difference(&self, other: &Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Computes the symmetric difference of two sets.
    pub fn symmetric_difference(&self, other: &Self) -> Self {
        Self(self.0 ^ other.0)
    }

    /// Returns the number of relations in the set.
    pub fn cardinality(&self) -> u32 {
        self.0.count_ones()
    }

    /// Returns `true` if the set contains no relations.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// An iterator visiting all relation indices in the set in ascending order.
    pub fn iter(&self) -> JoinSetIter {
        JoinSetIter { bits: self.0 }
    }
}

// --- Operator Overloading ---

impl BitOr for JoinSet {
    type Output = Self;
    /// Computes the union of two sets.
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(&rhs)
    }
}

impl BitOrAssign for JoinSet {
    /// Computes the union in-place.
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl BitAnd for JoinSet {
    type Output = Self;
    /// Computes the intersection of two sets.
    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersection(&rhs)
    }
}

impl BitAndAssign for JoinSet {
    /// Computes the intersection in-place.
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl Sub for JoinSet {
    type Output = Self;
    /// Computes the difference of two sets.
    fn sub(self, rhs: Self) -> Self::Output {
        self.difference(&rhs)
    }
}

impl SubAssign for JoinSet {
    /// Computes the difference in-place.
    fn sub_assign(&mut self, rhs: Self) {
        self.0 &= !rhs.0;
    }
}

impl BitXor for JoinSet {
    type Output = Self;
    /// Computes the symmetric difference of two sets.
    fn bitxor(self, rhs: Self) -> Self::Output {
        self.symmetric_difference(&rhs)
    }
}

impl BitXorAssign for JoinSet {
    /// Computes the symmetric difference in-place.
    fn bitxor_assign(&mut self, rhs: Self) {
        self.0 ^= rhs.0;
    }
}

/// An efficient iterator over the elements of a `JoinSet`.
#[derive(Debug)]
pub struct JoinSetIter {
    bits: u64,
}

impl Iterator for JoinSetIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bits == 0 {
            None
        } else {
            let index = self.bits.trailing_zeros() as usize;
            // Clear the lowest set bit to advance the iterator
            self.bits &= self.bits - 1;
            Some(index)
        }
    }
}

impl IntoIterator for JoinSet {
    type Item = usize;
    type IntoIter = JoinSetIter;

    fn into_iter(self) -> Self::IntoIter {
        JoinSetIter { bits: self.0 }
    }
}

impl IntoIterator for &JoinSet {
    type Item = usize;
    type IntoIter = JoinSetIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl JoinSet {
    /// Creates a JoinSet from an iterator of relation indices.
    pub fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Result<Self> {
        let mut bits = 0u64;
        for relation_idx in iter {
            if relation_idx >= MAX_RELATIONS {
                return Err(DataFusionError::Internal(format!(
                    "Relation index {} must be less than {}",
                    relation_idx, MAX_RELATIONS
                )));
            }
            bits |= 1 << relation_idx;
        }
        Ok(Self(bits))
    }
}

impl fmt::Display for JoinSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinSet(")?;
        let mut first = true;
        for item in self.iter() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", item)?;
            first = false;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton() {
        let set = JoinSet::new_singleton(5).unwrap();
        assert_eq!(set.cardinality(), 1);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![5]);

        // Test error case
        let result = JoinSet::new_singleton(MAX_RELATIONS);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_iter() {
        // Valid indices
        let set = JoinSet::from_iter([1, 3, 5]).unwrap();
        assert_eq!(set.cardinality(), 3);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![1, 3, 5]);

        // Invalid index
        let result = JoinSet::from_iter([1, MAX_RELATIONS, 5]);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_empty_and_len() {
        let empty_set = JoinSet::new();
        assert!(empty_set.is_empty());
        assert_eq!(empty_set.cardinality(), 0);

        let set = JoinSet::from_iter([1, 5, 10]).unwrap();
        assert!(!set.is_empty());
        assert_eq!(set.cardinality(), 3);
    }

    #[test]
    fn test_from_iterator() {
        let indices = vec![1, 3, 5];
        let set = JoinSet::from_iter(indices).unwrap();
        assert_eq!(set.cardinality(), 3);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![1, 3, 5]);
    }

    #[test]
    fn test_into_iterator() {
        let set = JoinSet::from_iter([2, 4, 8]).unwrap();
        let mut collected = vec![];
        for item in set {
            collected.push(item);
        }
        assert_eq!(collected, vec![2, 4, 8]);
    }

    #[test]
    fn test_operators() {
        let set1 = JoinSet::from_iter([1, 2, 3]).unwrap();
        let set2 = JoinSet::from_iter([3, 4, 5]).unwrap();

        // Union
        assert_eq!(set1 | set2, JoinSet::from_iter([1, 2, 3, 4, 5]).unwrap());

        // Intersection
        assert_eq!(set1 & set2, JoinSet::new_singleton(3).unwrap());

        // Difference
        assert_eq!(set1 - set2, JoinSet::from_iter([1, 2]).unwrap());

        // Symmetric Difference
        assert_eq!(set1 ^ set2, JoinSet::from_iter([1, 2, 4, 5]).unwrap());
    }

    #[test]
    fn test_assign_operators() {
        let mut set1 = JoinSet::from_iter([1, 2]).unwrap();
        let set2 = JoinSet::from_iter([2, 3]).unwrap();
        set1 |= set2;
        assert_eq!(set1, JoinSet::from_iter([1, 2, 3]).unwrap());

        let mut set3 = JoinSet::from_iter([1, 2, 3]).unwrap();
        set3 &= set2;
        assert_eq!(set3, JoinSet::from_iter([2, 3]).unwrap());
    }

    #[test]
    fn test_is_disjoint() {
        let set1 = JoinSet::new_singleton(1).unwrap();
        let set2 = JoinSet::new_singleton(3).unwrap();
        let set3 = JoinSet::new_singleton(1).unwrap();

        assert!(set1.is_disjoint(&set2));
        assert!(!set1.is_disjoint(&set3));
    }

    #[test]
    fn test_is_subset_and_superset() {
        let set1 = JoinSet::from_iter([1, 2]).unwrap();
        let set2 = JoinSet::from_iter([1, 2, 3]).unwrap();

        assert!(set1.is_subset(&set2));
        assert!(!set2.is_subset(&set1));
        assert!(set2.is_superset(&set1));
        assert!(!set1.is_superset(&set2));
        assert!(set1.is_subset(&set1));
        assert!(set1.is_superset(&set1));
    }

    #[test]
    fn test_display() {
        let set = JoinSet::from_iter([0, 10, 63]).unwrap();
        assert_eq!(format!("{}", set), "JoinSet(0, 10, 63)");
        let empty_set = JoinSet::new();
        assert_eq!(format!("{}", empty_set), "JoinSet()");
    }
}
