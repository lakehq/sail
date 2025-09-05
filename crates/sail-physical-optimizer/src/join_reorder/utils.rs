use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use itertools::Itertools;

/// Checks if a ProjectionExec only contains simple column references (no expressions).
pub fn is_simple_projection(projection: &ProjectionExec) -> bool {
    projection
        .expr()
        .iter()
        .all(|(expr, _)| expr.as_any().downcast_ref::<Column>().is_some())
}

/// Computes the union of two sorted slices.
pub fn union_sorted(left: &[usize], right: &[usize]) -> Vec<usize> {
    left.iter().merge(right.iter()).dedup().copied().collect()
}

/// Checks if two sorted slices have a non-empty intersection.
#[allow(dead_code)]
pub fn intersect_sorted<T: Ord>(left: &[T], right: &[T]) -> bool {
    left.iter()
        .merge_join_by(right.iter(), |a_val, b_val| a_val.cmp(b_val))
        .any(|item| item.is_both())
}

/// Checks if sorted slice `v1` is a subset of sorted slice `v2`.
pub fn is_subset_sorted<T: Ord>(v1: &[T], v2: &[T]) -> bool {
    v1.iter()
        .merge_join_by(v2.iter(), |v1_val, v2_val| v1_val.cmp(v2_val))
        .all(|item| !item.is_left())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_disjoint() {
        assert_eq!(union_sorted(&[1, 3, 5], &[2, 4, 6]), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_union_overlap() {
        assert_eq!(union_sorted(&[1, 2, 3], &[3, 4, 5]), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_intersect_true() {
        assert!(intersect_sorted(&[1, 2, 3], &[3, 4, 5]));
    }

    #[test]
    fn test_intersect_false() {
        assert!(!intersect_sorted(&[1, 2], &[3, 4, 5]));
    }

    #[test]
    fn test_is_subset_true() {
        assert!(is_subset_sorted(&[1, 2], &[1, 2, 3, 4]));
    }

    #[test]
    fn test_is_subset_false() {
        assert!(!is_subset_sorted(&[1, 5], &[1, 2, 3, 4]));
    }
}
