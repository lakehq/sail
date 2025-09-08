use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;

pub fn is_simple_projection(projection: &ProjectionExec) -> bool {
    projection
        .expr()
        .iter()
        .all(|(expr, _)| expr.as_any().downcast_ref::<Column>().is_some())
}

pub fn union(left: &[usize], right: &[usize]) -> Vec<usize> {
    let mut result = left.to_vec();
    for &item in right {
        if !result.contains(&item) {
            result.push(item);
        }
    }
    result.sort();
    result
}

pub fn is_subset<T: Ord + Clone>(v1: &[T], v2: &[T]) -> bool {
    v1.iter().all(|item| v2.contains(item))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_disjoint() {
        assert_eq!(union(&[1, 3, 5], &[2, 4, 6]), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_union_overlap() {
        assert_eq!(union(&[1, 2, 3], &[3, 4, 5]), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_is_subset_true() {
        assert!(is_subset(&[1, 2], &[1, 2, 3, 4]));
    }

    #[test]
    fn test_is_subset_false() {
        assert!(!is_subset(&[1, 5], &[1, 2, 3, 4]));
    }
}
