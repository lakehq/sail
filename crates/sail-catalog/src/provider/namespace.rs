use std::sync::Arc;

use crate::error::{CatalogError, CatalogResult};

/// A non-empty, multi-level name.
/// This is used to refer to a database in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct Namespace {
    pub head: Arc<str>,
    pub tail: Vec<Arc<str>>,
}

impl From<Namespace> for Vec<Arc<str>> {
    fn from(namespace: Namespace) -> Self {
        let mut result = vec![namespace.head];
        result.extend(namespace.tail);
        result
    }
}

impl From<Namespace> for Vec<String> {
    fn from(namespace: Namespace) -> Self {
        let mut result = vec![namespace.head.to_string()];
        result.extend(namespace.tail.iter().map(|s| s.to_string()));
        result
    }
}

impl<T: Into<Arc<str>>> TryFrom<Vec<T>> for Namespace {
    type Error = CatalogError;

    fn try_from(value: Vec<T>) -> CatalogResult<Self> {
        let mut iter = value.into_iter().map(Into::into);
        let head = iter
            .next()
            .ok_or_else(|| CatalogError::InvalidArgument("empty namespace".to_string()))?;
        let tail = iter.collect();
        Ok(Self { head, tail })
    }
}

impl<T: AsRef<str>> TryFrom<&[T]> for Namespace {
    type Error = CatalogError;

    fn try_from(value: &[T]) -> CatalogResult<Self> {
        let mut iter = value.iter().map(AsRef::as_ref);
        let head = iter
            .next()
            .ok_or_else(|| CatalogError::InvalidArgument("empty namespace".to_string()))?
            .into();
        let tail = iter.map(|s| s.into()).collect();
        Ok(Self { head, tail })
    }
}

impl<T: AsRef<str>> PartialEq<&[T]> for Namespace {
    fn eq(&self, other: &&[T]) -> bool {
        let mut iter = other.iter();
        iter.next()
            .is_some_and(|x| x.as_ref() == self.head.as_ref())
            && iter
                .map(|x| x.as_ref())
                .eq(self.tail.iter().map(|x| x.as_ref()))
    }
}

impl Namespace {
    pub fn is_child_of(&self, other: &Self) -> bool {
        self.head == other.head
            && self.tail.len() == other.tail.len() + 1
            && self.tail.iter().zip(other.tail.iter()).all(|(a, b)| a == b)
    }

    pub fn is_parent_of(&self, other: &Self) -> bool {
        other.is_child_of(self)
    }

    pub fn starts_with(&self, other: &Self) -> bool {
        self.head == other.head
            && self.tail.len() >= other.tail.len()
            && self.tail.iter().zip(other.tail.iter()).all(|(a, b)| a == b)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_starts_with() {
        let ns1 = Namespace::try_from(vec!["a", "b", "c"]).unwrap();
        let ns2 = Namespace::try_from(vec!["a", "b"]).unwrap();
        let ns3 = Namespace::try_from(vec!["a", "x"]).unwrap();
        let ns4 = Namespace::try_from(vec!["a"]).unwrap();

        assert!(ns1.starts_with(&ns2));
        assert!(ns1.starts_with(&ns4));
        assert!(ns2.starts_with(&ns4));
        assert!(!ns1.starts_with(&ns3));
        assert!(!ns2.starts_with(&ns1));
    }

    #[test]
    fn test_namespace_hierarchy() {
        let parent = Namespace::try_from(vec!["a", "b"]).unwrap();
        let child = Namespace::try_from(vec!["a", "b", "c"]).unwrap();
        let grandchild = Namespace::try_from(vec!["a", "b", "c", "d"]).unwrap();

        assert!(child.is_child_of(&parent));
        assert!(parent.is_parent_of(&child));
        assert!(!grandchild.is_child_of(&parent));
        assert!(!parent.is_parent_of(&grandchild));
    }

    #[test]
    fn test_namespace_conversions() {
        let vec_str = vec!["a", "b"];
        let ns = Namespace::try_from(vec_str).unwrap();

        let vec_arc: Vec<Arc<str>> = ns.clone().into();
        assert_eq!(vec_arc.len(), 2);
        assert_eq!(vec_arc[0].as_ref(), "a");
        assert_eq!(vec_arc[1].as_ref(), "b");

        let vec_string: Vec<String> = ns.into();
        assert_eq!(vec_string, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_namespace_empty_error() {
        let empty: Vec<String> = vec![];
        let result = Namespace::try_from(empty);
        assert!(result.is_err());
    }
}
