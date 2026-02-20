use std::fmt;
use std::sync::Arc;

use crate::error::{CatalogError, CatalogResult};
use crate::utils::{quote_name_if_needed, quote_namespace_if_needed};

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

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&quote_namespace_if_needed(self))
    }
}

impl Namespace {
    pub fn head_to_string(&self) -> String {
        quote_name_if_needed(&self.head)
    }

    pub fn tail_to_string(&self) -> String {
        self.tail
            .iter()
            .map(|s| quote_name_if_needed(s))
            .collect::<Vec<_>>()
            .join(".")
    }

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
