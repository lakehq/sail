use sqlparser::dialect::{Dialect, GenericDialect};
use std::any::TypeId;

#[derive(Debug)]
pub(crate) struct SparkDialect {}

impl Dialect for SparkDialect {
    fn dialect(&self) -> TypeId {
        GenericDialect {}.dialect()
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }
}
