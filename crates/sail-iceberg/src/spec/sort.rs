use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use super::transform::Transform;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Sort direction in a partition, either ascending or descending
pub enum SortDirection {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,
    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

impl Display for SortDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Ascending => write!(f, "asc"),
            SortDirection::Descending => write!(f, "desc"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Nulls are stored first
    First,
    #[serde(rename = "nulls-last")]
    /// Nulls are stored last
    Last,
}

impl Display for NullOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NullOrder::First => write!(f, "nulls-first"),
            NullOrder::Last => write!(f, "nulls-last"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

impl Display for SortField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.source_id, self.transform, self.direction, self.null_order
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    #[serde(default)]
    pub order_id: i64,
    /// Details of the sort
    #[serde(default)]
    pub fields: Vec<SortField>,
}

impl SortOrder {
    pub fn unsorted_order() -> SortOrder {
        SortOrder {
            order_id: 0,
            fields: vec![],
        }
    }

    pub fn is_unsorted(&self) -> bool {
        self.order_id == 0 || self.fields.is_empty()
    }
}
