pub mod delta;
pub mod display;
pub mod iceberg;
pub mod lakehouse;
pub mod managed;
mod status;

use datafusion_common::Column;
use datafusion_expr::expr;
pub use lakehouse::*;
use serde::{Deserialize, Serialize};
pub use status::*;

use crate::datasource::BucketBy;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum CatalogTableConstraint {
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Default, Serialize, Deserialize)]
pub enum PartitionTransform {
    #[default]
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogPartitionField {
    pub column: String,
    pub transform: Option<PartitionTransform>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogTableBucketBy {
    pub columns: Vec<String>,
    pub num_buckets: usize,
}

impl From<CatalogTableBucketBy> for BucketBy {
    fn from(value: CatalogTableBucketBy) -> Self {
        let CatalogTableBucketBy {
            columns,
            num_buckets,
        } = value;
        BucketBy {
            columns,
            num_buckets,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogTableSort {
    pub column: String,
    pub ascending: bool,
    #[serde(default)]
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogTableColumnIdentity {
    pub start: i64,
    pub step: i64,
    pub allow_explicit_insert: bool,
    pub high_water_mark: Option<i64>,
}

impl From<CatalogTableSort> for expr::Sort {
    fn from(value: CatalogTableSort) -> Self {
        let CatalogTableSort {
            column,
            ascending,
            nulls_first,
        } = value;
        expr::Sort {
            expr: expr::Expr::Column(Column {
                relation: None,
                name: column,
                spans: Default::default(),
            }),
            asc: ascending,
            nulls_first,
        }
    }
}
