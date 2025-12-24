pub mod display;
mod status;

use datafusion_common::Column;
use datafusion_expr::expr;
pub use status::*;

use crate::datasource::BucketBy;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CatalogTableSort {
    pub column: String,
    pub ascending: bool,
}

impl From<CatalogTableSort> for expr::Sort {
    fn from(value: CatalogTableSort) -> Self {
        let CatalogTableSort { column, ascending } = value;
        expr::Sort {
            expr: expr::Expr::Column(Column {
                relation: None,
                name: column,
                spans: Default::default(),
            }),
            asc: ascending,
            nulls_first: false,
        }
    }
}
