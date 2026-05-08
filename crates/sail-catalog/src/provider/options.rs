use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_expr::LogicalPlan;
pub use sail_common_datafusion::catalog::{CatalogPartitionField, PartitionTransform};
use sail_common_datafusion::catalog::{
    CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort, TableStatistics,
};
use serde::{Deserialize, Serialize};

/// Options for creating a database in a catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateDatabaseOptions {
    pub comment: Option<String>,
    pub location: Option<String>,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DropDatabaseOptions {
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateTableOptions {
    pub external: bool,
    pub columns: Vec<CreateTableColumnOptions>,
    pub comment: Option<String>,
    pub constraints: Vec<CatalogTableConstraint>,
    pub location: Option<String>,
    pub format: String,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_by: Vec<CatalogTableSort>,
    pub bucket_by: Option<CatalogTableBucketBy>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateTableColumnOptions {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DropTableOptions {
    pub if_exists: bool,
    pub purge: bool,
}

pub type PartitionSpec = Vec<(String, String)>;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub spec: PartitionSpec,
    pub location: Option<String>,
    pub parameters: HashMap<String, String>,
    pub create_time: Option<i64>,
    pub last_access_time: Option<i64>,
    pub statistics: Option<TableStatistics>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreatePartitionsOptions {
    pub ignore_if_exists: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DropPartitionsOptions {
    pub ignore_if_not_exists: bool,
    pub purge: bool,
    pub retain_data: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum PartitionFilter {
    #[default]
    All,
    Spec(PartitionSpec),
    Predicate(PartitionPredicate),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub struct GetPartitionsOptions {
    pub filter: PartitionFilter,
    pub max_parts: Option<i16>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum PartitionPredicate {
    Compare {
        column: String,
        op: PartitionPredicateOp,
        value: String,
    },
    And(Vec<PartitionPredicate>),
    Or(Vec<PartitionPredicate>),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum PartitionPredicateOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateViewOptions {
    pub columns: Vec<CreateViewColumnOptions>,
    pub definition: String,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateViewColumnOptions {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateTemporaryViewOptions<I = Arc<LogicalPlan>> {
    pub input: I,
    pub columns: Vec<CreateTemporaryViewColumnOptions>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CreateTemporaryViewColumnOptions {
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DropViewOptions {
    pub if_exists: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DropTemporaryViewOptions {
    pub if_exists: bool,
}

/// Options for altering a table in a catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum AlterTableOptions {
    SetTableProperties { properties: Vec<(String, String)> },
    UnsetTableProperties { keys: Vec<String>, if_exists: bool },
    SetLocation { location: String },
}

#[cfg(test)]
mod tests {
    use super::{PartitionFilter, PartitionPredicate, PartitionPredicateOp};

    #[test]
    fn partition_filter_models_restricted_predicates_without_datafusion_exprs() {
        let filter = PartitionFilter::Predicate(PartitionPredicate::And(vec![
            PartitionPredicate::Compare {
                column: "dt".to_string(),
                op: PartitionPredicateOp::Eq,
                value: "2026-04-26".to_string(),
            },
            PartitionPredicate::Compare {
                column: "country".to_string(),
                op: PartitionPredicateOp::NotEq,
                value: "NL".to_string(),
            },
        ]));

        assert!(matches!(filter, PartitionFilter::Predicate(_)));
    }
}
