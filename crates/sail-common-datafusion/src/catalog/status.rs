use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_expr::LogicalPlan;

use crate::catalog::{CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort};

#[derive(Debug, Clone)]
pub struct DatabaseStatus {
    pub catalog: String,
    pub database: Vec<String>,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct TableStatus {
    pub catalog: Option<String>,
    pub database: Vec<String>,
    pub name: String,
    pub kind: TableKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    Table {
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        constraints: Vec<CatalogTableConstraint>,
        location: Option<String>,
        format: String,
        partition_by: Vec<String>,
        sort_by: Vec<CatalogTableSort>,
        bucket_by: Option<CatalogTableBucketBy>,
        options: Vec<(String, String)>,
        properties: Vec<(String, String)>,
    },
    View {
        definition: String,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
    TemporaryView {
        plan: Arc<LogicalPlan>,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
    GlobalTemporaryView {
        plan: Arc<LogicalPlan>,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
}

impl TableKind {
    pub fn columns(&self) -> Vec<TableColumnStatus> {
        match &self {
            TableKind::Table { columns, .. }
            | TableKind::View { columns, .. }
            | TableKind::TemporaryView { columns, .. }
            | TableKind::GlobalTemporaryView { columns, .. } => columns.clone(),
        }
    }

    pub fn comment(&self) -> Option<String> {
        match &self {
            TableKind::Table { comment, .. }
            | TableKind::View { comment, .. }
            | TableKind::TemporaryView { comment, .. }
            | TableKind::GlobalTemporaryView { comment, .. } => comment.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnStatus {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
    pub is_partition: bool,
    pub is_bucket: bool,
    pub is_cluster: bool,
}

impl TableColumnStatus {
    pub fn field(&self) -> Field {
        Field::new(self.name.clone(), self.data_type.clone(), self.nullable)
    }
}
