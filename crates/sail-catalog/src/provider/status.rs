use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_expr::LogicalPlan;

#[derive(Debug, Clone)]
pub struct NamespaceStatus {
    pub catalog: String,
    pub namespace: Vec<String>,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TableStatus {
    pub name: String,
    pub kind: TableKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    Table {
        catalog: String,
        namespace: Vec<String>,
        schema: SchemaRef,
        format: String,
        comment: Option<String>,
        location: Option<String>,
        properties: HashMap<String, String>,
    },
    View {
        catalog: String,
        namespace: Vec<String>,
        schema: SchemaRef,
        definition: String,
        comment: Option<String>,
        properties: HashMap<String, String>,
    },
    TemporaryView {
        plan: Arc<LogicalPlan>,
    },
    GlobalTemporaryView {
        namespace: Vec<String>,
        plan: Arc<LogicalPlan>,
    },
}

impl TableKind {
    pub fn catalog(&self) -> Option<String> {
        match &self {
            TableKind::Table { catalog, .. } => Some(catalog.clone()),
            TableKind::View { catalog, .. } => Some(catalog.clone()),
            TableKind::TemporaryView { .. } => None,
            TableKind::GlobalTemporaryView { .. } => None,
        }
    }

    pub fn namespace(&self) -> Vec<String> {
        match &self {
            TableKind::Table { namespace, .. } => namespace.clone(),
            TableKind::View { namespace, .. } => namespace.clone(),
            TableKind::TemporaryView { .. } => vec![],
            TableKind::GlobalTemporaryView { namespace, .. } => namespace.clone(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match &self {
            TableKind::Table { schema, .. } => schema.clone(),
            TableKind::View { schema, .. } => schema.clone(),
            TableKind::TemporaryView { plan } => plan.schema().inner().clone(),
            TableKind::GlobalTemporaryView { plan, .. } => plan.schema().inner().clone(),
        }
    }

    pub fn description(&self) -> Option<String> {
        match &self {
            TableKind::Table { comment, .. } => comment.clone(),
            TableKind::View { comment, .. } => comment.clone(),
            TableKind::TemporaryView { .. } => None,
            TableKind::GlobalTemporaryView { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableColumnStatus {
    pub name: String,
    pub description: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_partition: bool,
    pub is_bucket: bool,
    pub metadata: Vec<(String, String)>,
}
