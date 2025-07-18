use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::Constraints;
use datafusion_expr::expr::Sort;
use datafusion_expr::Expr;

#[derive(Debug, Clone)]
pub struct CreateNamespaceOptions {
    pub if_not_exists: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteNamespaceOptions {
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTableOptions {
    pub schema: SchemaRef,
    pub file_format: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub column_defaults: Vec<(String, Expr)>,
    pub constraints: Constraints,
    pub table_partition_cols: Vec<String>,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub definition: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteTableOptions {
    pub if_exists: bool,
    pub purge: bool,
}

#[derive(Debug, Clone)]
pub struct CreateViewOptions {
    pub definition: String,
    pub schema: SchemaRef,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteViewOptions {
    pub if_exists: bool,
}
