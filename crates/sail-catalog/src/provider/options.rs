use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_expr::LogicalPlan;

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

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CatalogTableSort {
    pub column: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateDatabaseOptions {
    pub comment: Option<String>,
    pub location: Option<String>,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct DropDatabaseOptions {
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateTableOptions {
    pub columns: Vec<CreateTableColumnOptions>,
    pub comment: Option<String>,
    pub constraints: Vec<CatalogTableConstraint>,
    pub location: Option<String>,
    pub format: String,
    pub partition_by: Vec<String>,
    pub sort_by: Vec<CatalogTableSort>,
    pub bucket_by: Option<CatalogTableBucketBy>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub options: Vec<(String, String)>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateTableColumnOptions {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct DropTableOptions {
    pub if_exists: bool,
    pub purge: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateViewOptions {
    pub columns: Vec<CreateViewColumnOptions>,
    pub definition: String,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateViewColumnOptions {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateTemporaryViewOptions {
    pub input: Arc<LogicalPlan>,
    pub columns: Vec<CreateTemporaryViewColumnOptions>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct CreateTemporaryViewColumnOptions {
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct DropViewOptions {
    pub if_exists: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub struct DropTemporaryViewOptions {
    pub if_exists: bool,
}
