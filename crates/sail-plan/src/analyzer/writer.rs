use sail_catalog::provider::CatalogPartitionField;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{BucketBy, OptionLayer};

use super::model::*;

#[derive(Debug)]
pub struct WriteOperation {
    pub input: Query,
    pub format: String,
    pub mode: WriteDisposition,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_order: Vec<SortKey>,
    pub bucket_by: Option<BucketBy>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub preconditions: Vec<Command>,
}

#[derive(Debug)]
pub enum WriteDisposition {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwritePartitions,
    OverwriteIf {
        condition: Box<Expr>,
        schema: QuerySchema,
        source: Option<String>,
    },
}
