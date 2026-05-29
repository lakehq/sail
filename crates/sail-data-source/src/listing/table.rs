// The listing table source is adapted from the DataFusion `ListingTable` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use datafusion_common::{Constraints, Result};
use datafusion_datasource::TableSchema;

use crate::listing::source::ReadFormat;
use crate::listing::utils::can_be_evaluated_for_partition_pruning;

#[derive(Clone, Debug)]
pub struct ListingTableSourceConfig {
    pub table_paths: Vec<datafusion_datasource::ListingTableUrl>,
    pub schema: TableSchema,
    pub constraints: Constraints,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub collect_stat: bool,
    pub target_partitions: usize,
    pub read_format: Arc<dyn ReadFormat>,
    pub compression: Option<datafusion_common::parsers::CompressionTypeVariant>,
}

/// A [`TableSource`] that represents reading one or more files via listing.
///
/// This is a logical representation of the listing files data source,
/// adapted from `ListingTable` in DataFusion.
#[derive(Clone, Debug)]
pub struct ListingTableSource {
    config: ListingTableSourceConfig,
}

impl ListingTableSource {
    pub fn try_new(config: ListingTableSourceConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn config(&self) -> &ListingTableSourceConfig {
        &self.config
    }
}

impl TableSource for ListingTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.config.schema.table_schema())
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.config.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Expressions can be used for partition pruning if they can be evaluated
        // using only the partition columns and there are partition columns.
        let partition_column_names = self
            .config
            .schema
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        filters
            .iter()
            .map(|filter| {
                if can_be_evaluated_for_partition_pruning(&partition_column_names, filter) {
                    return Ok(TableProviderFilterPushDown::Exact);
                }
                Ok(TableProviderFilterPushDown::Inexact)
            })
            .collect()
    }
}
