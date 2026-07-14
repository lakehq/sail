// The listing table source is adapted from the DataFusion `ListingTable` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Constraints, Result, internal_err};
use datafusion_datasource::{ListingTableUrl, TableSchema};
use object_store::ObjectMeta;

use crate::listing::source::ReadFormat;
use crate::listing::utils::can_be_evaluated_for_partition_pruning;

#[derive(Clone, Debug)]
pub struct ListingTableSourceConfig {
    pub table_paths: Vec<ListingTableUrl>,
    pub schema: TableSchema,
    pub constraints: Constraints,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub collect_stat: bool,
    pub target_partitions: usize,
    pub read_format: Arc<dyn ReadFormat>,
    pub compression: CompressionTypeVariant,
}

/// A [`TableSource`] that represents reading one or more files via listing.
///
/// This is a logical representation of the listing files data source,
/// adapted from `ListingTable` in DataFusion.
#[derive(Clone, Debug)]
pub struct ListingTableSource {
    config: ListingTableSourceConfig,
    exact_file_metadata: Vec<Option<ObjectMeta>>,
}

impl ListingTableSource {
    pub fn try_new(config: ListingTableSourceConfig) -> Result<Self> {
        let exact_file_metadata = vec![None; config.table_paths.len()];
        Self::try_new_from_resolved_files(config, exact_file_metadata)
    }

    pub(crate) fn try_new_from_resolved_files(
        config: ListingTableSourceConfig,
        exact_file_metadata: Vec<Option<ObjectMeta>>,
    ) -> Result<Self> {
        if config.table_paths.len() != exact_file_metadata.len() {
            return internal_err!("listing paths and exact-file metadata must have equal lengths");
        }
        Ok(Self {
            config,
            exact_file_metadata,
        })
    }

    pub fn config(&self) -> &ListingTableSourceConfig {
        &self.config
    }

    pub(crate) fn exact_file_metadata(&self) -> &[Option<ObjectMeta>] {
        &self.exact_file_metadata
    }
}

impl TableSource for ListingTableSource {
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
