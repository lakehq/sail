// The listing table source is adapted from the DataFusion `ListingTable` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::execution::cache::cache_manager::FileStatisticsCache;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use datafusion_common::{Constraints, Result};
use datafusion_datasource::TableSchema;

use crate::listing::source::ReadFormat;

#[derive(Clone, Debug)]
pub struct ListingTableSourceConfig {
    pub table_paths: Vec<datafusion_datasource::ListingTableUrl>,
    pub file_extension: String,
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
/// This is a logical-table representation used by Sail so we can take full control
/// over physical planning (instead of delegating to DataFusion's `ListingTable`).
#[derive(Clone, Debug)]
pub struct ListingTableSource {
    table_paths: Vec<datafusion_datasource::ListingTableUrl>,
    file_extension: String,
    schema: TableSchema,
    constraints: Constraints,
    /// Equivalent sort orderings declared by the caller (logical).
    file_sort_order: Vec<Vec<Sort>>,
    collect_stat: bool,
    target_partitions: usize,
    read_format: Arc<dyn ReadFormat>,
    /// Per-scan compression override inferred from file names, if any.
    compression: Option<datafusion_common::parsers::CompressionTypeVariant>,
    collected_statistics: Arc<dyn FileStatisticsCache>,
}

impl ListingTableSource {
    pub fn try_new(config: ListingTableSourceConfig) -> Result<Self> {
        Ok(Self {
            table_paths: config.table_paths,
            file_extension: config.file_extension,
            schema: config.schema,
            constraints: config.constraints,
            file_sort_order: config.file_sort_order,
            collect_stat: config.collect_stat,
            target_partitions: config.target_partitions,
            read_format: config.read_format,
            compression: config.compression,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
        })
    }

    pub fn table_paths(&self) -> &[datafusion_datasource::ListingTableUrl] {
        &self.table_paths
    }

    pub fn file_schema(&self) -> SchemaRef {
        Arc::clone(self.schema.file_schema())
    }

    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn table_schema(&self) -> SchemaRef {
        Arc::clone(self.schema.table_schema())
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn table_partition_cols(&self) -> &Vec<FieldRef> {
        self.schema.table_partition_cols()
    }

    pub fn file_sort_order(&self) -> &[Vec<Sort>] {
        &self.file_sort_order
    }

    pub fn collect_stat(&self) -> bool {
        self.collect_stat
    }

    pub fn target_partitions(&self) -> usize {
        self.target_partitions
    }

    pub fn read_format(&self) -> Arc<dyn ReadFormat> {
        Arc::clone(&self.read_format)
    }

    pub fn compression(&self) -> Option<datafusion_common::parsers::CompressionTypeVariant> {
        self.compression
    }

    pub fn collected_statistics(&self) -> Arc<dyn FileStatisticsCache> {
        Arc::clone(&self.collected_statistics)
    }

    pub fn with_schema_field_names(&self, names: Vec<String>) -> Result<Self> {
        let table_fields = self.schema.table_schema().fields();
        if names.len() != table_fields.len() {
            return datafusion_common::internal_err!(
                "expected {} field names, got {}",
                table_fields.len(),
                names.len()
            );
        }

        let file_field_count = self.schema.file_schema().fields().len();
        let mut new_file_fields = Vec::with_capacity(file_field_count);
        for (i, field) in self.schema.file_schema().fields().iter().enumerate() {
            new_file_fields.push(field.as_ref().clone().with_name(names[i].clone()));
        }
        let new_file_schema = Arc::new(Schema::new_with_metadata(
            new_file_fields,
            self.schema.file_schema().metadata().clone(),
        ));

        let mut new_partition_cols = Vec::with_capacity(self.schema.table_partition_cols().len());
        for (idx, field) in self.schema.table_partition_cols().iter().enumerate() {
            let name_idx = file_field_count + idx;
            new_partition_cols.push(Arc::new(
                field.as_ref().clone().with_name(names[name_idx].clone()),
            ));
        }
        let new_schema = TableSchema::new(new_file_schema, new_partition_cols);

        Ok(Self {
            schema: new_schema,
            ..self.clone()
        })
    }
}

impl TableSource for ListingTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        // Expressions can be used for partition pruning if they can be evaluated
        // using only the partition columns and there are partition columns.
        let partition_column_names = self
            .schema
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        filters
            .iter()
            .map(|filter| {
                if !partition_column_names.is_empty()
                    && expr_applicable_for_cols(&partition_column_names, filter)
                {
                    return Ok(TableProviderFilterPushDown::Exact);
                }
                Ok(TableProviderFilterPushDown::Inexact)
            })
            .collect()
    }
}
