// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion_common::{Constraints, Result};
use datafusion::execution::cache::cache_manager::FileStatisticsCache;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};

use crate::listing::source::ReadFormat;

/// A [`TableSource`] that represents reading one or more files via listing.
///
/// This is a logical-table representation used by Sail so we can take full control
/// over physical planning (instead of delegating to DataFusion's `ListingTable`).
#[derive(Clone, Debug)]
pub struct ListingTableSource {
    table_paths: Vec<datafusion_datasource::ListingTableUrl>,
    file_extension: String,
    /// Columns physically stored in the data files.
    file_schema: SchemaRef,
    /// `file_schema` + partition columns.
    table_schema: SchemaRef,
    /// Partition columns derived from hive-style paths (names are *path keys*).
    table_partition_cols: Vec<(String, DataType)>,
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
    pub fn try_new(
        table_paths: Vec<datafusion_datasource::ListingTableUrl>,
        file_extension: String,
        file_schema: SchemaRef,
        table_partition_cols: Vec<(String, DataType)>,
        constraints: Constraints,
        file_sort_order: Vec<Vec<Sort>>,
        collect_stat: bool,
        target_partitions: usize,
        read_format: Arc<dyn ReadFormat>,
        compression: Option<datafusion_common::parsers::CompressionTypeVariant>,
    ) -> Result<Self> {
        let mut builder = SchemaBuilder::from(file_schema.as_ref().to_owned());
        for (part_col_name, part_col_type) in &table_partition_cols {
            builder.push(Field::new(part_col_name, part_col_type.clone(), false));
        }
        let table_schema = Arc::new(
            builder
                .finish()
                .with_metadata(file_schema.metadata().clone()),
        );

        Ok(Self {
            table_paths,
            file_extension,
            file_schema,
            table_schema,
            table_partition_cols,
            constraints,
            file_sort_order,
            collect_stat,
            target_partitions,
            read_format,
            compression,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
        })
    }

    pub fn table_paths(&self) -> &[datafusion_datasource::ListingTableUrl] {
        &self.table_paths
    }

    pub fn file_schema(&self) -> SchemaRef {
        Arc::clone(&self.file_schema)
    }

    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    pub fn table_schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn table_partition_cols(&self) -> &[(String, DataType)] {
        &self.table_partition_cols
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
        let table_fields = self.table_schema.fields();
        if names.len() != table_fields.len() {
            return datafusion_common::internal_err!(
                "expected {} field names, got {}",
                table_fields.len(),
                names.len()
            );
        }

        let file_field_count = self.file_schema.fields().len();
        let mut new_file_fields = Vec::with_capacity(file_field_count);
        for (i, field) in self.file_schema.fields().iter().enumerate() {
            new_file_fields.push(field.as_ref().clone().with_name(names[i].clone()));
        }
        let new_file_schema = Arc::new(
            datafusion::arrow::datatypes::Schema::new_with_metadata(
                new_file_fields,
                self.file_schema.metadata().clone(),
            ),
        );

        let mut new_table_fields = Vec::with_capacity(table_fields.len());
        for (i, field) in table_fields.iter().enumerate() {
            new_table_fields.push(field.as_ref().clone().with_name(names[i].clone()));
        }
        let new_table_schema = Arc::new(
            datafusion::arrow::datatypes::Schema::new_with_metadata(
                new_table_fields,
                self.table_schema.metadata().clone(),
            ),
        );

        Ok(Self {
            file_schema: new_file_schema,
            table_schema: new_table_schema,
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
            .table_partition_cols
            .iter()
            .map(|col| col.0.as_str())
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
