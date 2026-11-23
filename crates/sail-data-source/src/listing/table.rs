// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/catalog-listing/src/table.rs

// The listing table source is adapted from the DataFusion `ListingTable` implementation.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Constraints, Result};
use datafusion_datasource::{ListingTableUrl, TableSchema};

use crate::listing::partition::BasePathPartitions;
use crate::listing::source::ReadFormat;
use crate::listing::utils::{SchemaSampling, can_be_evaluated_for_partition_pruning};
use crate::url::PathGlobFilter;

#[derive(Clone, Debug)]
pub struct ListingTableSourceConfig {
    /// File scan indices in public column order when physical columns overlap partitions.
    pub output_projection: Option<Vec<usize>>,
    pub base_path: Option<Arc<BasePathPartitions>>,
    pub table_paths: Vec<(ListingTableUrl, Option<Vec<datafusion_common::ScalarValue>>)>,
    pub recursive_file_lookup: bool,
    pub schema: TableSchema,
    pub constraints: Constraints,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub collect_stat: bool,
    pub target_partitions: usize,
    pub read_format: Arc<dyn ReadFormat>,
    pub path_glob_filter: Option<PathGlobFilter>,
    pub compression: CompressionTypeVariant,
}

impl ListingTableSourceConfig {
    pub(super) fn direct_files_only(&self) -> bool {
        self.table_paths
            .first()
            .is_some_and(|(_, values)| values.is_some())
            || (self.base_path.is_some() && self.schema.table_partition_cols().is_empty())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RegisteredReadOptions {
    pub sampling: SchemaSampling,
    pub format: &'static str,
    pub case_sensitive: bool,
}

/// A [`TableSource`] that represents reading one or more files via listing.
///
/// This is a logical representation of the listing files data source,
/// adapted from `ListingTable` in DataFusion.
#[derive(Clone, Debug)]
pub struct ListingTableSource {
    config: ListingTableSourceConfig,
    output_schema: SchemaRef,
    pub(super) registered_read: Option<RegisteredReadOptions>,
}

impl ListingTableSource {
    pub fn try_new(config: ListingTableSourceConfig) -> Result<Self> {
        let schema = config.schema.table_schema();
        let output_schema = match &config.output_projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => Arc::clone(schema),
        };
        Ok(Self {
            config,
            output_schema,
            registered_read: None,
        })
    }

    pub(super) fn with_registered_read(mut self, options: RegisteredReadOptions) -> Self {
        self.registered_read = Some(options);
        self
    }

    pub(super) fn scan_projection(&self, projection: Option<&[usize]>) -> Option<Vec<usize>> {
        match (&self.config.output_projection, projection) {
            (Some(indices), Some(projection)) => {
                Some(projection.iter().map(|&i| indices[i]).collect())
            }
            (Some(indices), None) => Some(indices.clone()),
            (None, projection) => projection.map(<[_]>::to_vec),
        }
    }

    fn partition_column_names(&self) -> Vec<&str> {
        self.config
            .schema
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    pub(super) fn split_partition_filters(&self, filters: Vec<Expr>) -> (Vec<Expr>, Vec<Expr>) {
        let names = self.partition_column_names();
        filters
            .into_iter()
            .partition(|filter| can_be_evaluated_for_partition_pruning(&names, filter))
    }

    pub fn config(&self) -> &ListingTableSourceConfig {
        &self.config
    }
}

impl TableSource for ListingTableSource {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
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
        let partition_column_names = self.partition_column_names();

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
