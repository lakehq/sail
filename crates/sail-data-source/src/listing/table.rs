// The listing table source is adapted from the DataFusion `ListingTable` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Constraints, Result, TableReference};
use datafusion_datasource::{ListingTableUrl, TableSchema};

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
    statistics_cache_namespace: String,
}

impl ListingTableSource {
    pub fn try_new(config: ListingTableSourceConfig) -> Result<Self> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for path in &config.table_paths {
            path.to_string().hash(&mut hasher);
        }
        for field in config.schema.file_schema().fields() {
            field.name().hash(&mut hasher);
            field.data_type().to_string().hash(&mut hasher);
            field.is_nullable().hash(&mut hasher);
        }
        format!("{:?}", config.read_format).hash(&mut hasher);
        format!("{:?}", config.compression).hash(&mut hasher);
        let statistics_cache_namespace = format!("listing_{:016x}", hasher.finish());

        Ok(Self {
            config,
            statistics_cache_namespace,
        })
    }

    pub fn config(&self) -> &ListingTableSourceConfig {
        &self.config
    }

    pub(crate) fn statistics_cache_table(
        &self,
        statistics_columns: Option<&[usize]>,
    ) -> TableReference {
        let coverage = statistics_cache_coverage(statistics_columns);
        TableReference::bare(format!("{}_{}", self.statistics_cache_namespace, coverage))
    }
}

fn statistics_cache_coverage(statistics_columns: Option<&[usize]>) -> String {
    match statistics_columns {
        None => "all".to_string(),
        Some([]) => "rows".to_string(),
        Some(columns) => {
            let mut columns = columns.to_vec();
            columns.sort_unstable();
            columns.dedup();
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            columns.hash(&mut hasher);
            format!("columns_{:016x}", hasher.finish())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_cache_coverage_is_canonical_and_isolated() {
        assert_ne!(
            statistics_cache_coverage(None),
            statistics_cache_coverage(Some(&[]))
        );
        assert_ne!(
            statistics_cache_coverage(Some(&[])),
            statistics_cache_coverage(Some(&[1]))
        );
        assert_ne!(
            statistics_cache_coverage(Some(&[1])),
            statistics_cache_coverage(Some(&[2]))
        );
        assert_eq!(
            statistics_cache_coverage(Some(&[2, 1])),
            statistics_cache_coverage(Some(&[1, 2, 2]))
        );
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
