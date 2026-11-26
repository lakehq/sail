// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion_common::{Constraints, Result, ScalarValue, ToDFSchema, internal_datafusion_err};
use datafusion_datasource::TableSchema;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use sail_common_datafusion::datasource::{
    DataSourceRegistry, RegisteredPartition, SourceInfo, get_partition_columns_and_file_schema,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::listing::source::reconcile_schema_names_case_insensitive;
use crate::listing::table::{
    ListingTableSource, RegisteredReadOptions, listing_filter_pushdown, partition_column_names,
};
use crate::listing::utils::{can_be_evaluated_for_partition_pruning, sample_listing_files};

/// Catalog schema and partition values available before selecting a file reader.
#[derive(Debug)]
pub struct RegisteredTableSource {
    schema: TableSchema,
    format: String,
    info: SourceInfo,
    partitions: Vec<RegisteredPartition>,
}

impl RegisteredTableSource {
    pub fn try_new(
        format: String,
        info: SourceInfo,
        partitions: Vec<RegisteredPartition>,
    ) -> Result<Self> {
        let catalog_schema = info
            .schema
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("missing catalog schema"))?;
        let (fields, file_schema) =
            get_partition_columns_and_file_schema(catalog_schema, info.partition_by.clone())?;
        let schema = TableSchema::builder(Arc::new(file_schema))
            .with_table_partition_cols(fields)
            .build();
        Ok(Self {
            schema,
            format,
            info,
            partitions,
        })
    }

    pub(super) async fn resolve(
        &self,
        session: &dyn Session,
        filters: &[Expr],
    ) -> Result<Option<Arc<dyn TableSource>>> {
        if self.partitions.is_empty() {
            return Ok(None);
        }
        let partitions = self.prune_partitions(session, filters)?;
        if partitions.is_empty() {
            return Ok(None);
        }
        let source = session
            .extension::<DataSourceRegistry>()?
            .get_data_source(&self.format)?
            .create_partitioned_source(session, self.info.clone(), partitions)
            .await?;
        Ok(Some(source))
    }

    fn prune_partitions(
        &self,
        session: &dyn Session,
        filters: &[Expr],
    ) -> Result<Vec<RegisteredPartition>> {
        let names = partition_column_names(&self.schema);
        let filters = filters
            .iter()
            .filter(|expr| can_be_evaluated_for_partition_pruning(&names, expr));
        let Some(predicate) = datafusion_expr::utils::conjunction(filters.cloned()) else {
            return Ok(self.partitions.clone());
        };
        let fields = self.schema.table_partition_cols();
        let columns = (0..fields.len())
            .map(|i| {
                let values = self
                    .partitions
                    .iter()
                    .map(|p| {
                        p.values.get(i).cloned().ok_or_else(|| {
                            internal_datafusion_err!("missing registered partition value")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                ScalarValue::iter_to_array(values)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), columns)?;
        let predicate = session.create_physical_expr(predicate, &batch.schema().to_dfschema()?)?;
        let mask = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
        Ok(self
            .partitions
            .iter()
            .zip(mask.as_boolean().iter())
            .filter(|(_, keep)| *keep == Some(true))
            .map(|(partition, _)| partition.clone())
            .collect())
    }
}

impl TableSource for RegisteredTableSource {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.schema.table_schema())
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.info.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(listing_filter_pushdown(&self.schema, filters))
    }
}

pub(super) async fn prepare_registered_scan(
    source: &ListingTableSource,
    session: &dyn Session,
    options: RegisteredReadOptions,
) -> Result<ListingTableSource> {
    let mut config = source.config().clone();
    // Parquet uses the catalog schema and resolves physical field names for each file.
    if config.table_paths.is_empty() || options.format == "parquet" {
        return ListingTableSource::try_new(config);
    }

    // These partition locations share a reader and object store, so use one sampling budget.
    let urls = config
        .table_paths
        .iter()
        .map(|(url, _)| url.clone())
        .collect::<Vec<_>>();
    let (samples, _) = sample_listing_files(
        session,
        std::slice::from_ref(&urls),
        config.path_glob_filter.as_ref(),
        config.partition_directories_only,
        None,
        false,
        true,
        options.sampling,
    )
    .await?;
    config.compression = config
        .read_format
        .infer_compression(session, &samples)
        .await?;
    if !options.case_sensitive
        && let Ok(physical) = config
            .read_format
            .infer_schema(session, &samples, config.compression)
            .await
    {
        let file_schema = reconcile_schema_names_case_insensitive(
            config.schema.file_schema().as_ref().clone(),
            &physical,
            &[],
        )?;
        config.schema = TableSchema::builder(Arc::new(file_schema))
            .with_table_partition_cols(config.schema.table_partition_cols().clone())
            .build();
    }
    ListingTableSource::try_new(config)
}
