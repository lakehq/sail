use std::sync::Arc;

use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::metadata::{
    DFParquetMetadata, ordering_from_parquet_metadata,
};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;

use crate::listing::source::{ListingFileMeta, ListingFileSample, ListingScanInput, ReadFormat};
use crate::options::r#gen::ParquetReadOptions;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    pub(super) options: ParquetReadOptions,
}

fn fail_for_encryption_factory(options: &TableParquetOptions) -> Result<()> {
    if let Some(x) = &options.crypto.factory_id {
        Err(DataFusionError::Configuration(format!(
            "Parquet encryption factory ID is set to '{x}' but parquet encryption is unsupported"
        )))
    } else {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ReadFormat for ParquetReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let options = self.options.clone().into_table_options();
        fail_for_encryption_factory(&options)?;

        let coerce_int96 = options
            .global
            .coerce_int96
            .as_deref()
            .map(parse_coerce_int96_string)
            .transpose()?;

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata_size_hint = options.global.metadata_size_hint;
        let metadata_fetch_concurrency = ctx.config_options().execution.meta_fetch_concurrency;

        let objects = files
            .iter()
            .flat_map(|group| group.objects.iter().map(|object| (&group.store, object)));

        let mut schemas: Vec<(object_store::path::Path, Schema)> = futures::stream::iter(objects)
            .map(|(store, object)| async {
                let schema = DFParquetMetadata::new(store.as_ref(), object)
                    .with_metadata_size_hint(metadata_size_hint)
                    .with_file_metadata_cache(Some(Arc::clone(&metadata_cache)))
                    .with_coerce_int96(coerce_int96)
                    .fetch_schema()
                    .await?;
                Ok::<_, DataFusionError>((object.location.clone(), schema))
            })
            .boxed() // Workaround for https://github.com/rust-lang/rust/issues/64552
            // fetch schemas concurrently
            .buffer_unordered(metadata_fetch_concurrency)
            .try_collect()
            .await?;

        // Ensure deterministic ordering for stable schema inference.
        schemas.sort_unstable_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas.into_iter().map(|(_, schema)| schema);

        let merged = if options.global.skip_metadata {
            Schema::try_merge(schemas.map(clear_metadata))
        } else {
            Schema::try_merge(schemas)
        }?;

        let merged = if options.global.binary_as_string {
            datafusion::datasource::file_format::parquet::transform_binary_to_string(&merged)
        } else {
            merged
        };

        let merged = if options.global.schema_force_view_types {
            datafusion::datasource::file_format::parquet::transform_schema_to_view(&merged)
        } else {
            merged
        };

        Ok(Arc::new(merged))
    }

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        statistics_columns: Option<&[usize]>,
        _compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let options = self.options.clone().into_table_options();
        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(options.global.metadata_size_hint)
            .with_file_metadata_cache(Some(metadata_cache))
            .fetch_metadata()
            .await?;
        let statistics = match statistics_columns {
            None => DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &file_schema)?,
            Some(columns) => {
                let statistics_schema = Arc::new(file_schema.project(columns)?);
                let projected_statistics = DFParquetMetadata::statistics_from_parquet_metadata(
                    &metadata,
                    &statistics_schema,
                )?;
                expand_projected_statistics(&file_schema, columns, projected_statistics)
            }
        };
        let ordering = ordering_from_parquet_metadata(&metadata, &file_schema)?;
        Ok(ListingFileMeta {
            statistics,
            ordering,
        })
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let options = self.options.clone().into_table_options();
        fail_for_encryption_factory(&options)?;

        let mut source =
            ParquetSource::new(input.schema).with_table_parquet_options(options.clone());

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let store = ctx
            .runtime_env()
            .object_store(input.object_store_url.clone())?;
        let cached_parquet_read_factory =
            Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));
        source = source.with_parquet_file_reader_factory(cached_parquet_read_factory);

        if let Some(metadata_size_hint) = options.global.metadata_size_hint {
            source = source.with_metadata_size_hint(metadata_size_hint)
        }

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})))
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}

fn expand_projected_statistics(
    file_schema: &Schema,
    columns: &[usize],
    projected_statistics: Statistics,
) -> Statistics {
    let mut statistics = Statistics::new_unknown(file_schema);
    statistics.num_rows = projected_statistics.num_rows;
    statistics.total_byte_size = projected_statistics.total_byte_size;
    for (index, column_statistics) in columns
        .iter()
        .copied()
        .zip(projected_statistics.column_statistics)
    {
        statistics.column_statistics[index] = column_statistics;
    }
    statistics
}

/// Clears all metadata (Schema level and field level) for a schema.
fn clear_metadata(schema: Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(field.as_ref().clone().with_metadata(Default::default())) // clear meta
        })
        .collect::<Fields>();
    Schema::new(fields)
}

/// Parses `coerce_int96` setting into an Arrow [`TimeUnit`].
///
/// This is adapted from DataFusion's Parquet data source implementation.
fn parse_coerce_int96_string(setting: &str) -> Result<TimeUnit> {
    match setting.to_lowercase().as_str() {
        "ns" => Ok(TimeUnit::Nanosecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ms" => Ok(TimeUnit::Millisecond),
        "s" => Ok(TimeUnit::Second),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet `coerce_int96` setting: {setting}. Valid values are: ns, us, ms, and s."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_common::stats::Precision;

    use super::*;

    #[test]
    fn projected_statistics_keep_full_schema_positions() {
        let file_schema = Schema::new(vec![
            Arc::new(datafusion::arrow::datatypes::Field::new(
                "a",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            )),
            Arc::new(datafusion::arrow::datatypes::Field::new(
                "b",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            )),
            Arc::new(datafusion::arrow::datatypes::Field::new(
                "c",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            )),
        ]);
        let mut selected_column = datafusion_common::ColumnStatistics::new_unknown();
        selected_column.min_value = Precision::Exact(ScalarValue::Int32(Some(10)));
        selected_column.max_value = Precision::Exact(ScalarValue::Int32(Some(20)));
        let projected_statistics = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![selected_column.clone()],
        };

        let statistics = expand_projected_statistics(&file_schema, &[1], projected_statistics);

        assert_eq!(statistics.num_rows, Precision::Exact(100));
        assert_eq!(statistics.column_statistics.len(), 3);
        assert_eq!(
            statistics.column_statistics[0],
            datafusion_common::ColumnStatistics::new_unknown()
        );
        assert_eq!(statistics.column_statistics[1], selected_column);
        assert_eq!(
            statistics.column_statistics[2],
            datafusion_common::ColumnStatistics::new_unknown()
        );
    }

    #[test]
    fn row_count_statistics_do_not_require_columns() {
        let file_schema = Schema::new(vec![Arc::new(datafusion::arrow::datatypes::Field::new(
            "a",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ))]);
        let projected_statistics = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };

        let statistics = expand_projected_statistics(&file_schema, &[], projected_statistics);

        assert_eq!(statistics.num_rows, Precision::Exact(100));
        assert_eq!(
            statistics.column_statistics,
            vec![datafusion_common::ColumnStatistics::new_unknown()]
        );
    }
}
