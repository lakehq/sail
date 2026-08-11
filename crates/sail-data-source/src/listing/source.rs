use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Result, Statistics, not_impl_err, plan_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{ListingTableUrl, TableSchema};
use datafusion_expr::ScalarUDF;
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::array::record_batch::retag_timestamp_data_type;
use sail_common_datafusion::datasource::{
    OptionLayer, SinkInfo, SinkMode, SourceInfo, TableFormat, find_path_in_options,
    get_partition_columns_and_file_schema,
};
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionCastColumnExpr, StructFieldMatching,
};
use sail_function::scalar::datetime::spark_file_timestamp::SparkFileTimestamp;
use url::Url;

use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::{
    infer_partitions, rewrite_utf8view_fields, sample_listing_files, validate_partitions,
};
use crate::listing::write::{FileWriteNode, FileWriteOptions};
use crate::resolve_listing_urls;
use crate::url::{PathGlobFilter, resolve_listing_writer_url};

/// A trait for creating format instances when reading and writing listing files.
pub trait FormatFactory: Debug + Send + Sync + 'static {
    type Read: ReadFormat;
    type Write: WriteFormat;

    /// The name of the format.
    fn name() -> &'static str;

    /// Creates the read format.
    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read>;

    /// Creates the write format.
    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write>;
}

/// A trait for format-specific logic for reading listing files.
#[async_trait]
pub trait ReadFormat: Debug + Send + Sync + 'static {
    async fn infer_compression(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant>;

    /// Infer the file schema from the given files.
    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef>;

    /// Infer file-level metadata needed for planning.
    /// The metadata includes statistics and ordering.
    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let _ = (ctx, store, object, compression);
        Ok(ListingFileMeta {
            statistics: Statistics::new_unknown(&file_schema),
            ordering: None,
        })
    }

    /// Build a scan configuration for listing reads.
    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig>;

    /// Adapts a physical scan to the engine's canonical schema. Most formats already
    /// produce their canonical schema directly.
    fn adapt_scan_plan(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(input)
    }

    /// Whether validating an explicit schema requires the physical file schema.
    fn requires_explicit_schema_validation(&self) -> bool {
        false
    }

    /// Validate a user-provided file schema against the physical file schema.
    fn validate_explicit_schema(&self, _schema: &Schema, _physical: &Schema) -> Result<()> {
        Ok(())
    }

    /// File-name glob restricting which listed files compose the dataset.
    fn path_glob_filter(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug)]
pub struct ListingFileSample<'a> {
    pub url: &'a ListingTableUrl,
    pub store: Arc<dyn ObjectStore>,
    pub objects: Vec<ObjectMeta>,
}

#[derive(Debug, Clone)]
pub struct ListingFileMeta {
    pub statistics: Statistics,
    pub ordering: Option<LexOrdering>,
}

#[derive(Debug)]
pub struct ListingScanInput {
    pub object_store_url: ObjectStoreUrl,
    pub file_groups: Vec<FileGroup>,
    pub constraints: datafusion_common::Constraints,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    pub preserve_order: bool,
    pub output_ordering: Vec<LexOrdering>,
    pub statistics: Statistics,
    pub partitioned_by_file_group: bool,
    pub schema: TableSchema,
    pub compression: CompressionTypeVariant,
}

/// Configuration for creating a listing-file sink execution plan.
pub struct ListingSinkInput {
    pub input: Arc<dyn ExecutionPlan>,
    pub sink: FileSinkConfig,
    pub sort_order: Option<LexRequirement>,
    pub session_timezone: Arc<str>,
}

impl ListingSinkInput {
    /// Retags LTZ columns at text and Arrow serialization boundaries so those formats preserve
    /// Spark's session-zone presentation while execution continues to use canonical UTC metadata.
    pub fn retag_timestamps_for_output(mut self) -> Result<Self> {
        let input = retag_timestamp_plan(Arc::clone(&self.input), &self.session_timezone)?;
        if !Arc::ptr_eq(&input, &self.input) {
            self.input = input;
            self.sink.output_schema = self.input.schema();
        }
        Ok(self)
    }

    /// Prepares LTZ timestamps for CSV/JSON writers using Spark's formatter and
    /// explicit session timezone, without exposing timezone metadata to Arrow's formatter.
    pub fn format_timestamps_for_text_output(
        mut self,
        ctx: &dyn Session,
        timestamp_format: &Arc<str>,
    ) -> Result<Self> {
        let requires_formatting = self.input.schema().fields().iter().any(|field| {
            SparkFileTimestamp::output_type(field.data_type())
                .is_ok_and(|output| output != *field.data_type())
        });
        if !requires_formatting {
            return Ok(self);
        }
        if let Some(sort_order) = self.sort_order.take() {
            self.input = Arc::new(
                SortExec::new(sort_order.into(), self.input).with_preserve_partitioning(true),
            );
        }
        self.input = format_timestamp_plan_for_text_output(
            self.input,
            &self.session_timezone,
            timestamp_format,
            Arc::new(ctx.config_options().clone()),
        )?;
        self.sink.output_schema = self.input.schema();
        Ok(self)
    }
}

pub(crate) fn retag_timestamp_plan(
    input: Arc<dyn ExecutionPlan>,
    session_timezone: &Arc<str>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let mut changed = false;
    let expressions = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, input_field)| {
            let target_type = retag_timestamp_data_type(input_field.data_type(), session_timezone)?;
            let column = Arc::new(Column::new(input_field.name(), index)) as Arc<dyn PhysicalExpr>;
            let expression = if target_type == *input_field.data_type() {
                column
            } else {
                changed = true;
                let target_field =
                    Arc::new(input_field.as_ref().clone().with_data_type(target_type));
                Arc::new(SchemaEvolutionCastColumnExpr::new_relaxed_timezone(
                    column,
                    Arc::clone(input_field),
                    target_field,
                    None,
                    StructFieldMatching::Name,
                )) as Arc<dyn PhysicalExpr>
            };
            Ok((expression, input_field.name().clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    } else {
        Ok(input)
    }
}

fn format_timestamp_plan_for_text_output(
    input: Arc<dyn ExecutionPlan>,
    session_timezone: &Arc<str>,
    timestamp_format: &Arc<str>,
    config_options: Arc<datafusion_common::config::ConfigOptions>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let udf = Arc::new(ScalarUDF::from(SparkFileTimestamp::new(
        Arc::clone(session_timezone),
        Arc::clone(timestamp_format),
    )));
    let expressions = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, input_field)| {
            let output_type = SparkFileTimestamp::output_type(input_field.data_type())?;
            let column = Arc::new(Column::new(input_field.name(), index)) as Arc<dyn PhysicalExpr>;
            let expression = if output_type == *input_field.data_type() {
                column
            } else {
                Arc::new(ScalarFunctionExpr::try_new(
                    Arc::clone(&udf),
                    vec![column],
                    input_schema.as_ref(),
                    Arc::clone(&config_options),
                )?) as Arc<dyn PhysicalExpr>
            };
            Ok((expression, input_field.name().clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
}

/// A trait for format-specific logic for writing listing files.
#[async_trait]
pub trait WriteFormat: Debug + Send + Sync + 'static {
    async fn sink(
        &self,
        ctx: &dyn Session,
        input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug, Default)]
pub struct ListingTableFormat<T: FormatFactory> {
    phantom: PhantomData<T>,
}

#[async_trait]
impl<T: FormatFactory> TableFormat for ListingTableFormat<T> {
    fn name(&self) -> &str {
        T::name()
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            lakehouse_table: _,
            schema,
            constraints,
            partition_by,
            bucket_by: _,
            sort_order,
            options,
            read_case_sensitive,
        } = info;

        let read_format = T::read(ctx, options)?;
        let path_glob_filter = read_format
            .path_glob_filter()
            .map(PathGlobFilter::parse)
            .transpose()?;
        let urls = resolve_listing_urls(ctx, paths).await?;
        let sampled_files = sample_listing_files(ctx, &urls, path_glob_filter.as_ref()).await?;
        let compression = read_format.infer_compression(ctx, &sampled_files).await?;

        let (schema, partition_fields) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                let physical = if read_format.requires_explicit_schema_validation() {
                    Some(
                        read_format
                            .infer_schema(ctx, &sampled_files, compression)
                            .await?,
                    )
                } else if read_case_sensitive {
                    None
                } else {
                    read_format
                        .infer_schema(ctx, &sampled_files, compression)
                        .await
                        .ok()
                };
                // Spark matches a user-specified schema against the physical file
                // columns case-insensitively by default (`spark.sql.caseSensitive=false`).
                // Reconcile the user column names to the physical names up front so that both
                // the file stats and reader (which resolve columns by exact name) find the data.
                let schema = if read_case_sensitive {
                    schema
                } else if let Some(physical) = &physical {
                    reconcile_schema_names_case_insensitive(schema, physical)?
                } else {
                    // Keeps the user schema if physical schema inference is unavailable.
                    schema
                };
                // When the partition columns are not specified, auto-discover
                // them from `key=value` segments in the listing paths.
                // Without this, columns that exist only in the directory tree
                // are treated as file columns, and the file reader fails
                // because the file itself does not contain them.
                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files)?
                        .into_iter()
                        .filter(|name| {
                            schema
                                .fields()
                                .iter()
                                .any(|f| f.name().eq_ignore_ascii_case(name))
                        })
                        .collect::<Vec<_>>()
                } else {
                    partition_by
                };
                let (partition_fields, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                if let Some(physical) = physical {
                    read_format.validate_explicit_schema(&schema, &physical)?;
                }
                (Arc::new(schema), partition_fields)
            }
            _ => {
                let schema = read_format
                    .infer_schema(ctx, &sampled_files, compression)
                    .await?;
                let schema = rewrite_utf8view_fields(schema);

                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files)?
                } else {
                    partition_by
                };

                // TODO: infer concrete partition types from observed values to match
                //   the `spark.sql.sources.partitionColumnTypeInference.enabled` option.
                let partition_fields = partition_by
                    .into_iter()
                    .map(|col| Arc::new(Field::new(col, DataType::Utf8, false)))
                    .collect();
                (schema, partition_fields)
            }
        };

        validate_partitions(&sampled_files, &partition_fields)?;

        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            table_paths: urls,
            schema: TableSchema::new(schema, partition_fields),
            constraints,
            file_sort_order: vec![sort_order],
            collect_stat: ctx.config().collect_statistics(),
            target_partitions: ctx.config().target_partitions(),
            read_format: Arc::new(read_format),
            path_glob_filter,
            compression,
        })?;
        Ok(Arc::new(source))
    }

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in listing table options");
        };
        let SinkInfo {
            input,
            session_timezone,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table,
        } = info;
        let catalog_managed = lakehouse_table.is_some();
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing table format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for writing listing table format");
        }
        let url = resolve_listing_writer_url(path.clone())?;
        let overwrite = match mode {
            SinkMode::ErrorIfExists => {
                if (!catalog_managed && listing_target_exists(ctx, &url).await?)
                    || (catalog_managed && listing_target_nonempty(ctx, &url).await?)
                {
                    return plan_err!("listing table path already exists: {path}");
                }
                false
            }
            SinkMode::IgnoreIfExists => {
                if listing_target_exists(ctx, &url).await? {
                    return LogicalPlanBuilder::empty(false).build();
                }
                false
            }
            SinkMode::Append => false,
            SinkMode::Overwrite => true,
            mode => return not_impl_err!("unsupported sink mode for listing table: {mode:?}"),
        };
        let write_format = T::write(ctx, options)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileWriteNode::new(
                Arc::new(input),
                FileWriteOptions {
                    format: Arc::new(write_format),
                    url,
                    overwrite,
                    session_timezone,
                    partition_by,
                    sort_by: sort_order,
                },
            )),
        }))
    }
}
async fn listing_target_exists(ctx: &dyn Session, url: &Url) -> Result<bool> {
    // For file systems, treat the target as existing even if it is an empty directory.
    if url.scheme() == "file"
        && let Ok(path) = url.to_file_path()
        && path.exists()
    {
        return Ok(true);
    }
    listing_target_nonempty(ctx, url).await
}
async fn listing_target_nonempty(ctx: &dyn Session, url: &Url) -> Result<bool> {
    let path = ListingTableUrl::try_new(url.clone(), None)?;
    let store = ctx.runtime_env().object_store(&path)?;
    Ok(store.list(Some(path.prefix())).try_next().await?.is_some())
}

// Reconciles a user-specified schema's field names with the physical file schema
// case-insensitively, matching Spark's default `spark.sql.caseSensitive=false`.
fn reconcile_schema_names_case_insensitive(schema: Schema, physical: &Schema) -> Result<Schema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let mut matches = physical
            .fields()
            .iter()
            .filter(|f| f.name().eq_ignore_ascii_case(name));
        let reconciled = match matches.next() {
            None => Arc::clone(field),
            Some(first) => {
                if let Some(second) = matches.next() {
                    let mut names = vec![first.name().as_str(), second.name().as_str()];
                    names.extend(matches.map(|f| f.name().as_str()));
                    return plan_err!(
                        "Ambiguous case-insensitive column match for `{name}`: [{}]",
                        names.join(", ")
                    );
                }
                if first.name() == name {
                    Arc::clone(field)
                } else {
                    Arc::new(field.as_ref().clone().with_name(first.name().as_str()))
                }
            }
        };
        fields.push(reconciled);
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, TimestampMicrosecondArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DataFusionError;
    use futures::{StreamExt, TryStreamExt};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::formats::csv::CsvFormatFactory;
    use crate::formats::json::JsonFormatFactory;
    use crate::options::option_list;

    #[tokio::test]
    async fn output_boundary_retags_ltz_without_changing_instant() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ltz",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Microsecond,
                    Some(Arc::from("UTC")),
                ),
                false,
            ),
            Field::new(
                "ntz",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let ltz =
            Arc::new(TimestampMicrosecondArray::from(vec![-3_723_000_000]).with_timezone("UTC"))
                as ArrayRef;
        let ntz = Arc::new(TimestampMicrosecondArray::from(vec![0])) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ltz, ntz])?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;

        let output = retag_timestamp_plan(input, &Arc::from("America/Los_Angeles"))?;
        assert_eq!(
            output.schema().field(0).data_type(),
            &DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("America/Los_Angeles")),
            )
        );
        assert_eq!(
            output.schema().field(1).data_type(),
            &DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        );

        let context = SessionContext::new();
        let mut stream = output.execute(0, context.task_ctx())?;
        let output_batch = stream.next().await.transpose()?.ok_or_else(|| {
            DataFusionError::Execution("output boundary returned no batch".to_string())
        })?;
        let output_ltz = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("LTZ output was not a timestamp array".to_string())
            })?;
        assert_eq!(output_ltz.value(0), -3_723_000_000);
        Ok(())
    }

    async fn write_timestamp_file<F: WriteFormat>(format: F) -> Result<Vec<u8>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ),
            false,
        )]));
        let timestamp =
            Arc::new(TimestampMicrosecondArray::from(vec![-3_723_000_000]).with_timezone("UTC"))
                as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![timestamp])?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;

        let context = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_path = ListingTableUrl::parse("memory://spark-file-format/output/")?;
        let object_store_url = table_path.object_store();
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::clone(&store));
        let sink = FileSinkConfig {
            original_url: table_path.to_string(),
            object_store_url,
            file_group: Default::default(),
            table_paths: vec![table_path.clone()],
            output_schema: Arc::clone(&schema),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: String::new(),
            file_output_mode: FileOutputMode::Automatic,
        };
        let plan = format
            .sink(
                &context.state(),
                ListingSinkInput {
                    input,
                    sink,
                    sort_order: None,
                    session_timezone: Arc::from("+01:02:03"),
                },
            )
            .await?;
        collect(plan, context.task_ctx()).await?;

        let objects = store
            .list(Some(table_path.prefix()))
            .try_collect::<Vec<_>>()
            .await?;
        let [object] = objects.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "expected one output file, got {}",
                objects.len()
            )));
        };
        Ok(store.get(&object.location).await?.bytes().await?.to_vec())
    }

    #[tokio::test]
    async fn csv_and_json_writers_preserve_second_precision_session_offset() -> Result<()> {
        let context = SessionContext::new();
        let default_csv = CsvFormatFactory::write(&context.state(), vec![])?;
        assert_eq!(
            write_timestamp_file(default_csv).await?,
            b"1970-01-01T00:00:00.000+01:02:03\n"
        );
        let custom_csv = CsvFormatFactory::write(
            &context.state(),
            vec![option_list(&[(
                "timestampFormat",
                "yyyy/MM/dd HH:mm:ss XXXXX",
            )])],
        )?;
        assert_eq!(
            write_timestamp_file(custom_csv).await?,
            b"1970/01/01 00:00:00 +01:02:03\n"
        );

        let default_json = JsonFormatFactory::write(&context.state(), vec![])?;
        assert_eq!(
            write_timestamp_file(default_json).await?,
            b"{\"t\":\"1970-01-01T00:00:00.000+01:02:03\"}\n"
        );
        let custom_json = JsonFormatFactory::write(
            &context.state(),
            vec![option_list(&[(
                "timestampFormat",
                "yyyy/MM/dd HH:mm:ss XXXXX",
            )])],
        )?;
        assert_eq!(
            write_timestamp_file(custom_json).await?,
            b"{\"t\":\"1970/01/01 00:00:00 +01:02:03\"}\n"
        );
        Ok(())
    }
}
