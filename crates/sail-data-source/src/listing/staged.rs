use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::parquet::ParquetSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    PlanProperties, execute_stream,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::TryStreamExt;
use object_store::path::Path;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::listing::commit::{canonical_partition_path, cleanup_staging};

/// Writes Spark-compatible Hive paths with a fresh sink per execution. Staged
/// writes emit only successful file manifests; ordinary writes retain sink output.
#[derive(Debug)]
pub struct ParquetPartitionWriteExec {
    input: Arc<dyn ExecutionPlan>,
    staged: bool,
    properties: Arc<PlanProperties>,
}

impl ParquetPartitionWriteExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, staged: bool) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(if staged {
                Self::manifest_schema()
            } else {
                input.schema()
            }),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            staged,
            properties,
        }
    }
    pub fn staged(&self) -> bool {
        self.staged
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn manifest_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, false)]))
    }
}

impl DisplayAs for ParquetPartitionWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: staged={}", self.name(), self.staged)
    }
}

impl ExecutionPlan for ParquetPartitionWriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::SinglePartition])
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("ParquetPartitionWriteExec requires one child");
        };
        Ok(Arc::new(Self::new(input.clone(), self.staged)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("ParquetPartitionWriteExec expects partition 0");
        }
        let attempt = Uuid::new_v4().to_string();
        let mut sink = None;
        let plan = self
            .input
            .clone()
            .transform_down(|plan| {
                // Tracing wrappers delegate downcasts to their child; inspect only
                // concrete nodes so the same sink is never rewritten twice.
                let Some(exec) = (plan.as_ref() as &dyn Any).downcast_ref::<DataSinkExec>() else {
                    return Ok(Transformed::no(plan));
                };
                let Some(parquet) = exec.sink().downcast_ref::<ParquetSink>() else {
                    return internal_err!("expected Parquet sink for staged write");
                };
                if sink.is_some() {
                    return internal_err!("multiple sinks in staged write");
                }
                let mut config = parquet.config().clone();
                if self.staged {
                    let [table_path] = config.table_paths.as_slice() else {
                        return internal_err!("partition write requires one output path");
                    };
                    let mut url = table_path.get_url().clone();
                    url.path_segments_mut()
                        .map_err(|_| {
                            datafusion_common::exec_datafusion_err!("invalid staging URL")
                        })?
                        .pop_if_empty()
                        .push("attempts")
                        .push(&attempt)
                        .push("");
                    config.original_url = url.to_string();
                    config.table_paths = vec![ListingTableUrl::try_new(url, None)?];
                }
                let sorting_columns = parquet_sorting_columns(exec);
                let fresh = Arc::new(
                    ParquetSink::new(config, parquet.parquet_options().clone())
                        .with_sorting_columns(sorting_columns),
                );
                sink = Some(fresh.clone());
                Ok(Transformed::new(
                    Arc::new(DataSinkExec::new(
                        exec.input().clone(),
                        Arc::new(CanonicalParquetSink(fresh)),
                        exec.sort_order().clone(),
                    )) as Arc<dyn ExecutionPlan>,
                    true,
                    TreeNodeRecursion::Jump,
                ))
            })?
            .data;
        let sink = sink.ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("missing Parquet sink in staged write")
        })?;
        let staged = self.staged;
        let stream = futures::stream::once(async move {
            let result = async {
                let batches = execute_stream(plan, context.clone())?
                    .try_collect::<Vec<_>>()
                    .await?;
                if staged {
                    let paths = sink
                        .written()
                        .into_keys()
                        .map(|path| path.to_string())
                        .collect::<Vec<_>>();
                    Ok::<_, datafusion_common::DataFusionError>(vec![RecordBatch::try_new(
                        Self::manifest_schema(),
                        vec![Arc::new(StringArray::from(paths))],
                    )?])
                } else {
                    Ok(batches)
                }
            }
            .await;
            if staged && result.is_err() {
                let config = sink.config();
                if let Ok(store) = context.runtime_env().object_store(&config.object_store_url)
                    && let Err(error) =
                        cleanup_staging(store.as_ref(), config.table_paths[0].prefix()).await
                {
                    log::warn!("failed to clean staged Parquet attempt: {error}");
                }
            }
            result
        })
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

// [CREDIT]: https://github.com/apache/datafusion/blob/55.2.0/datafusion/datasource-parquet/src/metadata.rs#L954-L1016
fn parquet_sorting_columns(exec: &DataSinkExec) -> Option<Vec<SortingColumn>> {
    let parquet = exec.sink().downcast_ref::<ParquetSink>()?;
    let input_schema = parquet.config().output_schema();
    let writer_schema = datafusion_datasource::write::get_writer_schema(parquet.config());
    let ordering: LexOrdering = exec.sort_order().as_ref()?.clone().into();
    let mut columns = vec![];
    for sort in ordering.iter() {
        let column = sort.expr.downcast_ref::<Column>()?;
        let field = input_schema.fields().get(column.index())?;
        let Some((index, _)) = writer_schema.column_with_name(field.name()) else {
            continue;
        };
        columns.push(SortingColumn {
            column_idx: index.try_into().ok()?,
            descending: sort.options.descending,
            nulls_first: sort.options.nulls_first,
        });
    }
    (!columns.is_empty()).then_some(columns)
}

#[derive(Debug)]
struct CanonicalParquetSink(Arc<ParquetSink>);

impl DisplayAs for CanonicalParquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt_as(t, f)
    }
}

#[async_trait]
impl DataSink for CanonicalParquetSink {
    fn schema(&self) -> &SchemaRef {
        self.0.config().output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let config = self.0.config();
        let store = context
            .runtime_env()
            .object_store(&config.object_store_url)?;
        let [table_path] = config.table_paths.as_slice() else {
            return internal_err!("partition write requires one output path");
        };
        let root = table_path.prefix().clone();
        let columns = config
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let extension = config.file_extension.clone();
        let keep_columns = config.keep_partition_by_columns;
        let buffered_batches = context
            .session_config()
            .options()
            .execution
            .max_buffered_batches_per_output_file
            .get();
        let (tx, rx) = mpsc::unbounded_channel();
        let demux = SpawnedTask::spawn(demux_partitions(
            data,
            tx,
            root,
            columns,
            extension,
            keep_columns,
            buffered_batches,
        ));
        self.0
            .spawn_writer_tasks_and_join(context, demux, rx, store)
            .await
    }
}

// [CREDIT]: https://github.com/apache/datafusion/blob/55.2.0/datafusion/datasource/src/write/demux.rs#L293-L371
async fn demux_partitions(
    mut input: SendableRecordBatchStream,
    files: mpsc::UnboundedSender<(Path, mpsc::Receiver<RecordBatch>)>,
    root: Path,
    columns: Vec<String>,
    extension: String,
    keep_columns: bool,
    buffered_batches: usize,
) -> Result<()> {
    if columns.is_empty() {
        return internal_err!("partition demux requires partition columns");
    }
    let schema = input.schema();
    let indices = columns
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let projection = (0..schema.fields().len())
        .filter(|index| keep_columns || !indices.contains(index))
        .collect::<Vec<_>>();
    let write_id = Uuid::new_v4();
    let mut streams: HashMap<Vec<String>, mpsc::Sender<RecordBatch>> = HashMap::new();
    while let Some(batch) = input.try_next().await? {
        let values = indices
            .iter()
            .map(|index| arrow::compute::cast(batch.column(*index), &DataType::Utf8))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let values = values
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!(
                        "partition cast did not produce strings"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut groups: HashMap<Vec<String>, Vec<u64>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key = values
                .iter()
                .zip(&indices)
                .map(|(array, index)| {
                    if array.is_null(row) || array.value(row).is_empty() {
                        Ok("__HIVE_DEFAULT_PARTITION__".to_string())
                    } else if matches!(
                        batch.column(*index).data_type(),
                        DataType::Float16 | DataType::Float32 | DataType::Float64
                    ) {
                        // Match the original DataFusion writer's native float formatting.
                        Ok(
                            ScalarValue::try_from_array(batch.column(*index).as_ref(), row)?
                                .to_string(),
                        )
                    } else {
                        Ok(array.value(row).to_string())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            groups.entry(key).or_default().push(row as u64);
        }
        // Empty batches produce no files. One bounded channel per nonempty key
        // preserves backpressure while the outer file channel cannot deadlock.
        for (key, rows) in groups {
            let sender = match streams.entry(key.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let path = canonical_partition_path(&root, &columns, &key)?
                        .join(format!("{write_id}.{}", extension.trim_start_matches('.')));
                    let (sender, receiver) = mpsc::channel(buffered_batches);
                    files.send((path, receiver)).map_err(|_| {
                        datafusion_common::exec_datafusion_err!(
                            "Parquet writer stopped receiving partition files"
                        )
                    })?;
                    entry.insert(sender)
                }
            };
            let batch = arrow::compute::take_record_batch(&batch, &UInt64Array::from(rows))?
                .project(&projection)?;
            sender.send(batch).await.map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "Parquet writer stopped receiving partition batches"
                )
            })?;
        }
    }
    // Dropping all channels lets file writers finish; the sink also joins this
    // task, so an input error cannot produce a successful attempt manifest.
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion_common::config::TableParquetOptions;
    use datafusion_datasource::memory::MemorySourceConfig;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn repeated_execution_emits_only_fresh_attempt_files() -> Result<()> {
        let context = Arc::new(TaskContext::default());
        let url = ListingTableUrl::parse("memory://bucket/staging/")?;
        context
            .runtime_env()
            .register_object_store(url.get_url(), Arc::new(InMemory::new()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("p", DataType::Utf8, true),
        ]));
        let values = vec![None, Some(""), Some("a/b=c%?#"), Some("%2F"), Some("é'")];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(values.clone())),
            ],
        )?;
        let input = MemorySourceConfig::try_new_exec(
            &[vec![RecordBatch::new_empty(schema.clone()), batch]],
            schema.clone(),
            None,
        )?;
        let sink = ParquetSink::new(
            FileSinkConfig {
                original_url: url.as_str().to_string(),
                object_store_url: ObjectStoreUrl::parse("memory://bucket")?,
                file_group: Default::default(),
                table_paths: vec![url],
                output_schema: schema,
                table_partition_cols: vec![("p".into(), DataType::Utf8)],
                insert_op: InsertOp::Append,
                keep_partition_by_columns: false,
                file_extension: "parquet".into(),
                file_output_mode: FileOutputMode::Automatic,
            },
            TableParquetOptions::default(),
        );
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ParquetPartitionWriteExec::new(
            Arc::new(DataSinkExec::new(input, Arc::new(sink), None)),
            true,
        ));
        let mut manifests = vec![];
        for _ in 0..2 {
            let batches = execute_stream(plan.clone(), context.clone())?
                .try_collect::<Vec<_>>()
                .await?;
            let paths = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected manifest"))?;
            let paths = paths
                .iter()
                .flatten()
                .map(ToString::to_string)
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(paths.len(), 4);
            assert!(
                paths
                    .iter()
                    .any(|path| path.contains("p=__HIVE_DEFAULT_PARTITION__/"))
            );
            assert!(
                paths
                    .iter()
                    .any(|path| path.contains("p=a%2Fb%3Dc%25%3F%23/"))
            );
            assert!(paths.iter().any(|path| path.contains("p=%252F/")));
            assert!(paths.iter().any(|path| path.contains("p=é%27/")));
            manifests.push(paths);
        }
        assert!(manifests[0].is_disjoint(&manifests[1]));
        Ok(())
    }
}
