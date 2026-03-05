use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::compute::{concat_batches, lexsort_to_indices, take, SortOptions};
use arrow_schema::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::hash_utils::create_hashes;
use futures::StreamExt;
use log::info;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::bucketing::{
    bucket_file_name, create_bucketed_writer_properties, inject_schema_metadata,
    resolve_bucket_column_indices, BucketingConfig,
};

/// Physical plan node that writes data into bucketed Parquet files.
///
/// For each execution:
/// 1. Collects all input batches (single partition — caller adds CoalescePartitionsExec)
/// 2. Hash-partitions rows by bucket columns into N buckets
/// 3. Optionally sorts within each bucket
/// 4. Writes each bucket to `{output_path}/bucket_{:05d}.parquet`
/// 5. Embeds bucketing metadata in each file's Arrow schema and KV metadata
#[derive(Debug, Clone)]
pub struct BucketedParquetSinkExec {
    input: Arc<dyn ExecutionPlan>,
    config: BucketingConfig,
    output_path: String,
    file_schema: SchemaRef,
    writer_props: WriterProperties,
    properties: PlanProperties,
}

impl BucketedParquetSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        config: BucketingConfig,
        output_path: String,
        file_schema: SchemaRef,
        writer_props: WriterProperties,
    ) -> Result<Self> {
        // Validate bucket columns exist in schema
        resolve_bucket_column_indices(&file_schema, &config.columns)?;

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(arrow_schema::Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            input,
            config,
            output_path,
            file_schema,
            writer_props,
            properties,
        })
    }

    pub fn config(&self) -> &BucketingConfig {
        &self.config
    }

    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    pub fn writer_props(&self) -> &WriterProperties {
        &self.writer_props
    }
}

impl DisplayAs for BucketedParquetSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BucketedParquetSinkExec: columns=[{}], num_buckets={}, path={}",
            self.config.columns.join(", "),
            self.config.num_buckets,
            self.output_path,
        )
    }
}

impl ExecutionPlan for BucketedParquetSinkExec {
    fn name(&self) -> &str {
        "BucketedParquetSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self {
                input,
                ..self.as_ref().clone()
            })),
            _ => plan_err!("BucketedParquetSinkExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("BucketedParquetSinkExec only supports partition 0, got {partition}");
        }

        let input_stream = self.input.execute(0, context.clone())?;
        let config = self.config.clone();
        let output_path = self.output_path.clone();
        let file_schema = self.file_schema.clone();
        let writer_props = self.writer_props.clone();
        let output_schema = Arc::new(arrow_schema::Schema::empty());

        let output = futures::stream::once(async move {
            let result = write_bucketed(
                input_stream,
                &config,
                &output_path,
                &file_schema,
                &writer_props,
                &context,
            )
            .await;

            match result {
                Ok(total_rows) => {
                    info!(
                        "BucketedParquetSinkExec: wrote {total_rows} rows across {} buckets to {output_path}",
                        config.num_buckets
                    );
                    Ok(RecordBatch::new_empty(Arc::new(
                        arrow_schema::Schema::empty(),
                    )))
                }
                Err(e) => Err(e),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }
}

/// Core write logic: collect batches, partition into buckets, write files.
async fn write_bucketed(
    mut stream: SendableRecordBatchStream,
    config: &BucketingConfig,
    output_path: &str,
    file_schema: &SchemaRef,
    writer_props: &WriterProperties,
    context: &TaskContext,
) -> Result<u64> {
    // 1. Collect all batches from the input stream
    let mut all_batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() > 0 {
            all_batches.push(batch);
        }
    }

    if all_batches.is_empty() {
        return Ok(0);
    }

    // 2. Concatenate into a single batch for partitioning
    let combined = concat_batches(file_schema, &all_batches)?;

    // 3. Resolve bucket column indices
    let col_indices = resolve_bucket_column_indices(file_schema, &config.columns)?;

    // 4. Compute bucket assignment: hash(bucket_cols) % num_buckets
    let bucket_arrays: Vec<_> = col_indices
        .iter()
        .map(|&idx| Arc::clone(combined.column(idx)))
        .collect();

    let num_rows = combined.num_rows();
    let num_buckets = config.num_buckets;
    let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    let mut hashes = vec![0u64; num_rows];
    create_hashes(&bucket_arrays, &random_state, &mut hashes)?;

    let avg_per_bucket = num_rows / num_buckets + 1;
    let mut bucket_indices: Vec<Vec<u32>> = (0..num_buckets)
        .map(|_| Vec::with_capacity(avg_per_bucket))
        .collect();
    for (row, hash) in hashes.iter().enumerate() {
        let bucket = (*hash as usize) % num_buckets;
        bucket_indices[bucket].push(row as u32);
    }

    // 5. Inject schema metadata
    let enriched_schema = inject_schema_metadata(file_schema, config);

    // 6. Get object store
    let glob_urls = crate::url::GlobUrl::parse(output_path)?;
    let glob_url = glob_urls
        .into_iter()
        .next()
        .ok_or_else(|| exec_datafusion_err!("empty output path: {output_path}"))?;
    let store = context.runtime_env().object_store(&glob_url)?;

    // 7. Build bucket batches (empty buckets get empty files to maintain 1:1 mapping)
    let mut bucket_batches: Vec<(usize, RecordBatch)> = Vec::with_capacity(num_buckets);
    let mut total_written = 0u64;

    for (bucket_id, indices) in bucket_indices.iter().enumerate() {
        if indices.is_empty() {
            bucket_batches.push((bucket_id, RecordBatch::new_empty(enriched_schema.clone())));
            continue;
        }

        // Take rows for this bucket
        let indices_array = arrow::array::UInt32Array::from(indices.clone());
        let bucket_columns: Vec<_> = combined
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut bucket_batch = RecordBatch::try_new(enriched_schema.clone(), bucket_columns)?;

        // Sort within bucket if requested
        if !config.sort_columns.is_empty() {
            bucket_batch = sort_batch(&bucket_batch, &config.sort_columns)?;
        }

        total_written += bucket_batch.num_rows() as u64;
        bucket_batches.push((bucket_id, bucket_batch));
    }

    // 8. Write bucket files in parallel
    let write_futures: Vec<_> = bucket_batches
        .iter()
        .map(|(bucket_id, batch)| {
            write_bucket_file(
                &store,
                output_path,
                *bucket_id,
                batch,
                config,
                writer_props,
                &enriched_schema,
            )
        })
        .collect();

    futures::future::try_join_all(write_futures).await?;

    Ok(total_written)
}

/// Sort a RecordBatch by the given columns.
fn sort_batch(batch: &RecordBatch, sort_columns: &[(String, bool)]) -> Result<RecordBatch> {
    let sort_cols: Vec<arrow::compute::SortColumn> = sort_columns
        .iter()
        .map(|(name, ascending)| {
            let col_idx = batch
                .schema()
                .index_of(name)
                .map_err(|_| exec_datafusion_err!("sort column '{name}' not found in schema"))?;
            Ok(arrow::compute::SortColumn {
                values: Arc::clone(batch.column(col_idx)),
                options: Some(SortOptions {
                    descending: !ascending,
                    nulls_first: !ascending, // NULLs first for DESC, last for ASC
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_cols, None)?;
    let sorted_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}

/// Write a single bucket's data to a Parquet file.
async fn write_bucket_file(
    store: &Arc<dyn object_store::ObjectStore>,
    output_path: &str,
    bucket_id: usize,
    batch: &RecordBatch,
    config: &BucketingConfig,
    base_props: &WriterProperties,
    schema: &SchemaRef,
) -> Result<()> {
    let row_count = batch.num_rows() as u64;
    let props = create_bucketed_writer_properties(base_props, config, bucket_id, row_count);
    let file_name = bucket_file_name(bucket_id);

    // Build object_store path
    let base = Path::from(output_path.trim_start_matches('/'));
    let file_path = base.child(file_name);

    // Write to in-memory buffer, then put to object store
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(batch)?;
        }
        writer.close()?;
    }

    store
        .put(&file_path, bytes::Bytes::from(buf).into())
        .await
        .map_err(|e| exec_datafusion_err!("failed to write bucket file {file_path}: {e}"))?;

    info!(
        "Wrote bucket {bucket_id}: {} rows to {file_path}",
        batch.num_rows()
    );

    Ok(())
}
