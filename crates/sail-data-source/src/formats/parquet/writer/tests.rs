// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, Int64Builder, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeListViewArray, ListArray,
    ListViewArray, MapBuilder, NullArray, PrimitiveArray, RecordBatch, RecordBatchOptions,
    RunArray, StringArray, StringBuilder, StringViewArray, StructArray, UInt32Array, UnionArray,
    make_array, new_null_array,
};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::{
    SortOptions, can_cast_types, cast, concat, concat_batches, take, take_record_batch,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float16Type, Float32Type, Int32Type, Int64Type,
    IntervalDayTime, IntervalMonthDayNano, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionFields,
    UnionMode,
};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
    UnboundedMemoryPool,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, SortingColumn};
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Distribution, PhysicalExpr};
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    ReplaceChildrenOptions, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_datafusion_err};
use datafusion_datasource::ListingTableUrl;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use uuid::Uuid;

use crate::formats::parquet::writer::exec::ParquetWriteExec;
use crate::formats::parquet::writer::file::ParquetFileWriter;
use crate::formats::parquet::writer::lane::{Lane, LaneInput, logical_size};
use crate::formats::parquet::writer::resources::{ActiveWriter, EncodeResources, EncodeSettings};
use crate::formats::parquet::writer::task::{
    ParquetWriteMetrics, ParquetWriteTask, write_partition,
};
use crate::listing::commit::{FileWriteCommitExec, manifest_schema};
use crate::listing::partition::pruned_partition_list;

const WRITE_ID: Uuid = Uuid::from_u128(0x0f1e2d3c_4b5a_4978_8a9b_acbdcedfe0f1);

fn test_task(store: Arc<dyn ObjectStore>, partition: usize) -> ParquetWriteTask {
    ParquetWriteTask {
        store,
        table_path: Path::from("table"),
        write_id: WRITE_ID,
        partition,
        partitions: partition + 1,
        attempt: 0,
        file_suffix: ".parquet".to_string(),
        partition_columns: vec![],
        options: TableParquetOptions::default(),
        ordering: None,
        write_sorting_columns: false,
        buffer_size: 10 * 1024 * 1024,
        max_records_per_file: 0,
        max_file_size: 0,
        max_open_files: 100,
        context: Arc::new(TaskContext::default()),
        encode: fixed_degree(1),
    }
}

/// The resources of a writer task for a file writer that is used without a task.
/// The memory pool counts only the input that waits to be encoded.
fn test_resources(settings: EncodeSettings, pool: &Arc<dyn MemoryPool>) -> Arc<EncodeResources> {
    Arc::new(EncodeResources::new(
        settings,
        MemoryConsumer::new("test").register(pool),
        Time::new(),
    ))
}

/// Settings for a task that always keeps the given number of threads busy
/// with encoding, since it does not share the threads with other tasks.
fn fixed_degree(degree: usize) -> EncodeSettings {
    EncodeSettings {
        threads: Some(degree),
        active_writers: Arc::new(AtomicUsize::new(0)),
        ..EncodeSettings::new(0)
    }
}

fn encode_settings(
    degree: usize,
    lanes: Option<usize>,
    window: Option<usize>,
    read_ahead_bytes: Option<usize>,
) -> EncodeSettings {
    EncodeSettings {
        lanes,
        window,
        read_ahead_bytes,
        ..fixed_degree(degree)
    }
}

/// The ways to encode a file that must all give the same file.
fn encode_cases() -> Vec<EncodeSettings> {
    let mut cases: Vec<_> = [1, 2, 3, 8, 64].into_iter().map(fixed_degree).collect();
    // One lane with several row groups at the same time.
    cases.push(encode_settings(8, Some(1), Some(4), None));
    // Several lanes with one row group at a time.
    cases.push(encode_settings(8, Some(5), Some(1), None));
    // More lanes than columns in most files,
    // and a read-ahead budget that is smaller than any input.
    cases.push(encode_settings(4, Some(1000), Some(2), Some(1)));
    cases.push(encode_settings(3, None, Some(3), Some(64 * 1024)));
    cases
}

fn test_stream(schema: &SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(schema),
        futures::stream::iter(batches.into_iter().map(Ok)),
    ))
}

async fn run_task(
    task: ParquetWriteTask,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    run_task_with_pool(task, schema, batches, &pool).await
}

async fn run_task_with_pool(
    task: ParquetWriteTask,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    pool: &Arc<dyn MemoryPool>,
) -> Result<RecordBatch> {
    let reservation = MemoryConsumer::new("test").register(pool);
    let metrics = ExecutionPlanMetricsSet::new();
    let partition = task.partition;
    read_manifest(write_partition(
        task,
        test_stream(schema, batches),
        reservation,
        ParquetWriteMetrics::new(&metrics, partition),
    ))
    .await
}

/// Runs the writer task and returns the value of one of its metrics.
async fn run_task_for_metric(
    task: ParquetWriteTask,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    metric: &str,
) -> Result<usize> {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let reservation = MemoryConsumer::new("test").register(&pool);
    let metrics = ExecutionPlanMetricsSet::new();
    let partition = task.partition;
    read_manifest(write_partition(
        task,
        test_stream(schema, batches),
        reservation,
        ParquetWriteMetrics::new(&metrics, partition),
    ))
    .await?;
    metrics
        .clone_inner()
        .sum_by_name(metric)
        .map(|x| x.as_usize())
        .ok_or_else(|| internal_datafusion_err!("missing metric {metric}"))
}

/// Reads the stream of a writer task to its end and returns the manifest.
async fn read_manifest(stream: impl Stream<Item = Result<RecordBatch>>) -> Result<RecordBatch> {
    let mut manifests: Vec<_> = stream.try_collect().await?;
    assert_eq!(manifests.len(), 1);
    manifests
        .pop()
        .ok_or_else(|| internal_datafusion_err!("missing manifest"))
}

/// A row of the manifest: partition, attempt, path, size, and number of rows.
type ManifestRow = (u64, u64, Option<String>, u64, u64);

fn manifest_rows(manifest: &RecordBatch) -> Result<Vec<ManifestRow>> {
    use datafusion_common::cast::{as_string_array, as_uint64_array};

    let partition = as_uint64_array(manifest.column(0))?;
    let attempt = as_uint64_array(manifest.column(2))?;
    let path = as_string_array(manifest.column(3))?;
    let size = as_uint64_array(manifest.column(4))?;
    let num_rows = as_uint64_array(manifest.column(5))?;
    Ok((0..manifest.num_rows())
        .map(|i| {
            (
                partition.value(i),
                attempt.value(i),
                path.is_valid(i).then(|| path.value(i).to_string()),
                size.value(i),
                num_rows.value(i),
            )
        })
        .collect())
}

async fn list_files(store: &dyn ObjectStore) -> Result<Vec<(String, u64)>> {
    let mut files: Vec<_> = store
        .list(None)
        .map_ok(|meta| (meta.location.to_string(), meta.size))
        .try_collect()
        .await?;
    files.sort();
    Ok(files)
}

async fn read_bytes(store: &dyn ObjectStore, path: &str) -> Result<Bytes> {
    Ok(store.get(&Path::parse(path)?).await?.bytes().await?)
}

fn read_parquet(data: Bytes) -> Result<(Arc<ParquetMetaData>, RecordBatch)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    let metadata = Arc::clone(builder.metadata());
    let schema = Arc::clone(builder.schema());
    let batches = builder.build()?.collect::<Result<Vec<_>, _>>()?;
    Ok((metadata, concat_batches(&schema, &batches)?))
}

fn int_batch(schema: &SchemaRef, values: std::ops::Range<i64>) -> Result<RecordBatch> {
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(values));
    Ok(RecordBatch::try_new(Arc::clone(schema), vec![column])?)
}

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn file_name(partition: usize, counter: usize, suffix: &str) -> String {
    format!("table/part-{partition:05}-{WRITE_ID}-c{counter:03}{suffix}")
}

async fn check_one_file_per_stream(store: Arc<dyn ObjectStore>) -> Result<()> {
    let schema = int_schema();
    let batches = vec![
        int_batch(&schema, 0..10)?,
        int_batch(&schema, 0..0)?,
        int_batch(&schema, 10..25)?,
    ];
    let manifest = run_task(test_task(Arc::clone(&store), 3), &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    let name = file_name(3, 0, ".parquet");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].0, name);
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![(3, 0, Some(name.clone()), files[0].1, 25)]
    );
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
    assert_eq!(data, int_batch(&schema, 0..25)?);
    Ok(())
}

#[tokio::test]
async fn test_one_file_per_stream_in_memory() -> Result<()> {
    check_one_file_per_stream(Arc::new(InMemory::new())).await
}

#[tokio::test]
async fn test_one_file_per_stream_local() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    check_one_file_per_stream(store).await?;
    let entries = std::fs::read_dir(directory.path().join("table"))?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().to_string()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(entries, vec![format!("part-00003-{WRITE_ID}-c000.parquet")]);
    Ok(())
}

#[tokio::test]
async fn test_upload_larger_than_buffer() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let mut task = test_task(Arc::clone(&store), 0);
    task.buffer_size = 1024;
    task.options.global.compression = Some("uncompressed".to_string());
    let batches = vec![
        int_batch(&schema, 0..5000)?,
        int_batch(&schema, 5000..10000)?,
    ];
    let manifest = run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    let name = file_name(0, 0, ".parquet");
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![(0, 0, Some(name.clone()), files[0].1, 10000)]
    );
    assert!(files[0].1 > 1024 * 10);
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
    assert_eq!(data, int_batch(&schema, 0..10000)?);
    Ok(())
}

#[tokio::test]
async fn test_row_limit_starts_new_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let mut task = test_task(Arc::clone(&store), 1);
    task.max_records_per_file = 10;
    task.options.global.max_row_group_size = 5;
    let batches = vec![
        int_batch(&schema, 0..7)?,
        int_batch(&schema, 7..14)?,
        int_batch(&schema, 14..21)?,
    ];
    let manifest = run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    let first = file_name(1, 0, ".parquet");
    let second = file_name(1, 1, ".parquet");
    let third = file_name(1, 2, ".parquet");
    // No file has more rows than the limit. The batch with which a file
    // reaches the limit is divided, and its other rows are in the next file.
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![
            (1, 0, Some(first.clone()), files[0].1, 10),
            (1, 0, Some(second.clone()), files[1].1, 10),
            (1, 0, Some(third.clone()), files[2].1, 1),
        ]
    );
    let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &first).await?)?;
    assert_eq!(data, int_batch(&schema, 0..10)?);
    assert_eq!(metadata.num_row_groups(), 2);
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &second).await?)?;
    assert_eq!(data, int_batch(&schema, 10..20)?);
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &third).await?)?;
    assert_eq!(data, int_batch(&schema, 20..21)?);
    Ok(())
}

#[tokio::test]
async fn test_row_limit_divides_batches() -> Result<()> {
    let schema = int_schema();
    let batches = vec![
        int_batch(&schema, 0..8192)?,
        int_batch(&schema, 8192..16384)?,
    ];
    // (the limit, the number of rows in each file)
    for (limit, expected) in [
        (1000, [vec![1000; 16], vec![384]].concat()),
        (8192, vec![8192, 8192]),
        (16384, vec![16384]),
        (20000, vec![16384]),
        // The rows of a file are not limited.
        (0, vec![16384]),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.max_records_per_file = limit;
        task.options.global.max_row_group_size = 100;
        let manifest = manifest_rows(&run_task(task, &schema, batches.clone()).await?)?;
        let rows: Vec<_> = manifest.iter().map(|x| x.4).collect();
        assert_eq!(rows, expected, "limit {limit}");
        // The files have the rows in the order of the input,
        // and the files are in that order by their names.
        let mut start = 0;
        for (counter, file) in manifest.iter().enumerate() {
            let name = file_name(0, counter, ".parquet");
            assert_eq!(file.2.as_ref(), Some(&name));
            let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
            let end = start + file.4 as i64;
            assert_eq!(data, int_batch(&schema, start..end)?);
            start = end;
        }
        assert_eq!(list_files(store.as_ref()).await?.len(), expected.len());
    }

    // A file can have a single row.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = test_task(Arc::clone(&store), 0);
    task.max_records_per_file = 1;
    let manifest = run_task(task, &schema, vec![int_batch(&schema, 0..3)?]).await?;
    let rows: Vec<_> = manifest_rows(&manifest)?.iter().map(|x| x.4).collect();
    assert_eq!(rows, vec![1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn test_file_size_limit_starts_new_file() -> Result<()> {
    let schema = int_schema();
    let batches: Vec<_> = (0..40)
        .map(|i| int_batch(&schema, i * 1000..(i + 1) * 1000))
        .collect::<Result<_>>()?;
    for chunking in [false, true] {
        // The size of a file without a limit, and the size of a row group in it.
        let new_task = |store: &Arc<dyn ObjectStore>| {
            let mut task = test_task(Arc::clone(store), 0);
            task.options.global.max_row_group_size = 1000;
            task.options.global.dictionary_enabled = Some(false);
            task.options.global.compression = Some("uncompressed".to_string());
            task.options.global.content_defined_chunking.enabled = chunking;
            task
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        run_task(new_task(&store), &schema, batches.clone()).await?;
        let files = list_files(store.as_ref()).await?;
        assert_eq!(files.len(), 1);
        let row_group = files[0].1 / 40;

        // A file ends at the row group with which it is expected to have the size.
        let limit = 10 * row_group;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = new_task(&store);
        task.max_file_size = limit;
        let manifest = manifest_rows(&run_task(task, &schema, batches.clone()).await?)?;
        assert!(manifest.len() > 1, "{manifest:?}");
        let mut start = 0;
        for (counter, file) in manifest.iter().enumerate() {
            let name = file_name(0, counter, ".parquet");
            assert_eq!(file.2.as_ref(), Some(&name));
            // The row group at which a file ends depends on the row groups
            // that are in the file when the size is checked.
            assert_eq!(file.4 % 1000, 0);
            if counter + 1 < manifest.len() {
                assert!(file.3 + 2 * row_group >= limit, "{chunking} {manifest:?}");
            }
            assert!(file.3 <= limit + 2 * row_group, "{chunking} {manifest:?}");
            let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
            let end = start + file.4 as i64;
            assert_eq!(data, int_batch(&schema, start..end)?);
            start = end;
        }
        assert_eq!(start, 40_000);
    }
    Ok(())
}

#[tokio::test]
async fn test_row_limit_at_end_of_input() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let mut task = test_task(Arc::clone(&store), 0);
    task.max_records_per_file = 10;
    task.options.global.max_row_group_size = 5;
    let manifest = run_task(task, &schema, vec![int_batch(&schema, 0..10)?]).await?;
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![(0, 0, Some(file_name(0, 0, ".parquet")), files[0].1, 10)]
    );
    assert_eq!(files.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_empty_first_partition_writes_schema_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    for batches in [vec![], vec![RecordBatch::new_empty(Arc::clone(&schema))]] {
        let manifest = run_task(test_task(Arc::clone(&store), 0), &schema, batches).await?;
        let files = list_files(store.as_ref()).await?;
        let name = file_name(0, 0, ".parquet");
        assert_eq!(
            manifest_rows(&manifest)?,
            vec![(0, 0, Some(name.clone()), files[0].1, 0)]
        );
        let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
        assert_eq!(metadata.file_metadata().num_rows(), 0);
        assert_eq!(data.num_rows(), 0);
        assert_eq!(data.schema().fields(), schema.fields());
    }
    Ok(())
}

#[tokio::test]
async fn test_empty_other_partition_writes_no_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    for batches in [vec![], vec![int_batch(&schema, 0..0)?]] {
        let manifest = run_task(test_task(Arc::clone(&store), 2), &schema, batches).await?;
        assert_eq!(manifest_rows(&manifest)?, vec![(2, 0, None, 0, 0)]);
        assert_eq!(list_files(store.as_ref()).await?, vec![]);
    }
    Ok(())
}

#[tokio::test]
async fn test_attempt_and_suffix_in_file_name() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let mut task = test_task(Arc::clone(&store), 12);
    task.attempt = 2;
    task.file_suffix = ".zstd.parquet".to_string();
    let manifest = run_task(task, &schema, vec![int_batch(&schema, 0..3)?]).await?;
    let name = format!("table/part-00012-{WRITE_ID}-c000-a2.zstd.parquet");
    let files = list_files(store.as_ref()).await?;
    assert_eq!(files[0].0, name);
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![(12, 2, Some(name), files[0].1, 3)]
    );
    Ok(())
}

#[tokio::test]
async fn test_failed_input_leaves_no_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(vec![
            int_batch(&schema, 0..3),
            Err(internal_datafusion_err!("input failure")),
        ]),
    ));
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let metrics = ExecutionPlanMetricsSet::new();
    let result = read_manifest(write_partition(
        test_task(Arc::clone(&store), 0),
        input,
        MemoryConsumer::new("test").register(&pool),
        ParquetWriteMetrics::new(&metrics, 0),
    ))
    .await;
    assert!(matches!(result, Err(e) if e.to_string().contains("input failure")));
    assert_eq!(list_files(store.as_ref()).await?, vec![]);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn test_memory_limit() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16));
    let result = run_task_with_pool(
        test_task(Arc::clone(&store), 0),
        &schema,
        vec![int_batch(&schema, 0..1000)?],
        &pool,
    )
    .await;
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(list_files(store.as_ref()).await?, vec![]);

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
    run_task_with_pool(
        test_task(Arc::clone(&store), 0),
        &schema,
        vec![int_batch(&schema, 0..1000)?],
        &pool,
    )
    .await?;
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn test_metrics() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let metrics = ExecutionPlanMetricsSet::new();
    read_manifest(write_partition(
        test_task(Arc::clone(&store), 0),
        test_stream(&schema, vec![int_batch(&schema, 0..3)?]),
        MemoryConsumer::new("test").register(&pool),
        ParquetWriteMetrics::new(&metrics, 0),
    ))
    .await?;
    let files = list_files(store.as_ref()).await?;
    let metrics = metrics.clone_inner();
    let value = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|x| x.as_usize())
            .unwrap_or_default()
    };
    assert_eq!(value("rows_written"), 3);
    assert_eq!(value("bytes_written") as u64, files[0].1);
    assert_eq!(value("files_written"), 1);
    Ok(())
}

#[tokio::test]
async fn test_page_row_limit_is_checked_within_the_limit() -> Result<()> {
    // The page limits of a column without levels and of a column with nulls
    // are only checked each time a number of rows has been written.
    // The last column can have nulls and has none.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let batch = |rows: i64| -> Result<RecordBatch> {
        let a = Int64Array::from_iter_values(0..rows);
        let b: Int64Array = (0..rows).map(|x| (x % 7 != 0).then_some(x)).collect();
        let c = Int64Array::from_iter_values(0..rows);
        Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )?)
    };
    // A limit of zero means that the rows of a page are not limited. The pages
    // are limited by their size then, which is checked every 1024 rows.
    // The size limit of the last cases is the size of 1024 values.
    let mut files = vec![];
    for (limit, size_limit, rows, pages) in [
        (100, None, 5000, [50, 50, 50]),
        (1, None, 3, [3, 3, 3]),
        (0, None, 3, [1, 1, 1]),
        (0, None, 5000, [1, 1, 1]),
        (20000, None, 5000, [1, 1, 1]),
        (0, Some(8192), 5000, [5, 3, 5]),
        (1_000_000, Some(8192), 5000, [5, 3, 5]),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.options.global.data_page_row_count_limit = limit;
        if let Some(size_limit) = size_limit {
            task.options.global.data_pagesize_limit = size_limit;
            task.options.global.dictionary_enabled = Some(false);
        }
        run_task(task, &schema, vec![batch(rows)?]).await?;
        let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
        files.push(data.clone());
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            data,
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
        )?;
        let metadata = Arc::clone(builder.metadata());
        assert_eq!(metadata.num_row_groups(), 1);
        let index = metadata
            .offset_index()
            .ok_or_else(|| internal_datafusion_err!("missing offset index"))?;
        let actual: Vec<usize> = index[0]
            .iter()
            .map(|column| column.page_locations().len())
            .collect();
        assert_eq!(actual, pages, "limit {limit} {size_limit:?}");
        let batches = builder.build()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(concat_batches(&schema, &batches)?, batch(rows)?);
    }
    // A limit of zero writes the same file as a limit that is not reached.
    assert!(files[3] == files[4]);
    assert!(files[5] == files[6]);
    Ok(())
}

#[tokio::test]
async fn test_options_reach_file_metadata() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..10)),
            Arc::new(Int64Array::from_iter_values((0..10).rev())),
        ],
    )?;
    let ordering = LexOrdering::new([
        PhysicalSortExpr::new(
            Arc::new(Column::new("b", 1)),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0))),
    ])
    .ok_or_else(|| internal_datafusion_err!("empty ordering"))?;

    let mut task = test_task(Arc::clone(&store), 0);
    task.options.global.created_by = "sail version test".to_string();
    task.options.global.compression = Some("snappy".to_string());
    task.options.global.max_row_group_size = 4;
    task.ordering = Some(ordering);
    task.write_sorting_columns = true;
    run_task(task, &schema, vec![batch.clone()]).await?;
    let name = file_name(0, 0, ".parquet");
    let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
    assert_eq!(data, batch);
    let file = metadata.file_metadata();
    assert_eq!(file.created_by(), Some("sail version test"));
    assert!(
        file.key_value_metadata()
            .is_some_and(|x| x.iter().any(|kv| kv.key == "ARROW:schema"))
    );
    assert_eq!(metadata.num_row_groups(), 3);
    for row_group in metadata.row_groups() {
        assert_eq!(row_group.column(0).compression(), Compression::SNAPPY);
        assert_eq!(
            row_group.sorting_columns(),
            Some(&vec![
                SortingColumn {
                    column_idx: 1,
                    descending: true,
                    nulls_first: false,
                },
                SortingColumn {
                    column_idx: 0,
                    descending: false,
                    nulls_first: true,
                },
            ])
        );
    }

    let mut task = test_task(Arc::clone(&store), 1);
    task.options.global.skip_arrow_metadata = true;
    run_task(task, &schema, vec![batch.clone()]).await?;
    let name = file_name(1, 0, ".parquet");
    let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
    assert_eq!(data, batch);
    let file = metadata.file_metadata();
    assert!(
        !file
            .key_value_metadata()
            .is_some_and(|x| x.iter().any(|kv| kv.key == "ARROW:schema"))
    );
    assert_eq!(metadata.row_group(0).sorting_columns(), None);
    assert_eq!(
        metadata.row_group(0).column(0).compression(),
        Compression::ZSTD(Default::default())
    );
    Ok(())
}

fn cast_values(values: &dyn Array, data_types: &[DataType]) -> Result<Vec<ArrayRef>> {
    data_types
        .iter()
        .map(|data_type| Ok(cast(values, data_type)?))
        .collect()
}

fn type_cases() -> Result<Vec<ArrayRef>> {
    let mut cases: Vec<ArrayRef> = vec![];
    cases.push(Arc::new(NullArray::new(4)));
    cases.push(Arc::new(BooleanArray::from(vec![
        Some(true),
        None,
        Some(false),
        Some(true),
    ])));

    let int32 = Int32Array::from(vec![Some(1), None, Some(3), Some(100)]);
    let int64 = Int64Array::from(vec![Some(1), None, Some(3), Some(100)]);
    cases.extend(cast_values(
        &int64,
        &[
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::Date64,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
        ],
    )?);
    cases.extend(cast_values(
        &int32,
        &[
            DataType::Date32,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
        ],
    )?);
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        cases.extend(cast_values(
            &int64,
            &[
                DataType::Timestamp(unit, None),
                DataType::Timestamp(unit, Some("UTC".into())),
                DataType::Timestamp(unit, Some("+08:00".into())),
                DataType::Timestamp(unit, Some("America/Los_Angeles".into())),
                DataType::Duration(unit),
            ],
        )?);
    }

    let float64 = Float64Array::from(vec![
        Some(1.5),
        None,
        Some(f64::NAN),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ]);
    cases.extend(cast_values(
        &float64,
        &[DataType::Float16, DataType::Float32, DataType::Float64],
    )?);

    let string = StringArray::from(vec![Some("a"), None, Some(""), Some("h\u{e9}llo")]);
    cases.extend(cast_values(
        &string,
        &[
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ],
    )?);
    let binary = BinaryArray::from(vec![Some(&b"ab"[..]), None, Some(b""), Some(b"\x00\xff")]);
    cases.extend(cast_values(
        &binary,
        &[
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ],
    )?);
    cases.push(Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(b"ab"), None, Some(b"cd"), Some(b"\x00\xff")].into_iter(),
            2,
        )?,
    ));

    cases.push(Arc::new(IntervalYearMonthArray::from(vec![
        Some(1),
        None,
        Some(-3),
        Some(100),
    ])));
    cases.push(Arc::new(IntervalDayTimeArray::from(vec![
        Some(IntervalDayTime::new(1, 2)),
        None,
        Some(IntervalDayTime::new(-3, 4)),
        Some(IntervalDayTime::new(100, 0)),
    ])));
    cases.push(Arc::new(IntervalMonthDayNanoArray::from(vec![
        Some(IntervalMonthDayNano::new(1, 2, 3)),
        None,
        Some(IntervalMonthDayNano::new(-3, 4, -5)),
        Some(IntervalMonthDayNano::new(100, 0, 0)),
    ])));

    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3), Some(4)]),
        Some(vec![Some(5), Some(6)]),
    ]);
    let item = Arc::new(Field::new_list_field(DataType::Int64, true));
    cases.extend(cast_values(
        &list,
        &[
            DataType::List(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
        ],
    )?);
    let full_list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), None]),
        Some(vec![Some(2), Some(3)]),
        Some(vec![Some(3), Some(4)]),
        Some(vec![Some(5), Some(6)]),
    ]);
    cases.push(cast(
        &full_list,
        &DataType::FixedSizeList(Arc::clone(&item), 2),
    )?);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)]));
    cases.push(Arc::new(ListViewArray::try_new(
        Arc::clone(&item),
        ScalarBuffer::from(vec![0, 2, 0, 1]),
        ScalarBuffer::from(vec![2, 2, 0, 3]),
        Arc::clone(&values),
        None,
    )?));
    cases.push(Arc::new(LargeListViewArray::try_new(
        Arc::clone(&item),
        ScalarBuffer::from(vec![0_i64, 2, 0, 1]),
        ScalarBuffer::from(vec![2_i64, 2, 0, 3]),
        Arc::clone(&values),
        None,
    )?));

    let inner = StructArray::from(vec![(
        Arc::new(Field::new("x", list.data_type().clone(), true)),
        Arc::new(list.clone()) as ArrayRef,
    )]);
    cases.push(Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int64, true)),
            Arc::new(int64.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", inner.data_type().clone(), true)),
            Arc::new(inner) as ArrayRef,
        ),
    ])));

    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for i in 0..4 {
        if i == 1 {
            map.append(false)?;
            continue;
        }
        map.keys().append_value(format!("k{i}"));
        map.values().append_option((i != 2).then_some(i));
        map.append(true)?;
    }
    cases.push(Arc::new(map.finish()));

    cases.push(Arc::new(RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 4]),
        &StringArray::from(vec![Some("a"), None]),
    )?));

    let fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ],
    )?;
    for (mode, offsets) in [
        (UnionMode::Sparse, None),
        (UnionMode::Dense, Some(ScalarBuffer::from(vec![0, 0, 1, 1]))),
    ] {
        let children: Vec<ArrayRef> = match mode {
            UnionMode::Sparse => vec![Arc::new(int64.clone()), Arc::new(string.clone())],
            UnionMode::Dense => vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        };
        cases.push(Arc::new(UnionArray::try_new(
            fields.clone(),
            ScalarBuffer::from(vec![0_i8, 1, 0, 1]),
            offsets,
            children,
        )?));
    }
    Ok(cases)
}

/// Writes the batch with the Arrow writer and the properties
/// that the writer task is expected to use.
fn reference_file(
    options: &TableParquetOptions,
    batch: &RecordBatch,
) -> Result<Vec<u8>, datafusion::parquet::errors::ParquetError> {
    let mut options = options.clone();
    options.arrow_schema(&batch.schema());
    options.global.write_batch_size = options.global.data_page_row_count_limit.clamp(1, 1024);
    let props = WriterPropertiesBuilder::try_from(&options)
        .map_err(|e| datafusion::parquet::errors::ParquetError::General(e.to_string()))?
        .build();
    let mut writer = ArrowWriter::try_new_with_options(
        vec![],
        batch.schema(),
        ArrowWriterOptions::new().with_properties(props),
    )?;
    writer.write(batch)?;
    writer.into_inner()
}

/// The message of a panic.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    match panic.downcast::<String>() {
        Ok(message) => *message,
        Err(panic) => match panic.downcast::<&str>() {
            Ok(message) => message.to_string(),
            Err(_) => "unknown panic".to_string(),
        },
    }
}

#[tokio::test]
async fn test_arrow_types() -> Result<()> {
    let mut unsupported = vec![];
    for (i, values) in type_cases()?.into_iter().enumerate() {
        let data_type = values.data_type().clone();
        let nullable = !matches!(data_type, DataType::Union(_, _));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c",
            data_type.clone(),
            nullable,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let task = test_task(Arc::clone(&store), i);
        let task_options = task.options.clone();
        let actual = run_task(task, &schema, vec![batch.clone()]).await;
        if matches!(
            data_type,
            DataType::Union(_, _) | DataType::Interval(IntervalUnit::MonthDayNano)
        ) {
            let Err(actual) = actual else {
                return Err(internal_datafusion_err!(
                    "expected an error for {data_type}"
                ));
            };
            assert_eq!(
                actual.strip_backtrace(),
                format!(
                    "This feature is not implemented: Parquet write does not support the data type {data_type} of column c"
                )
            );
            assert_eq!(list_files(store.as_ref()).await?, vec![], "{data_type}");
            unsupported.push(data_type.to_string());
            continue;
        }
        let expected = reference_file(&task_options, &batch);
        match (expected, actual) {
            (Ok(expected), Ok(manifest)) => {
                let name = file_name(i, 0, ".parquet");
                assert_eq!(
                    manifest_rows(&manifest)?,
                    vec![(
                        i as u64,
                        0,
                        Some(name.clone()),
                        expected.len() as u64,
                        batch.num_rows() as u64
                    )],
                    "{data_type}"
                );
                let data = read_bytes(store.as_ref(), &name).await?;
                assert!(data == expected, "{data_type}");
                let (_, data) = read_parquet(data)?;
                assert_eq!(data.num_rows(), batch.num_rows(), "{data_type}");
                if data.schema().field(0).data_type() == &data_type {
                    assert_eq!(data.column(0), batch.column(0), "{data_type}");
                } else {
                    // The reader does not restore the type, so compare the values.
                    let restored = cast(data.column(0), &data_type)?;
                    assert_eq!(&restored, batch.column(0), "{data_type}");
                }
            }
            (expected, actual) => {
                return Err(internal_datafusion_err!(
                    "different outcomes for {data_type}: {:?} and {:?}",
                    expected.map(|x| x.len()),
                    actual.map(|x| x.num_rows())
                ));
            }
        }
    }
    // These are the types that the Arrow writer rejects with an error (interval)
    // or panics for (union).
    assert_eq!(
        unsupported,
        vec![
            "Interval(MonthDayNano)",
            "Union(Sparse, 0: (\"i\": Int64), 1: (\"s\": Utf8))",
            "Union(Dense, 0: (\"i\": Int64), 1: (\"s\": Utf8))",
        ]
    );
    Ok(())
}

/// The rows of each directory, where a row is formatted as text and the rows are sorted.
type DirectoryRows = BTreeMap<String, Vec<String>>;

async fn read_directories(store: &dyn ObjectStore) -> Result<DirectoryRows> {
    let mut directories = DirectoryRows::new();
    for (file, _) in list_files(store).await? {
        let (directory, _) = file
            .rsplit_once('/')
            .ok_or_else(|| internal_datafusion_err!("invalid file path: {file}"))?;
        let (_, data) = read_parquet(read_bytes(store, &file).await?)?;
        let rows = directories.entry(directory.to_string()).or_default();
        for line in pretty_format_batches(&[data])?.to_string().lines().skip(3) {
            if !line.starts_with('+') {
                rows.push(line.to_string());
            }
        }
        rows.sort();
    }
    Ok(directories)
}

fn partition_task(
    store: Arc<dyn ObjectStore>,
    partition: usize,
    partition_columns: &[&str],
) -> ParquetWriteTask {
    let mut task = test_task(store, partition);
    task.partition_columns = partition_columns.iter().map(|x| x.to_string()).collect();
    task
}

/// Writes the batch with the DataFusion Parquet sink, which is how
/// partitioned data was written before the writer task existed.
async fn write_with_data_sink(
    batch: &RecordBatch,
    partition_columns: &[&str],
) -> Result<DirectoryRows> {
    write_batches_with_data_sink(std::slice::from_ref(batch), partition_columns).await
}

async fn write_batches_with_data_sink(
    batches: &[RecordBatch],
    partition_columns: &[&str],
) -> Result<DirectoryRows> {
    let batch = &batches[0];
    let ctx = SessionContext::new();
    let store = Arc::new(InMemory::new());
    let url = ObjectStoreUrl::parse("memory://")?;
    ctx.register_object_store(url.as_ref(), store.clone());
    let sink = ParquetSink::new(
        FileSinkConfig {
            original_url: "memory:///table/".to_string(),
            object_store_url: url,
            file_group: Default::default(),
            table_paths: vec![ListingTableUrl::parse("memory:///table/")?],
            output_schema: batch.schema(),
            table_partition_cols: partition_columns
                .iter()
                .map(|x| (x.to_string(), DataType::Null))
                .collect(),
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
            file_output_mode: FileOutputMode::Automatic,
        },
        TableParquetOptions::default(),
    );
    let input = test_stream(&batch.schema(), batches.to_vec());
    sink.write_all(input, &ctx.task_ctx()).await?;
    read_directories(store.as_ref()).await
}

/// Returns whether each row of a partition column is in the default partition,
/// which has the rows without a value and the rows with the empty string.
fn default_partition_rows(column: &ArrayRef) -> Result<Vec<bool>> {
    let strings = match column.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
            Some(cast(column, &DataType::Utf8)?)
        }
        _ => None,
    };
    let nulls = column.logical_nulls();
    Ok((0..column.len())
        .map(|i| {
            nulls.as_ref().is_some_and(|x| x.is_null(i))
                || strings
                    .as_ref()
                    .is_some_and(|x| x.as_string::<i32>().value(i).is_empty())
        })
        .collect())
}

/// Returns the directories and their rows that are expected for the batches.
///
/// The rows are written with the DataFusion Parquet sink, which names the
/// directories of the values in the same way. The sink has no default partition,
/// so each partition column of a row in the default partition is replaced
/// with a column of strings that has the name of the default partition.
async fn expected_directories(
    batches: &[RecordBatch],
    partition_columns: &[&str],
) -> Result<DirectoryRows> {
    let mut patterns: BTreeMap<Vec<bool>, Vec<RecordBatch>> = BTreeMap::new();
    for batch in batches {
        let defaults = partition_columns
            .iter()
            .map(|name| default_partition_rows(batch.column(batch.schema().index_of(name)?)))
            .collect::<Result<Vec<_>>>()?;
        let mut rows: BTreeMap<Vec<bool>, Vec<u32>> = BTreeMap::new();
        for i in 0..batch.num_rows() {
            let pattern = defaults.iter().map(|x| x[i]).collect();
            rows.entry(pattern).or_default().push(i as u32);
        }
        for (pattern, rows) in rows {
            let part = take_record_batch(batch, &UInt32Array::from(rows))?;
            let mut fields = part.schema().fields().to_vec();
            let mut columns = part.columns().to_vec();
            for (name, is_default) in partition_columns.iter().zip(&pattern) {
                if *is_default {
                    let i = part.schema().index_of(name)?;
                    fields[i] = Arc::new(Field::new(*name, DataType::Utf8, true));
                    columns[i] = Arc::new(StringArray::from(vec![
                        "__HIVE_DEFAULT_PARTITION__";
                        part.num_rows()
                    ]));
                }
            }
            let part = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
            patterns.entry(pattern).or_default().push(part);
        }
    }
    let mut directories = DirectoryRows::new();
    for batches in patterns.values() {
        for (directory, rows) in write_batches_with_data_sink(batches, partition_columns).await? {
            assert!(directories.insert(directory, rows).is_none());
        }
    }
    Ok(directories)
}

/// Writes rows with the partition column `p` and the data column `v`,
/// and returns the directories of the rows. The directories and their rows
/// must be the same as the ones that the DataFusion Parquet sink produces
/// for the rows that are not in the default partition.
async fn partition_directories(values: ArrayRef) -> Result<Vec<String>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", values.data_type().clone(), true),
        Field::new("v", DataType::Int64, true),
    ]));
    let v: ArrayRef = Arc::new(Int64Array::from_iter_values(0..values.len() as i64));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values, v])?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let task = partition_task(Arc::clone(&store), 0, &["p"]);
    run_task(task, &schema, vec![batch.clone()]).await?;
    let actual = read_directories(store.as_ref()).await?;
    assert_eq!(actual, expected_directories(&[batch], &["p"]).await?);
    Ok(actual.into_keys().collect())
}

#[tokio::test]
async fn test_partition_directory_names() -> Result<()> {
    let strings = StringArray::from(vec![
        Some("a"),
        Some(""),
        None,
        Some("a/b"),
        Some("x=y"),
        Some("50%"),
        Some("sp ace"),
        Some("h\u{e9}llo"),
        Some("a"),
    ]);
    // The row without a value and the row with the empty string
    // are in the default partition.
    let expected = vec![
        "table/p=50%25",
        "table/p=__HIVE_DEFAULT_PARTITION__",
        "table/p=a",
        "table/p=a%2Fb",
        "table/p=h%C3%A9llo",
        "table/p=sp ace",
        "table/p=x=y",
    ];
    for data_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        let directories = partition_directories(cast(&strings, &data_type)?).await?;
        assert_eq!(directories, expected, "{data_type}");
    }

    let booleans = BooleanArray::from(vec![Some(true), None, Some(false)]);
    assert_eq!(
        partition_directories(Arc::new(booleans)).await?,
        vec![
            "table/p=__HIVE_DEFAULT_PARTITION__",
            "table/p=false",
            "table/p=true"
        ]
    );

    let integers = Int64Array::from(vec![Some(1), None, Some(0), Some(-3), Some(100), Some(1)]);
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let directories = partition_directories(cast(&integers, &data_type)?).await?;
        assert_eq!(
            directories,
            vec![
                "table/p=-3",
                "table/p=0",
                "table/p=1",
                "table/p=100",
                "table/p=__HIVE_DEFAULT_PARTITION__"
            ],
            "{data_type}"
        );
    }
    let integers = Int64Array::from(vec![Some(1), None, Some(0), Some(100), Some(1)]);
    for data_type in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        let directories = partition_directories(cast(&integers, &data_type)?).await?;
        assert_eq!(
            directories,
            vec![
                "table/p=0",
                "table/p=1",
                "table/p=100",
                "table/p=__HIVE_DEFAULT_PARTITION__"
            ],
            "{data_type}"
        );
    }

    let floats = Float64Array::from(vec![
        Some(1.5),
        None,
        Some(f64::NAN),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(100.0),
    ]);
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let directories = partition_directories(cast(&floats, &data_type)?).await?;
        assert_eq!(
            directories,
            vec![
                "table/p=-0",
                "table/p=-inf",
                "table/p=0",
                "table/p=1.5",
                "table/p=100",
                "table/p=NaN",
                "table/p=__HIVE_DEFAULT_PARTITION__",
                "table/p=inf",
            ],
            "{data_type}"
        );
    }

    let days = Int32Array::from(vec![Some(0), None, Some(19000), Some(-1)]);
    assert_eq!(
        partition_directories(cast(&days, &DataType::Date32)?).await?,
        vec![
            "table/p=1969-12-31",
            "table/p=1970-01-01",
            "table/p=2022-01-08",
            "table/p=__HIVE_DEFAULT_PARTITION__",
        ]
    );
    // The time of day is dropped by a division that rounds towards zero,
    // so the last value is in the directory of the first day.
    let milliseconds = Int64Array::from(vec![Some(0), None, Some(1641600000123), Some(-1)]);
    assert_eq!(
        partition_directories(cast(&milliseconds, &DataType::Date64)?).await?,
        vec![
            "table/p=1970-01-01",
            "table/p=2022-01-08",
            "table/p=__HIVE_DEFAULT_PARTITION__"
        ]
    );
    Ok(())
}

/// Returns the array with the given rows marked as rows without a value.
/// The content of the array at these rows is kept.
fn with_null_rows(array: &ArrayRef, null_rows: impl Fn(usize) -> bool) -> Result<ArrayRef> {
    let nulls: NullBuffer = (0..array.len()).map(|i| !null_rows(i)).collect();
    let data = array.to_data().into_builder().nulls(Some(nulls)).build()?;
    Ok(make_array(data))
}

/// Returns an array with the default value of the type, which names the directory
/// of the rows without a value.
fn default_partition_value(data_type: &DataType) -> Result<ArrayRef> {
    let value: ArrayRef = match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
            Arc::new(StringArray::from(vec![""]))
        }
        DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            Arc::new(Float64Array::from(vec![0.0]))
        }
        DataType::Date32 => Arc::new(Int32Array::from(vec![0])),
        _ => Arc::new(Int64Array::from(vec![0])),
    };
    Ok(cast(&value, data_type)?)
}

/// Returns the array with the default value of its type in the given rows.
fn with_default_rows(array: &ArrayRef, default_rows: impl Fn(usize) -> bool) -> Result<ArrayRef> {
    let rows = array.len() as u32;
    let default = default_partition_value(array.data_type())?;
    let values = concat(&[array.as_ref(), default.as_ref()])?;
    let indices = UInt32Array::from_iter_values(
        (0..rows).map(|i| if default_rows(i as usize) { rows } else { i }),
    );
    Ok(take(&values, &indices, None)?)
}

/// Arrays of each type that a partition column can have, with values that
/// are repeated and that are not next to each other.
fn partition_value_cases() -> Result<Vec<(String, ArrayRef)>> {
    let mut cases: Vec<(String, ArrayRef)> = vec![];
    let mut add = |name: &str, array: ArrayRef| cases.push((name.to_string(), array));

    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        "a",
        "b",
        "a",
        "",
        "a/b",
        "a value that is not stored in the view",
        "b",
        "x=y",
        "a value that is not stored in the view",
        "",
        "a",
    ]));
    for data_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
    ] {
        add(&data_type.to_string(), cast(&strings, &data_type)?);
    }
    add(
        "Boolean",
        Arc::new(BooleanArray::from(vec![
            true, false, false, true, true, false, true,
        ])),
    );
    let integers: ArrayRef = Arc::new(Int64Array::from(vec![
        1, 0, -3, 100, 1, -128, 127, 0, -3, 1, 100,
    ]));
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        add(&data_type.to_string(), cast(&integers, &data_type)?);
    }
    let integers: ArrayRef = Arc::new(Int64Array::from(vec![
        1, 0, 3, 100, 1, 255, 0, 3, 1, 100, 255,
    ]));
    for data_type in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        add(&data_type.to_string(), cast(&integers, &data_type)?);
    }
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        0.5,
        1.0,
        f64::INFINITY,
        f64::NAN,
        0.0,
        -0.0,
        f64::NEG_INFINITY,
        -139.25,
        1.0,
        0.5,
        f64::INFINITY,
        -0.0,
        0.0,
        f64::NAN,
        285.5,
    ]));
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        add(&data_type.to_string(), cast(&floats, &data_type)?);
    }
    // Values that are not a number and that differ in their bits, among other values.
    let bits: [u16; 9] = [
        0x7e00, 0x3c00, 0x7e01, 0xfe00, 0x7c00, 0x7dff, 0x3c00, 0x7e00, 0xfc00,
    ];
    add(
        "Float16 bits",
        Arc::new(PrimitiveArray::<Float16Type>::from_iter_values(
            bits.iter()
                .map(|x| <Float16Type as ArrowPrimitiveType>::Native::from_bits(*x)),
        )),
    );
    let bits: [u32; 9] = [
        0x7fc0_0000,
        0x3f80_0000,
        0x7fc0_0001,
        0xffc0_0000,
        0x7f80_0000,
        0x7fbf_ffff,
        0x3f80_0000,
        0x7fc0_0000,
        0xff80_0000,
    ];
    add(
        "Float32 bits",
        Arc::new(PrimitiveArray::<Float32Type>::from_iter_values(
            bits.iter().map(|x| f32::from_bits(*x)),
        )),
    );
    let bits: [u64; 9] = [
        0x7ff8_0000_0000_0000,
        0x3ff0_0000_0000_0000,
        0x7ff8_0000_0000_0001,
        0xfff8_0000_0000_0000,
        0x7ff0_0000_0000_0000,
        0x7ff7_ffff_ffff_ffff,
        0x3ff0_0000_0000_0000,
        0x7ff8_0000_0000_0000,
        0xfff0_0000_0000_0000,
    ];
    add(
        "Float64 bits",
        Arc::new(Float64Array::from_iter_values(
            bits.iter().map(|x| f64::from_bits(*x)),
        )),
    );
    let days: ArrayRef = Arc::new(Int32Array::from(vec![0, 19000, -1, 0, 1, 19000, -1, 2, 0]));
    add("Date32", cast(&days, &DataType::Date32)?);
    // Some of the values are on the same day.
    let milliseconds: ArrayRef = Arc::new(Int64Array::from(vec![
        0,
        1641600000123,
        86400000,
        1,
        1641600000000,
        -86400000,
        86400001,
        0,
    ]));
    add("Date64", cast(&milliseconds, &DataType::Date64)?);
    Ok(cases)
}

/// Writes the batches with the partition column `p`, and returns the rows of each directory.
async fn write_partitioned(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<DirectoryRows> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let task = partition_task(Arc::clone(&store), 0, &["p"]);
    run_task(task, schema, batches.to_vec()).await?;
    read_directories(store.as_ref()).await
}

#[tokio::test]
async fn test_partition_rows_match_data_sink() -> Result<()> {
    for (name, values) in partition_value_cases()? {
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", values.data_type().clone(), true),
            Field::new("v", DataType::Int64, true),
        ]));
        let rows = values.len();
        let reversed = UInt32Array::from_iter_values((0..rows as u32).rev());
        type NullRows = fn(usize) -> bool;
        let null_cases: [(&str, NullRows); 4] = [
            ("no row", |_| false),
            ("some rows", |i| i % 3 == 1),
            ("first rows", |i| i < 5),
            ("every row", |_| true),
        ];
        for (null_rows_name, null_rows) in null_cases {
            let message = format!("{name}, {null_rows_name} without a value");
            // The rows without a value have the content of other rows of the array,
            // as in arrays that are read from Parquet files, or the default value,
            // or they are rows with the default value.
            let mut leftover = vec![];
            let mut zeroed = vec![];
            let mut filled = vec![];
            let mut null_ids = vec![];
            // The second batch has the values in another order,
            // and the last batch has a single row.
            for (i, values) in [
                Arc::clone(&values),
                take(&values, &reversed, None)?,
                values.slice(2, 1),
            ]
            .into_iter()
            .enumerate()
            {
                let start = (i * rows) as i64;
                let v: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    start..start + values.len() as i64,
                ));
                null_ids.extend(
                    (0..values.len())
                        .filter(|x| null_rows(*x))
                        .map(|x| start + x as i64),
                );
                let with_default = with_default_rows(&values, null_rows)?;
                for (batches, values) in [
                    (&mut leftover, with_null_rows(&values, null_rows)?),
                    (&mut zeroed, with_null_rows(&with_default, null_rows)?),
                    (&mut filled, with_default),
                ] {
                    batches.push(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![values, Arc::clone(&v)],
                    )?);
                }
            }
            // A row without a value is written to the default partition,
            // whatever the content of the array at the row is.
            let expected = expected_directories(&leftover, &["p"]).await?;
            assert_eq!(
                write_partitioned(&schema, &leftover).await?,
                expected,
                "{message}"
            );
            assert_eq!(
                write_partitioned(&schema, &zeroed).await?,
                expected,
                "{message}"
            );
            // A row with the default value of the type is written to the directory
            // of that value, which is the default partition only for the empty string.
            assert_eq!(
                write_partitioned(&schema, &filled).await?,
                expected_directories(&filled, &["p"]).await?,
                "{message}"
            );
            let default_rows = expected
                .get("table/p=__HIVE_DEFAULT_PARTITION__")
                .cloned()
                .unwrap_or_default();
            for id in null_ids {
                assert!(
                    default_rows
                        .iter()
                        .any(|row| row.trim_matches(['|', ' ']) == id.to_string()),
                    "{message}: row {id} is not in the default partition"
                );
            }
            assert!(
                expected.len() >= 2 || null_rows_name == "every row",
                "{message}"
            );
            assert_eq!(
                expected.values().map(|x| x.len()).sum::<usize>(),
                2 * rows + 1,
                "{message}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_partition_rows_match_data_sink_for_several_columns() -> Result<()> {
    let cases = partition_value_cases()?;
    let column = |name: &str| -> Result<ArrayRef> {
        cases
            .iter()
            .find(|(x, _)| x == name)
            .map(|(_, values)| values.slice(0, 7))
            .ok_or_else(|| internal_datafusion_err!("missing case {name}"))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Float16, true),
        Field::new("v", DataType::Int64, true),
        Field::new("q", DataType::Utf8View, true),
        Field::new("r", DataType::Boolean, true),
    ]));
    let v: ArrayRef = Arc::new(Int64Array::from_iter_values(0..7));
    for null_rows in [[7, 7], [1, 7], [7, 2], [3, 3], [1, 5]] {
        let p_rows = |i: usize| i.is_multiple_of(null_rows[0]);
        let q_rows = |i: usize| i % null_rows[1] == 1;
        let batch = |p: ArrayRef, q: ArrayRef| -> Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![p, Arc::clone(&v), q, column("Boolean")?],
            )?)
        };
        // The rows without a value have the content of other rows.
        let leftover = batch(
            with_null_rows(&column("Float16")?, p_rows)?,
            with_null_rows(&column("Utf8View")?, q_rows)?,
        )?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let task = partition_task(Arc::clone(&store), 0, &["r", "p", "q"]);
        run_task(task, &schema, vec![leftover.clone(), leftover.clone()]).await?;
        let actual = read_directories(store.as_ref()).await?;
        let expected =
            expected_directories(&[leftover.clone(), leftover], &["r", "p", "q"]).await?;
        assert_eq!(actual, expected, "{null_rows:?}");
        assert!(actual.len() >= 3, "{null_rows:?}");
    }
    Ok(())
}

#[tokio::test]
async fn test_partition_column_type_not_supported() -> Result<()> {
    let integers = Int64Array::from(vec![Some(1), None]);
    for data_type in [
        DataType::Decimal128(10, 2),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Binary,
    ] {
        let values = match &data_type {
            DataType::Binary => Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), None])),
            _ => cast(&integers, &data_type)?,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", data_type.clone(), true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![values, Arc::new(integers.clone())],
        )?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let task = partition_task(Arc::clone(&store), 0, &["p"]);
        let actual = run_task(task, &schema, vec![batch.clone()]).await;
        let expected = write_with_data_sink(&batch, &["p"]).await;
        let (Err(actual), Err(expected)) = (actual, expected) else {
            return Err(internal_datafusion_err!("expected errors for {data_type}"));
        };
        assert_eq!(actual.strip_backtrace(), expected.strip_backtrace());
        assert!(
            actual.to_string().contains(&format!(
                "it is not yet supported to write to hive partitions with datatype {data_type}"
            )),
            "{actual}"
        );
        assert_eq!(list_files(store.as_ref()).await?, vec![]);
    }
    Ok(())
}

fn partitioned_schema() -> SchemaRef {
    Arc::new(
        Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("a", DataType::Int32, true),
            Field::new("w", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ])
        .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
    )
}

fn partitioned_batch(rows: &[(i64, i32, &str)]) -> Result<RecordBatch> {
    let v: Int64Array = rows.iter().map(|x| Some(x.0)).collect();
    let a: Int32Array = rows.iter().map(|x| Some(x.1)).collect();
    let w: StringArray = rows.iter().map(|x| Some(format!("w{}", x.0))).collect();
    let b: StringArray = rows.iter().map(|x| Some(x.2)).collect();
    Ok(RecordBatch::try_new(
        partitioned_schema(),
        vec![Arc::new(v), Arc::new(a), Arc::new(w), Arc::new(b)],
    )?)
}

#[tokio::test]
async fn test_partition_columns_are_not_written() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let batches = vec![
        partitioned_batch(&[(1, 1, "x"), (2, 2, "x"), (3, 1, "y"), (4, 1, "x")])?,
        partitioned_batch(&[(5, 1, "x"), (6, 2, "y")])?,
    ];
    // The directories follow the order of the partition columns of the task.
    let task = partition_task(Arc::clone(&store), 4, &["b", "a"]);
    let manifest = run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    let name = |directory: &str| format!("table/{directory}/part-00004-{WRITE_ID}-c000.parquet");
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            name("b=x/a=1"),
            name("b=x/a=2"),
            name("b=y/a=1"),
            name("b=y/a=2"),
        ]
    );
    let mut rows = manifest_rows(&manifest)?;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (4, 0, Some(name("b=x/a=1")), files[0].1, 3),
            (4, 0, Some(name("b=x/a=2")), files[1].1, 1),
            (4, 0, Some(name("b=y/a=1")), files[2].1, 1),
            (4, 0, Some(name("b=y/a=2")), files[3].1, 1),
        ]
    );
    let file_schema = Arc::new(schema.project(&[0, 2])?);
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &name("b=x/a=1")).await?)?;
    assert_eq!(data.schema(), file_schema);
    assert_eq!(
        data,
        partitioned_batch(&[(1, 1, "x"), (4, 1, "x"), (5, 1, "x")])?.project(&[0, 2])?
    );
    Ok(())
}

#[tokio::test]
async fn test_partition_single_value_batch() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let batch = partitioned_batch(&[(1, 7, "x"), (2, 7, "x"), (3, 7, "x")])?;
    let task = partition_task(Arc::clone(&store), 0, &["a", "b"]);
    let manifest = run_task(task, &schema, vec![batch.clone()]).await?;
    let name = format!("table/a=7/b=x/part-00000-{WRITE_ID}-c000.parquet");
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![(0, 0, Some(name.clone()), files[0].1, 3)]
    );
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
    assert_eq!(data, batch.project(&[0, 2])?);
    Ok(())
}

/// The partition values that the writer task puts in the directory names
/// are the values that the listing reads from the directory names.
#[tokio::test]
async fn test_partition_values_are_read_back_from_directories() -> Result<()> {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some(""),
        Some("a/b"),
        Some("x=y"),
        Some("50%"),
        Some("sp ace"),
        Some("h\u{e9}llo"),
        Some("it's: {a}[b]^c\\d"),
        Some("a"),
    ]));
    let integers: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        None,
        Some(1),
        Some(-3),
        None,
        Some(0),
    ]));
    let mut cases: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
        cast(
            &Int32Array::from(vec![Some(0), None, Some(19000), Some(-1)]),
            &DataType::Date32,
        )?,
        Arc::new(Float64Array::from(vec![Some(0.0), None, Some(1.5)])),
    ];
    for data_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        cases.push(cast(&strings, &data_type)?);
    }
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        cases.push(cast(&integers, &data_type)?);
    }
    for values in cases {
        let data_type = values.data_type().clone();
        // The values of a dictionary are read as strings.
        let (read_type, values_read) = match &data_type {
            DataType::Dictionary(_, value_type) => {
                (value_type.as_ref().clone(), cast(&values, value_type)?)
            }
            _ => (data_type.clone(), Arc::clone(&values)),
        };
        let is_default = default_partition_rows(&values)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", data_type.clone(), true),
            Field::new("v", DataType::Int64, true),
        ]));
        let v: ArrayRef = Arc::new(Int64Array::from_iter_values(0..values.len() as i64));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values, v])?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let task = partition_task(Arc::clone(&store), 0, &["p"]);
        run_task(task, &schema, vec![batch.clone()]).await?;

        let state = SessionContext::new().state();
        let files: Vec<_> = pruned_partition_list(
            &state,
            store.as_ref(),
            &ListingTableUrl::parse("memory:///table/")?,
            &[],
            "",
            &[("p".to_string(), read_type.clone())],
        )
        .await?
        .try_collect()
        .await?;
        let mut actual = BTreeMap::new();
        for file in files {
            let data = read_bytes(store.as_ref(), file.object_meta.location.as_ref()).await?;
            let (_, data) = read_parquet(data)?;
            for id in data.column(0).as_primitive::<Int64Type>().values() {
                assert!(
                    actual
                        .insert(*id as usize, file.partition_values[0].clone())
                        .is_none()
                );
            }
        }
        let expected: BTreeMap<_, _> = (0..batch.num_rows())
            .map(|i| {
                let value = match is_default[i] {
                    true => ScalarValue::try_new_null(&read_type)?,
                    false => ScalarValue::try_from_array(&values_read, i)?,
                };
                Ok((i, value))
            })
            .collect::<Result<_>>()?;
        assert_eq!(actual, expected, "{data_type}");
    }
    Ok(())
}

#[tokio::test]
async fn test_partition_null_and_empty_string_are_in_default_partition() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Int32, true),
        Field::new("q", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(1),
                None,
                None,
                Some(0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some(""),
                None,
                None,
                Some("a"),
                Some(""),
            ])),
            Arc::new(Int64Array::from_iter_values(0..6)),
        ],
    )?;
    let task = partition_task(Arc::clone(&store), 0, &["p", "q"]);
    run_task(task, &schema, vec![batch.clone(), batch]).await?;
    let mut rows = vec![];
    for (file, _) in list_files(store.as_ref()).await? {
        let (_, data) = read_parquet(read_bytes(store.as_ref(), &file).await?)?;
        assert_eq!(data.num_columns(), 1);
        let mut values = data.column(0).as_primitive::<Int64Type>().values().to_vec();
        values.sort();
        rows.push((file, values));
    }
    // The rows without a value are not in the directory of the value 0, and the rows
    // without a value and with the empty string are in the same file.
    let default = "__HIVE_DEFAULT_PARTITION__";
    let name = format!("part-00000-{WRITE_ID}-c000.parquet");
    assert_eq!(
        rows,
        vec![
            (format!("table/p=0/q={default}/{name}"), vec![5, 5]),
            (format!("table/p=0/q=a/{name}"), vec![0, 0]),
            (format!("table/p=1/q={default}/{name}"), vec![2, 2]),
            (
                format!("table/p={default}/q={default}/{name}"),
                vec![1, 1, 3, 3]
            ),
            (format!("table/p={default}/q=a/{name}"), vec![4, 4]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_partition_open_file_limit() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let batches = vec![
        partitioned_batch(&[(1, 1, "x")])?,
        partitioned_batch(&[(2, 2, "x")])?,
        partitioned_batch(&[(3, 1, "x")])?,
        // The rows of `a=3` and `a=4` are sorted, since two directories are open.
        partitioned_batch(&[(4, 3, "x")])?,
        partitioned_batch(&[(5, 1, "x"), (8, 4, "x")])?,
        partitioned_batch(&[(6, 2, "x"), (7, 1, "x"), (9, 3, "x")])?,
    ];
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.max_open_files = 2;
    let manifest = run_task(task, &schema, batches.clone()).await?;
    let files = list_files(store.as_ref()).await?;
    // No file is completed to make room for another one, so each directory has one file.
    let name = |a: i32| format!("table/a={a}/part-00000-{WRITE_ID}-c000.parquet");
    let mut rows = manifest_rows(&manifest)?;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (0, 0, Some(name(1)), files[0].1, 4),
            (0, 0, Some(name(2)), files[1].1, 2),
            (0, 0, Some(name(3)), files[2].1, 2),
            (0, 0, Some(name(4)), files[3].1, 1),
        ]
    );
    assert_eq!(files.len(), 4);
    let read = |name: String| {
        let store = Arc::clone(&store);
        async move {
            let (_, data) = read_parquet(read_bytes(store.as_ref(), &name).await?)?;
            let values = datafusion_common::cast::as_int64_array(data.column(0))?;
            Ok::<_, DataFusionError>(values.values().to_vec())
        }
    };
    assert_eq!(read(name(1)).await?, vec![1, 3, 5, 7]);
    assert_eq!(read(name(2)).await?, vec![2, 6]);
    assert_eq!(read(name(3)).await?, vec![4, 9]);
    assert_eq!(read(name(4)).await?, vec![8]);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = partition_task(store, 0, &["a"]);
    task.max_open_files = 2;
    assert_eq!(
        run_task_for_metric(task, &schema, batches, "rows_sorted").await?,
        3
    );
    Ok(())
}

/// A batch with the partition column `p`, which has the given keys, and the column `v`,
/// which numbers the rows from the given value on.
fn keyed_batch(keys: &[Option<i64>], start: i64) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let p: Int64Array = keys.iter().copied().collect();
    let v = Int64Array::from_iter_values(start..start + keys.len() as i64);
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(p), Arc::new(v)],
    )?)
}

/// Batches of the given number of rows, with the keys that the function returns for the rows.
fn keyed_batches(
    rows: usize,
    batch_size: usize,
    key: impl Fn(i64) -> Option<i64>,
) -> Result<Vec<RecordBatch>> {
    (0..rows)
        .step_by(batch_size)
        .map(|start| {
            let end = (start + batch_size).min(rows);
            let keys: Vec<_> = (start..end).map(|i| key(i as i64)).collect();
            keyed_batch(&keys, start as i64)
        })
        .collect()
}

/// Checks that the store has one file for each key, in the directory of the key,
/// and that the files have the rows of the batches.
async fn check_one_file_per_key(store: &dyn ObjectStore, batches: &[RecordBatch]) -> Result<()> {
    let mut expected: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for batch in batches {
        let p = batch.column(0).as_primitive::<Int64Type>();
        let v = batch.column(1).as_primitive::<Int64Type>();
        for (p, v) in p.iter().zip(v.values()) {
            let directory = match p {
                Some(p) => format!("table/p={p}"),
                None => "table/p=__HIVE_DEFAULT_PARTITION__".to_string(),
            };
            expected.entry(directory).or_default().push(*v);
        }
    }
    let mut actual: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    let files = list_files(store).await?;
    let files: Vec<_> = files
        .into_iter()
        .filter(|(file, _)| !file.ends_with("_SUCCESS"))
        .collect();
    for (file, _) in &files {
        let (directory, _) = file
            .rsplit_once('/')
            .ok_or_else(|| internal_datafusion_err!("invalid file path: {file}"))?;
        let (_, data) = read_parquet(read_bytes(store, file).await?)?;
        assert_eq!(data.num_columns(), 1);
        let rows = actual.entry(directory.to_string()).or_default();
        rows.extend(data.column(0).as_primitive::<Int64Type>().values());
    }
    assert_eq!(files.len(), expected.len());
    for rows in actual.values_mut().chain(expected.values_mut()) {
        rows.sort();
    }
    assert!(actual == expected);
    Ok(())
}

#[tokio::test]
async fn test_partition_writes_one_file_per_key() -> Result<()> {
    // The keys of the rows in the order of the rows, the number of directories
    // that can be open, and the number of rows that are sorted.
    type Case = (&'static str, Box<dyn Fn(i64) -> Option<i64>>, usize, usize);
    let rows = 20_000;
    let cases: Vec<Case> = vec![
        // Every key has an open directory.
        (
            "fewer keys than the limit",
            Box::new(|i| Some(i * 7919 % 50)),
            100,
            0,
        ),
        (
            "as many keys as the limit",
            Box::new(|i| Some(i * 7919 % 100)),
            100,
            0,
        ),
        // The keys 0, 7919 % 1000, ... of the first rows have the open directories.
        (
            "more keys than the limit",
            Box::new(|i| Some(i * 7919 % 1000)),
            100,
            18_000,
        ),
        (
            "one open directory",
            Box::new(|i| Some(i * 7919 % 1000)),
            1,
            19_980,
        ),
        // A tenth of the rows has one of 990 other keys than the 9 keys of most rows.
        (
            "skewed keys",
            Box::new(|i| match i % 10 {
                0 => Some(10 + (i / 10) * 7919 % 990),
                _ => Some(i % 10),
            }),
            20,
            // The first rows of the other keys open 11 directories, which get 3 rows each.
            2_000 - 11 * 3,
        ),
        (
            "keys without a value",
            Box::new(|i| match i * 7919 % 300 {
                0..100 => None,
                key => Some(key),
            }),
            50,
            // The rows without a value have one of the 50 directories that are open.
            10_052,
        ),
    ];
    for (name, key, max_open_files, rows_sorted) in cases {
        let batches = keyed_batches(rows, 1024, key)?;
        let schema = batches[0].schema();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = partition_task(Arc::clone(&store), 0, &["p"]);
        task.max_open_files = max_open_files;
        let sorted = run_task_for_metric(task, &schema, batches.clone(), "rows_sorted").await?;
        check_one_file_per_key(store.as_ref(), &batches).await?;
        assert_eq!(sorted, rows_sorted, "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn test_partition_clustered_input_is_not_sorted() -> Result<()> {
    // The rows of each key are next to each other, also the rows without a key,
    // which are the last rows.
    let rows = 20_000;
    let batches = keyed_batches(rows, 1024, |i| Some(i / 20).filter(|x| *x < 990))?;
    let schema = batches[0].schema();
    let ordering = |columns: &[(&str, usize)]| {
        LexOrdering::new(columns.iter().map(|(name, index)| {
            PhysicalSortExpr::new(
                Arc::new(Column::new(name, *index)),
                SortOptions::new(false, false),
            )
        }))
    };
    for (ordering, rows_sorted) in [
        (ordering(&[("p", 0)]), 0),
        (ordering(&[("p", 0), ("v", 1)]), 0),
        // An ordering that does not start with the partition column
        // does not put the rows of a key next to each other.
        (ordering(&[("v", 1), ("p", 0)]), 19_980),
        (None, 19_980),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = partition_task(Arc::clone(&store), 0, &["p"]);
        task.max_open_files = 1;
        task.ordering = ordering.clone();
        let sorted = run_task_for_metric(task, &schema, batches.clone(), "rows_sorted").await?;
        check_one_file_per_key(store.as_ref(), &batches).await?;
        assert_eq!(sorted, rows_sorted, "{ordering:?}");
    }
    Ok(())
}

#[tokio::test]
async fn test_partition_clustered_input_with_repeated_key() -> Result<()> {
    // The empty string and the missing value have the same directory, and other
    // values are between them in the order of the input.
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("a"),
                Some("a"),
                Some("b"),
                None,
            ])),
            Arc::new(Int64Array::from_iter_values(0..5)),
        ],
    )?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = partition_task(Arc::clone(&store), 0, &["p"]);
    task.ordering = LexOrdering::new([PhysicalSortExpr::new(
        Arc::new(Column::new("p", 0)),
        SortOptions::new(false, false),
    )]);
    run_task(task, &schema, vec![batch]).await?;
    let mut rows = vec![];
    for (file, _) in list_files(store.as_ref()).await? {
        let (_, data) = read_parquet(read_bytes(store.as_ref(), &file).await?)?;
        let values = data.column(0).as_primitive::<Int64Type>().values().to_vec();
        rows.push((file, values));
    }
    // The second file of the directory does not replace the first file.
    let name = |directory: &str, counter: usize| {
        format!("table/p={directory}/part-00000-{WRITE_ID}-c{counter:03}.parquet")
    };
    assert_eq!(
        rows,
        vec![
            (name("__HIVE_DEFAULT_PARTITION__", 0), vec![0]),
            (name("__HIVE_DEFAULT_PARTITION__", 1), vec![4]),
            (name("a", 0), vec![1, 2]),
            (name("b", 0), vec![3]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_partition_sorted_keys_keep_input_ordering() -> Result<()> {
    // The input is sorted by the column `v` in descending order, and the rows
    // of all keys but the first one are sorted by their key before they are written.
    let rows = 5_000_i64;
    let p: Int64Array = (0..rows).map(|i| Some(i * 31 % 40)).collect();
    let v: Int64Array = (0..rows).map(|i| Some(rows - i)).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(p), Arc::new(v)])?;
    let batches: Vec<_> = (0..5).map(|i| batch.slice(i * 1000, 1000)).collect();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = partition_task(Arc::clone(&store), 0, &["p"]);
    task.max_open_files = 1;
    task.max_records_per_file = 50;
    task.ordering = LexOrdering::new([PhysicalSortExpr::new(
        Arc::new(Column::new("v", 1)),
        SortOptions::new(true, false),
    )]);
    task.write_sorting_columns = true;
    run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    // Each directory has 125 rows in files of at most 50 rows.
    assert_eq!(files.len(), 40 * 3);
    let mut last: HashMap<String, i64> = HashMap::new();
    let mut total = 0;
    for (file, _) in files {
        let (directory, _) = file
            .rsplit_once('/')
            .ok_or_else(|| internal_datafusion_err!("invalid file path: {file}"))?;
        let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &file).await?)?;
        assert_eq!(
            metadata.row_group(0).sorting_columns(),
            Some(&vec![SortingColumn {
                column_idx: 0,
                descending: true,
                nulls_first: false,
            }])
        );
        // The files of a directory are listed in the order in which they were written.
        for v in data.column(0).as_primitive::<Int64Type>().values() {
            let last = last.entry(directory.to_string()).or_insert(i64::MAX);
            assert!(*v < *last, "{file}");
            *last = *v;
            total += 1;
        }
    }
    assert_eq!(total, rows);
    Ok(())
}

#[tokio::test]
async fn test_partition_sorted_keys_use_temporary_files() -> Result<()> {
    // The rows that are sorted do not fit in the memory pool of the task.
    let rows = 400_000;
    let batches = keyed_batches(rows, 8192, |i| Some(i * 7919 % 2000))?;
    let schema = batches[0].schema();
    let size: usize = batches.iter().map(|x| x.get_array_memory_size()).sum();
    let limit = 2 * 1024 * 1024;
    assert!(size > 2 * limit);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(limit)))
        .build_arc()?;
    let config = SessionConfig::new()
        .with_sort_spill_reservation_bytes(64 * 1024)
        .with_sort_in_place_threshold_bytes(0);
    let context = TaskContext::default()
        .with_session_config(config)
        .with_runtime(Arc::clone(&runtime));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = partition_task(Arc::clone(&store), 0, &["p"]);
    task.max_open_files = 10;
    task.context = Arc::new(context);
    let sorted = run_task_for_metric(task, &schema, batches.clone(), "rows_sorted").await?;
    assert_eq!(sorted, rows - 10 * 200);
    check_one_file_per_key(store.as_ref(), &batches).await?;
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}

/// Returns the plan that writes the input with the partition column `p`, after
/// the distribution that the writer requires has been added to the plan.
fn partitioned_write_plan(
    input: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let writer = ParquetWriteExec::try_new(
        input,
        ObjectStoreUrl::parse("memory://")?,
        Path::from("table"),
        WRITE_ID,
        vec!["p".to_string()],
        TableParquetOptions::default(),
        ".parquet".to_string(),
        None,
        false,
        0,
        NonZeroUsize::new(100).ok_or_else(|| internal_datafusion_err!("zero"))?,
    )?;
    let mut config = ConfigOptions::default();
    config.execution.target_partitions = target_partitions;
    EnsureRequirements::new().optimize(commit_exec(Arc::new(writer))?, &config)
}

/// Returns the input of the writer in the plan.
fn writer_input(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plan = plan;
    loop {
        if let Some(writer) = plan.downcast_ref::<ParquetWriteExec>() {
            return Ok(Arc::clone(writer.input()));
        }
        match plan.children().as_slice() {
            [child] => plan = child,
            _ => return Err(internal_datafusion_err!("missing writer in the plan")),
        }
    }
}

#[test]
fn test_write_plan_distributes_rows_by_partition_columns() -> Result<()> {
    let schema = keyed_batch(&[], 0)?.schema();
    let source = |partitions: usize| -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = vec![vec![]; partitions];
        Ok(MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&schema),
            None,
        )?)
    };
    let p: Arc<dyn PhysicalExpr> = Arc::new(Column::new("p", 0));
    let hash = |partitions: usize| Partitioning::Hash(vec![Arc::clone(&p)], partitions);

    // The rows of each key are written by one task, whatever the partitions of the input are.
    for input in [
        source(4)?,
        source(32)?,
        Arc::new(RepartitionExec::try_new(
            source(4)?,
            Partitioning::RoundRobinBatch(16),
        )?),
        Arc::new(RepartitionExec::try_new(source(2)?, hash(4))?),
    ] {
        let input = writer_input(&partitioned_write_plan(input, 8)?)?;
        let Some(repartition) = input.downcast_ref::<RepartitionExec>() else {
            return Err(internal_datafusion_err!("missing repartition"));
        };
        assert_eq!(repartition.partitioning(), &hash(8));
        // The plan has no other repartition by the partition columns below.
        assert!(
            !repartition.input().is::<RepartitionExec>()
                || !matches!(
                    repartition.input().output_partitioning(),
                    Partitioning::Hash(_, _)
                )
        );
    }

    // A single task writes one file for each key without a repartition.
    let input = writer_input(&partitioned_write_plan(source(1)?, 8)?)?;
    assert!(input.is::<DataSourceExec>());

    // The partitions that the query asks for are kept.
    for partitioning in [
        Partitioning::RoundRobinBatch(3),
        Partitioning::UnknownPartitioning(1),
        Partitioning::UnknownPartitioning(2),
        hash(3),
    ] {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            source(4)?,
            partitioning.clone(),
        ));
        let input = writer_input(&partitioned_write_plan(Arc::clone(&explicit), 8)?)?;
        assert!(input.is::<ExplicitRepartitionExec>(), "{partitioning}");
        assert_eq!(
            input.output_partitioning().partition_count(),
            partitioning.partition_count()
        );
        // The partitions are also kept below a plan that keeps the partitions of its input.
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: Arc::new(Column::new("p", 0)),
                    alias: "p".to_string(),
                },
                ProjectionExpr {
                    expr: Arc::new(Column::new("v", 1)),
                    alias: "v".to_string(),
                },
            ],
            explicit,
        )?);
        let input = writer_input(&partitioned_write_plan(projection, 8)?)?;
        assert!(input.is::<ProjectionExec>(), "{partitioning}");
        assert_eq!(
            input.output_partitioning().partition_count(),
            partitioning.partition_count()
        );
    }

    // A write without partition columns keeps the partitions of its input.
    let writer = write_exec(&[vec![], vec![], vec![]], &schema, None, false)?;
    let mut config = ConfigOptions::default();
    config.execution.target_partitions = 8;
    let plan = EnsureRequirements::new().optimize(commit_exec(Arc::new(writer))?, &config)?;
    assert!(writer_input(&plan)?.is::<DataSourceExec>());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_plan_writes_one_file_per_key() -> Result<()> {
    let rows = 40_000;
    let batches = keyed_batches(rows, 1000, |i| match i * 7919 % 1001 {
        1000 => None,
        key => Some(key),
    })?;
    let schema = batches[0].schema();
    for partitions in [1, 4, 32] {
        let (ctx, store) = memory_context()?;
        // Every input partition has rows of every key.
        let mut input = vec![vec![]; partitions];
        for (i, batch) in batches.iter().enumerate() {
            input[i % partitions].push(batch.clone());
        }
        let source = MemorySourceConfig::try_new_exec(&input, Arc::clone(&schema), None)?;
        let plan = partitioned_write_plan(source, 8)?;
        let output = collect(plan, ctx.task_ctx()).await?;
        assert_eq!(count_value(&output)?, vec![rows as u64]);
        check_one_file_per_key(store.as_ref(), &batches).await?;
    }

    // The rows of a key are divided among the partitions that the query asks for.
    let (ctx, store) = memory_context()?;
    let source = MemorySourceConfig::try_new_exec(
        std::slice::from_ref(&batches),
        Arc::clone(&schema),
        None,
    )?;
    let explicit = Arc::new(ExplicitRepartitionExec::new(
        source,
        Partitioning::RoundRobinBatch(3),
    ));
    let plan = partitioned_write_plan(explicit, 8)?;
    collect(plan, ctx.task_ctx()).await?;
    let files = list_files(store.as_ref()).await?;
    assert_eq!(files.len() - 1, 3 * 1001);
    Ok(())
}

#[tokio::test]
async fn test_partition_row_limit_starts_new_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.max_records_per_file = 2;
    task.options.global.max_row_group_size = 2;
    let batches = vec![
        partitioned_batch(&[(1, 1, "x"), (2, 1, "x"), (3, 2, "x")])?,
        partitioned_batch(&[(4, 1, "x"), (5, 2, "x")])?,
    ];
    run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            format!("table/a=1/part-00000-{WRITE_ID}-c000.parquet"),
            format!("table/a=1/part-00000-{WRITE_ID}-c001.parquet"),
            format!("table/a=2/part-00000-{WRITE_ID}-c000.parquet"),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_partition_empty_input_writes_no_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let task = partition_task(Arc::clone(&store), 0, &["a"]);
    let manifest = run_task(task, &schema, vec![partitioned_batch(&[])?]).await?;
    assert_eq!(manifest_rows(&manifest)?, vec![(0, 0, None, 0, 0)]);
    assert_eq!(list_files(store.as_ref()).await?, vec![]);
    Ok(())
}

#[tokio::test]
async fn test_partition_column_missing() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let task = partition_task(Arc::clone(&store), 0, &["missing"]);
    let result = run_task(task, &schema, vec![]).await;
    assert!(
        matches!(&result, Err(e) if e.to_string().contains("partition column missing does not exist")),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn test_partition_sorting_columns() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let ordering = LexOrdering::new([
        PhysicalSortExpr::new_default(Arc::new(Column::new("a", 1))),
        PhysicalSortExpr::new_default(Arc::new(Column::new("w", 2))),
    ])
    .ok_or_else(|| internal_datafusion_err!("empty ordering"))?;
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.ordering = Some(ordering);
    task.write_sorting_columns = true;
    run_task(task, &schema, vec![partitioned_batch(&[(1, 1, "x")])?]).await?;
    let files = list_files(store.as_ref()).await?;
    let (metadata, _) = read_parquet(read_bytes(store.as_ref(), &files[0].0).await?)?;
    // The partition column is not in the file,
    // and the index of the other column is the index in the file.
    assert_eq!(
        metadata.row_group(0).sorting_columns(),
        Some(&vec![SortingColumn {
            column_idx: 1,
            descending: false,
            nulls_first: true,
        }])
    );
    Ok(())
}

fn write_exec(
    partitions: &[Vec<RecordBatch>],
    schema: &SchemaRef,
    ordering: Option<LexOrdering>,
    write_sorting_columns: bool,
) -> Result<ParquetWriteExec> {
    let input = MemorySourceConfig::try_new_exec(partitions, Arc::clone(schema), None)?;
    ParquetWriteExec::try_new(
        input,
        ObjectStoreUrl::parse("memory://")?,
        Path::from("table"),
        WRITE_ID,
        vec![],
        TableParquetOptions::default(),
        ".zstd.parquet".to_string(),
        ordering,
        write_sorting_columns,
        0,
        NonZeroUsize::MIN,
    )
}

fn commit_exec(writer: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(FileWriteCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(writer)),
        ObjectStoreUrl::parse("memory://")?,
        Path::from("table"),
        WRITE_ID,
    )))
}

fn memory_context() -> Result<(SessionContext, Arc<dyn ObjectStore>)> {
    let ctx = SessionContext::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ctx.register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );
    Ok((ctx, store))
}

fn count_value(batches: &[RecordBatch]) -> Result<Vec<u64>> {
    let mut values = vec![];
    for batch in batches {
        let count = datafusion_common::cast::as_uint64_array(batch.column(0))?;
        values.extend(count.values().iter().copied());
    }
    Ok(values)
}

#[tokio::test]
#[expect(deprecated)]
async fn test_write_and_commit_plan() -> Result<()> {
    let (ctx, store) = memory_context()?;
    let schema = int_schema();
    let partitions = vec![
        vec![int_batch(&schema, 0..5)?],
        vec![],
        vec![int_batch(&schema, 5..7)?, int_batch(&schema, 7..12)?],
    ];
    let writer = Arc::new(write_exec(&partitions, &schema, None, false)?);
    assert_eq!(
        writer.properties().output_partitioning().partition_count(),
        3
    );
    assert_eq!(writer.schema(), manifest_schema());
    assert_eq!(writer.benefits_from_input_partitioning(), vec![false]);
    assert_eq!(writer.required_input_ordering(), vec![None]);
    assert!(matches!(
        writer.required_input_distribution().as_slice(),
        [Distribution::UnspecifiedDistribution]
    ));

    let plan = commit_exec(writer.clone())?;
    assert!(matches!(
        plan.required_input_distribution().as_slice(),
        [Distribution::SinglePartition]
    ));
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    let output = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_eq!(output[0].schema().field(0).name(), "count");
    assert_eq!(count_value(&output)?, vec![12]);

    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files,
        vec![
            ("table/_SUCCESS".to_string(), 0),
            (file_name(0, 0, ".zstd.parquet"), files[1].1),
            (file_name(2, 0, ".zstd.parquet"), files[2].1),
        ]
    );
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &files[2].0).await?)?;
    assert_eq!(data, int_batch(&schema, 5..12)?);

    let metrics = writer
        .metrics()
        .ok_or_else(|| internal_datafusion_err!("missing metrics"))?;
    assert_eq!(
        metrics.sum_by_name("rows_written").map(|x| x.as_usize()),
        Some(12)
    );
    assert_eq!(
        metrics.sum_by_name("files_written").map(|x| x.as_usize()),
        Some(2)
    );

    assert!(plan.execute(1, ctx.task_ctx()).is_err());
    Ok(())
}

#[tokio::test]
async fn test_write_and_commit_plan_with_empty_input() -> Result<()> {
    let (ctx, store) = memory_context()?;
    let schema = int_schema();
    let writer = Arc::new(write_exec(&[vec![], vec![]], &schema, None, false)?);
    let output = collect(commit_exec(writer)?, ctx.task_ctx()).await?;
    assert_eq!(count_value(&output)?, vec![0]);
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            "table/_SUCCESS".to_string(),
            file_name(0, 0, ".zstd.parquet")
        ]
    );
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &files[1].0).await?)?;
    assert_eq!(data.num_rows(), 0);
    assert_eq!(data.schema().fields(), schema.fields());
    Ok(())
}

#[tokio::test]
async fn test_failed_writer_leaves_no_success_file() -> Result<()> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(16)))
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ctx.register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );
    let schema = int_schema();
    // The table has files of an earlier write, and files of writer tasks of
    // this write that have completed and handed over their manifests, which
    // the commit has not read.
    let old = vec![
        "table/_SUCCESS".to_string(),
        "table/old.parquet".to_string(),
        format!(
            "table/{}",
            crate::listing::part_file::part_file_name(
                0,
                &Uuid::from_u128(77),
                0,
                0,
                ".zstd.parquet"
            )
        ),
    ];
    for file in &old {
        store.put(&Path::from(file.as_str()), "old".into()).await?;
    }
    let completed = vec![
        vec![int_batch(&schema, 0..5)?],
        vec![int_batch(&schema, 5..9)?],
    ];
    let writer = Arc::new(write_exec(&completed, &schema, None, false)?);
    let other = SessionContext::new();
    other.register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );
    collect(
        Arc::new(CoalescePartitionsExec::new(writer)),
        other.task_ctx(),
    )
    .await?;
    assert_eq!(list_files(store.as_ref()).await?.len(), old.len() + 2);
    let partitions = vec![vec![], vec![int_batch(&schema, 0..1000)?]];
    let writer = Arc::new(write_exec(&partitions, &schema, None, false)?);
    let result = collect(commit_exec(writer)?, ctx.task_ctx()).await;
    assert!(matches!(
        result.map_err(|e| e.find_root().strip_backtrace()),
        Err(e) if e.contains("Resources exhausted")
    ));
    // The write leaves none of its files, and the other files are kept.
    wait_for_files(store.as_ref(), &old).await?;
    Ok(())
}

#[tokio::test]
async fn test_commit_removes_files_of_earlier_attempt() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let ctx = SessionContext::new();
    ctx.register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );
    let schema = int_schema();
    let partitions = vec![
        vec![int_batch(&schema, 0..5)?],
        vec![int_batch(&schema, 5..12)?],
    ];
    let writer = write_exec(&partitions, &schema, None, false)?;
    assert_eq!(writer.attempt(), 0);

    // The first attempt writes its files, but the write is not committed.
    collect(Arc::new(writer.with_attempt(0)), ctx.task_ctx()).await?;
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            file_name(0, 0, ".zstd.parquet"),
            file_name(1, 0, ".zstd.parquet")
        ]
    );

    let retry = Arc::new(writer.with_attempt(1));
    assert_eq!(retry.attempt(), 1);
    let output = collect(commit_exec(retry)?, ctx.task_ctx()).await?;
    assert_eq!(count_value(&output)?, vec![12]);
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            "table/_SUCCESS".to_string(),
            format!("table/part-00000-{WRITE_ID}-c000-a1.zstd.parquet"),
            format!("table/part-00001-{WRITE_ID}-c000-a1.zstd.parquet"),
        ]
    );
    Ok(())
}

#[test]
fn test_write_plan_requires_input_ordering() -> Result<()> {
    let schema = int_schema();
    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))])
        .ok_or_else(|| internal_datafusion_err!("empty ordering"))?;
    let writer: Arc<dyn ExecutionPlan> = Arc::new(write_exec(
        &[vec![]],
        &schema,
        Some(ordering.clone()),
        true,
    )?);
    let required = vec![Some(ordering.into())];
    assert_eq!(writer.required_input_ordering(), required);
    assert!(writer.properties().output_ordering().is_none());
    assert_eq!(writer.maintains_input_order(), vec![false]);

    // The properties of the plan are kept when the input is replaced.
    let input = MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema, None)?;
    let writer = writer.replace_children(
        vec![input],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    assert_eq!(writer.required_input_ordering(), required);
    assert_eq!(writer.output_partitioning().partition_count(), 2);
    let writer = writer
        .downcast_ref::<ParquetWriteExec>()
        .ok_or_else(|| internal_datafusion_err!("unexpected plan"))?;
    assert!(writer.write_sorting_columns());
    assert_eq!(writer.file_suffix(), ".zstd.parquet");
    assert_eq!(writer.write_id(), &WRITE_ID);
    Ok(())
}

#[test]
fn test_write_plan_validation() -> Result<()> {
    let schema = int_schema();
    let input = MemorySourceConfig::try_new_exec(&[vec![]], schema, None)?;
    let result = ParquetWriteExec::try_new(
        input,
        ObjectStoreUrl::parse("memory://")?,
        Path::from("table"),
        WRITE_ID,
        vec!["missing".to_string()],
        TableParquetOptions::default(),
        ".parquet".to_string(),
        None,
        false,
        0,
        NonZeroUsize::MIN,
    );
    assert!(
        matches!(&result, Err(e) if e.to_string().contains("partition column missing does not exist"))
    );
    Ok(())
}

#[tokio::test]
async fn test_rows_require_column_in_files() -> Result<()> {
    // The files would have no columns, and a Parquet file without columns has no rows.
    // So the write fails when it has rows, and it writes no data file otherwise.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = |rows: std::ops::Range<i64>| -> Result<RecordBatch> {
        let b: StringArray = rows.clone().map(|x| Some(format!("b{}", x % 2))).collect();
        Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(rows)), Arc::new(b)],
        )?)
    };
    let empty = Arc::new(Schema::empty());
    let without_columns = |rows: usize| -> Result<RecordBatch> {
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&empty),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    };
    let all_partition_columns =
        "Parquet write does not support data whose columns are all partition columns";
    let no_columns = "Parquet write does not support data without columns";
    for (partitions, schema, partition_columns, error) in [
        (
            vec![vec![batch(0..1)?]],
            Arc::clone(&schema),
            vec!["a", "b"],
            Some(all_partition_columns),
        ),
        (
            vec![
                vec![batch(0..0)?, batch(0..4)?, batch(4..9)?],
                vec![],
                vec![batch(9..10)?],
            ],
            Arc::clone(&schema),
            vec!["b", "a"],
            Some(all_partition_columns),
        ),
        (
            vec![vec![], vec![batch(0..0)?]],
            Arc::clone(&schema),
            vec!["a", "b"],
            None,
        ),
        (
            vec![vec![without_columns(3)?]],
            Arc::clone(&empty),
            vec![],
            Some(no_columns),
        ),
        (
            vec![vec![without_columns(0)?], vec![]],
            Arc::clone(&empty),
            vec![],
            None,
        ),
    ] {
        let (ctx, store) = memory_context()?;
        let writer = Arc::new(ParquetWriteExec::try_new(
            MemorySourceConfig::try_new_exec(&partitions, schema, None)?,
            ObjectStoreUrl::parse("memory://")?,
            Path::from("table"),
            WRITE_ID,
            partition_columns.iter().map(|x| x.to_string()).collect(),
            TableParquetOptions::default(),
            ".parquet".to_string(),
            None,
            false,
            0,
            NonZeroUsize::MIN,
        )?);
        let result = collect(commit_exec(writer)?, ctx.task_ctx()).await;
        let files = list_files(store.as_ref()).await?;
        let files: Vec<_> = files.iter().map(|(path, _)| path.as_str()).collect();
        match error {
            Some(error) => {
                assert!(
                    matches!(&result, Err(e) if e.to_string().contains(error)),
                    "{result:?}"
                );
                assert!(files.is_empty(), "{files:?}");
            }
            // A write without rows succeeds, and there is no file that has the schema.
            None => {
                assert_eq!(count_value(&result?)?, vec![0]);
                assert_eq!(files, vec!["table/_SUCCESS"]);
            }
        }
    }

    // One column that is not a partition column is enough.
    let (ctx, store) = memory_context()?;
    let writer = Arc::new(ParquetWriteExec::try_new(
        MemorySourceConfig::try_new_exec(&[vec![batch(0..3)?]], Arc::clone(&schema), None)?,
        ObjectStoreUrl::parse("memory://")?,
        Path::from("table"),
        WRITE_ID,
        vec!["a".to_string()],
        TableParquetOptions::default(),
        ".parquet".to_string(),
        None,
        false,
        0,
        NonZeroUsize::MIN,
    )?);
    let output = collect(commit_exec(writer)?, ctx.task_ctx()).await?;
    assert_eq!(count_value(&output)?, vec![3]);
    let files = list_files(store.as_ref()).await?;
    let files: Vec<_> = files.iter().map(|(path, _)| path.as_str()).collect();
    let name = |a: i32| format!("table/a={a}/part-00000-{WRITE_ID}-c000.parquet");
    assert_eq!(files, vec!["table/_SUCCESS", &name(0), &name(1), &name(2)]);
    Ok(())
}

#[tokio::test]
async fn test_write_plan_writes_planned_schema() -> Result<()> {
    // The input of a plan that was decoded on a worker can have fields without
    // the metadata that the fields had when the write was planned.
    let input_schema = partitioned_schema();
    let extension = HashMap::from([(
        "ARROW:extension:name".to_string(),
        "arrow.parquet.variant".to_string(),
    )]);
    let planned = Arc::new(Schema::new_with_metadata(
        input_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "w" => field.as_ref().clone().with_metadata(extension.clone()),
                _ => field.as_ref().clone(),
            })
            .collect::<Vec<_>>(),
        HashMap::from([("planned".to_string(), "true".to_string())]),
    ));
    let batch = partitioned_batch(&[(1, 1, "x"), (2, 2, "y"), (3, 1, "x")])?;
    // The second write uses the Arrow writer for the files.
    let mut chunked = TableParquetOptions::default();
    chunked.global.content_defined_chunking.enabled = true;
    for (partition_columns, files, options) in [
        (
            vec![],
            vec!["table/part-00000"],
            TableParquetOptions::default(),
        ),
        (vec![], vec!["table/part-00000"], chunked),
        (
            vec!["a"],
            vec!["table/a=1/part-00000", "table/a=2/part-00000"],
            TableParquetOptions::default(),
        ),
    ] {
        let (ctx, store) = memory_context()?;
        let writer = ParquetWriteExec::try_new(
            MemorySourceConfig::try_new_exec(
                &[vec![batch.clone()]],
                Arc::clone(&input_schema),
                None,
            )?,
            ObjectStoreUrl::parse("memory://")?,
            Path::from("table"),
            WRITE_ID,
            partition_columns.iter().map(|x| x.to_string()).collect(),
            options,
            ".parquet".to_string(),
            None,
            false,
            0,
            NonZeroUsize::MAX,
        )?;
        assert_eq!(writer.data_schema(), &input_schema);
        let writer = Arc::new(writer.with_schema(Arc::clone(&planned)));
        let output = collect(commit_exec(writer)?, ctx.task_ctx()).await?;
        assert_eq!(count_value(&output)?, vec![3]);
        let written: Vec<_> = list_files(store.as_ref())
            .await?
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| !path.ends_with("_SUCCESS"))
            .collect();
        assert_eq!(written.len(), files.len(), "{written:?}");
        for (path, prefix) in written.iter().zip(files) {
            assert!(path.starts_with(prefix), "{path}");
            let (_, data) = read_parquet(read_bytes(store.as_ref(), path).await?)?;
            let schema = data.schema();
            assert_eq!(schema.field_with_name("w")?.metadata(), &extension);
            assert_eq!(schema.metadata().get("planned"), Some(&"true".to_string()));
            assert_eq!(schema.fields().len(), 4 - partition_columns.len());
        }
    }
    Ok(())
}

#[test]
fn test_file_size_is_limited_by_upload_parts() {
    use crate::formats::parquet::writer::exec::effective_max_file_size;

    // A file of this size has nine tenths of the parts that an upload can have.
    let buffer_size = 10 * 1024 * 1024;
    assert_eq!(effective_max_file_size(0, buffer_size), 94_371_840_000);
    assert_eq!(effective_max_file_size(5, buffer_size), 5);
    assert_eq!(
        effective_max_file_size(u64::MAX, buffer_size),
        94_371_840_000
    );
    assert_eq!(effective_max_file_size(0, 1024), 9_216_000);
    assert_eq!(effective_max_file_size(0, usize::MAX), u64::MAX / 10 * 9);
}

#[tokio::test]
async fn test_write_plan_passes_file_limits() -> Result<()> {
    let schema = int_schema();
    let partitions = vec![vec![int_batch(&schema, 0..5)?]];
    for (max_records_per_file, max_file_size, files) in [(0, 0, 1), (2, 0, 3), (0, 1, 1)] {
        let (ctx, store) = memory_context()?;
        let writer = Arc::new(
            write_exec(&partitions, &schema, None, false)?
                .with_file_limits(max_records_per_file, max_file_size),
        );
        // The limits are kept when the plan is rewritten.
        let input = Arc::clone(writer.input());
        let writer = writer.replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let limits = writer
            .downcast_ref::<ParquetWriteExec>()
            .map(|x| (x.max_records_per_file(), x.max_file_size()));
        assert_eq!(limits, Some((max_records_per_file, max_file_size)));
        collect(writer, ctx.task_ctx()).await?;
        assert_eq!(list_files(store.as_ref()).await?.len(), files);
    }
    Ok(())
}

#[tokio::test]
async fn test_write_plan_passes_task_limits() -> Result<()> {
    let schema = partitioned_schema();
    let batches = vec![
        partitioned_batch(&[(1, 1, "x")])?,
        partitioned_batch(&[(2, 2, "x")])?,
        partitioned_batch(&[(3, 1, "x")])?,
    ];
    let name = |a: i32| format!("table/a={a}/part-00000-{WRITE_ID}-c000.parquet");
    let expected = vec![name(1), name(2)];
    // The row of `a=2` is sorted when one directory can be open.
    for (max_open_files, rows_sorted) in [(1, 1), (2, 0)] {
        let (ctx, store) = memory_context()?;
        let max_open_files = NonZeroUsize::new(max_open_files)
            .ok_or_else(|| internal_datafusion_err!("invalid number of open files"))?;
        let writer = Arc::new(ParquetWriteExec::try_new(
            MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                Arc::clone(&schema),
                None,
            )?,
            ObjectStoreUrl::parse("memory://")?,
            Path::from("table"),
            WRITE_ID,
            vec!["a".to_string()],
            TableParquetOptions::default(),
            ".parquet".to_string(),
            None,
            false,
            3,
            max_open_files,
        )?);
        // The limits are kept when the plan is rewritten.
        let input = Arc::clone(writer.input());
        let writer = writer.replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let writer = writer
            .downcast_ref::<ParquetWriteExec>()
            .ok_or_else(|| internal_datafusion_err!("unexpected plan"))?
            .with_attempt(0);
        assert_eq!(writer.encoding_parallelism(), 3);
        assert_eq!(writer.max_open_files_per_task(), max_open_files);

        let writer = Arc::new(writer);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&writer) as _;
        let output = collect(commit_exec(plan)?, ctx.task_ctx()).await?;
        assert_eq!(count_value(&output)?, vec![3]);
        let files = list_files(store.as_ref()).await?;
        let files: Vec<_> = files
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| !path.ends_with("_SUCCESS"))
            .collect();
        assert_eq!(files, expected);
        let sorted = writer
            .metrics()
            .and_then(|x| x.sum_by_name("rows_sorted"))
            .map(|x| x.as_usize());
        assert_eq!(sorted, Some(rows_sorted));
    }
    Ok(())
}

#[tokio::test]
async fn test_union_column_is_rejected_without_rows() -> Result<()> {
    let fields = UnionFields::try_new(vec![0], vec![Field::new("i", DataType::Int64, true)])?;
    let union = DataType::Union(fields, UnionMode::Sparse);
    for data_type in [
        union.clone(),
        DataType::List(Arc::new(Field::new_list_field(union.clone(), true))),
        DataType::Struct(vec![Field::new("u", union, false)].into()),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", data_type, false),
        ]));
        for partition in [0, 1] {
            let task = test_task(Arc::clone(&store), partition);
            let Err(e) = run_task(task, &schema, vec![]).await else {
                return Err(internal_datafusion_err!("expected an error"));
            };
            assert!(
                e.to_string()
                    .contains("Parquet write does not support the data type Union"),
                "{e}"
            );
        }
        assert_eq!(list_files(store.as_ref()).await?, vec![]);
    }
    Ok(())
}

#[tokio::test]
async fn test_write_and_commit_plan_without_input_partitions() -> Result<()> {
    let (ctx, store) = memory_context()?;
    let schema = int_schema();
    let writer = Arc::new(write_exec(&[], &schema, None, false)?);
    assert_eq!(writer.input().output_partitioning().partition_count(), 0);
    assert_eq!(
        writer.properties().output_partitioning().partition_count(),
        1
    );
    let output = collect(commit_exec(writer)?, ctx.task_ctx()).await?;
    assert_eq!(count_value(&output)?, vec![0]);
    let files = list_files(store.as_ref()).await?;
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            "table/_SUCCESS".to_string(),
            file_name(0, 0, ".zstd.parquet")
        ]
    );
    Ok(())
}

/// Waits until the store has exactly the expected files.
/// The files of an abandoned writer task are deleted in the background.
async fn wait_for_files(store: &dyn ObjectStore, expected: &[String]) -> Result<()> {
    let mut files = vec![];
    for _ in 0..500 {
        files = list_files(store)
            .await?
            .into_iter()
            .map(|x| x.0)
            .collect::<Vec<_>>();
        if files == expected {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err(internal_datafusion_err!(
        "expected files {expected:?} but found {files:?}"
    ))
}

/// A task whose first file is complete after the second batch.
fn two_file_task(store: Arc<dyn ObjectStore>) -> ParquetWriteTask {
    let mut task = test_task(store, 0);
    // The tests write batches of seven rows.
    task.max_records_per_file = 14;
    task.options.global.max_row_group_size = 5;
    task
}

fn write_stream(
    task: ParquetWriteTask,
    input: SendableRecordBatchStream,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let metrics = ExecutionPlanMetricsSet::new();
    write_partition(
        task,
        input,
        MemoryConsumer::new("test").register(&pool),
        ParquetWriteMetrics::new(&metrics, 0),
    )
}

#[tokio::test]
async fn test_failed_task_deletes_completed_files() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(vec![
            int_batch(&schema, 0..7),
            int_batch(&schema, 7..14),
            int_batch(&schema, 14..21),
            Err(internal_datafusion_err!("input failure")),
        ]),
    ));
    let mut stream = Box::pin(write_stream(two_file_task(Arc::clone(&store)), input));
    let result = stream.next().await;
    assert!(matches!(result, Some(Err(e)) if e.to_string().contains("input failure")));
    assert!(stream.next().await.is_none());
    wait_for_files(store.as_ref(), &[]).await
}

#[tokio::test]
async fn test_canceled_task_deletes_completed_files() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    // The input does not end, so the task waits for more data
    // after it has completed its first file.
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(vec![
            int_batch(&schema, 0..7),
            int_batch(&schema, 7..14),
            int_batch(&schema, 14..21),
        ])
        .chain(futures::stream::pending()),
    ));
    let mut stream = Box::pin(write_stream(two_file_task(Arc::clone(&store)), input));
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(200), stream.next());
    assert!(waiting.await.is_err());
    wait_for_files(store.as_ref(), &[file_name(0, 0, ".parquet")]).await?;
    drop(stream);
    wait_for_files(store.as_ref(), &[]).await
}

#[tokio::test]
async fn test_task_keeps_files_only_when_read_to_end() -> Result<()> {
    let schema = int_schema();
    let files = [file_name(0, 0, ".parquet"), file_name(0, 1, ".parquet")];
    let input = |schema: &SchemaRef| -> Result<SendableRecordBatchStream> {
        Ok(test_stream(
            schema,
            vec![
                int_batch(schema, 0..7)?,
                int_batch(schema, 7..14)?,
                int_batch(schema, 14..21)?,
            ],
        ))
    };

    // The reader stops after the manifest, so the manifest may not have been delivered.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut stream = Box::pin(write_stream(
        two_file_task(Arc::clone(&store)),
        input(&schema)?,
    ));
    assert!(matches!(stream.next().await, Some(Ok(_))));
    wait_for_files(store.as_ref(), &files).await?;
    drop(stream);
    wait_for_files(store.as_ref(), &[]).await?;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut stream = Box::pin(write_stream(
        two_file_task(Arc::clone(&store)),
        input(&schema)?,
    ));
    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(stream.next().await.is_none());
    drop(stream);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    wait_for_files(store.as_ref(), &files).await
}

/// An object store that completes a write only when it is allowed to.
#[derive(Debug)]
struct GatedStore {
    inner: InMemory,
    started: tokio::sync::Notify,
    gate: tokio::sync::Semaphore,
    /// The number of completed writes.
    completed: AtomicUsize,
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore")
    }
}

#[async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.started.notify_one();
        let permit = self.gate.acquire().await;
        drop(permit);
        let result = self.inner.put_opts(location, payload, options).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn test_canceled_task_deletes_file_that_completes_later() -> Result<()> {
    let gated = Arc::new(GatedStore {
        inner: InMemory::new(),
        started: tokio::sync::Notify::new(),
        gate: tokio::sync::Semaphore::new(0),
        completed: AtomicUsize::new(0),
    });
    let store: Arc<dyn ObjectStore> = gated.clone();
    let schema = int_schema();
    let input = test_stream(&schema, vec![int_batch(&schema, 0..7)?]);
    let mut stream = Box::pin(write_stream(test_task(Arc::clone(&store), 0), input));

    // The task is canceled while the request that makes its file visible is running.
    tokio::select! {
        _ = stream.next() => return Err(internal_datafusion_err!("the write must wait")),
        _ = gated.started.notified() => {}
    }
    drop(stream);
    assert_eq!(list_files(&gated.inner).await?, vec![]);

    // The request completes after the cancellation, and the file is deleted.
    gated.gate.add_permits(1);
    for _ in 0..500 {
        if gated.completed.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(gated.completed.load(Ordering::SeqCst), 1);
    wait_for_files(&gated.inner, &[]).await?;
    Ok(())
}

/// A batch with one column for each type that the Parquet writer supports.
/// The four rows of each type are repeated.
fn all_types_batch(repeat: usize) -> Result<RecordBatch> {
    let mut fields = vec![];
    let mut columns = vec![];
    for values in type_cases()? {
        let values = match values.data_type() {
            DataType::Union(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => continue,
            // The other columns have four rows.
            DataType::Float16 | DataType::Float32 | DataType::Float64 => values.slice(1, 4),
            _ => values,
        };
        let name = format!("c{}", fields.len());
        fields.push(Field::new(name, values.data_type().clone(), true));
        columns.push(values);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    Ok(concat_batches(&schema, &vec![batch; repeat])?)
}

/// Writes the batches with the Arrow writer and the properties
/// that the writer task is expected to use.
fn reference_file_of_batches(
    options: &TableParquetOptions,
    batches: &[RecordBatch],
) -> Result<Vec<u8>> {
    let schema = batches[0].schema();
    let mut options = options.clone();
    options.arrow_schema(&schema);
    options.global.write_batch_size = options.global.data_page_row_count_limit.clamp(1, 1024);
    let props = WriterPropertiesBuilder::try_from(&options)?.build();
    let mut writer = ArrowWriter::try_new_with_options(
        vec![],
        schema,
        ArrowWriterOptions::new().with_properties(props),
    )?;
    for batch in batches {
        writer.write(batch)?;
    }
    Ok(writer.into_inner()?)
}

/// Returns the file that a writer task must write for the batches,
/// whichever way the task encodes the file.
///
/// This is the file of the Arrow writer, unless the size of a row group
/// is limited in bytes. The encoder ends such row groups at the rows where
/// the Arrow writer of version 60.2.0 ends them, which are other rows than
/// the ones of the Arrow version in use. The expected file is then the file
/// of a task that encodes all columns itself.
async fn expected_file(options: &TableParquetOptions, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    // TODO: Uncomment after Arrow 60.2.0, and remove the rest of the function.
    // reference_file_of_batches(options, batches)
    if options.global.max_row_group_bytes.is_none() {
        return reference_file_of_batches(options, batches);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = test_task(Arc::clone(&store), 0);
    task.options = options.clone();
    task.encode = fixed_degree(1);
    run_task(task, &batches[0].schema(), batches.to_vec()).await?;
    let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
    Ok(data.to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_row_group_byte_limit_applies_to_first_batch() -> Result<()> {
    // The size of the encoded data follows the number of rows
    // without dictionaries and with small pages.
    let mut options = TableParquetOptions::default();
    options.global.dictionary_enabled = Some(false);
    options.global.data_pagesize_limit = 512;
    options.global.data_page_row_count_limit = 16;
    options.global.max_row_group_bytes = Some("9000".parse()?);
    // The first batch alone is larger than the limit.
    let batches = vec![all_types_batch(78)?, all_types_batch(25)?];
    let schema = batches[0].schema();
    let rows: usize = batches.iter().map(|x| x.num_rows()).sum();
    let expected = expected_file(&options, &batches).await?;
    let (metadata, data) = read_parquet(Bytes::from(expected.clone()))?;
    assert_eq!(data.num_rows(), rows);
    let row_groups: Vec<_> = metadata
        .row_groups()
        .iter()
        .map(|x| x.num_rows() as usize)
        .collect();
    assert_eq!(row_groups.iter().sum::<usize>(), rows);
    // A row group receives one row before the size of its rows is known,
    // and it then ends where the limit is expected to be reached,
    // so the first row group ends inside the first batch.
    assert!(row_groups.len() > 2, "{row_groups:?}");
    assert!(row_groups[0] > 1, "{row_groups:?}");
    assert!(row_groups[0] < batches[0].num_rows() / 2, "{row_groups:?}");
    // The file is the same when the columns are encoded by other tasks.
    let name = file_name(0, 0, ".parquet");
    for encode in encode_cases() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.options = options.clone();
        task.encode = encode.clone();
        run_task(task, &schema, batches.clone()).await?;
        let data = read_bytes(store.as_ref(), &name).await?;
        assert!(data == expected, "{encode:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_columns_write_the_same_file() -> Result<()> {
    // The batches have different sizes, so that row groups end inside batches,
    // and so that some parts of the batches are below the size for parallel encoding.
    let batches = vec![
        all_types_batch(600)?,
        all_types_batch(1)?,
        all_types_batch(900)?,
        all_types_batch(300)?,
    ];
    let schema = batches[0].schema();
    let rows: usize = batches.iter().map(|x| x.num_rows()).sum();

    let mut by_rows = TableParquetOptions::default();
    by_rows.global.max_row_group_size = 3000;
    by_rows.global.data_pagesize_limit = 512;
    by_rows.global.data_page_row_count_limit = 64;
    let mut by_bytes = TableParquetOptions::default();
    by_bytes.global.max_row_group_bytes = Some("65536".parse()?);
    by_bytes.global.compression = Some("snappy".to_string());
    let mut plain = TableParquetOptions::default();
    plain.global.dictionary_enabled = Some(false);
    plain.global.writer_version = "2.0".parse()?;
    plain.global.bloom_filter_on_write = true;

    for (i, options) in [by_rows, by_bytes, plain].into_iter().enumerate() {
        let expected = expected_file(&options, &batches).await?;
        for encode in encode_cases() {
            let lanes = format!("{encode:?}");
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut task = test_task(Arc::clone(&store), 0);
            task.options = options.clone();
            task.encode = encode;
            // The upload buffer is small, so that the file is uploaded in parts.
            task.buffer_size = 64 * 1024;
            let manifest = run_task(task, &schema, batches.clone()).await?;
            let name = file_name(0, 0, ".parquet");
            assert_eq!(
                manifest_rows(&manifest)?,
                vec![(0, 0, Some(name.clone()), expected.len() as u64, rows as u64)],
                "options {i} lanes {lanes}"
            );
            let data = read_bytes(store.as_ref(), &name).await?;
            assert!(data == expected, "options {i} lanes {lanes}");
            let (metadata, _) = read_parquet(data)?;
            if i == 0 {
                assert_eq!(metadata.num_row_groups(), rows.div_ceil(3000));
            } else if i == 1 {
                assert!(metadata.num_row_groups() > 1);
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_columns_with_row_limit_and_partitions() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let rows: Vec<(i64, i32, &str)> = (0..5000).map(|i| (i, (i % 2) as i32, "x")).collect();
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.encode = fixed_degree(4);
    task.max_records_per_file = 2000;
    task.options.global.max_row_group_size = 1500;
    let manifest = run_task(task, &schema, vec![partitioned_batch(&rows)?]).await?;
    let mut manifest = manifest_rows(&manifest)?;
    manifest.sort();
    let files = list_files(store.as_ref()).await?;
    // Each directory has 2500 rows, and the row limit applies to
    // the files of each directory.
    let name =
        |a: i32, counter: usize| format!("table/a={a}/part-00000-{WRITE_ID}-c{counter:03}.parquet");
    assert_eq!(
        manifest,
        vec![
            (0, 0, Some(name(0, 0)), files[0].1, 2000),
            (0, 0, Some(name(0, 1)), files[1].1, 500),
            (0, 0, Some(name(1, 0)), files[2].1, 2000),
            (0, 0, Some(name(1, 1)), files[3].1, 500),
        ]
    );
    let directories = read_directories(store.as_ref()).await?;
    assert_eq!(directories["table/a=0"].len(), 2500);
    assert_eq!(directories["table/a=1"].len(), 2500);
    Ok(())
}

/// Writes the batches to a file without a writer task, which rejects the data types
/// that cannot be encoded before it creates a file.
async fn write_file_without_task(
    store: &Arc<dyn ObjectStore>,
    options: &TableParquetOptions,
    encode: EncodeSettings,
    batches: &[RecordBatch],
) -> Result<()> {
    let schema = batches[0].schema();
    let mut options = options.clone();
    options.arrow_schema(&schema);
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut writer = ParquetFileWriter::try_new(
        Arc::clone(store),
        Path::from("file.parquet"),
        schema,
        WriterPropertiesBuilder::try_from(&options)?.build(),
        false,
        10 * 1024 * 1024,
        test_resources(encode, &pool),
    )?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_columns_report_encoding_error() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let intervals: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3);
            2000
        ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", intervals.data_type().clone(), true),
        Field::new("c", DataType::Int64, true),
    ]));
    let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..2000));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&integers), intervals, integers],
    )?;
    let expected = reference_file_of_batches(
        &TableParquetOptions::default(),
        std::slice::from_ref(&batch),
    );
    let actual = write_file_without_task(
        &store,
        &TableParquetOptions::default(),
        fixed_degree(3),
        &[batch],
    )
    .await;
    let (Err(expected), Err(actual)) = (expected, actual) else {
        return Err(internal_datafusion_err!("expected errors"));
    };
    assert_eq!(actual.strip_backtrace(), expected.strip_backtrace());
    assert_eq!(list_files(store.as_ref()).await?, vec![]);
    Ok(())
}

#[tokio::test]
async fn test_parallel_columns_with_content_defined_chunking() -> Result<()> {
    // Content-defined chunking is not available for parallel columns,
    // so the columns are encoded in order and the option keeps its effect.
    let batches = vec![all_types_batch(600)?, all_types_batch(600)?];
    let schema = batches[0].schema();
    let mut options = TableParquetOptions::default();
    options.global.content_defined_chunking.enabled = true;
    options.global.max_row_group_size = 1000;
    let expected = reference_file_of_batches(&options, &batches)?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = test_task(Arc::clone(&store), 0);
    task.options = options;
    task.encode = fixed_degree(8);
    run_task(task, &schema, batches).await?;
    let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
    assert!(data == expected);
    Ok(())
}

#[tokio::test]
async fn test_columns_in_order_write_the_same_file() -> Result<()> {
    // The batches have 4, 100, 8, 0 and 200 rows, so that row groups end
    // before, at and inside the batches for the row limits below.
    let batches = vec![
        all_types_batch(1)?,
        all_types_batch(25)?,
        all_types_batch(2)?,
        all_types_batch(0)?,
        all_types_batch(50)?,
    ];
    let schema = batches[0].schema();
    let rows: usize = batches.iter().map(|x| x.num_rows()).sum();

    let mut cases = vec![];
    for limit in [1, 4, 7, 100, 104, 312, 1024 * 1024] {
        let mut options = TableParquetOptions::default();
        options.global.max_row_group_size = limit;
        cases.push((options, Some(rows.div_ceil(limit))));
    }
    // The first two limits are below the size of the encoded rows of the batches,
    // so row groups end inside batches.
    for limit in ["9000", "24000", "1073741824"] {
        let mut options = TableParquetOptions::default();
        // The size of the encoded data follows the number of rows
        // without dictionaries and with small pages.
        options.global.dictionary_enabled = Some(false);
        options.global.data_pagesize_limit = 512;
        options.global.data_page_row_count_limit = 16;
        options.global.max_row_group_bytes = Some(limit.parse()?);
        cases.push((options, None));
    }
    let mut both = TableParquetOptions::default();
    both.global.max_row_group_size = 100;
    both.global.max_row_group_bytes = Some("65536".parse()?);
    both.global.writer_version = "2.0".parse()?;
    both.global.bloom_filter_on_write = true;
    cases.push((both, None));

    let mut row_groups = vec![];
    for (i, (options, expected_row_groups)) in cases.into_iter().enumerate() {
        let expected = expected_file(&options, &batches).await?;
        let name = file_name(0, 0, ".parquet");
        // The file is the same when the columns are encoded by other tasks.
        for encode in encode_cases() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut task = test_task(Arc::clone(&store), 0);
            task.options = options.clone();
            task.encode = encode.clone();
            run_task(task, &schema, batches.clone()).await?;
            let data = read_bytes(store.as_ref(), &name).await?;
            assert!(data == expected, "case {i} {encode:?}");
        }
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.options = options;
        let manifest = run_task(task, &schema, batches.clone()).await?;
        assert_eq!(
            manifest_rows(&manifest)?,
            vec![(0, 0, Some(name.clone()), expected.len() as u64, rows as u64)],
            "case {i}"
        );
        let data = read_bytes(store.as_ref(), &name).await?;
        assert!(data == expected, "case {i}");
        let (metadata, _) = read_parquet(data)?;
        if let Some(expected_row_groups) = expected_row_groups {
            assert_eq!(metadata.num_row_groups(), expected_row_groups, "case {i}");
        }
        row_groups.push(
            metadata
                .row_groups()
                .iter()
                .map(|x| x.num_rows() as usize)
                .collect::<Vec<_>>(),
        );
    }
    // The byte limits give fewer row groups as they increase.
    assert!(row_groups[7].len() > row_groups[8].len(), "{row_groups:?}");
    assert!(row_groups[8].len() > 1, "{row_groups:?}");
    assert_eq!(row_groups[9], vec![rows]);
    let ends_of_batches = [4, 104, 112, 312];
    for case in [7, 8] {
        assert!(
            !ends_of_batches.contains(&row_groups[case][0]),
            "{row_groups:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_columns_in_order_write_the_same_file_without_rows() -> Result<()> {
    let schema = all_types_batch(1)?.schema();
    let options = TableParquetOptions::default();
    let expected = reference_file_of_batches(&options, &[RecordBatch::new_empty(schema.clone())])?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    run_task(test_task(Arc::clone(&store), 0), &schema, vec![]).await?;
    let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
    assert!(data == expected);
    Ok(())
}

#[tokio::test]
async fn test_file_writer_memory_size() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let schema = int_schema();
    // A row group has 1.6 MB of values, which exceeds the size of the encoded data
    // that the writer keeps before the data is handed to the upload.
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(200_000))
        .build();
    let mut writer = ParquetFileWriter::try_new(
        store,
        Path::from("file.parquet"),
        Arc::clone(&schema),
        props,
        false,
        10 * 1024 * 1024,
        test_resources(fixed_degree(1), &pool),
    )?;
    assert_eq!(writer.memory_size(), 0);
    assert!(!writer.write(&int_batch(&schema, 0..199_999)?).await?);
    assert!(writer.memory_size() > 1_500_000);
    assert!(writer.write(&int_batch(&schema, 0..1)?).await?);
    assert!(writer.memory_size() <= 1024 * 1024);
    let file = writer.close().await?;
    assert_eq!(file.num_rows, 200_000);
    Ok(())
}

/// A generator of numbers for tests that are reproducible from a seed.
struct TestRng(u64);

impl TestRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }

    /// A number that is less than the limit.
    fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit as u64) as usize
    }

    fn choose<T: Copy>(&mut self, values: &[T]) -> T {
        values[self.below(values.len())]
    }
}

/// Writes a file with batches, options and settings that are derived from the seed,
/// and compares the file with the file of the Arrow writer.
async fn check_encoding_with_random_settings(
    source: RecordBatch,
    seed: u64,
    active_writers: Arc<AtomicUsize>,
) -> Result<()> {
    let mut rng = TestRng(seed);
    // The batches are slices of one batch, as the batches of a query often are.
    let mut batches = vec![];
    let mut offset = 0;
    let largest = rng.choose(&[1, 7, 64, 300]);
    while offset < source.num_rows() {
        let rows = (1 + rng.below(largest)).min(source.num_rows() - offset);
        batches.push(source.slice(offset, rows));
        offset += rows;
    }
    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = rng.choose(&[5, 50, 257, 1024 * 1024]);
    if rng.below(4) == 0 {
        // The size of the encoded data follows the number of rows
        // without dictionaries and with small pages.
        options.global.dictionary_enabled = Some(false);
        options.global.data_pagesize_limit = 512;
        options.global.data_page_row_count_limit = 16;
        options.global.max_row_group_bytes = Some((5000 + rng.below(30000)).to_string().parse()?);
    }
    if rng.below(4) == 0 {
        options.global.writer_version = "2.0".parse()?;
        options.global.bloom_filter_on_write = true;
    }
    let lanes = 1 + rng.below(50);
    let window = 1 + rng.below(5);
    let encode = encode_settings(
        1 + rng.below(9),
        rng.choose(&[None, Some(lanes)]),
        rng.choose(&[None, Some(window)]),
        rng.choose(&[None, Some(1), Some(1024), Some(64 * 1024)]),
    );
    let encode = if rng.below(3) == 0 {
        // The degree of the task follows the number of tasks of the test that are running.
        shared_settings(64, &active_writers)
    } else {
        encode
    };
    let expected = expected_file(&options, &batches).await?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut task = test_task(Arc::clone(&store), 0);
    task.options = options;
    task.encode = encode.clone();
    task.buffer_size = rng.choose(&[64 * 1024, 10 * 1024 * 1024]);
    run_task(task, &source.schema(), batches).await?;
    let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
    assert!(data == expected, "seed {seed} {encode:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_encoding_with_random_settings_writes_the_same_file() -> Result<()> {
    let source = all_types_batch(100)?;
    // The files are written at the same time, so that the lanes of the files
    // share the threads of the runtime.
    let active_writers = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..200)
        .map(|seed| {
            tokio::spawn(check_encoding_with_random_settings(
                source.clone(),
                seed,
                Arc::clone(&active_writers),
            ))
        })
        .collect();
    for task in tasks {
        task.await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;
    }
    assert_eq!(active_writers.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_encoding_writes_the_same_files_with_file_row_limit() -> Result<()> {
    let batches = vec![all_types_batch(700)?, all_types_batch(550)?];
    let schema = batches[0].schema();
    let mut outputs = vec![];
    for encode in encode_cases() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.encode = encode;
        task.max_records_per_file = 1000;
        task.options.global.max_row_group_size = 300;
        let manifest = run_task(task, &schema, batches.clone()).await?;
        let mut files = vec![];
        for (name, _) in list_files(store.as_ref()).await? {
            let data = read_bytes(store.as_ref(), &name).await?;
            files.push((name, data));
        }
        outputs.push((manifest_rows(&manifest)?, files));
    }
    // A file is completed when it has 1000 rows,
    // and the next file receives rows while the file is completed.
    let rows: Vec<_> = outputs[0].0.iter().map(|x| x.4).collect();
    assert_eq!(rows, vec![1000; 5]);
    for output in &outputs[1..] {
        assert!(output == &outputs[0]);
    }
    Ok(())
}

fn three_int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]))
}

fn three_int_batch(values: std::ops::Range<i64>) -> Result<RecordBatch> {
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(values));
    Ok(RecordBatch::try_new(
        three_int_schema(),
        vec![Arc::clone(&column), Arc::clone(&column), column],
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_row_groups_in_flight_and_waiting_input_are_limited() -> Result<()> {
    let schema = three_int_schema();
    let batches = (0..100)
        .map(|i| three_int_batch(i * 37..(i + 1) * 37))
        .collect::<Result<Vec<_>>>()?;
    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = 100;
    let expected = reference_file_of_batches(&options, &batches)?;
    options.arrow_schema(&schema);
    // The second budget is smaller than the columns of one lane for one batch.
    for (window, read_ahead_bytes) in [(1, 64 * 1024), (2, 100), (3, 4096), (5, 1024 * 1024)] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let resources = test_resources(
            encode_settings(4, Some(2), Some(window), Some(read_ahead_bytes)),
            &pool,
        );
        let mut writer = ParquetFileWriter::try_new(
            Arc::clone(&store),
            Path::from("file.parquet"),
            Arc::clone(&schema),
            WriterPropertiesBuilder::try_from(&options)?.build(),
            false,
            10 * 1024 * 1024,
            Arc::clone(&resources),
        )?;
        // A lane has at most two of the three columns.
        let largest_input = 2 * 37 * 8;
        let limit = read_ahead_bytes.next_multiple_of(1024).max(largest_input);
        let mut most_row_groups = 0;
        for batch in &batches {
            writer.write(batch).await?;
            most_row_groups = most_row_groups.max(writer.row_groups_in_flight());
            assert!(resources.waiting_input_size() <= limit, "window {window}");
        }
        assert!(most_row_groups <= window, "window {window}");
        writer.close().await?;
        assert_eq!(resources.waiting_input_size(), 0);
        assert_eq!(pool.reserved(), 0);
        let data = read_bytes(store.as_ref(), "file.parquet").await?;
        assert!(data == expected, "window {window}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_waiting_input_is_counted_by_the_size_of_its_values() -> Result<()> {
    // The batches are slices of large arrays, and the strings are views of one buffer.
    let rows = 100_000;
    let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let strings: ArrayRef =
        Arc::new(StringViewArray::from_iter_values((0..rows).map(|i| {
            format!("a string that is not stored in the view {i:08}")
        })));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8View, true),
    ]));
    let source = RecordBatch::try_new(Arc::clone(&schema), vec![integers, strings])?;
    assert!(source.get_array_memory_size() > 4 * 1024 * 1024);
    let batches: Vec<_> = (0..100).map(|i| source.slice(i * 1000, 1000)).collect();

    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = 30_000;
    let expected = reference_file_of_batches(&options, &batches)?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let read_ahead_bytes = 256 * 1024;
    let resources = test_resources(
        encode_settings(4, None, None, Some(read_ahead_bytes)),
        &pool,
    );
    let mut options = options.clone();
    options.arrow_schema(&schema);
    let mut writer = ParquetFileWriter::try_new(
        Arc::clone(&store),
        Path::from("file.parquet"),
        Arc::clone(&schema),
        WriterPropertiesBuilder::try_from(&options)?.build(),
        false,
        10 * 1024 * 1024,
        Arc::clone(&resources),
    )?;
    for batch in &batches {
        writer.write(batch).await?;
        // The input of a batch is 8 KB of integers and 62 KB of strings,
        // so the input of several batches fits in the budget.
        assert!(resources.waiting_input_size() <= read_ahead_bytes);
    }
    writer.close().await?;
    assert_eq!(pool.reserved(), 0);
    let data = read_bytes(store.as_ref(), "file.parquet").await?;
    assert!(data == expected);
    Ok(())
}

#[test]
fn test_logical_size_of_arrays() -> Result<()> {
    let integers = Int64Array::from_iter_values(0..1000);
    assert_eq!(logical_size(&integers), 8000);
    assert_eq!(logical_size(&integers.slice(10, 20)), 160);

    let strings = StringArray::from_iter_values((0..1000).map(|i| format!("{i:010}")));
    assert_eq!(logical_size(&strings), 1001 * 4 + 10_000);
    assert_eq!(logical_size(&strings.slice(10, 20)), 21 * 4 + 200);

    // A view has 16 bytes. Values of up to 12 bytes are stored in the view.
    let views = StringViewArray::from_iter_values((0..1000).map(|i| {
        if i % 2 == 0 {
            format!("{i:030}")
        } else {
            format!("{i:05}")
        }
    }));
    assert_eq!(logical_size(&views), 1000 * 16 + 500 * 30);
    assert_eq!(logical_size(&views.slice(10, 20)), 20 * 16 + 10 * 30);
    assert!(views.slice(10, 20).get_array_memory_size() > 15_000);
    let binary_views = BinaryViewArray::from_iter_values((0..10).map(|_| [7u8; 100]));
    assert_eq!(logical_size(&binary_views.slice(2, 3)), 3 * 16 + 300);

    // The views of a column inside a struct are counted in the same way.
    let fields = vec![
        Arc::new(Field::new("x", DataType::Int64, true)),
        Arc::new(Field::new("y", DataType::Utf8View, true)),
    ];
    let nested = StructArray::try_new(
        fields.into(),
        vec![Arc::new(integers) as ArrayRef, Arc::new(views) as ArrayRef],
        None,
    )?;
    assert_eq!(logical_size(&nested.slice(10, 20)), 160 + 20 * 16 + 10 * 30);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_encoding_reports_batch_with_other_columns() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&integers), Arc::clone(&integers)],
    )?;
    // The second batch does not have the columns of the schema of the input.
    let other = RecordBatch::try_new(int_schema(), vec![integers])?;
    for encode in encode_cases() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.encode = encode.clone();
        let result = run_task(task, &schema, vec![batch.clone(), other.clone()]).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains(
                "Incompatible schema: writer has 2 top-level fields but batch has 1 columns"
            )),
            "{encode:?} {result:?}"
        );
        assert_eq!(list_files(store.as_ref()).await?, vec![]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_encoding_reports_error_of_lane() -> Result<()> {
    let intervals: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3);
            2000
        ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", intervals.data_type().clone(), true),
        Field::new("c", DataType::Int64, true),
    ]));
    let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..2000));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&integers), intervals, integers],
    )?;
    for byte_limit in [false, true] {
        let mut options = TableParquetOptions::default();
        options.global.max_row_group_size = 1500;
        if byte_limit {
            options.global.max_row_group_bytes = Some("65536".parse()?);
        }
        // The error is found when the lane is given its second input,
        // when the lane is asked for the size of its encoded data,
        // or when the file is completed.
        for batches in [1, 20] {
            let batches = vec![batch.clone(); batches];
            let Err(expected) = reference_file_of_batches(&options, &batches) else {
                return Err(internal_datafusion_err!("expected an error"));
            };
            for encode in encode_cases() {
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let Err(actual) =
                    write_file_without_task(&store, &options, encode.clone(), &batches).await
                else {
                    return Err(internal_datafusion_err!("expected an error"));
                };
                assert_eq!(
                    actual.strip_backtrace(),
                    expected.strip_backtrace(),
                    "{encode:?}"
                );
                assert_eq!(list_files(store.as_ref()).await?, vec![]);
            }
        }
    }
    Ok(())
}

/// A memory pool that panics the first time that memory is returned to it.
#[derive(Debug, Default)]
struct PanickingPool {
    panicked: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for PanickingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PanickingPool")
    }
}

impl MemoryPool for PanickingPool {
    fn name(&self) -> &str {
        "PanickingPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, _additional: usize) {}

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        if !self.panicked.swap(true, Ordering::SeqCst) {
            // The index is out of bounds, which panics.
            let values: [u8; 0] = [];
            std::hint::black_box(values[std::hint::black_box(shrink)]);
        }
    }

    fn try_grow(&self, _reservation: &MemoryReservation, _additional: usize) -> Result<()> {
        Ok(())
    }

    fn reserved(&self) -> usize {
        0
    }
}

/// Lets the task of the lane run until it has ended.
/// The test must run on a single thread, so that the task runs only here.
async fn run_lane_to_its_end(lane: &Lane) {
    for _ in 0..1000 {
        if lane.has_ended() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(lane.has_ended());
}

/// Asks a lane that has reported its failure for its failure again,
/// in every way that the lane can be asked.
async fn check_failure_is_reported_again(
    mut lane: Lane,
    input: LaneInput,
    expected: &str,
) -> Result<()> {
    for _ in 0..2 {
        let error = lane.failure().await;
        assert!(error.to_string().contains(expected), "{error}");
    }
    let Err(error) = lane.send(input).await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(error.to_string().contains(expected), "{error}");
    let error = lane.failure().await;
    assert!(error.to_string().contains(expected), "{error}");
    lane.seal();
    let Err(error) = lane.join().await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(error.to_string().contains(expected), "{error}");
    Ok(())
}

// The test runs on a single thread, so that the task of a lane
// runs only while the test waits.
#[tokio::test]
async fn test_lane_reports_panic_of_its_task() -> Result<()> {
    use datafusion::parquet::errors::ParquetError;

    // The task of the lane panics when it has encoded its first input,
    // which is when the input is no longer counted as waiting.
    let spawn = || {
        let pool: Arc<dyn MemoryPool> = Arc::new(PanickingPool::default());
        let resources = test_resources(fixed_degree(2), &pool);
        let lane = Lane::spawn(
            vec![],
            vec![],
            vec![],
            resources.new_reservation(),
            Time::new(),
        );
        (lane, resources)
    };
    let input = |resources: Arc<EncodeResources>, encoded| async move {
        Ok::<_, DataFusionError>(LaneInput {
            columns: vec![],
            encoded,
            admission: resources.admit(80).await?,
        })
    };

    // The panic is reported when the lane is given more input
    // after its task has ended.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    assert!(!lane.has_ended());
    run_lane_to_its_end(&lane).await;
    let Err(error) = lane.send(input(Arc::clone(&resources), None).await?).await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(matches!(error, ParquetError::External(_)), "{error:?}");
    assert!(error.to_string().contains("panicked"), "{error}");
    assert_eq!(resources.waiting_input_size(), 0);
    let more = input(Arc::clone(&resources), None).await?;
    check_failure_is_reported_again(lane, more, "panicked").await?;
    assert_eq!(resources.waiting_input_size(), 0);

    // The panic is reported when the lane is asked for the size of its encoded data.
    // The size is sent before the panic, so the lane is given a second input.
    // The task ends after it has been given the second input.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    let (encoded, report) = tokio::sync::oneshot::channel();
    lane.send(input(Arc::clone(&resources), Some(encoded)).await?)
        .await?;
    assert!(!lane.has_ended());
    assert!(report.await.is_err());
    assert!(lane.has_ended());
    let error = lane.failure().await;
    assert!(matches!(error, ParquetError::External(_)), "{error:?}");
    assert!(error.to_string().contains("panicked"), "{error}");
    let more = input(Arc::clone(&resources), None).await?;
    check_failure_is_reported_again(lane, more, "panicked").await?;
    assert_eq!(resources.waiting_input_size(), 0);

    // The same, when the task ends before it is given the second input.
    // The second input is not accepted then, which is not the report of the panic
    // for a caller that asks for the size of the encoded data.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    run_lane_to_its_end(&lane).await;
    let (encoded, report) = tokio::sync::oneshot::channel();
    let _ = lane
        .send(input(Arc::clone(&resources), Some(encoded)).await?)
        .await;
    assert!(report.await.is_err());
    let error = lane.failure().await;
    assert!(error.to_string().contains("panicked"), "{error}");
    assert_eq!(resources.waiting_input_size(), 0);

    // The panic is reported when the columns of the lane are requested,
    // before and after the task has ended.
    for ended in [false, true] {
        let (mut lane, resources) = spawn();
        lane.send(input(Arc::clone(&resources), None).await?)
            .await?;
        lane.seal();
        if ended {
            run_lane_to_its_end(&lane).await;
        }
        assert_eq!(lane.has_ended(), ended);
        let Err(error) = lane.join().await else {
            return Err(internal_datafusion_err!("expected an error"));
        };
        assert!(matches!(error, ParquetError::External(_)), "{error:?}");
        assert!(error.to_string().contains("panicked"), "{error}");
        assert_eq!(resources.waiting_input_size(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn test_lane_reports_error_of_its_task_to_every_request() -> Result<()> {
    use datafusion::parquet::errors::ParquetError;

    let schema = int_schema();
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let resources = test_resources(fixed_degree(2), &pool);
    // The lane has no writer for its column, so its task fails at the first input.
    let spawn = || {
        Lane::spawn(
            vec![0],
            schema.fields().to_vec(),
            vec![],
            resources.new_reservation(),
            Time::new(),
        )
    };
    let input = || async {
        Ok::<_, DataFusionError>(LaneInput {
            columns: vec![Arc::clone(&column)],
            encoded: None,
            admission: resources.admit(80).await?,
        })
    };
    let expected = "missing column writer for leaf column";
    let is_error_of_task =
        |error: &ParquetError| matches!(error, ParquetError::General(x) if x == expected);

    // The error is first reported when the lane is given more input.
    let mut lane = spawn();
    lane.send(input().await?).await?;
    run_lane_to_its_end(&lane).await;
    let Err(error) = lane.send(input().await?).await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(is_error_of_task(&error), "{error:?}");
    check_failure_is_reported_again(lane, input().await?, expected).await?;

    // The error is first reported when the lane is asked for its failure,
    // before and after its task has ended.
    for ended in [false, true] {
        let mut lane = spawn();
        lane.send(input().await?).await?;
        if ended {
            run_lane_to_its_end(&lane).await;
        }
        assert_eq!(lane.has_ended(), ended);
        let error = lane.failure().await;
        assert!(is_error_of_task(&error), "{error:?}");
        check_failure_is_reported_again(lane, input().await?, expected).await?;
    }

    // The error is first reported when the columns of the lane are requested.
    let mut lane = spawn();
    lane.send(input().await?).await?;
    lane.seal();
    let Err(error) = lane.join().await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(is_error_of_task(&error), "{error:?}");

    // A lane that is asked for its failure after its task has completed
    // the columns reports that each time.
    let mut lane = Lane::spawn(
        vec![],
        vec![],
        vec![],
        resources.new_reservation(),
        Time::new(),
    );
    lane.seal();
    run_lane_to_its_end(&lane).await;
    let expected = "the task of a column encoder has ended before its input";
    let Err(error) = lane.send(input().await?).await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(
        matches!(&error, ParquetError::General(x) if x == expected),
        "{error:?}"
    );
    check_failure_is_reported_again(lane, input().await?, expected).await?;

    assert_eq!(Arc::strong_count(&column), 1);
    assert_eq!(resources.waiting_input_size(), 0);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_dropped_lane_does_not_encode_its_queue() -> Result<()> {
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let schema = int_schema();
    let (_, factory) =
        ArrowWriter::try_new(vec![], Arc::clone(&schema), None)?.into_serialized_writer()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let resources = test_resources(fixed_degree(2), &pool);
    let mut lane = Lane::spawn(
        vec![0],
        schema.fields().to_vec(),
        factory.create_column_writers(0)?,
        resources.new_reservation(),
        Time::new(),
    );
    // The queue has input that takes much longer to encode than the test takes
    // to drop the lane. The task of the lane is running when the lane is dropped.
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..500_000));
    let mut reports = vec![];
    for _ in 0..100 {
        let (encoded, report) = tokio::sync::oneshot::channel();
        reports.push(report);
        lane.send(LaneInput {
            columns: vec![Arc::clone(&column)],
            encoded: Some(encoded),
            admission: resources.admit(1024).await?,
        })
        .await?;
    }
    match reports.remove(0).await {
        Ok(_) => {}
        Err(_) => return Err(internal_datafusion_err!("the first input must be encoded")),
    }
    drop(lane);
    wait_for_tasks(tasks).await?;
    let mut encoded = 0;
    for report in reports {
        if report.await.is_ok() {
            encoded += 1;
        }
    }
    // The task ends after the input that it was encoding when the lane was dropped.
    assert!(encoded < 50, "{encoded} inputs were encoded");
    assert_eq!(Arc::strong_count(&column), 1);
    assert_eq!(resources.waiting_input_size(), 0);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_degree_is_limited_by_settings() -> Result<()> {
    let batch = all_types_batch(15)?;
    let schema = batch.schema();
    let mut options = TableParquetOptions::default();
    options.arrow_schema(&schema);
    for (max_degree, lanes) in [(0, 8), (20, 8), (3, 3), (1, 0)] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let settings = EncodeSettings {
            max_degree,
            ..fixed_degree(8)
        };
        let mut writer = ParquetFileWriter::try_new(
            Arc::clone(&store),
            Path::from("file.parquet"),
            Arc::clone(&schema),
            WriterPropertiesBuilder::try_from(&options)?.build(),
            false,
            10 * 1024 * 1024,
            test_resources(settings, &pool),
        )?;
        writer.write(&batch).await?;
        assert_eq!(writer.lanes(), lanes, "limit {max_degree}");
        writer.close().await?;
    }
    Ok(())
}

/// Waits until the runtime has no more tasks than the given number.
async fn wait_for_tasks(tasks: usize) -> Result<()> {
    let metrics = tokio::runtime::Handle::current().metrics();
    for _ in 0..500 {
        if metrics.num_alive_tasks() <= tasks {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err(internal_datafusion_err!(
        "expected {tasks} tasks but found {}",
        metrics.num_alive_tasks()
    ))
}

async fn check_dropped_file_writer_releases_waiting_input() -> Result<()> {
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let schema = three_int_schema();
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1000));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::clone(&column),
            Arc::clone(&column),
            Arc::clone(&column),
        ],
    )?;
    for byte_limit in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let resources = test_resources(encode_settings(4, None, Some(4), None), &pool);
        let mut props = WriterProperties::builder().set_max_row_group_row_count(Some(10_000));
        if byte_limit {
            props = props.set_max_row_group_bytes(Some(1024 * 1024 * 1024));
        }
        let mut writer = ParquetFileWriter::try_new(
            Arc::clone(&store),
            Path::from("file.parquet"),
            Arc::clone(&schema),
            props.build(),
            false,
            10 * 1024 * 1024,
            Arc::clone(&resources),
        )?;
        // The third row group is receiving rows when the writer is dropped.
        for _ in 0..25 {
            writer.write(&batch).await?;
        }
        drop(writer);
        for _ in 0..500 {
            if Arc::strong_count(&column) == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // The batch has the remaining references.
        assert_eq!(Arc::strong_count(&column), 4);
        assert_eq!(resources.waiting_input_size(), 0);
        wait_for_tasks(tasks).await?;
        assert_eq!(pool.reserved(), 0);
        assert_eq!(list_files(store.as_ref()).await?, vec![]);
    }
    Ok(())
}

#[tokio::test]
async fn test_dropped_file_writer_releases_waiting_input() -> Result<()> {
    // The lanes do not run before the writer is dropped unless the writer waits,
    // since the runtime of the test has one thread.
    check_dropped_file_writer_releases_waiting_input().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_dropped_file_writer_releases_waiting_input_with_running_lanes() -> Result<()> {
    check_dropped_file_writer_releases_waiting_input().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_canceled_task_with_lanes_deletes_files_and_ends_tasks() -> Result<()> {
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    for encode in encode_cases() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = int_schema();
        // The input does not end, so the task waits for more data after it
        // has completed its first file, with row groups of the second file
        // that are not in the file yet.
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![
                int_batch(&schema, 0..7),
                int_batch(&schema, 7..14),
                int_batch(&schema, 14..21),
                int_batch(&schema, 21..23),
            ])
            .chain(futures::stream::pending()),
        ));
        let mut task = two_file_task(Arc::clone(&store));
        task.encode = encode;
        let mut stream = Box::pin(write_stream(task, input));
        let waiting = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next());
        assert!(waiting.await.is_err());
        wait_for_files(store.as_ref(), &[file_name(0, 0, ".parquet")]).await?;
        drop(stream);
        wait_for_files(store.as_ref(), &[]).await?;
        wait_for_tasks(tasks).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_canceled_task_with_lanes_deletes_file_that_completes_later() -> Result<()> {
    let gated = Arc::new(GatedStore {
        inner: InMemory::new(),
        started: tokio::sync::Notify::new(),
        gate: tokio::sync::Semaphore::new(0),
        completed: AtomicUsize::new(0),
    });
    let store: Arc<dyn ObjectStore> = gated.clone();
    let schema = int_schema();
    let input = test_stream(
        &schema,
        vec![int_batch(&schema, 0..7)?, int_batch(&schema, 7..30)?],
    );
    let mut task = test_task(Arc::clone(&store), 0);
    task.options.global.max_row_group_size = 5;
    task.encode = fixed_degree(8);
    let mut stream = Box::pin(write_stream(task, input));

    // The task is canceled while the request that makes its file visible is running.
    tokio::select! {
        _ = stream.next() => return Err(internal_datafusion_err!("the write must wait")),
        _ = gated.started.notified() => {}
    }
    drop(stream);
    assert_eq!(list_files(&gated.inner).await?, vec![]);

    // The request completes after the cancellation, and the file is deleted.
    gated.gate.add_permits(1);
    for _ in 0..500 {
        if gated.completed.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(gated.completed.load(Ordering::SeqCst), 1);
    wait_for_files(&gated.inner, &[]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_slow_input_does_not_keep_completed_row_groups() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    // The window is larger than the number of row groups of the test,
    // so the writer never waits for a row group because of the window.
    let resources = test_resources(encode_settings(4, None, Some(100), None), &pool);
    // A row group has 2.4 MB of values, and the parts of the upload have 64 KiB.
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(100_000))
        .build();
    let mut writer = ParquetFileWriter::try_new(
        Arc::clone(&store),
        Path::from("file.parquet"),
        three_int_schema(),
        props,
        false,
        64 * 1024,
        Arc::clone(&resources),
    )?;
    let uploaded = || -> Result<u64> {
        let mut size = 0;
        for entry in std::fs::read_dir(directory.path())? {
            size += entry?.metadata()?.len();
        }
        Ok(size)
    };
    // The first two row groups have all their rows, and the third row group has one row.
    writer.write(&three_int_batch(0..200_001)?).await?;
    // The input is slow from here on. A batch arrives after the lanes have
    // completed the row groups, and the row groups are added to the file
    // and uploaded when the batch is written, although the row group that
    // receives rows is far from complete.
    let mut batches = 0;
    while writer.row_groups_in_flight() > 1 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        writer.write(&three_int_batch(0..1)?).await?;
        batches += 1;
        assert!(batches < 100);
    }
    // The uploaded parts are complete when the next part is accepted, so wait for them.
    for _ in 0..500 {
        if uploaded()? > 4_000_000 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(uploaded()? > 4_000_000);
    let file = writer.close().await?;
    assert_eq!(file.num_rows, 200_001 + batches);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_task_does_not_poll_slow_input_while_waiting() -> Result<()> {
    let batches = vec![all_types_batch(50)?; 20];
    let schema = batches[0].schema();
    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = 300;
    let expected = reference_file_of_batches(&options, &batches)?;
    for encode in encode_cases() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut slow = Box::pin(futures::stream::iter(batches.clone()).then(|batch| async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(batch)
        }));
        let counter = Arc::clone(&polls);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::poll_fn(move |cx| {
                counter.fetch_add(1, Ordering::SeqCst);
                slow.poll_next_unpin(cx)
            }),
        ));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut task = test_task(Arc::clone(&store), 0);
        task.options = options.clone();
        task.encode = encode.clone();
        read_manifest(write_stream(task, input)).await?;
        let data = read_bytes(store.as_ref(), &file_name(0, 0, ".parquet")).await?;
        assert!(data == expected, "{encode:?}");
        // The input is polled when a batch is requested and when the batch is ready.
        let polls = polls.load(Ordering::SeqCst);
        assert!(polls <= 2 * batches.len() + 1, "{polls} polls {encode:?}");
    }
    Ok(())
}

/// Settings for a task that shares the given number of threads
/// with the writer tasks that the counter has.
fn shared_settings(threads: usize, active_writers: &Arc<AtomicUsize>) -> EncodeSettings {
    EncodeSettings {
        threads: Some(threads),
        active_writers: Arc::clone(active_writers),
        ..EncodeSettings::new(0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_degree_follows_active_writers_at_each_row_group() -> Result<()> {
    const MB: usize = 1024 * 1024;
    // A batch has 60 rows and more columns than the task has threads.
    let batch = all_types_batch(15)?;
    let schema = batch.schema();
    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = 100;
    let expected = reference_file_of_batches(&options, &vec![batch.clone(); 7])?;
    options.arrow_schema(&schema);

    let active_writers = Arc::new(AtomicUsize::new(0));
    let _this = ActiveWriter::new(&active_writers);
    let mut others: Vec<_> = (0..3).map(|_| ActiveWriter::new(&active_writers)).collect();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let resources = test_resources(shared_settings(8, &active_writers), &pool);
    let mut writer = ParquetFileWriter::try_new(
        Arc::clone(&store),
        Path::from("file.parquet"),
        Arc::clone(&schema),
        WriterPropertiesBuilder::try_from(&options)?.build(),
        false,
        10 * 1024 * 1024,
        Arc::clone(&resources),
    )?;
    // Four writer tasks share eight threads.
    writer.write(&batch).await?;
    assert_eq!(writer.lanes(), 2);
    assert_eq!(resources.read_ahead_size(), 2 * 16 * MB);
    // Two of the other tasks end. The second row group starts with this batch.
    others.truncate(1);
    writer.write(&batch).await?;
    assert_eq!(writer.lanes(), 4);
    assert_eq!(resources.read_ahead_size(), 4 * 16 * MB);
    // The last of the other tasks ends, which has no effect on the second row group.
    others.clear();
    writer.write(&batch).await?;
    assert_eq!(writer.lanes(), 4);
    // The third row group starts with this batch.
    writer.write(&batch).await?;
    assert_eq!(writer.lanes(), 8);
    assert_eq!(resources.read_ahead_size(), 8 * 16 * MB);
    // More tasks start than there are threads. The third row group ends with this batch.
    others.extend((0..20).map(|_| ActiveWriter::new(&active_writers)));
    writer.write(&batch).await?;
    // The writer encodes the columns of the fourth row group itself.
    writer.write(&batch).await?;
    assert_eq!(writer.lanes(), 0);
    assert!(writer.row_groups_in_flight() >= 1);
    // The budget does not fall below the budget for one thread, and the part
    // of the budget that is in use is not taken away.
    assert!(resources.read_ahead_size() >= 16 * MB);
    assert!(resources.read_ahead_size() <= 8 * 16 * MB);
    writer.write(&batch).await?;
    writer.close().await?;
    assert_eq!(pool.reserved(), 0);
    let data = read_bytes(store.as_ref(), "file.parquet").await?;
    assert!(data == expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_degree_is_divided_among_open_files_of_task() -> Result<()> {
    let batch = all_types_batch(15)?;
    let schema = batch.schema();
    let mut options = TableParquetOptions::default();
    options.global.max_row_group_size = 100;
    options.arrow_schema(&schema);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let resources = test_resources(fixed_degree(8), &pool);
    let open = |name: &str| {
        ParquetFileWriter::try_new(
            Arc::clone(&store),
            Path::from(name),
            Arc::clone(&schema),
            WriterPropertiesBuilder::try_from(&options)?.build(),
            false,
            10 * 1024 * 1024,
            Arc::clone(&resources),
        )
    };
    let mut first = open("first.parquet")?;
    first.write(&batch).await?;
    assert_eq!(first.lanes(), 8);
    let mut second = open("second.parquet")?;
    second.write(&batch).await?;
    assert_eq!(second.lanes(), 4);
    // The first file has its lanes until its next row group.
    assert_eq!(first.lanes(), 8);
    first.write(&batch).await?;
    assert_eq!(first.lanes(), 4);
    second.close().await?;
    first.write(&batch).await?;
    assert_eq!(first.lanes(), 4);
    first.write(&batch).await?;
    assert_eq!(first.lanes(), 8);
    first.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_active_writers_are_counted_until_the_task_ends() -> Result<()> {
    let active_writers = Arc::new(AtomicUsize::new(0));
    let count = || active_writers.load(Ordering::SeqCst);
    let schema = int_schema();
    let task = |store: &Arc<dyn ObjectStore>| {
        let mut task = two_file_task(Arc::clone(store));
        task.encode = shared_settings(4, &active_writers);
        task
    };
    let batches = |schema: &SchemaRef| {
        vec![
            int_batch(schema, 0..7),
            int_batch(schema, 7..14),
            int_batch(schema, 14..21),
        ]
    };

    // The task writes its files.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let input = test_stream(
        &schema,
        batches(&schema).into_iter().collect::<Result<_>>()?,
    );
    read_manifest(write_stream(task(&store), input)).await?;
    assert_eq!(count(), 0);

    // The input of the task fails.
    let mut failing = batches(&schema);
    failing.push(Err(internal_datafusion_err!("input failure")));
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(failing),
    ));
    assert!(
        read_manifest(write_stream(task(&store), input))
            .await
            .is_err()
    );
    assert_eq!(count(), 0);

    // A lane of the task fails.
    let intervals: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 3);
        20
    ]));
    let interval_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", intervals.data_type().clone(), true),
    ]));
    let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..20));
    let batch = RecordBatch::try_new(Arc::clone(&interval_schema), vec![integers, intervals])?;
    let input = test_stream(&interval_schema, vec![batch.clone(), batch]);
    assert!(
        read_manifest(write_stream(task(&store), input))
            .await
            .is_err()
    );
    assert_eq!(count(), 0);

    // The tasks are canceled while they wait for input.
    let mut streams = vec![];
    for i in 0..3 {
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(batches(&schema)).chain(futures::stream::pending()),
        ));
        let mut stream = Box::pin(write_stream(task(&store), input));
        // A task is counted when it starts to read its input.
        assert_eq!(count(), i);
        let waiting = tokio::time::timeout(std::time::Duration::from_millis(50), stream.next());
        assert!(waiting.await.is_err());
        assert_eq!(count(), i + 1);
        streams.push(stream);
    }
    streams.pop();
    assert_eq!(count(), 2);
    drop(streams);
    assert_eq!(count(), 0);
    Ok(())
}

#[test]
fn test_writer_tasks_share_one_counter_by_default() {
    let first = EncodeSettings::new(0);
    let second = EncodeSettings::new(0);
    assert_eq!(first.max_degree, 0);
    assert!(Arc::ptr_eq(&first.active_writers, &second.active_writers));
}

/// Data types that combine the leaf types with the types that have children.
fn nested_type_cases() -> Result<Vec<DataType>> {
    let item = |t: DataType| Arc::new(Field::new_list_field(t, true));
    let dict = |k: DataType, v: DataType| DataType::Dictionary(Box::new(k), Box::new(v));
    let ree = |k: DataType, v: DataType| {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", k, false)),
            Arc::new(Field::new("values", v, true)),
        )
    };
    let st = |t: DataType| DataType::Struct(vec![Field::new("x", t, true)].into());
    let map = |k: DataType, v: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![Field::new("key", k, false), Field::new("value", v, true)].into(),
                ),
                false,
            )),
            false,
        )
    };
    let union = DataType::Union(
        UnionFields::try_new(vec![0], vec![Field::new("i", DataType::Int64, true)])?,
        UnionMode::Sparse,
    );
    let leaves = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int64,
        DataType::UInt8,
        DataType::Float16,
        DataType::Float64,
        DataType::Decimal32(9, 2),
        DataType::Decimal32(1, 0),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(3),
        DataType::FixedSizeBinary(0),
        DataType::Decimal64(1, 0),
        DataType::Decimal64(5, 2),
        DataType::Decimal64(9, 2),
        DataType::Decimal128(1, 0),
        DataType::Decimal128(5, -2),
        DataType::Decimal128(18, 2),
        DataType::Decimal256(1, 0),
        DataType::Decimal256(9, 2),
        DataType::Decimal256(18, 2),
        DataType::Decimal32(2, 2),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Duration(TimeUnit::Second),
        DataType::UInt64,
        DataType::Float32,
        DataType::Int8,
    ];
    let mut types = vec![];
    for t in &leaves {
        types.push(dict(DataType::Int32, ree(DataType::Int32, t.clone())));
        types.push(ree(DataType::Int32, dict(DataType::Int32, t.clone())));
        types.push(dict(DataType::Int32, dict(DataType::Int8, t.clone())));
        types.push(ree(DataType::Int32, ree(DataType::Int16, t.clone())));
        types.push(DataType::List(item(DataType::List(item(t.clone())))));
        types.push(map(t.clone(), DataType::Int64));
        types.push(DataType::ListView(item(st(t.clone()))));
        types.push(DataType::LargeList(item(t.clone())));
        types.push(DataType::FixedSizeList(item(t.clone()), 0));
    }
    for t in &leaves {
        types.push(t.clone());
        for k in [DataType::Int8, DataType::UInt64, DataType::Int32] {
            types.push(dict(k, t.clone()));
        }
        for k in [DataType::Int16, DataType::Int32, DataType::Int64] {
            types.push(ree(k, t.clone()));
        }
        types.push(DataType::List(item(t.clone())));
        types.push(DataType::LargeListView(item(t.clone())));
        types.push(DataType::FixedSizeList(item(t.clone()), 2));
        types.push(st(t.clone()));
        types.push(map(DataType::Utf8, t.clone()));
        types.push(DataType::List(item(dict(DataType::Int32, t.clone()))));
        types.push(st(ree(DataType::Int32, t.clone())));
    }
    for t in [
        DataType::List(item(DataType::Int64)),
        st(DataType::Int64),
        map(DataType::Utf8, DataType::Int64),
        DataType::Struct(Vec::<Field>::new().into()),
        union.clone(),
        DataType::FixedSizeList(item(DataType::Int64), 0),
    ] {
        types.push(dict(DataType::Int32, t.clone()));
        types.push(ree(DataType::Int32, t.clone()));
        types.push(DataType::List(item(t.clone())));
        types.push(DataType::ListView(item(t.clone())));
        types.push(st(t.clone()));
        types.push(map(DataType::Utf8, t.clone()));
        types.push(map(t.clone(), DataType::Utf8));
        types.push(dict(DataType::Int32, dict(DataType::Int32, t.clone())));
        types.push(ree(DataType::Int32, ree(DataType::Int32, t.clone())));
        types.push(t);
    }
    Ok(types)
}

/// The data types that the writer rejects are exactly the data types
/// that the Arrow writer fails or panics for, and a write of such
/// a data type fails before a file is created.
#[tokio::test]
async fn test_arrow_type_support() -> Result<()> {
    let base = StringArray::from(vec![Some("1"), None, Some("3"), Some("1")]);
    let mut types = nested_type_cases()?;
    types.extend(type_cases()?.iter().map(|x| x.data_type().clone()));
    let (mut supported, mut unsupported) = (0, 0);
    for data_type in types {
        let schema = Arc::new(Schema::new(vec![Field::new("c", data_type.clone(), true)]));
        let expected = {
            let schema = Arc::clone(&schema);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![new_null_array(&data_type, 4)])?;
            tokio::task::spawn_blocking(move || {
                let mut writer = ArrowWriter::try_new(vec![], schema, None)?;
                writer.write(&batch)?;
                writer.into_inner()
            })
            .await
        };
        let is_supported = matches!(expected, Ok(Ok(_)));
        let mut inputs = vec![vec![], vec![new_null_array(&data_type, 4)]];
        if can_cast_types(&DataType::Utf8, &data_type)
            && let Ok(values) = cast(&base, &data_type)
        {
            inputs.push(vec![values]);
        }
        for values in inputs {
            let batches = values
                .into_iter()
                .map(|x| RecordBatch::try_new(Arc::clone(&schema), vec![x]))
                .collect::<Result<Vec<_>, _>>()?;
            let rows: usize = batches.iter().map(|x| x.num_rows()).sum();
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let task = test_task(Arc::clone(&store), 0);
            let actual = {
                let schema = Arc::clone(&schema);
                tokio::spawn(async move { run_task(task, &schema, batches).await })
                    .await
                    .map_err(|e| {
                        internal_datafusion_err!(
                            "panic for {data_type}: {}",
                            panic_message(e.into_panic())
                        )
                    })?
            };
            match actual {
                Ok(manifest) => {
                    assert!(is_supported, "{data_type}");
                    let name = file_name(0, 0, ".parquet");
                    let files = list_files(store.as_ref()).await?;
                    assert_eq!(files.len(), 1, "{data_type}");
                    assert_eq!(
                        manifest_rows(&manifest)?,
                        vec![(0, 0, Some(name), files[0].1, rows as u64)],
                        "{data_type}"
                    );
                }
                Err(e) => {
                    assert!(!is_supported, "{data_type}: {e}");
                    let e = e.strip_backtrace();
                    assert!(
                        e.starts_with(
                            "This feature is not implemented: Parquet write does not support the data type"
                        ),
                        "{data_type}: {e}"
                    );
                    assert_eq!(list_files(store.as_ref()).await?, vec![], "{data_type}");
                }
            }
        }
        if is_supported {
            supported += 1;
        } else {
            unsupported += 1;
        }
    }
    assert!(supported > 500, "{supported}");
    assert!(unsupported > 100, "{unsupported}");
    Ok(())
}

#[test]
fn test_write_plan_rejects_unsupported_column_types() -> Result<()> {
    let fields = UnionFields::try_new(vec![0], vec![Field::new("i", DataType::Int64, true)])?;
    let union = DataType::Union(fields, UnionMode::Sparse);
    for (data_type, message) in [
        (
            union.clone(),
            "Parquet write does not support the data type Union(Sparse, 0: (\"i\": Int64)) of column c",
        ),
        (
            DataType::List(Arc::new(Field::new_list_field(union.clone(), true))),
            "Parquet write does not support the data type Union(Sparse, 0: (\"i\": Int64)) in column c",
        ),
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            "Parquet write does not support the data type Interval(MonthDayNano) of column c",
        ),
        (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View)),
            "Parquet write does not support the data type Dictionary(Int32, Utf8View) of column c",
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", data_type, true),
        ]));
        let Err(e) = write_exec(&[vec![]], &schema, None, false) else {
            return Err(internal_datafusion_err!("expected an error"));
        };
        assert_eq!(
            e.strip_backtrace(),
            format!("This feature is not implemented: {message}")
        );
    }
    Ok(())
}
