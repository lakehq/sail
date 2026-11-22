// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, Int64Builder, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeListViewArray, ListArray,
    ListViewArray, MapBuilder, NullArray, RecordBatch, RunArray, StringArray, StringBuilder,
    StringViewArray, StructArray, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::{SortOptions, cast, concat_batches};
use datafusion::arrow::datatypes::{
    DataType, Field, Int32Type, Int64Type, IntervalDayTime, IntervalMonthDayNano, IntervalUnit,
    Schema, SchemaRef, TimeUnit, UnionFields, UnionMode,
};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::metadata::{ParquetMetaData, SortingColumn};
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::physical_expr::Distribution;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
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
use uuid::Uuid;

use crate::formats::parquet::writer::exec::ParquetWriteExec;
use crate::formats::parquet::writer::file::ParquetFileWriter;
use crate::formats::parquet::writer::lane::{Lane, LaneInput, logical_size};
use crate::formats::parquet::writer::resources::{ActiveWriter, EncodeResources, EncodeSettings};
use crate::formats::parquet::writer::task::{
    ParquetWriteMetrics, ParquetWriteTask, write_partition,
};
use crate::listing::commit::{FileWriteCommitExec, manifest_schema};

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
        sorting: None,
        buffer_size: 10 * 1024 * 1024,
        max_rows_per_file: 50_000_000,
        max_open_files: 100,
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
    task.max_rows_per_file = 10;
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
    // The limit is checked when a row group is completed,
    // so the first file ends with the batch that reaches the limit.
    assert_eq!(
        manifest_rows(&manifest)?,
        vec![
            (1, 0, Some(first.clone()), files[0].1, 14),
            (1, 0, Some(second.clone()), files[1].1, 7),
        ]
    );
    let (metadata, data) = read_parquet(read_bytes(store.as_ref(), &first).await?)?;
    assert_eq!(data, int_batch(&schema, 0..14)?);
    assert_eq!(metadata.num_row_groups(), 3);
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &second).await?)?;
    assert_eq!(data, int_batch(&schema, 14..21)?);
    Ok(())
}

#[tokio::test]
async fn test_row_limit_at_end_of_input() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = int_schema();
    let mut task = test_task(Arc::clone(&store), 0);
    task.max_rows_per_file = 10;
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
    task.sorting = Some(ordering);
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

/// The message of a panic, for the types that the Arrow writer does not implement.
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
        let expected = {
            let options = task.options.clone();
            let batch = batch.clone();
            tokio::task::spawn_blocking(move || reference_file(&options, &batch))
                .await
                .map_err(|e| panic_message(e.into_panic()))
        };
        let actual = {
            let schema = Arc::clone(&schema);
            let batch = batch.clone();
            tokio::spawn(async move { run_task(task, &schema, vec![batch]).await })
                .await
                .map_err(|e| panic_message(e.into_panic()))
        };
        match (expected, actual) {
            (Ok(Ok(expected)), Ok(Ok(manifest))) => {
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
            (Ok(Err(expected)), Ok(Err(actual))) => {
                assert_eq!(
                    actual.strip_backtrace(),
                    DataFusionError::from(expected).strip_backtrace(),
                    "{data_type}"
                );
                assert_eq!(list_files(store.as_ref()).await?, vec![], "{data_type}");
                unsupported.push(data_type.to_string());
            }
            (Err(expected), Err(actual)) => {
                assert_eq!(actual, expected, "{data_type}");
                assert_eq!(list_files(store.as_ref()).await?, vec![], "{data_type}");
                unsupported.push(data_type.to_string());
            }
            (expected, actual) => {
                return Err(internal_datafusion_err!(
                    "different outcomes for {data_type}: {:?} and {:?}",
                    expected.map(|x| x.map(|x| x.len())),
                    actual.map(|x| x.map(|x| x.num_rows()))
                ));
            }
        }
    }
    // These are the types that the Arrow writer rejects with an error (interval)
    // or does not implement (union).
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
    let input = test_stream(&batch.schema(), vec![batch.clone()]);
    sink.write_all(input, &ctx.task_ctx()).await?;
    read_directories(store.as_ref()).await
}

/// Writes rows with the partition column `p` and the data column `v`,
/// and returns the directories of the rows. The directories and their rows
/// must be the same as the ones that the DataFusion Parquet sink produces.
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
    let expected = write_with_data_sink(&batch, &["p"]).await?;
    assert_eq!(actual, expected);
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
    let expected = vec![
        "table/p=",
        "table/p=50%25",
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
        vec!["table/p=false", "table/p=true"]
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
            vec!["table/p=-3", "table/p=0", "table/p=1", "table/p=100"],
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
            vec!["table/p=0", "table/p=1", "table/p=100"],
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
        ]
    );
    // The time of day is dropped by a division that rounds towards zero,
    // so the last value is in the directory of the first day.
    let milliseconds = Int64Array::from(vec![Some(0), None, Some(1641600000123), Some(-1)]);
    assert_eq!(
        partition_directories(cast(&milliseconds, &DataType::Date64)?).await?,
        vec!["table/p=1970-01-01", "table/p=2022-01-08"]
    );
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

#[tokio::test]
async fn test_partition_null_shares_directory_with_default_value() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Int32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), None, Some(1), None])),
            Arc::new(Int64Array::from_iter_values(0..4)),
        ],
    )?;
    let task = partition_task(Arc::clone(&store), 0, &["p"]);
    run_task(task, &schema, vec![batch]).await?;
    let files = list_files(store.as_ref()).await?;
    // The rows with a null value and with the value 0 are in the same file.
    assert_eq!(
        files.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
        vec![
            format!("table/p=0/part-00000-{WRITE_ID}-c000.parquet"),
            format!("table/p=1/part-00000-{WRITE_ID}-c000.parquet"),
        ]
    );
    let (_, data) = read_parquet(read_bytes(store.as_ref(), &files[0].0).await?)?;
    assert_eq!(
        data.column(0).as_ref(),
        &Int64Array::from(vec![Some(0), Some(1), Some(3)])
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
        // The file of `a=2` is completed here since it was used least recently.
        partitioned_batch(&[(4, 3, "x")])?,
        partitioned_batch(&[(5, 1, "x")])?,
        // The file of `a=3` is completed here, and `a=2` gets a second file.
        partitioned_batch(&[(6, 2, "x"), (7, 1, "x")])?,
    ];
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.max_open_files = 2;
    let manifest = run_task(task, &schema, batches).await?;
    let files = list_files(store.as_ref()).await?;
    let name =
        |a: i32, counter: usize| format!("table/a={a}/part-00000-{WRITE_ID}-c{counter:03}.parquet");
    let mut rows = manifest_rows(&manifest)?;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (0, 0, Some(name(1, 0)), files[0].1, 4),
            (0, 0, Some(name(2, 0)), files[1].1, 1),
            (0, 0, Some(name(2, 1)), files[2].1, 1),
            (0, 0, Some(name(3, 0)), files[3].1, 1),
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
    assert_eq!(read(name(1, 0)).await?, vec![1, 3, 5, 7]);
    assert_eq!(read(name(2, 0)).await?, vec![2]);
    assert_eq!(read(name(2, 1)).await?, vec![6]);
    assert_eq!(read(name(3, 0)).await?, vec![4]);
    Ok(())
}

#[tokio::test]
async fn test_partition_row_limit_starts_new_file() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = partitioned_schema();
    let mut task = partition_task(Arc::clone(&store), 0, &["a"]);
    task.max_rows_per_file = 2;
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
    task.sorting = Some(ordering);
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
    let partitions = vec![vec![], vec![int_batch(&schema, 0..1000)?]];
    let writer = Arc::new(write_exec(&partitions, &schema, None, false)?);
    let result = collect(commit_exec(writer)?, ctx.task_ctx()).await;
    assert!(matches!(
        result.map_err(|e| e.find_root().strip_backtrace()),
        Err(e) if e.contains("Resources exhausted")
    ));
    let files = list_files(store.as_ref()).await?;
    assert!(files.iter().all(|x| x.0 != "table/_SUCCESS"), "{files:?}");
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
    );
    assert!(
        matches!(&result, Err(e) if e.to_string().contains("partition column missing does not exist"))
    );
    Ok(())
}

#[tokio::test]
async fn test_empty_first_partition_with_union_writes_no_file() -> Result<()> {
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
        let manifest = run_task(test_task(Arc::clone(&store), 0), &schema, vec![]).await?;
        assert_eq!(manifest_rows(&manifest)?, vec![(0, 0, None, 0, 0)]);
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
    task.max_rows_per_file = 10;
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
    by_rows.global.write_batch_size = 64;
    let mut by_bytes = TableParquetOptions::default();
    by_bytes.global.max_row_group_bytes = Some("65536".parse()?);
    by_bytes.global.compression = Some("snappy".to_string());
    let mut plain = TableParquetOptions::default();
    plain.global.dictionary_enabled = Some(false);
    plain.global.writer_version = "2.0".parse()?;
    plain.global.bloom_filter_on_write = true;

    for (i, options) in [by_rows, by_bytes, plain].into_iter().enumerate() {
        let expected = reference_file_of_batches(&options, &batches)?;
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
    task.max_rows_per_file = 2000;
    task.options.global.max_row_group_size = 1500;
    let manifest = run_task(task, &schema, vec![partitioned_batch(&rows)?]).await?;
    let mut manifest = manifest_rows(&manifest)?;
    manifest.sort();
    let files = list_files(store.as_ref()).await?;
    // Each directory has 2500 rows. A file is completed at the end of
    // the row group that reaches the row limit of the file.
    let name =
        |a: i32, counter: usize| format!("table/a={a}/part-00000-{WRITE_ID}-c{counter:03}.parquet");
    assert_eq!(
        manifest,
        vec![
            (0, 0, Some(name(0, 0)), files[0].1, 2500),
            (0, 0, Some(name(1, 0)), files[1].1, 2500),
        ]
    );
    let directories = read_directories(store.as_ref()).await?;
    assert_eq!(directories["table/a=0"].len(), 2500);
    assert_eq!(directories["table/a=1"].len(), 2500);
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
    let mut task = test_task(Arc::clone(&store), 0);
    task.encode = fixed_degree(3);
    let actual = run_task(task, &schema, vec![batch]).await;
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
        options.global.write_batch_size = 16;
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
        let expected = reference_file_of_batches(&options, &batches)?;
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
        options.global.write_batch_size = 16;
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
    let expected = reference_file_of_batches(&options, &batches)?;
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
        task.max_rows_per_file = 1000;
        task.options.global.max_row_group_size = 300;
        let manifest = run_task(task, &schema, batches.clone()).await?;
        let mut files = vec![];
        for (name, _) in list_files(store.as_ref()).await? {
            let data = read_bytes(store.as_ref(), &name).await?;
            files.push((name, data));
        }
        outputs.push((manifest_rows(&manifest)?, files));
    }
    // A file is completed after the batch with which the file reaches 1000 rows,
    // and the next file receives rows while the file is completed.
    let rows: Vec<_> = outputs[0].0.iter().map(|x| x.4).collect();
    assert_eq!(rows, vec![2800, 2200]);
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
                let mut task = test_task(Arc::clone(&store), 0);
                task.options = options.clone();
                task.encode = encode.clone();
                let Err(actual) = run_task(task, &schema, batches.clone()).await else {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lane_reports_panic_of_its_task() -> Result<()> {
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
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

    // The panic is reported when the lane is given more input.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    for _ in 0..500 {
        if lane.has_ended() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(lane.has_ended());
    let Err(error) = lane.send(input(Arc::clone(&resources), None).await?).await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(error.to_string().contains("panicked"), "{error}");
    assert_eq!(resources.waiting_input_size(), 0);

    // The panic is reported when the lane is asked for the size of its encoded data.
    // The size is sent before the panic, so the lane is given a second input.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    let (encoded, report) = tokio::sync::oneshot::channel();
    let _ = lane
        .send(input(Arc::clone(&resources), Some(encoded)).await?)
        .await;
    assert!(report.await.is_err());
    let error = lane.failure().await;
    assert!(error.to_string().contains("panicked"), "{error}");

    // The panic is reported when the columns of the lane are requested.
    let (mut lane, resources) = spawn();
    lane.send(input(Arc::clone(&resources), None).await?)
        .await?;
    lane.seal();
    let Err(error) = lane.join().await else {
        return Err(internal_datafusion_err!("expected an error"));
    };
    assert!(error.to_string().contains("panicked"), "{error}");
    assert_eq!(Arc::strong_count(&column), 1);
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
