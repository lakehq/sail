// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array,
    Int64Array, Int64Builder, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeListViewArray, ListArray, ListViewArray, MapBuilder, NullArray,
    RecordBatch, RunArray, StringArray, StringBuilder, StructArray, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::{SortOptions, cast, concat_batches};
use datafusion::arrow::datatypes::{
    DataType, Field, Int32Type, Int64Type, IntervalDayTime, IntervalMonthDayNano, Schema,
    SchemaRef, TimeUnit, UnionFields, UnionMode,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::metadata::{ParquetMetaData, SortingColumn};
use datafusion::parquet::file::properties::WriterPropertiesBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use uuid::Uuid;

use crate::formats::parquet::writer::task::{
    ParquetWriteMetrics, ParquetWriteTask, write_partition,
};

const WRITE_ID: Uuid = Uuid::from_u128(0x0f1e2d3c_4b5a_4978_8a9b_acbdcedfe0f1);

fn test_task(store: Arc<dyn ObjectStore>, partition: usize) -> ParquetWriteTask {
    ParquetWriteTask {
        store,
        table_path: Path::from("table"),
        write_id: WRITE_ID,
        partition,
        attempt: 0,
        file_suffix: ".parquet".to_string(),
        options: TableParquetOptions::default(),
        sorting: None,
        buffer_size: 10 * 1024 * 1024,
        max_rows_per_file: 50_000_000,
    }
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
    write_partition(
        task,
        test_stream(schema, batches),
        reservation,
        ParquetWriteMetrics::new(&metrics, partition),
    )
    .await
}

/// A row of the manifest: partition, attempt, path, size, and number of rows.
type ManifestRow = (u64, u64, Option<String>, u64, u64);

fn manifest_rows(manifest: &RecordBatch) -> Result<Vec<ManifestRow>> {
    use datafusion_common::cast::{as_string_array, as_uint64_array};

    let partition = as_uint64_array(manifest.column(0))?;
    let attempt = as_uint64_array(manifest.column(1))?;
    let path = as_string_array(manifest.column(2))?;
    let size = as_uint64_array(manifest.column(3))?;
    let num_rows = as_uint64_array(manifest.column(4))?;
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
    let result = write_partition(
        test_task(Arc::clone(&store), 0),
        input,
        MemoryConsumer::new("test").register(&pool),
        ParquetWriteMetrics::new(&metrics, 0),
    )
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
    write_partition(
        test_task(Arc::clone(&store), 0),
        test_stream(&schema, vec![int_batch(&schema, 0..3)?]),
        MemoryConsumer::new("test").register(&pool),
        ParquetWriteMetrics::new(&metrics, 0),
    )
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
