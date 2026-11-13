use arrow::array::{Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatchOptions;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;

use super::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn batch(values: Vec<i64>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(values))],
    )?)
}

pub(super) fn runtime(bytes: usize) -> Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)))
        .build_arc()
}

fn registry() -> Arc<ReplayRegistry> {
    Arc::new(ReplayRegistry::new(
        ReplayEpoch {
            session_id: "test".into(),
            job_id: 1,
        },
        CancellationToken::new(),
    ))
}

fn id() -> ReplayId {
    ReplayId {
        scope: Uuid::new_v4(),
        ordinal: 0,
    }
}

pub(super) fn assert_clean(runtime: &RuntimeEnv) -> Result<()> {
    assert_eq!(runtime.memory_pool.reserved(), 0);
    assert_eq!(runtime.disk_manager.used_disk_space(), 0);
    for directory in runtime.disk_manager.temp_dir_paths() {
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn replay_corrupt_spool_is_terminal_for_all_readers() -> Result<()> {
    for length in [0, 7] {
        let runtime = runtime(8 * 1024 * 1024)?;
        let registry = registry();
        let id = id();
        registry.register(id, schema(), Partitioning::UnknownPartitioning(1), &runtime)?;
        registry.begin(id)?;
        let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
        builder.append(batch(vec![11, 22, 33])?)?;
        let data = builder.seal()?;
        let path = data
            .file
            .as_ref()
            .and_then(|file| file.path())
            .ok_or_else(|| internal_datafusion_err!("expected local spill file"))?
            .to_owned();
        registry.publish(id, vec![Arc::new(data)])?;
        let mut second = registry.open_reader(id, 0, &runtime)?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)?
            .set_len(length)?;
        assert!(
            registry
                .open_reader(id, 0, &runtime)?
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert!(matches!(second.next().await, Some(Err(_))));
        assert!(registry.begin(id).is_err());
        assert!(registry.open_reader(id, 0, &runtime).is_err());
        drop(second);
        assert!(!path.exists());
        assert_clean(&runtime)?;
    }
    Ok(())
}

#[tokio::test]
async fn replay_spool_reopens_independent_readers() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    let expected = vec![batch(vec![7, 1, 7])?, batch(vec![8, 9])?];
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    for batch in &expected {
        builder.append(batch.clone())?;
    }
    let data = builder.seal()?;
    assert_eq!(runtime.memory_pool.reserved(), 0);
    assert_eq!(data.row_count(), 5);
    let mut first = data.open_reader(&runtime)?;
    let second = data.open_reader(&runtime)?;
    assert_eq!(first.next().await.transpose()?, Some(expected[0].clone()));
    assert_eq!(second.try_collect::<Vec<_>>().await?, expected);
    assert_eq!(
        first.try_collect::<Vec<_>>().await?,
        vec![expected[1].clone()]
    );
    assert_eq!(
        data.open_reader(&runtime)?.try_collect::<Vec<_>>().await?,
        expected
    );
    drop(data);
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_spool_low_memory_large_input_spills() -> Result<()> {
    let runtime = runtime(2 * 1024 * 1024)?;
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    // 16 MiB of values under a 2 MiB pool, without a resident batch vector.
    for i in 0..256 {
        builder.append(batch(vec![i; 8192])?)?;
    }
    let data = builder.seal()?;
    assert!(runtime.disk_manager.used_disk_space() > 16 * 1024 * 1024);
    assert_eq!(runtime.memory_pool.reserved(), 0);
    for _ in 0..2 {
        let mut stream = data.open_reader(&runtime)?;
        let mut rows = 0;
        let mut sum = 0;
        while let Some(batch) = stream.next().await.transpose()? {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| internal_datafusion_err!("expected i64"))?;
            rows += values.len();
            sum += values.values().iter().sum::<i64>();
            assert!(runtime.memory_pool.reserved() <= 2 * 1024 * 1024);
        }
        assert_eq!(rows, 256 * 8192);
        assert_eq!(sum, (0..256i64).sum::<i64>() * 8192);
    }
    drop(data);
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_spool_splits_large_batches_and_preserves_views() -> Result<()> {
    let runtime = runtime(16 * 1024 * 1024)?;
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
    let values: StringViewArray = (0..20_000)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(format!("long non-inline value {i}"))
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])?;
    let mut builder = ReplayPartitionBuilder::try_new(schema.clone(), runtime.clone(), 0)?;
    builder.append(batch.clone())?;
    let data = builder.seal()?;
    let batches = data.open_reader(&runtime)?.try_collect::<Vec<_>>().await?;
    assert!(batches.len() > 1);
    assert_eq!(arrow::compute::concat_batches(&schema, &batches)?, batch);
    drop(data);
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_spool_empty_and_zero_column_rows() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    let data = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?.seal()?;
    assert_eq!(data.row_count(), 0);
    assert!(data.file.is_none());
    assert!(
        data.open_reader(&runtime)?
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    drop(data);
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        empty_schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(17)),
    )?;
    let mut builder = ReplayPartitionBuilder::try_new(empty_schema, runtime.clone(), 0)?;
    builder.append(batch.clone())?;
    let data = builder.seal()?;
    assert_eq!(
        data.open_reader(&runtime)?.try_collect::<Vec<_>>().await?,
        vec![batch]
    );
    drop(data);
    assert_clean(&runtime)
}

#[test]
fn replay_spool_abort_schema_error_and_drop_clean_files() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    for explicit_abort in [false, true] {
        let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
        builder.append(batch(vec![1, 2, 3])?)?;
        if explicit_abort {
            builder.abort();
        }
        drop(builder);
        assert_clean(&runtime)?;
    }
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    builder.append(batch(vec![1])?)?;
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["a"]))],
    )?;
    assert!(builder.append(wrong).is_err());
    assert!(builder.seal().is_err());
    assert_clean(&runtime)
}

#[test]
fn replay_spool_disk_quota_aborts() -> Result<()> {
    let runtime = RuntimeEnvBuilder::new()
        .with_max_temp_directory_size(512)
        .build_arc()?;
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    assert!(builder.append(batch(vec![1; 8192])?).is_err());
    assert!(builder.seal().is_err());
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_spool_large_single_row_grows_workspace_and_reopens() -> Result<()> {
    let runtime = runtime(8 * 1024 * 1024)?;
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)]))],
    )?;
    let mut builder = ReplayPartitionBuilder::try_new(schema, runtime.clone(), 0)?;
    builder.append(batch.clone())?;
    assert!(builder.max_batch_bytes > MAX_BATCH_BYTES);
    let data = builder.seal()?;
    assert_eq!(runtime.memory_pool.reserved(), 0);
    for _ in 0..2 {
        assert_eq!(
            data.open_reader(&runtime)?.try_collect::<Vec<_>>().await?,
            vec![batch.clone()]
        );
    }
    drop(data);
    assert_clean(&runtime)
}

#[test]
fn replay_spool_large_single_row_insufficient_memory_cannot_publish() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    let registry = registry();
    let id = id();
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    registry.register(
        id,
        schema.clone(),
        Partitioning::UnknownPartitioning(1),
        &runtime,
    )?;
    registry.begin(id)?;
    let mut builder = ReplayPartitionBuilder::try_new(schema.clone(), runtime.clone(), 0)?;
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)]))],
    )?;
    // Input backing fits alongside the initial workspace; the enlarged IPC
    // workspace does not. No completed partition can escape this failure.
    assert!(runtime.memory_pool.reserved() + batch.get_array_memory_size() < 4 * 1024 * 1024);
    assert!(matches!(
        builder.append(batch),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(builder.seal().is_err());
    assert!(registry.open_reader(id, 0, &runtime).is_err());
    registry.retire();
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_registry_freezes_layout_and_publication() -> Result<()> {
    let runtime = runtime(8 * 1024 * 1024)?;
    let registry = registry();
    let id = id();
    assert!(registry.register(id, schema(), Partitioning::UnknownPartitioning(2), &runtime)?);
    assert!(!registry.register(id, schema(), Partitioning::UnknownPartitioning(2), &runtime)?);
    assert!(
        registry
            .register(id, schema(), Partitioning::RoundRobinBatch(2), &runtime)
            .is_err()
    );
    assert!(
        registry
            .register(
                id,
                Arc::new(Schema::empty()),
                Partitioning::UnknownPartitioning(2),
                &runtime
            )
            .is_err()
    );
    assert!(registry.open_reader(id, 0, &runtime).is_err());
    registry.begin(id)?;
    let mut partitions = vec![];
    for value in [10, 20] {
        let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
        builder.append(batch(vec![value])?)?;
        partitions.push(Arc::new(builder.seal()?));
    }
    assert!(registry.open_reader(id, 0, &runtime).is_err());
    registry.publish(id, partitions)?;
    assert!(registry.begin(id).is_err());
    assert!(registry.publish(id, vec![]).is_err());
    assert_eq!(
        registry
            .open_reader(id, 1, &runtime)?
            .try_collect::<Vec<_>>()
            .await?,
        vec![batch(vec![20])?]
    );
    registry.retire();
    assert!(
        registry
            .register(id, schema(), Partitioning::UnknownPartitioning(2), &runtime)
            .is_err()
    );
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_registry_root_drop_cancels_leases_and_cleans() -> Result<()> {
    let runtime = runtime(8 * 1024 * 1024)?;
    let registry = registry();
    let id = id();
    registry.register(id, schema(), Partitioning::UnknownPartitioning(1), &runtime)?;
    registry.begin(id)?;
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    builder.append(batch(vec![1, 2])?)?;
    registry.publish(id, vec![Arc::new(builder.seal()?)])?;
    let mut reader = registry.open_reader(id, 0, &runtime)?;
    let root = registry.wrap_stream(
        Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::pending(),
        )),
        true,
    );
    drop(root);
    assert!(registry.open_reader(id, 0, &runtime).is_err());
    assert!(matches!(reader.next().await, Some(Err(_))));
    drop(reader);
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_registry_cancel_before_publish_is_terminal() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    let registry = registry();
    let id = id();
    registry.register(id, schema(), Partitioning::UnknownPartitioning(1), &runtime)?;
    registry.begin(id)?;
    let mut builder = ReplayPartitionBuilder::try_new(schema(), runtime.clone(), 0)?;
    builder.append(batch(vec![1])?)?;
    registry.cancellation().cancel();
    assert!(
        registry
            .publish(id, vec![Arc::new(builder.seal()?)])
            .is_err()
    );
    assert!(registry.open_reader(id, 0, &runtime).is_err());
    registry.retire();
    assert_clean(&runtime)
}
