use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};

use super::*;

fn id() -> ReplayId {
    ReplayId {
        scope: Default::default(),
        ordinal: 1,
    }
}
fn runtime() -> Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(12 * 1024 * 1024)))
        .build_arc()
}
fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["z".repeat(1024 * 1024)]))],
    )?)
}
fn clean(runtime: &RuntimeEnv) {
    assert_eq!(runtime.memory_pool.reserved(), 0);
    assert_eq!(runtime.disk_manager.used_disk_space(), 0);
}
async fn publish(
    store: Arc<dyn ObjectStore>,
    runtime: Arc<RuntimeEnv>,
    prefix: Path,
    job: Arc<ReplayJob>,
    count: usize,
) -> Result<SchemaRef> {
    let batch = batch()?;
    let mut sink = ReplayStorageSink::new(store, prefix, id(), batch.schema(), runtime, 0, job)?;
    for _ in 0..count {
        sink.write(batch.clone()).await?;
    }
    Box::new(sink).commit().await?;
    Ok(batch.schema())
}

#[tokio::test]
async fn replay_storage_streams_more_than_pool_and_reopens_large_rows() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let runtime = runtime()?;
    let prefix = Path::from("session/job-1/stage-2/partition-0/attempt-0/channel-0/replay");
    let job = Arc::new(ReplayJob::default());
    let schema = publish(
        store.clone(),
        runtime.clone(),
        prefix.clone(),
        job.clone(),
        32,
    )
    .await?;
    clean(&runtime);
    // Data lives in the object store, not in a batch vector retained by the sink.
    for meta in store.list(Some(&prefix)).try_collect::<Vec<_>>().await? {
        assert!(meta.size <= CHUNK_BYTES as u64 + 4);
    }
    let mut abandoned = fetch(
        store.clone(),
        prefix.clone(),
        id(),
        schema.clone(),
        runtime.clone(),
        job.clone(),
    );
    assert!(
        abandoned
            .next()
            .await
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .is_some()
    );
    for _ in 0..2 {
        let mut reader = fetch(
            store.clone(),
            prefix.clone(),
            id(),
            schema.clone(),
            runtime.clone(),
            job.clone(),
        );
        let mut count = 0;
        while let Some(batch) = reader
            .next()
            .await
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal_datafusion_err!("expected string"))?;
            assert_eq!(array.len(), 1);
            assert_eq!(array.value(0).len(), 1024 * 1024);
            count += 1;
        }
        assert_eq!(count, 32);
    }
    drop(abandoned);
    job.close();
    job.wait().await;
    remove_prefix(store.clone(), &prefix).await?;
    assert!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    clean(&runtime);
    Ok(())
}

#[tokio::test]
async fn replay_storage_missing_and_corrupt_publication_is_an_error() -> Result<()> {
    for kind in [0, 1, 2, 3] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let runtime = runtime()?;
        let prefix = Path::from("replay");
        let job = Arc::new(ReplayJob::default());
        let schema = publish(
            store.clone(),
            runtime.clone(),
            prefix.clone(),
            job.clone(),
            2,
        )
        .await?;
        let path = prefix.clone().join(if kind == 0 {
            "manifest.json"
        } else {
            "chunk-00000000000000000001"
        });
        if kind <= 1 {
            store.delete(&path).await?;
        } else if kind == 2 {
            store
                .put(&path, PutPayload::from(vec![0; CHUNK_BYTES + 4]))
                .await?;
        } else {
            store.put(&path, PutPayload::from(vec![0; 7])).await?;
        }
        assert!(
            fetch(
                store.clone(),
                prefix.clone(),
                id(),
                schema,
                runtime.clone(),
                job.clone()
            )
            .try_collect::<Vec<_>>()
            .await
            .is_err()
        );
        job.close();
        job.wait().await;
        remove_prefix(store, &prefix).await?;
        clean(&runtime);
    }
    Ok(())
}

#[tokio::test]
async fn replay_storage_close_rejects_late_sinks_and_publication() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let runtime = runtime()?;
    let prefix = Path::from("replay");
    let job = Arc::new(ReplayJob::default());
    let batch = batch()?;
    let mut sink = ReplayStorageSink::new(
        store.clone(),
        prefix.clone(),
        id(),
        batch.schema(),
        runtime.clone(),
        0,
        job.clone(),
    )?;
    sink.write(batch.clone()).await?;
    job.close();
    assert!(sink.write(batch.clone()).await.is_err());
    assert!(Box::new(sink).commit().await.is_err());
    job.wait().await;
    assert!(
        ReplayStorageSink::new(
            store.clone(),
            prefix.clone(),
            id(),
            batch.schema(),
            runtime.clone(),
            0,
            job
        )
        .is_err()
    );
    assert!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    clean(&runtime);
    Ok(())
}

#[tokio::test]
async fn replay_storage_abandoned_commit_waits_for_owned_upload_cleanup() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_put_per_call: std::time::Duration::from_millis(30),
            ..Default::default()
        },
    ));
    let runtime = runtime()?;
    let prefix = Path::from("replay");
    let job = Arc::new(ReplayJob::default());
    let batch = batch()?;
    let mut sink = ReplayStorageSink::new(
        store.clone(),
        prefix.clone(),
        id(),
        batch.schema(),
        runtime.clone(),
        0,
        job.clone(),
    )?;
    sink.write(batch).await?;
    let task = tokio::spawn(Box::new(sink).commit());
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
        Ok::<_, DataFusionError>(())
    })
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))??;
    task.abort();
    let _ = task.await;
    job.close();
    job.wait().await;
    assert!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    clean(&runtime);
    Ok(())
}
