#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;
    use sail_common::cache_id::CacheId;

    use crate::local_cache_store::LocalCacheStore;
    use crate::plan::{CacheReadExec, CacheWriteExec};

    /// Verifies that CacheWriteExec stores batches and CacheReadExec retrieves them from the same store.
    #[tokio::test]
    async fn test_cache_write_then_read() {
        let cache_id = CacheId::from(1_u64);
        let partition = 0usize;
        let ctx = Arc::new(TaskContext::default());
        // The store is shared between write and read, simulating the TaskRunner injecting
        // the same LocalCacheStore into both nodes on a worker.
        let store = Arc::new(LocalCacheStore::new());

        // Build a simple one-column batch to use as the source data.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // --- Write ---
        // MemorySourceConfig holds the data; DataSourceExec wraps it as an ExecutionPlan.
        let source =
            MemorySourceConfig::try_new(&[vec![batch.clone()]], schema.clone(), None).unwrap();
        let source_exec = Arc::new(DataSourceExec::new(Arc::new(source)));
        // CacheWriteExec drains the child plan and stores all batches in the cache store.
        let write_exec = CacheWriteExec::new(source_exec, store.clone(), cache_id);
        let mut write_stream = write_exec.execute(partition, ctx.clone()).unwrap();
        while let Some(b) = write_stream.next().await {
            let _: RecordBatch = b.unwrap();
        }

        // --- Read ---
        // In production this injection is done by TaskRunner after the plan is deserialized on a worker.
        let mut read_exec = CacheReadExec::new(cache_id, schema.clone(), 1);
        read_exec.set_cache_store(store.clone());
        let mut read_stream = read_exec.execute(partition, ctx).unwrap();
        let result: RecordBatch = read_stream.next().await.unwrap().unwrap();

        assert_eq!(result, batch);
    }
}
