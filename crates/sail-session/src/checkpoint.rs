use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::object_store::ObjectStoreExt;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::StreamExt;
use sail_common_datafusion::array::record_batch::write_record_batches_file;
use sail_common_datafusion::checkpoint::ReliableCheckpointExec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::checkpoint::{CheckpointStore, ReliableCheckpoint};
use sail_common_datafusion::session::job::JobService;
use sail_object_store::{delete_object_store_prefix, resolve_object_store_path};

#[derive(Debug, Default)]
pub struct ObjectStoreCheckpointStore;

#[async_trait]
impl CheckpointStore for ObjectStoreCheckpointStore {
    async fn write_reliable_checkpoint(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        path: &str,
        schema: SchemaRef,
    ) -> Result<ReliableCheckpoint> {
        let runtime_env = ctx.runtime_env();
        let checkpoint_path = resolve_object_store_path(runtime_env.as_ref(), path)?;
        let partition_count = plan.output_partitioning().partition_count();
        if partition_count == 0 {
            let bytes = write_record_batches_file(&[], schema.as_ref())?;
            let file_path = checkpoint_path.child("part-00000.arrow");
            let file = checkpoint_path.put_bytes(&file_path, bytes).await?;
            return Ok(ReliableCheckpoint::new(
                checkpoint_path.object_store_url().clone(),
                vec![file],
            ));
        }

        let checkpoint_exec = ReliableCheckpointExec::new(
            plan,
            checkpoint_path.object_store_url().clone(),
            checkpoint_path.prefix().clone(),
        );
        let service = ctx.extension::<JobService>()?;
        let mut stream = service
            .runner()
            .execute(ctx, Arc::new(checkpoint_exec))
            .await?;
        let mut completed = vec![false; partition_count];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_columns() != 1 {
                return Err(internal_datafusion_err!(
                    "reliable checkpoint returned {} columns instead of 1",
                    batch.num_columns()
                ));
            }
            let partitions = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!("invalid reliable checkpoint partition column")
                })?;
            for row in 0..batch.num_rows() {
                if partitions.is_null(row) {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned a null partition"
                    ));
                }
                let partition = usize::try_from(partitions.value(row)).map_err(|_| {
                    internal_datafusion_err!("reliable checkpoint partition index is too large")
                })?;
                let Some(done) = completed.get_mut(partition) else {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned invalid partition {partition}"
                    ));
                };
                if std::mem::replace(done, true) {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned duplicate partition {partition}"
                    ));
                }
            }
        }

        let mut files = Vec::with_capacity(partition_count);
        for (partition, completed) in completed.into_iter().enumerate() {
            if !completed {
                return Err(internal_datafusion_err!(
                    "reliable checkpoint did not return partition {partition}"
                ));
            }
            let file_path = checkpoint_path.child(&format!("part-{partition:05}.arrow"));
            files.push(
                checkpoint_path
                    .store()
                    .head(&file_path)
                    .await
                    .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?,
            );
        }

        Ok(ReliableCheckpoint::new(
            checkpoint_path.object_store_url().clone(),
            files,
        ))
    }

    async fn cleanup_checkpoint_path(&self, ctx: &SessionContext, path: &str) -> Result<()> {
        let runtime_env = ctx.runtime_env();
        delete_object_store_prefix(runtime_env.as_ref(), path).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion_common::{DataFusionError, internal_datafusion_err};

    use super::*;

    #[tokio::test]
    async fn cleanup_checkpoint_path_deletes_prefix_files() -> Result<()> {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| internal_datafusion_err!("{e}"))?
            .as_nanos();
        let root = std::env::temp_dir().join(format!("sail-session-checkpoint-cleanup-{suffix}"));
        let checkpoint = root.join("checkpoint");
        let nested = checkpoint.join("nested");
        tokio::fs::create_dir_all(&nested)
            .await
            .map_err(DataFusionError::IoError)?;
        tokio::fs::write(checkpoint.join("part-00000.arrow"), b"checkpoint")
            .await
            .map_err(DataFusionError::IoError)?;
        tokio::fs::write(nested.join("part-00001.arrow"), b"checkpoint")
            .await
            .map_err(DataFusionError::IoError)?;
        tokio::fs::write(root.join("outside.arrow"), b"outside")
            .await
            .map_err(DataFusionError::IoError)?;

        let ctx = SessionContext::new();
        ObjectStoreCheckpointStore
            .cleanup_checkpoint_path(&ctx, checkpoint.to_string_lossy().as_ref())
            .await?;

        assert!(!checkpoint.join("part-00000.arrow").exists());
        assert!(!nested.join("part-00001.arrow").exists());
        assert!(root.join("outside.arrow").exists());

        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }
}
