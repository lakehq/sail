use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::{execute_stream_partitioned, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use futures::StreamExt;
use sail_common_datafusion::array::record_batch::write_record_batches_file;
use sail_common_datafusion::session::checkpoint::{CheckpointStore, ReliableCheckpoint};
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
        let streams = execute_stream_partitioned(plan, ctx.task_ctx())?;
        let mut files = vec![];

        for (file_index, mut stream) in streams.into_iter().enumerate() {
            let mut batches = vec![];
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            if batches.iter().all(|batch| batch.num_rows() == 0) {
                continue;
            }
            let bytes = write_record_batches_file(&batches, schema.as_ref())?;
            let file_path = checkpoint_path.child(&format!("part-{file_index:05}.arrow"));
            files.push(checkpoint_path.put_bytes(&file_path, bytes).await?);
        }

        if files.is_empty() {
            let bytes = write_record_batches_file(&[], schema.as_ref())?;
            let file_path = checkpoint_path.child("part-00000.arrow");
            files.push(checkpoint_path.put_bytes(&file_path, bytes).await?);
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

    use datafusion_common::{internal_datafusion_err, DataFusionError};

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
