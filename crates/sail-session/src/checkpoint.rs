use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStoreExt};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::{StreamExt, TryStreamExt};
use sail_cache::checkpoint::{CheckpointStore, ReliableCheckpoint};
use sail_common_datafusion::array::record_batch::write_record_batches_file;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_object_store::{
    ResolvedObjectStorePath, delete_object_store_prefix, resolve_object_store_path,
};
use sail_physical_plan::checkpoint::ReliableCheckpointExec;

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
        let mut attempts = vec![None; partition_count];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_columns() != 2 {
                return Err(internal_datafusion_err!(
                    "reliable checkpoint returned {} columns instead of 2",
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
            let locations = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!("invalid reliable checkpoint location column")
                })?;
            for row in 0..batch.num_rows() {
                if partitions.is_null(row) || locations.is_null(row) {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned null metadata"
                    ));
                }
                let partition = usize::try_from(partitions.value(row)).map_err(|_| {
                    internal_datafusion_err!("reliable checkpoint partition index is too large")
                })?;
                let location = Path::parse(locations.value(row)).map_err(|error| {
                    internal_datafusion_err!(
                        "reliable checkpoint returned invalid location: {error}"
                    )
                })?;
                validate_checkpoint_attempt_location(&checkpoint_path, partition, &location)?;
                let Some(attempt) = attempts.get_mut(partition) else {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned invalid partition {partition}"
                    ));
                };
                if attempt.replace(location).is_some() {
                    return Err(internal_datafusion_err!(
                        "reliable checkpoint returned duplicate partition {partition}"
                    ));
                }
            }
        }

        let attempts = attempts
            .into_iter()
            .enumerate()
            .map(|(partition, attempt)| {
                attempt.ok_or_else(|| {
                    internal_datafusion_err!(
                        "reliable checkpoint did not return partition {partition}"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let files = commit_checkpoint_attempts(&checkpoint_path, attempts).await?;

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

fn validate_checkpoint_attempt_location(
    checkpoint_path: &ResolvedObjectStorePath,
    partition: usize,
    location: &Path,
) -> Result<()> {
    let temporary = checkpoint_path.child("_temporary");
    let filename = format!("part-{partition:05}.arrow");
    if !location.prefix_matches(&temporary)
        || location.parts_count() != temporary.parts_count() + 2
        || location.filename() != Some(filename.as_str())
    {
        return Err(internal_datafusion_err!(
            "reliable checkpoint returned unexpected attempt location {location} for partition {partition}"
        ));
    }
    Ok(())
}

async fn commit_checkpoint_attempts(
    checkpoint_path: &ResolvedObjectStorePath,
    attempts: Vec<Path>,
) -> Result<Vec<ObjectMeta>> {
    let mut files = Vec::with_capacity(attempts.len());
    for (partition, attempt) in attempts.into_iter().enumerate() {
        let file_path = checkpoint_path.child(&format!("part-{partition:05}.arrow"));
        checkpoint_path
            .store()
            .copy(&attempt, &file_path)
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        files.push(
            checkpoint_path
                .store()
                .head(&file_path)
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?,
        );
    }

    let temporary = checkpoint_path.child("_temporary");
    let temporary_files = checkpoint_path
        .store()
        .list(Some(&temporary))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
    for file in temporary_files {
        match checkpoint_path.store().delete(&file.location).await {
            Ok(()) | Err(datafusion::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::object_store::memory::InMemory;
    use datafusion_common::{DataFusionError, internal_datafusion_err};

    use super::*;

    #[tokio::test]
    async fn late_checkpoint_attempt_cannot_overwrite_committed_winner() -> Result<()> {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| internal_datafusion_err!("{e}"))?
            .as_nanos();
        let ctx = SessionContext::new();
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        ctx.runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
        let checkpoint = resolve_object_store_path(
            ctx.runtime_env().as_ref(),
            &format!("memory:///{suffix}/checkpoint"),
        )?;
        let winner = checkpoint
            .child("_temporary")
            .join("winner")
            .join("part-00000.arrow");
        let stale = checkpoint
            .child("_temporary")
            .join("stale")
            .join("part-00000.arrow");
        checkpoint.put_bytes(&winner, b"winner".to_vec()).await?;
        checkpoint.put_bytes(&stale, b"stale".to_vec()).await?;

        let files = commit_checkpoint_attempts(&checkpoint, vec![winner.clone()]).await?;

        let committed = checkpoint.child("part-00000.arrow");
        assert_eq!(files[0].location, committed);
        let winner_head = checkpoint.store().head(&winner).await;
        assert!(
            matches!(
                &winner_head,
                Err(datafusion::object_store::Error::NotFound { .. })
            ),
            "winner temporary object was not deleted: {winner_head:?}"
        );

        checkpoint.put_bytes(&stale, b"late stale".to_vec()).await?;
        let bytes = checkpoint
            .store()
            .get(&committed)
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?
            .bytes()
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        assert_eq!(bytes.as_ref(), b"winner");
        Ok(())
    }

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
