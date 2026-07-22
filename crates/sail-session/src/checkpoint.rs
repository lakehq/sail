use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::StreamExt;
use object_store::path::Path;
use sail_cache::remote_checkpoint::{
    RemoteCheckpointDescriptor, RemoteCheckpointFile, RemoteCheckpointPartition,
    RemoteCheckpointRegistry,
};
use sail_common::config::ExecutionMode;
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_common_datafusion::session::job::JobService;
use sail_object_store::{
    ResolvedObjectStorePath, delete_object_store_prefix_objects, resolve_object_store_path,
};
use sail_physical_plan::remote_checkpoint::{
    LOCATION_COLUMN, PARTITION_COLUMN, ROW_COUNT_COLUMN, ROW_MARKER_COLUMN,
    RemoteCheckpointWriteExec, SIZE_COLUMN,
};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug)]
pub struct RemoteCheckpointService {
    root: Option<String>,
    session_namespace: String,
    mode: ExecutionMode,
    lifecycle: RwLock<RemoteCheckpointLifecycle>,
}

#[derive(Debug, Default)]
struct RemoteCheckpointLifecycle {
    closed: bool,
}

impl RemoteCheckpointService {
    pub fn new(root: Option<String>, mode: ExecutionMode) -> Self {
        Self {
            root,
            session_namespace: Uuid::new_v4().to_string(),
            mode,
            lifecycle: RwLock::new(RemoteCheckpointLifecycle::default()),
        }
    }

    pub async fn materialize(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        logical_schema: SchemaRef,
    ) -> Result<Arc<RemoteCheckpointDescriptor>> {
        // This read lock is a session lease: cleanup waits for every in-flight publication.
        let lifecycle = self.lifecycle.read().await;
        if lifecycle.closed {
            return Err(DataFusionError::Plan(
                "checkpoint session is stopping".to_string(),
            ));
        }
        let root = self.resolve_root(ctx.runtime_env().as_ref())?;
        let registry = ctx.extension::<RemoteCheckpointRegistry>()?;
        let relation_id = self.new_relation_id(registry.as_ref())?;
        let relation_prefix = self
            .session_prefix(root.prefix())
            .join(relation_id.as_str());
        let result = self
            .materialize_at(
                ctx,
                plan,
                logical_schema,
                relation_id.as_str(),
                &root,
                relation_prefix.clone(),
            )
            .await;
        let output = match result {
            Ok(descriptor) => Ok(descriptor),
            Err(error) => {
                match delete_object_store_prefix_objects(root.store().as_ref(), &relation_prefix)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(internal_datafusion_err!(
                        "checkpoint failed: {error}; additionally failed to clean {relation_prefix}: {cleanup_error}"
                    )),
                }
            }
        };
        drop(lifecycle);
        output
    }

    pub async fn cleanup_session(
        &self,
        runtime_env: &RuntimeEnv,
        registry: &RemoteCheckpointRegistry,
    ) -> Result<()> {
        let mut lifecycle = self.lifecycle.write().await;
        lifecycle.closed = true;
        let storage_cleanup = if let Some(root) = &self.root {
            match resolve_object_store_path(runtime_env, root) {
                Ok(root) => match self.validate_store(&root) {
                    Ok(()) => {
                        delete_object_store_prefix_objects(
                            root.store().as_ref(),
                            &self.session_prefix(root.prefix()),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        let registry_cleanup = registry.clear();
        match (storage_cleanup, registry_cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(storage_error), Err(registry_error)) => Err(internal_datafusion_err!(
                "checkpoint storage cleanup failed: {storage_error}; registry cleanup failed: {registry_error}"
            )),
        }
    }

    async fn materialize_at(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        logical_schema: SchemaRef,
        relation_id: &str,
        root: &ResolvedObjectStorePath,
        relation_prefix: Path,
    ) -> Result<Arc<RemoteCheckpointDescriptor>> {
        let registry = ctx.extension::<RemoteCheckpointRegistry>()?;
        let storage_schema = checkpoint_storage_schema(&logical_schema);
        let partition_count = plan.output_partitioning().partition_count();
        let output_partitioning = plan.output_partitioning().clone();
        let output_ordering = plan.output_ordering().cloned();
        let checkpoint = RemoteCheckpointWriteExec::try_new(
            plan,
            root.object_store_url().clone(),
            relation_prefix.clone(),
            Arc::clone(&storage_schema),
        )?;
        let service = ctx.extension::<JobService>()?;
        let stream = service.runner().execute(ctx, Arc::new(checkpoint)).await?;
        let partitions =
            collect_checkpoint_partitions(stream, partition_count, &relation_prefix).await?;
        let descriptor = RemoteCheckpointDescriptor {
            relation_id: relation_id.to_string(),
            object_store_url: root.object_store_url().clone(),
            prefix: relation_prefix,
            logical_schema,
            storage_schema,
            output_partitioning,
            output_ordering,
            partitions,
        };
        // Registry insertion is the publication point; readers never observe partial output.
        registry.insert(descriptor)?;
        registry.get(relation_id)?.ok_or_else(|| {
            internal_datafusion_err!(
                "checkpoint relation disappeared immediately after commit: {relation_id}"
            )
        })
    }

    fn resolve_root(&self, runtime_env: &RuntimeEnv) -> Result<ResolvedObjectStorePath> {
        let root = self.root.as_deref().ok_or_else(|| {
            DataFusionError::Plan(
                "checkpoint object-store root is not configured; set checkpoint.root".to_string(),
            )
        })?;
        let root = resolve_object_store_path(runtime_env, root)?;
        self.validate_store(&root)?;
        Ok(root)
    }

    fn validate_store(&self, root: &ResolvedObjectStorePath) -> Result<()> {
        let object_store_url = root.object_store_url().as_str();
        if matches!(
            self.mode,
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster
        ) && (object_store_url.starts_with("file:") || object_store_url.starts_with("memory:"))
        {
            return Err(DataFusionError::Plan(format!(
                "checkpoint.root must use shared object storage in cluster mode, not {object_store_url}"
            )));
        }
        Ok(())
    }

    fn session_prefix(&self, root: &Path) -> Path {
        // `v1` versions the object layout only; checkpoint discovery remains session-local.
        root.clone()
            .join("_sail")
            .join("checkpoints")
            .join("v1")
            .join(self.session_namespace.as_str())
    }

    fn new_relation_id(&self, registry: &RemoteCheckpointRegistry) -> Result<String> {
        for _ in 0..8 {
            let relation_id = Uuid::new_v4().to_string();
            if registry.get(&relation_id)?.is_none() {
                return Ok(relation_id);
            }
        }
        Err(internal_datafusion_err!(
            "failed to allocate a unique checkpoint relation ID"
        ))
    }
}

impl SessionExtension for RemoteCheckpointService {
    fn name() -> &'static str {
        "remote checkpoint service"
    }
}

fn checkpoint_storage_schema(logical_schema: &SchemaRef) -> SchemaRef {
    if logical_schema.fields().is_empty() {
        // A marker column lets Parquet carry row counts for zero-column relations.
        return Arc::new(Schema::new_with_metadata(
            vec![Field::new(ROW_MARKER_COLUMN, DataType::UInt8, false)],
            logical_schema.metadata().clone(),
        ));
    }
    // Positional storage names preserve duplicate logical names and make property rebinding
    // unambiguous.
    let fields = logical_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(format!("__sail_checkpoint_col_{index:05}")),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ))
}

async fn collect_checkpoint_partitions(
    mut stream: datafusion::execution::SendableRecordBatchStream,
    partition_count: usize,
    relation_prefix: &Path,
) -> Result<Vec<RemoteCheckpointPartition>> {
    let mut partitions = vec![None; partition_count];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_columns() != 4 {
            return Err(internal_datafusion_err!(
                "checkpoint returned {} metadata columns instead of 4",
                batch.num_columns()
            ));
        }
        let partition_values = checkpoint_u64_column(&batch, 0, PARTITION_COLUMN)?;
        let locations = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                internal_datafusion_err!("invalid checkpoint {LOCATION_COLUMN} column")
            })?;
        let sizes = checkpoint_u64_column(&batch, 2, SIZE_COLUMN)?;
        let row_counts = checkpoint_u64_column(&batch, 3, ROW_COUNT_COLUMN)?;
        for row in 0..batch.num_rows() {
            if partition_values.is_null(row) || sizes.is_null(row) || row_counts.is_null(row) {
                return Err(internal_datafusion_err!(
                    "checkpoint returned null required metadata"
                ));
            }
            let partition = usize::try_from(partition_values.value(row))
                .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
            let size = sizes.value(row);
            let row_count = row_counts.value(row);
            let file = if locations.is_null(row) {
                if size != 0 || row_count != 0 {
                    return Err(internal_datafusion_err!(
                        "empty checkpoint partition {partition} has nonzero metadata"
                    ));
                }
                None
            } else {
                if size == 0 || row_count == 0 {
                    return Err(internal_datafusion_err!(
                        "checkpoint partition {partition} has an empty file"
                    ));
                }
                let location = Path::parse(locations.value(row)).map_err(|error| {
                    internal_datafusion_err!("invalid checkpoint object location: {error}")
                })?;
                validate_attempt_location(relation_prefix, partition, &location)?;
                Some(RemoteCheckpointFile {
                    location,
                    size,
                    row_count,
                })
            };
            let Some(slot) = partitions.get_mut(partition) else {
                return Err(internal_datafusion_err!(
                    "checkpoint returned invalid partition {partition}"
                ));
            };
            if slot
                .replace(RemoteCheckpointPartition { partition, file })
                .is_some()
            {
                return Err(internal_datafusion_err!(
                    "checkpoint returned duplicate partition {partition}"
                ));
            }
        }
    }
    partitions
        .into_iter()
        .enumerate()
        .map(|(partition, value)| {
            value.ok_or_else(|| {
                internal_datafusion_err!("checkpoint did not return partition {partition}")
            })
        })
        .collect()
}

fn checkpoint_u64_column<'a>(
    batch: &'a datafusion::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a UInt64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| internal_datafusion_err!("invalid checkpoint {name} column"))
}

fn validate_attempt_location(
    relation_prefix: &Path,
    partition: usize,
    location: &Path,
) -> Result<()> {
    let partition_prefix = relation_prefix
        .clone()
        .join("attempts")
        .join(format!("partition-{partition:020}"));
    if !location.prefix_matches(&partition_prefix)
        || location.parts_count() != partition_prefix.parts_count() + 1
        || location
            .filename()
            .is_none_or(|filename| !filename.ends_with(".parquet"))
    {
        return Err(internal_datafusion_err!(
            "checkpoint returned unexpected object {location} for partition {partition}"
        ));
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionConfig;
    use datafusion_expr::{Extension, LogicalPlan};
    use object_store::memory::InMemory;
    use sail_execution::job_runner::LocalJobRunner;
    use sail_logical_plan::remote_checkpoint::RemoteCheckpointRelationNode;

    use super::*;
    use crate::planner::new_query_planner;

    #[test]
    fn storage_schema_has_unique_names_and_preserves_metadata() {
        let logical = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int64, false).with_metadata(
                    [("field-key".to_string(), "field-value".to_string())]
                        .into_iter()
                        .collect(),
                ),
                Field::new("value", DataType::Utf8, true),
            ],
            [("schema-key".to_string(), "schema-value".to_string())]
                .into_iter()
                .collect(),
        ));

        let storage = checkpoint_storage_schema(&logical);

        assert_eq!(storage.field(0).name(), "__sail_checkpoint_col_00000");
        assert_eq!(storage.field(1).name(), "__sail_checkpoint_col_00001");
        assert_eq!(storage.field(0).metadata(), logical.field(0).metadata());
        assert_eq!(storage.metadata(), logical.metadata());

        let empty = checkpoint_storage_schema(&Arc::new(Schema::empty()));
        assert_eq!(empty.fields().len(), 1);
        assert_eq!(empty.field(0).name(), ROW_MARKER_COLUMN);
        assert_eq!(empty.field(0).data_type(), &DataType::UInt8);
    }

    #[tokio::test]
    async fn publishes_descriptor_and_reads_checkpoint_as_parquet() -> Result<()> {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        runtime_env.register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
        let checkpoint = Arc::new(RemoteCheckpointService::new(
            Some("memory:///checkpoint-test".to_string()),
            ExecutionMode::Local,
        ));
        let registry = Arc::new(RemoteCheckpointRegistry::default());
        let config = SessionConfig::new()
            .with_extension(Arc::new(JobService::new(Box::new(LocalJobRunner::new()))))
            .with_extension(Arc::clone(&registry))
            .with_extension(Arc::clone(&checkpoint));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::clone(&runtime_env))
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let ctx = SessionContext::new_with_state(state);
        let logical_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int32, false)],
            [("schema-key".to_string(), "schema-value".to_string())]
                .into_iter()
                .collect(),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&logical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input = MemorySourceConfig::try_new_exec(
            &[vec![batch], vec![]],
            Arc::clone(&logical_schema),
            None,
        )?;

        let descriptor = checkpoint
            .materialize(&ctx, input, Arc::clone(&logical_schema))
            .await?;

        assert_eq!(descriptor.partitions.len(), 2);
        assert!(descriptor.partitions[0].file.is_some());
        assert!(descriptor.partitions[1].file.is_none());
        let store = runtime_env.object_store(&descriptor.object_store_url)?;
        let objects = store
            .list(Some(&descriptor.prefix))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, object_store::Error>>()
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].location,
            descriptor.partitions[0]
                .file
                .as_ref()
                .expect("non-empty partition must have a file")
                .location
        );

        let logical = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteCheckpointRelationNode::try_new(
                descriptor.relation_id.clone(),
                Arc::clone(&descriptor.logical_schema),
            )?),
        });
        let session_state = ctx.state();
        let read = session_state
            .query_planner()
            .create_physical_plan(&logical, &session_state)
            .await?;
        let batches = collect(read, ctx.task_ctx()).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(batches[0].schema().field(0).name(), "value");
        assert_eq!(
            batches[0].schema().metadata().get("schema-key"),
            Some(&"schema-value".to_string())
        );

        checkpoint
            .cleanup_session(runtime_env.as_ref(), registry.as_ref())
            .await?;
        assert!(registry.get(&descriptor.relation_id)?.is_none());
        assert!(store.list(Some(&descriptor.prefix)).next().await.is_none());
        let error = checkpoint
            .materialize(
                &ctx,
                Arc::new(EmptyExec::new(Arc::clone(&logical_schema))),
                Arc::clone(&logical_schema),
            )
            .await
            .expect_err("materialization must not restart after session cleanup");
        assert!(error.to_string().contains("checkpoint session is stopping"));
        Ok(())
    }
}
