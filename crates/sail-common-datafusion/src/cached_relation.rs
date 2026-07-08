use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    collect_partitioned, with_new_children_if_necessary, DisplayAs, DisplayFormatType,
    ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_common::{
    internal_datafusion_err, DFSchema, DFSchemaRef, DataFusionError, Result, Statistics,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use futures::future::BoxFuture;
use object_store::ObjectMeta;
use sail_common::spec;

use crate::array::record_batch::{read_record_batches, write_record_batches};
use crate::extension::{SessionExtension, SessionExtensionAccessor};
use crate::session::checkpoint::{CheckpointStoreService, ReliableCheckpoint};

#[derive(Debug, Clone)]
enum CachedRelationCleanup {
    ObjectStorePath(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CachedRelationNode {
    relation_id: String,
    schema: DFSchemaRef,
}

impl CachedRelationNode {
    fn try_new(relation_id: String, schema: SchemaRef) -> Result<Self> {
        let schema = Arc::new(DFSchema::try_from(schema.as_ref().clone())?);
        Ok(Self {
            relation_id,
            schema,
        })
    }

    pub fn relation_id(&self) -> &str {
        &self.relation_id
    }
}

impl PartialOrd for CachedRelationNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.relation_id.partial_cmp(&other.relation_id)
    }
}

impl UserDefinedLogicalNodeCore for CachedRelationNode {
    fn name(&self) -> &str {
        "CachedRelation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CachedRelation: relation_id={}", self.relation_id)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return Err(internal_datafusion_err!(
                "CachedRelation does not support expressions"
            ));
        }
        if !inputs.is_empty() {
            return Err(internal_datafusion_err!(
                "CachedRelation does not support inputs"
            ));
        }
        Ok(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    schema: SchemaRef,
    data: Arc<tokio::sync::Mutex<CachedRelationData>>,
    cleanup: Option<CachedRelationCleanup>,
}

#[derive(Debug, Clone)]
enum CachedRelationData {
    Materialized(CachedRelationMaterialized),
    Pending(CachedRelationPending),
}

#[derive(Debug, Clone)]
enum CachedRelationMaterialized {
    Local(CachedRelationLocalMaterialized),
    Reliable {
        schema: SchemaRef,
        checkpoint: ReliableCheckpoint,
    },
}

#[derive(Debug, Clone)]
struct CachedRelationLocalMaterialized {
    schema: SchemaRef,
    memory_partitions: Option<Arc<Vec<Vec<RecordBatch>>>>,
    serialized_memory_partitions: Option<Arc<Vec<Vec<u8>>>>,
    disk_partitions: Option<Arc<Vec<CachedRelationDiskPartition>>>,
}

#[derive(Debug, Clone)]
struct CachedRelationDiskPartition {
    files: Vec<RefCountedTempFile>,
}

#[derive(Debug, Clone)]
struct CachedRelationPending {
    plan: Arc<dyn ExecutionPlan>,
    target: CachedRelationPendingTarget,
}

#[derive(Debug, Clone)]
enum CachedRelationPendingTarget {
    Local { storage_level: spec::StorageLevel },
    Reliable { path: String },
}

#[derive(Debug, Clone)]
struct PendingCachedRelationExec {
    relation_id: String,
    relation: CachedRelation,
    properties: Arc<PlanProperties>,
}

impl PendingCachedRelationExec {
    fn new(relation_id: String, relation: CachedRelation, plan: Arc<dyn ExecutionPlan>) -> Self {
        let schema = plan.schema();
        let partition_count = plan.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            relation_id,
            relation,
            properties,
        }
    }

    async fn materialize(&self, ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
        self.relation.materialize_pending(ctx).await
    }
}

impl DisplayAs for PendingCachedRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PendingCachedRelationExec: relation_id={}",
            self.relation_id
        )
    }
}

impl ExecutionPlan for PendingCachedRelationExec {
    fn name(&self) -> &str {
        "PendingCachedRelationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(internal_datafusion_err!(
                "PendingCachedRelationExec should have no children"
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(internal_datafusion_err!(
            "PendingCachedRelationExec should be materialized before execution"
        ))
    }
}

impl CachedRelation {
    pub async fn new_local_checkpoint(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        storage_level: spec::StorageLevel,
    ) -> Result<Self> {
        let data = materialize_local_checkpoint(ctx, plan, storage_level).await?;
        let schema = data.schema();
        Ok(Self {
            schema,
            data: Arc::new(tokio::sync::Mutex::new(data)),
            cleanup: None,
        })
    }

    pub async fn new_reliable_checkpoint(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        path: &str,
    ) -> Result<Self> {
        let data = materialize_reliable_checkpoint(ctx, plan, path).await?;
        let schema = data.schema();
        Ok(Self {
            schema,
            data: Arc::new(tokio::sync::Mutex::new(data)),
            cleanup: Some(CachedRelationCleanup::ObjectStorePath(path.to_string())),
        })
    }

    pub fn new_pending_local_checkpoint(
        plan: Arc<dyn ExecutionPlan>,
        storage_level: spec::StorageLevel,
    ) -> Self {
        let schema = plan.schema();
        Self {
            schema,
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    target: CachedRelationPendingTarget::Local { storage_level },
                },
            ))),
            cleanup: None,
        }
    }

    pub fn new_pending_reliable_checkpoint(plan: Arc<dyn ExecutionPlan>, path: String) -> Self {
        let cleanup_path = path.clone();
        let schema = plan.schema();
        Self {
            schema,
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    target: CachedRelationPendingTarget::Reliable { path },
                },
            ))),
            cleanup: Some(CachedRelationCleanup::ObjectStorePath(cleanup_path)),
        }
    }

    pub fn to_logical_plan(&self, relation_id: &str) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CachedRelationNode::try_new(
                relation_id.to_string(),
                Arc::clone(&self.schema),
            )?),
        }))
    }

    pub async fn to_physical_plan(&self, relation_id: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let data = self.data.lock().await;
        match &*data {
            CachedRelationData::Materialized(materialized) => materialized.to_physical_plan().await,
            CachedRelationData::Pending(pending) => Ok(Arc::new(PendingCachedRelationExec::new(
                relation_id.to_string(),
                self.clone(),
                Arc::clone(&pending.plan),
            ))),
        }
    }

    async fn materialize_pending(&self, ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
        let mut data = self.data.lock().await;
        if let CachedRelationData::Pending(pending) = &*data {
            *data = pending.materialize(ctx).await?;
        }
        match &*data {
            CachedRelationData::Materialized(materialized) => materialized.to_physical_plan().await,
            CachedRelationData::Pending(_) => Err(internal_datafusion_err!(
                "cached relation materialization did not complete"
            )),
        }
    }

    fn into_cleanup(self) -> Option<CachedRelationCleanup> {
        self.cleanup
    }
}

#[derive(Debug, Default)]
pub struct CachedRelationRegistry {
    relations: RwLock<HashMap<String, CachedRelation>>,
}

impl CachedRelationRegistry {
    pub fn insert(&self, relation_id: String, relation: CachedRelation) -> Result<()> {
        let mut relations = self
            .relations
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        match relations.entry(relation_id) {
            Entry::Occupied(entry) => Err(internal_datafusion_err!(
                "cached relation already exists: {}",
                entry.key()
            )),
            Entry::Vacant(entry) => {
                entry.insert(relation);
                Ok(())
            }
        }
    }

    pub fn get(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let relations = self
            .relations
            .read()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(relations.get(relation_id).cloned())
    }

    pub fn remove(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let mut relations = self
            .relations
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(relations.remove(relation_id))
    }

    pub fn drain(&self) -> Result<Vec<CachedRelation>> {
        let mut relations = self
            .relations
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(relations.drain().map(|(_, relation)| relation).collect())
    }
}

impl SessionExtension for CachedRelationRegistry {
    fn name() -> &'static str {
        "cached relation registry"
    }
}

impl CachedRelationData {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Materialized(materialized) => materialized.schema(),
            Self::Pending(pending) => pending.schema(),
        }
    }
}

impl CachedRelationPending {
    async fn materialize(&self, ctx: &SessionContext) -> Result<CachedRelationData> {
        match &self.target {
            CachedRelationPendingTarget::Local { storage_level } => {
                materialize_local_checkpoint(ctx, Arc::clone(&self.plan), storage_level.clone())
                    .await
            }
            CachedRelationPendingTarget::Reliable { path } => {
                materialize_reliable_checkpoint(ctx, Arc::clone(&self.plan), path).await
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
}

impl CachedRelationMaterialized {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Local(local) => Arc::clone(&local.schema),
            Self::Reliable { schema, .. } => Arc::clone(schema),
        }
    }

    async fn to_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Local(local) => local.to_physical_plan().await,
            Self::Reliable { schema, checkpoint } => create_arrow_checkpoint_scan(
                checkpoint.object_store_url().clone(),
                checkpoint.object_meta().to_vec(),
                schema,
            ),
        }
    }
}

impl CachedRelationLocalMaterialized {
    async fn try_new(
        ctx: &SessionContext,
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        storage_level: spec::StorageLevel,
    ) -> Result<Self> {
        let use_memory = storage_level.use_memory;
        let use_serialized_memory = use_memory && !storage_level.deserialized;
        let use_deserialized_memory = use_memory && storage_level.deserialized;
        let use_disk = storage_level.use_disk;
        if !use_deserialized_memory && !use_serialized_memory && !use_disk {
            return Err(internal_datafusion_err!(
                "local checkpoint storage level must use memory or disk"
            ));
        }

        let mut serialized_partitions = if use_serialized_memory {
            Some(Vec::with_capacity(partitions.len()))
        } else {
            None
        };
        let mut disk_partitions = if use_disk {
            Some(Vec::with_capacity(partitions.len()))
        } else {
            None
        };

        for partition in &partitions {
            let bytes = if use_serialized_memory || use_disk {
                Some(write_record_batches(partition, schema.as_ref())?)
            } else {
                None
            };

            if let Some(serialized_partitions) = serialized_partitions.as_mut() {
                let bytes = bytes
                    .as_ref()
                    .ok_or_else(|| internal_datafusion_err!("missing serialized partition"))?;
                serialized_partitions.push(bytes.clone());
            }

            if let Some(disk_partitions) = disk_partitions.as_mut() {
                let bytes = bytes
                    .as_ref()
                    .ok_or_else(|| internal_datafusion_err!("missing serialized partition"))?;
                let replication = storage_level.replication.max(1);
                let mut files = Vec::with_capacity(replication);
                for _ in 0..replication {
                    files.push(write_disk_partition(ctx, bytes).await?);
                }
                disk_partitions.push(CachedRelationDiskPartition { files });
            }
        }

        let memory_partitions = if use_deserialized_memory {
            Some(Arc::new(partitions))
        } else {
            None
        };
        let serialized_memory_partitions = serialized_partitions.map(Arc::new);
        let disk_partitions = disk_partitions.map(Arc::new);

        Ok(Self {
            schema,
            memory_partitions,
            serialized_memory_partitions,
            disk_partitions,
        })
    }

    async fn to_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.load_partitions().await?;
        let plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&self.schema), None)?;
        Ok(plan)
    }

    async fn load_partitions(&self) -> Result<Vec<Vec<RecordBatch>>> {
        if let Some(partitions) = &self.memory_partitions {
            return Ok(partitions.as_ref().clone());
        }
        if let Some(partitions) = &self.serialized_memory_partitions {
            return partitions
                .iter()
                .map(|bytes| read_record_batches(bytes))
                .collect();
        }
        if let Some(partitions) = &self.disk_partitions {
            let mut batches = Vec::with_capacity(partitions.len());
            for partition in partitions.iter() {
                batches.push(partition.read().await?);
            }
            return Ok(batches);
        }
        Err(internal_datafusion_err!(
            "cached relation has no materialized data"
        ))
    }
}

impl CachedRelationDiskPartition {
    async fn read(&self) -> Result<Vec<RecordBatch>> {
        let mut last_error = None;
        for file in &self.files {
            match tokio::fs::read(file.path())
                .await
                .map_err(DataFusionError::IoError)
                .and_then(|bytes| read_record_batches(&bytes))
            {
                Ok(batches) => return Ok(batches),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DataFusionError::Internal("cached relation disk partition has no files".to_string())
        }))
    }
}

pub fn materialize_cached_relations<'a>(
    ctx: &'a SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        let mut children = Vec::with_capacity(plan.children().len());
        for child in plan.children() {
            children.push(materialize_cached_relations(ctx, Arc::clone(child)).await?);
        }
        let plan = with_new_children_if_necessary(plan, children)?;
        if let Some(pending) = plan.downcast_ref::<PendingCachedRelationExec>() {
            pending.materialize(ctx).await
        } else {
            Ok(plan)
        }
    })
}

async fn materialize_local_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    storage_level: spec::StorageLevel,
) -> Result<CachedRelationData> {
    let plan = materialize_cached_relations(ctx, plan).await?;
    let schema = plan.schema();
    let partitions = collect_checkpoint_partitions(ctx, plan).await?;
    let materialized =
        CachedRelationLocalMaterialized::try_new(ctx, schema, partitions, storage_level).await?;
    Ok(CachedRelationData::Materialized(
        CachedRelationMaterialized::Local(materialized),
    ))
}

async fn materialize_reliable_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    path: &str,
) -> Result<CachedRelationData> {
    let plan = materialize_cached_relations(ctx, plan).await?;
    let schema = plan.schema();
    let service = ctx.extension::<CheckpointStoreService>()?;
    let checkpoint = match service
        .write_reliable_checkpoint(ctx, plan, path, Arc::clone(&schema))
        .await
    {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            let _ = cleanup_checkpoint_path(ctx, path).await;
            return Err(error);
        }
    };
    Ok(CachedRelationData::Materialized(
        CachedRelationMaterialized::Reliable { schema, checkpoint },
    ))
}

fn create_arrow_checkpoint_scan(
    object_store_url: ObjectStoreUrl,
    object_meta: Vec<ObjectMeta>,
    schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if object_meta.is_empty() {
        return Err(internal_datafusion_err!(
            "reliable checkpoint did not produce any files"
        ));
    }
    let source = ArrowSource::new_file_source(TableSchema::new(Arc::clone(schema), vec![]));
    let file_groups = object_meta
        .into_iter()
        .map(|object_meta| FileGroup::new(vec![PartitionedFile::new_from_meta(object_meta)]))
        .collect();
    let config = FileScanConfigBuilder::new(object_store_url, Arc::new(source))
        .with_file_groups(file_groups)
        .with_statistics(Statistics::new_unknown(schema))
        .build();
    Ok(DataSourceExec::from_data_source(config))
}

pub async fn cleanup_checkpoint_path(ctx: &SessionContext, path: &str) -> Result<()> {
    let service = ctx.extension::<CheckpointStoreService>()?;
    service.cleanup_checkpoint_path(ctx, path).await
}

pub async fn cleanup_cached_relation(ctx: &SessionContext, relation: CachedRelation) -> Result<()> {
    match relation.into_cleanup() {
        Some(CachedRelationCleanup::ObjectStorePath(path)) => {
            cleanup_checkpoint_path(ctx, &path).await
        }
        None => Ok(()),
    }
}

pub async fn cleanup_cached_relations(ctx: &SessionContext) -> Result<()> {
    let relations = ctx.extension::<CachedRelationRegistry>()?.drain()?;
    for relation in relations {
        cleanup_cached_relation(ctx, relation).await?;
    }
    Ok(())
}

async fn collect_checkpoint_partitions(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<Vec<RecordBatch>>> {
    collect_partitioned(plan, ctx.task_ctx()).await
}

async fn write_disk_partition(ctx: &SessionContext, bytes: &[u8]) -> Result<RefCountedTempFile> {
    let mut file = ctx
        .runtime_env()
        .disk_manager
        .create_tmp_file("writing local checkpoint cache partition")?;
    tokio::fs::write(file.path(), bytes)
        .await
        .map_err(DataFusionError::IoError)?;
    file.update_disk_usage()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::session::checkpoint::{CheckpointStore, ReliableCheckpoint};

    #[test]
    fn pending_reliable_checkpoint_tracks_cleanup_path() -> Result<()> {
        let path = "memory://checkpoint-root/session/relation".to_string();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let relation = CachedRelation::new_pending_reliable_checkpoint(plan, path.clone());

        match relation.into_cleanup() {
            Some(CachedRelationCleanup::ObjectStorePath(actual)) => {
                assert_eq!(actual, path);
            }
            other => {
                return Err(internal_datafusion_err!(
                    "unexpected cleanup value: {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn registry_insert_does_not_replace_existing_relation() -> Result<()> {
        let registry = CachedRelationRegistry::default();
        let first_path = "memory:///checkpoint-root/first".to_string();
        let second_path = "memory:///checkpoint-root/second".to_string();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let first =
            CachedRelation::new_pending_reliable_checkpoint(Arc::clone(&plan), first_path.clone());
        let second = CachedRelation::new_pending_reliable_checkpoint(plan, second_path);

        registry.insert("relation".to_string(), first)?;
        assert!(registry.insert("relation".to_string(), second).is_err());

        let relation = registry.get("relation")?.ok_or_else(|| {
            internal_datafusion_err!("cached relation missing after duplicate insert")
        })?;
        match relation.into_cleanup() {
            Some(CachedRelationCleanup::ObjectStorePath(actual)) => {
                assert_eq!(actual, first_path);
            }
            other => {
                return Err(internal_datafusion_err!(
                    "unexpected cleanup value: {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn pending_cached_relation_logical_plan_uses_plan_schema() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let relation = CachedRelation::new_pending_local_checkpoint(
            plan,
            spec::StorageLevel {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            },
        );

        let plan = relation.to_logical_plan("relation")?;
        let fields = plan.schema().fields();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "value");
        Ok(())
    }

    #[derive(Default)]
    struct TestCheckpointStore {
        cleanup_paths: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CheckpointStore for TestCheckpointStore {
        async fn write_reliable_checkpoint(
            &self,
            _ctx: &SessionContext,
            _plan: Arc<dyn ExecutionPlan>,
            _path: &str,
            _schema: SchemaRef,
        ) -> Result<ReliableCheckpoint> {
            Err(internal_datafusion_err!(
                "test store does not write checkpoints"
            ))
        }

        async fn cleanup_checkpoint_path(&self, _ctx: &SessionContext, path: &str) -> Result<()> {
            self.cleanup_paths
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?
                .push(path.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn cleanup_checkpoint_path_delegates_to_store() -> Result<()> {
        let store = Arc::new(TestCheckpointStore::default());
        let config = datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(CheckpointStoreService::new(store.clone())));
        let ctx = SessionContext::new_with_config(config);
        let path = "memory:///checkpoint-root/session/relation";

        cleanup_checkpoint_path(&ctx, path).await?;

        let paths = store
            .cleanup_paths
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        assert_eq!(paths.as_slice(), &[path.to_string()]);
        Ok(())
    }
}
