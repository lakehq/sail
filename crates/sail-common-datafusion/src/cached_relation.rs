use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::{internal_datafusion_err, DataFusionError, Result, TableReference};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use sail_common::spec;

use crate::array::record_batch::{read_record_batches, write_record_batches};
use crate::extension::{SessionExtension, SessionExtensionAccessor};
use crate::rename::physical_plan::rename_physical_plan;
use crate::session::job::JobService;

#[derive(Debug, Clone)]
pub enum CachedRelationCleanup {
    LocalPath(String),
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    data: Arc<tokio::sync::Mutex<CachedRelationData>>,
    cleanup: Option<CachedRelationCleanup>,
}

#[derive(Debug, Clone)]
enum CachedRelationData {
    LogicalPlan(Arc<LogicalPlan>),
    Materialized(CachedRelationMaterialized),
    Pending(CachedRelationPending),
}

#[derive(Debug, Clone)]
struct CachedRelationMaterialized {
    schema: SchemaRef,
    memory_partitions: Option<Arc<Vec<Vec<RecordBatch>>>>,
    serialized_memory_partitions: Option<Arc<Vec<Vec<u8>>>>,
    disk_partitions: Option<Arc<Vec<CachedRelationDiskPartition>>>,
    storage_level: Option<spec::StorageLevel>,
}

#[derive(Debug, Clone)]
struct CachedRelationDiskPartition {
    files: Vec<RefCountedTempFile>,
}

#[derive(Debug, Clone)]
struct CachedRelationPending {
    plan: Arc<LogicalPlan>,
    fields: Option<Vec<String>>,
    target: CachedRelationPendingTarget,
}

#[derive(Debug, Clone)]
enum CachedRelationPendingTarget {
    Local { storage_level: spec::StorageLevel },
    Reliable { path: String },
}

impl CachedRelation {
    pub fn new(plan: Arc<LogicalPlan>, cleanup: Option<CachedRelationCleanup>) -> Self {
        Self {
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::LogicalPlan(
                plan,
            ))),
            cleanup,
        }
    }

    pub async fn new_local_checkpoint(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        storage_level: spec::StorageLevel,
    ) -> Result<Self> {
        let data = materialize_local_checkpoint(ctx, plan, storage_level).await?;
        Ok(Self {
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
        Ok(Self {
            data: Arc::new(tokio::sync::Mutex::new(data)),
            cleanup: None,
        })
    }

    pub fn new_pending_local_checkpoint(
        plan: Arc<LogicalPlan>,
        fields: Option<Vec<String>>,
        storage_level: spec::StorageLevel,
    ) -> Self {
        Self {
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    fields,
                    target: CachedRelationPendingTarget::Local { storage_level },
                },
            ))),
            cleanup: None,
        }
    }

    pub fn new_pending_reliable_checkpoint(
        plan: Arc<LogicalPlan>,
        fields: Option<Vec<String>>,
        path: String,
    ) -> Self {
        Self {
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    fields,
                    target: CachedRelationPendingTarget::Reliable { path },
                },
            ))),
            cleanup: None,
        }
    }

    pub async fn to_logical_plan(
        &self,
        ctx: &SessionContext,
        relation_id: &str,
    ) -> Result<LogicalPlan> {
        let mut data = self.data.lock().await;
        if let CachedRelationData::Pending(pending) = &*data {
            *data = pending.materialize(ctx).await?;
        }
        match &*data {
            CachedRelationData::LogicalPlan(plan) => Ok(plan.as_ref().clone()),
            CachedRelationData::Materialized(materialized) => {
                materialized.to_logical_plan(relation_id).await
            }
            CachedRelationData::Pending(_) => Err(internal_datafusion_err!(
                "cached relation materialization did not complete"
            )),
        }
    }

    pub fn into_cleanup(self) -> Option<CachedRelationCleanup> {
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
        if relations.insert(relation_id.clone(), relation).is_some() {
            return Err(internal_datafusion_err!(
                "cached relation already exists: {relation_id}"
            ));
        }
        Ok(())
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
}

impl SessionExtension for CachedRelationRegistry {
    fn name() -> &'static str {
        "cached relation registry"
    }
}

impl CachedRelationPending {
    async fn materialize(&self, ctx: &SessionContext) -> Result<CachedRelationData> {
        let physical_plan =
            create_physical_plan(ctx, self.plan.as_ref().clone(), self.fields.as_ref()).await?;
        match &self.target {
            CachedRelationPendingTarget::Local { storage_level } => {
                materialize_local_checkpoint(ctx, physical_plan, storage_level.clone()).await
            }
            CachedRelationPendingTarget::Reliable { path } => {
                materialize_reliable_checkpoint(ctx, physical_plan, path).await
            }
        }
    }
}

impl CachedRelationMaterialized {
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
            storage_level: Some(storage_level),
        })
    }

    async fn to_logical_plan(&self, relation_id: &str) -> Result<LogicalPlan> {
        let _ = &self.storage_level;
        let partitions = self.load_partitions().await?;
        let table = MemTable::try_new(Arc::clone(&self.schema), partitions)?;
        LogicalPlanBuilder::scan(
            TableReference::bare(format!("__sail_cached_relation_{relation_id}")),
            provider_as_source(Arc::new(table)),
            None,
        )?
        .build()
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

async fn create_physical_plan(
    ctx: &SessionContext,
    plan: LogicalPlan,
    fields: Option<&Vec<String>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let df = ctx.execute_logical_plan(plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let plan = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await?;
    if let Some(fields) = fields {
        rename_physical_plan(plan, fields)
    } else {
        Ok(plan)
    }
}

async fn materialize_local_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    storage_level: spec::StorageLevel,
) -> Result<CachedRelationData> {
    let schema = plan.schema();
    let partitions = collect_checkpoint_partitions(ctx, plan).await?;
    let materialized =
        CachedRelationMaterialized::try_new(ctx, schema, partitions, storage_level).await?;
    Ok(CachedRelationData::Materialized(materialized))
}

async fn materialize_reliable_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    path: &str,
) -> Result<CachedRelationData> {
    let schema = plan.schema();
    if let Err(error) = write_reliable_checkpoint(ctx, plan, path).await {
        let _ = cleanup_checkpoint_path(ctx, path).await;
        return Err(error);
    }
    let read_options = ParquetReadOptions::new().schema(schema.as_ref());
    let df = ctx.read_parquet(path, read_options).await?;
    let (_, read_plan) = df.into_parts();
    Ok(CachedRelationData::LogicalPlan(Arc::new(read_plan)))
}

pub async fn cleanup_checkpoint_path(ctx: &SessionContext, path: &str) -> Result<()> {
    let checkpoint_dir = format!("{}/", path.trim_end_matches('/'));
    let checkpoint_url = ListingTableUrl::parse(&checkpoint_dir)?;
    let store = ctx
        .runtime_env()
        .object_store(checkpoint_url.object_store())?;
    let prefix = checkpoint_url.prefix().clone();
    let files = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    for file in files {
        match store.delete(&file.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
    }
    Ok(())
}

async fn collect_checkpoint_partitions(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let service = ctx.extension::<JobService>()?;
    let mut stream = service.runner().execute(ctx, plan).await?;
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(vec![batches])
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

async fn write_reliable_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    path: &str,
) -> Result<()> {
    let schema = plan.schema();
    let checkpoint_dir = format!("{}/", path.trim_end_matches('/'));
    let checkpoint_url = ListingTableUrl::parse(&checkpoint_dir)?;
    let store = ctx
        .runtime_env()
        .object_store(checkpoint_url.object_store())?;
    let file_path = checkpoint_url.prefix().clone().join("part-00000.parquet");
    let object_writer = ParquetObjectWriter::new(store, file_path);
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let service = ctx.extension::<JobService>()?;
    let mut stream = service.runner().execute(ctx, plan).await?;
    while let Some(batch) = stream.next().await {
        writer
            .write(&batch?)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }
    writer
        .close()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(())
}
