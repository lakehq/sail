use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use object_store::ObjectMeta;

use crate::extension::SessionExtension;

#[derive(Debug, Clone)]
pub struct ReliableCheckpoint {
    object_store_url: ObjectStoreUrl,
    object_meta: Vec<ObjectMeta>,
}

impl ReliableCheckpoint {
    pub fn new(object_store_url: ObjectStoreUrl, object_meta: Vec<ObjectMeta>) -> Self {
        Self {
            object_store_url,
            object_meta,
        }
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn object_meta(&self) -> &[ObjectMeta] {
        &self.object_meta
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync + 'static {
    async fn write_reliable_checkpoint(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        path: &str,
        schema: SchemaRef,
    ) -> Result<ReliableCheckpoint>;

    async fn cleanup_checkpoint_path(&self, ctx: &SessionContext, path: &str) -> Result<()>;
}

#[derive(Clone)]
pub struct CheckpointStoreService {
    store: Arc<dyn CheckpointStore>,
}

impl CheckpointStoreService {
    pub fn new(store: Arc<dyn CheckpointStore>) -> Self {
        Self { store }
    }

    pub async fn write_reliable_checkpoint(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        path: &str,
        schema: SchemaRef,
    ) -> Result<ReliableCheckpoint> {
        self.store
            .write_reliable_checkpoint(ctx, plan, path, schema)
            .await
    }

    pub async fn cleanup_checkpoint_path(&self, ctx: &SessionContext, path: &str) -> Result<()> {
        self.store.cleanup_checkpoint_path(ctx, path).await
    }
}

impl SessionExtension for CheckpointStoreService {
    fn name() -> &'static str {
        "CheckpointStoreService"
    }
}
