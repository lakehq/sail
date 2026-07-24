use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_expr::{LexOrdering, Partitioning};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use object_store::path::Path;
use sail_common_datafusion::extension::SessionExtension;
use sail_object_store::{
    ResolvedObjectStorePath, delete_object_store_prefix_objects, resolve_object_store_path,
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCheckpointFile {
    pub location: Path,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCheckpointPartition {
    pub partition: usize,
    pub row_count: u64,
    pub file: Option<RemoteCheckpointFile>,
}

#[derive(Clone, Debug)]
pub struct RemoteCheckpointDescriptor {
    pub relation_id: String,
    pub object_store_url: ObjectStoreUrl,
    pub prefix: Path,
    pub logical_schema: SchemaRef,
    pub storage_schema: SchemaRef,
    pub output_partitioning: Partitioning,
    pub output_ordering: Option<LexOrdering>,
    pub partitions: Vec<RemoteCheckpointPartition>,
}

/// Session-local publication index for complete checkpoints; object-store contents are not
/// reloaded after a process restart.
#[derive(Debug)]
pub struct RemoteCheckpointRegistry {
    path: Option<String>,
    session_namespace: String,
    closed: AtomicBool,
    relations: RwLock<HashMap<String, Arc<RemoteCheckpointDescriptor>>>,
}

impl RemoteCheckpointRegistry {
    pub fn new(path: Option<String>) -> Self {
        Self {
            path,
            session_namespace: Uuid::new_v4().to_string(),
            closed: AtomicBool::new(false),
            relations: RwLock::new(HashMap::new()),
        }
    }

    pub fn resolve_relation(
        &self,
        runtime_env: &RuntimeEnv,
        relation_id: &str,
    ) -> Result<(ObjectStoreUrl, Path)> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DataFusionError::Plan(
                "checkpoint session is stopping".to_string(),
            ));
        }
        let root = self.resolve_root(runtime_env)?;
        let prefix = self.session_prefix(root.prefix()).join(relation_id);
        Ok((root.object_store_url().clone(), prefix))
    }

    pub fn insert(&self, descriptor: RemoteCheckpointDescriptor) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DataFusionError::Plan(
                "checkpoint session is stopping".to_string(),
            ));
        }
        let relation_id = descriptor.relation_id.clone();
        let mut relations = self.relations.write().map_err(|error| {
            internal_datafusion_err!("checkpoint registry is poisoned: {error}")
        })?;
        match relations.entry(relation_id.clone()) {
            Entry::Occupied(_) => {
                return Err(internal_datafusion_err!(
                    "checkpoint relation already exists: {relation_id}"
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(descriptor));
            }
        }
        Ok(())
    }

    pub fn get(&self, relation_id: &str) -> Result<Option<Arc<RemoteCheckpointDescriptor>>> {
        let relations = self.relations.read().map_err(|error| {
            internal_datafusion_err!("checkpoint registry is poisoned: {error}")
        })?;
        Ok(relations.get(relation_id).cloned())
    }

    pub async fn cleanup_relation(
        &self,
        runtime_env: &RuntimeEnv,
        object_store_url: &ObjectStoreUrl,
        prefix: &Path,
    ) -> Result<()> {
        let store = runtime_env.object_store(object_store_url)?;
        delete_object_store_prefix_objects(store.as_ref(), prefix).await
    }

    pub async fn cleanup_session(&self, runtime_env: &RuntimeEnv) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let storage_cleanup = if self.path.is_some() {
            match self.resolve_root(runtime_env) {
                Ok(root) => {
                    delete_object_store_prefix_objects(
                        root.store().as_ref(),
                        &self.session_prefix(root.prefix()),
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        let registry_cleanup = self.clear();
        match (storage_cleanup, registry_cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(storage_error), Err(registry_error)) => Err(internal_datafusion_err!(
                "checkpoint storage cleanup failed: {storage_error}; registry cleanup failed: {registry_error}"
            )),
        }
    }

    fn resolve_root(&self, runtime_env: &RuntimeEnv) -> Result<ResolvedObjectStorePath> {
        let path = self.path.as_deref().ok_or_else(|| {
            DataFusionError::Plan(
                "checkpoint object-store path is not configured; set execution.checkpoint.path"
                    .to_string(),
            )
        })?;
        resolve_object_store_path(runtime_env, path)
    }

    fn session_prefix(&self, root: &Path) -> Path {
        root.clone().join(self.session_namespace.as_str())
    }

    fn clear(&self) -> Result<()> {
        let mut relations = self.relations.write().map_err(|error| {
            internal_datafusion_err!("checkpoint registry is poisoned: {error}")
        })?;
        relations.clear();
        Ok(())
    }
}

impl Default for RemoteCheckpointRegistry {
    fn default() -> Self {
        Self::new(None)
    }
}

impl SessionExtension for RemoteCheckpointRegistry {
    fn name() -> &'static str {
        "remote checkpoint registry"
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::Schema;

    use super::*;

    fn descriptor(relation_id: &str) -> RemoteCheckpointDescriptor {
        let schema = Arc::new(Schema::empty());
        RemoteCheckpointDescriptor {
            relation_id: relation_id.to_string(),
            object_store_url: ObjectStoreUrl::parse("memory://").unwrap(),
            prefix: Path::from("checkpoint"),
            logical_schema: Arc::clone(&schema),
            storage_schema: schema,
            output_partitioning: Partitioning::UnknownPartitioning(1),
            output_ordering: None,
            partitions: vec![],
        }
    }

    #[test]
    fn registry_owns_only_ready_descriptors() -> Result<()> {
        let registry = RemoteCheckpointRegistry::default();
        registry.insert(descriptor("relation"))?;
        assert_eq!(
            registry
                .get("relation")?
                .expect("descriptor must be present")
                .relation_id,
            "relation"
        );
        let original = registry
            .get("relation")?
            .expect("descriptor must be present");
        assert!(registry.insert(descriptor("relation")).is_err());
        let retained = registry
            .get("relation")?
            .expect("descriptor must remain present");
        assert!(Arc::ptr_eq(&original, &retained));
        registry.clear()?;
        assert!(registry.get("relation")?.is_none());
        Ok(())
    }
}
