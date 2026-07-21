use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{LexOrdering, Partitioning};
use datafusion_common::{Result, internal_datafusion_err};
use object_store::path::Path;
use sail_common_datafusion::extension::SessionExtension;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCheckpointFile {
    pub location: Path,
    pub size: u64,
    pub row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteCheckpointPartition {
    pub partition: usize,
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

#[derive(Debug, Default)]
pub struct RemoteCheckpointRegistry {
    relations: RwLock<HashMap<String, Arc<RemoteCheckpointDescriptor>>>,
}

impl RemoteCheckpointRegistry {
    pub fn insert(&self, descriptor: RemoteCheckpointDescriptor) -> Result<()> {
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

    pub fn clear(&self) -> Result<()> {
        let mut relations = self.relations.write().map_err(|error| {
            internal_datafusion_err!("checkpoint registry is poisoned: {error}")
        })?;
        relations.clear();
        Ok(())
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
