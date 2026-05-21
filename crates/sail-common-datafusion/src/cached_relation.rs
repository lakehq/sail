use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_common::{internal_datafusion_err, Result, TableReference};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;

use crate::extension::SessionExtension;

#[derive(Debug, Clone)]
pub enum CachedRelationCleanup {
    LocalPath(String),
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    data: CachedRelationData,
    cleanup: Option<CachedRelationCleanup>,
}

#[derive(Debug, Clone)]
enum CachedRelationData {
    LogicalPlan(Arc<LogicalPlan>),
    Memory {
        schema: SchemaRef,
        partitions: Arc<Vec<Vec<RecordBatch>>>,
        storage_level: Option<spec::StorageLevel>,
    },
}

impl CachedRelation {
    pub fn new(plan: Arc<LogicalPlan>, cleanup: Option<CachedRelationCleanup>) -> Self {
        Self {
            data: CachedRelationData::LogicalPlan(plan),
            cleanup,
        }
    }

    pub fn new_memory(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        storage_level: Option<spec::StorageLevel>,
    ) -> Self {
        Self {
            data: CachedRelationData::Memory {
                schema,
                partitions: Arc::new(partitions),
                storage_level,
            },
            cleanup: None,
        }
    }

    pub fn to_logical_plan(&self, relation_id: &str) -> Result<LogicalPlan> {
        match &self.data {
            CachedRelationData::LogicalPlan(plan) => Ok(plan.as_ref().clone()),
            CachedRelationData::Memory {
                schema,
                partitions,
                storage_level,
            } => {
                let _ = storage_level;
                // TODO: Use Sail's full cache storage tiers once memory/disk/off-heap cache exists.
                // Sail's physical-plan codec serializes MemorySourceConfig batches, so this cache
                // representation can still be executed by local-cluster workers.
                let table = MemTable::try_new(Arc::clone(schema), partitions.as_ref().clone())?;
                LogicalPlanBuilder::scan(
                    TableReference::bare(format!("__sail_cached_relation_{relation_id}")),
                    provider_as_source(Arc::new(table)),
                    None,
                )?
                .build()
            }
        }
    }

    pub fn into_cleanup(self) -> Option<CachedRelationCleanup> {
        self.cleanup
    }
}

#[derive(Debug, Default)]
pub struct CachedRelationRegistry {
    // TODO: Clean cached local checkpoint relations when a session is closed.
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
