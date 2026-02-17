use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use crate::local_cache_store::LocalCacheStore;

/// Physical execution leaf node that reads cached RecordBatches from the worker-local cache.
///
/// This node is a placeholder created during physical planning from an InMemoryRelationNode.
/// At execution time, the TaskRunner injects the worker's LocalCacheStore before execution.
pub struct CacheReadExec {
    cache_id: u64,
    cache_store: Option<Arc<LocalCacheStore>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl CacheReadExec {
    /// Creates a new CacheReadExec for the given cache ID and schema.
    pub fn new(cache_id: u64, schema: SchemaRef, num_partitions: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            cache_id,
            cache_store: None,
            schema,
            properties,
        }
    }

    /// Returns the cache ID this node reads from.
    pub fn cache_id(&self) -> u64 {
        self.cache_id
    }

    /// Sets the cache store on this node after deserialization on a worker.
    pub fn set_cache_store(&mut self, cache_store: Arc<LocalCacheStore>) {
        self.cache_store = Some(cache_store);
    }
}

impl fmt::Debug for CacheReadExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CacheReadExec")
            .field("cache_id", &self.cache_id)
            .finish()
    }
}

impl Clone for CacheReadExec {
    fn clone(&self) -> Self {
        Self {
            cache_id: self.cache_id,
            cache_store: self.cache_store.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
        }
    }
}

impl DisplayAs for CacheReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CacheReadExec: cache_id={}", self.cache_id)
    }
}

impl ExecutionPlan for CacheReadExec {
    fn name(&self) -> &str {
        "CacheReadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("CacheReadExec should have no children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cache_store = match &self.cache_store {
            Some(store) => store.clone(),
            None => {
                return plan_err!(
                    "CacheReadExec has no cache store; was it deserialized without injection?"
                )
            }
        };
        let cache_id = self.cache_id;
        let schema = self.schema.clone();
        let batches = cache_store.get(cache_id, partition).unwrap_or_default();
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
