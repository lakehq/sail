use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Physical execution leaf node that reads cached RecordBatches from the worker-local cache.
///
/// This node is a placeholder created during physical planning from an InMemoryRelationNode.
/// At execution time, the TaskRunner (or job runner) is responsible for ensuring the cached
/// data is available in the LocalCacheStore before this node's execute() is called.
pub struct CacheReadExec {
    cache_id: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl CacheReadExec {
    /// Creates a new CacheReadExec for the given cache ID and schema.
    pub fn new(cache_id: String, schema: SchemaRef, num_partitions: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            cache_id,
            schema,
            properties,
        }
    }

    /// Returns the cache ID this node reads from.
    pub fn cache_id(&self) -> &str {
        &self.cache_id
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
            cache_id: self.cache_id.clone(),
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: Read from LocalCacheStore once the TaskRunner or job runner
        // wires up the cache store reference. For now, this is a placeholder
        // that will be replaced during plan rewriting before execution.
        internal_err!(
            "CacheReadExec for cache_id={} should be rewritten before execution",
            self.cache_id
        )
    }
}
