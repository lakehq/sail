use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;
use sail_common::cache_id::CacheId;

use crate::local_cache_store::LocalCacheStore;
use crate::plan::CachePartitionReporter;

/// Physical execution node that consumes a child plan's output and stores it in the worker-local cache.
#[derive(Clone)]
pub(crate) struct CacheWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    cache_store: Option<Arc<LocalCacheStore>>,
    cache_reporter: Option<Arc<dyn CachePartitionReporter>>,
    cache_id: CacheId,
    properties: PlanProperties,
}

impl CacheWriteExec {
    /// Creates a new CacheWriteExec wrapping the given child plan.
    /// This constructor is only compiled for tests.
    #[cfg(test)]
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        cache_store: Arc<LocalCacheStore>,
        cache_id: CacheId,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            plan,
            cache_store: Some(cache_store),
            cache_reporter: None,
            cache_id,
            properties,
        }
    }

    /// Creates a stub CacheWriteExec without a cache store, for serialization on the driver side.
    pub fn new_stub(plan: Arc<dyn ExecutionPlan>, cache_id: CacheId) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            plan,
            cache_store: None,
            cache_reporter: None,
            cache_id,
            properties,
        }
    }

    /// Sets the cache store on a stub CacheWriteExec after deserialization on a worker.
    pub fn set_cache_store(&mut self, cache_store: Arc<LocalCacheStore>) {
        self.cache_store = Some(cache_store);
    }

    /// Sets the reporter used for cache partition storage notifications.
    pub fn set_cache_reporter(&mut self, cache_reporter: Arc<dyn CachePartitionReporter>) {
        self.cache_reporter = Some(cache_reporter);
    }

    /// Returns the cache ID for this node.
    pub fn cache_id(&self) -> CacheId {
        self.cache_id
    }
}

impl fmt::Debug for CacheWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CacheWriteExec")
            .field("cache_id", &self.cache_id)
            .field("plan", &self.plan)
            .finish()
    }
}

impl DisplayAs for CacheWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CacheWriteExec: cache_id={}", self.cache_id)
    }
}

impl ExecutionPlan for CacheWriteExec {
    fn name(&self) -> &str {
        "CacheWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(plan), true) => {
                let mut node = Self::new_stub(plan, self.cache_id);
                if let Some(store) = &self.cache_store {
                    node.set_cache_store(store.clone());
                }
                if let Some(reporter) = &self.cache_reporter {
                    node.set_cache_reporter(reporter.clone());
                }
                Ok(Arc::new(node))
            }
            _ => plan_err!("CacheWriteExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cache_store = match &self.cache_store {
            Some(store) => store.clone(),
            None => {
                return plan_err!(
                    "CacheWriteExec has no cache store; was it deserialized without injection?"
                )
            }
        };
        let mut stream = self.plan.execute(partition, context)?;
        let cache_id = self.cache_id;
        let schema = self.schema();
        let cache_reporter = self.cache_reporter.clone();

        let output = futures::stream::once(async move {
            while let Some(batch) = stream.next().await {
                cache_store.store_individual(cache_id, partition, batch?);
            }
            if let Some(reporter) = cache_reporter {
                reporter.report_partition_stored(cache_id, partition);
            }
            Ok(RecordBatch::new_empty(schema))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
