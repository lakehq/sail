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

use crate::local_cache_store::LocalCacheStore;

/// Physical execution node that consumes a child plan's output and stores it in the worker-local cache.
pub(crate) struct CacheWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    cache_store: Arc<LocalCacheStore>,
    cache_id: String,
    properties: PlanProperties,
}

impl CacheWriteExec {
    /// Creates a new CacheWriteExec wrapping the given child plan.
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        cache_store: Arc<LocalCacheStore>,
        cache_id: String,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            plan,
            cache_store,
            cache_id,
            properties,
        }
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

impl Clone for CacheWriteExec {
    fn clone(&self) -> Self {
        Self {
            plan: self.plan.clone(),
            cache_store: self.cache_store.clone(),
            cache_id: self.cache_id.clone(),
            properties: self.properties.clone(),
        }
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
            (Some(plan), true) => Ok(Arc::new(Self::new(
                plan,
                self.cache_store.clone(),
                self.cache_id.clone(),
            ))),
            _ => plan_err!("CacheWriteExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = self.plan.execute(partition, context)?;
        let cache_store = self.cache_store.clone();
        let cache_id = self.cache_id.clone();
        let schema = self.schema();

        let output = futures::stream::once(async move {
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            cache_store.store(&cache_id, partition, batches);
            Ok(RecordBatch::new_empty(schema))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
