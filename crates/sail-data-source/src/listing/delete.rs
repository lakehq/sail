use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStoreExt;

/// Deletes all object-store files under a listing table output path.
#[derive(Debug, Clone)]
pub struct FileDeleteExec {
    object_store_url: ObjectStoreUrl,
    path: Path,
    properties: Arc<PlanProperties>,
}

impl FileDeleteExec {
    pub fn new(object_store_url: ObjectStoreUrl, path: Path) -> Self {
        let schema = Arc::new(Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            object_store_url,
            path,
            properties,
        }
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DisplayAs for FileDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: path={}", self.name(), self.path)
    }
}

impl ExecutionPlan for FileDeleteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
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
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let object_store_url = self.object_store_url.clone();
        let path = self.path.clone();
        let stream = futures::stream::once(async move {
            let store = context.runtime_env().object_store(&object_store_url)?;
            let files = store
                .list(Some(&path))
                .map_ok(|meta| meta.location)
                .try_collect::<Vec<_>>()
                .await?;
            for file in files {
                store.delete(&file).await?;
            }
            Ok(datafusion::arrow::record_batch::RecordBatch::new_empty(
                Arc::new(Schema::empty()),
            ))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
