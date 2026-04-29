use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, exec_err, Result};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

/// The marker file name written after a successful file write, matching Spark's
/// behavior controlled by `mapreduce.fileoutputcommitter.marksuccessfuljobs` (default true).
pub const SUCCESS_MARKER_FILE_NAME: &str = "_SUCCESS";

/// A physical plan node that wraps a file writer plan and writes an empty `_SUCCESS`
/// marker file under the output directory after the inner stream(s) complete successfully.
///
/// The marker is written exactly once, only after every output partition of the inner
/// plan has been fully consumed without error. If any partition fails or the stream
/// is dropped before completion, the marker is not written.
#[derive(Debug, Clone)]
pub struct FileWriteSuccessMarkerExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    /// The directory under which the marker file should be written.
    /// The marker itself is `<output_dir>/_SUCCESS`.
    output_dir: String,
    properties: Arc<PlanProperties>,
    /// Tracks the number of output partitions still to drain.
    /// When it reaches zero, the marker is written.
    remaining_partitions: Arc<AtomicUsize>,
}

impl FileWriteSuccessMarkerExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        output_dir: String,
    ) -> Self {
        let properties = Arc::new(input.properties().as_ref().clone());
        let partition_count = properties.output_partitioning().partition_count();
        Self {
            input,
            object_store_url,
            output_dir,
            properties,
            remaining_partitions: Arc::new(AtomicUsize::new(partition_count)),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn output_dir(&self) -> &str {
        &self.output_dir
    }

    fn marker_path(&self) -> Path {
        let trimmed = self.output_dir.trim_end_matches('/');
        if trimmed.is_empty() {
            Path::from(SUCCESS_MARKER_FILE_NAME)
        } else {
            Path::from(format!("{trimmed}/{SUCCESS_MARKER_FILE_NAME}"))
        }
    }
}

impl DisplayAs for FileWriteSuccessMarkerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "FileWriteSuccessMarkerExec: output_dir={}",
            self.output_dir
        )
    }
}

impl ExecutionPlan for FileWriteSuccessMarkerExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!(
                "{} requires exactly 1 child, got {}",
                self.name(),
                children.len()
            );
        }
        let input = children
            .pop()
            .ok_or_else(|| exec_datafusion_err!("{} requires exactly 1 child", self.name()))?;
        Ok(Arc::new(Self::new(
            input,
            self.object_store_url.clone(),
            self.output_dir.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let num_partitions = self.properties.output_partitioning().partition_count();
        if partition >= num_partitions {
            return exec_err!(
                "{}: partition index {} out of range ({})",
                self.name(),
                partition,
                num_partitions
            );
        }
        let inner = self.input.execute(partition, context.clone())?;
        let schema = inner.schema();
        let object_store_url = self.object_store_url.clone();
        let marker_path = self.marker_path();
        let remaining = self.remaining_partitions.clone();
        let runtime = context.runtime_env();

        let stream = async_stream::try_stream! {
            let mut s = inner;
            while let Some(batch) = s.next().await {
                yield batch?;
            }
            // Only write the marker once all partitions have completed successfully.
            if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                let store = runtime.object_store(&object_store_url)?;
                store
                    .put(&marker_path, PutPayload::new())
                    .await
                    .map_err(|e| exec_datafusion_err!("failed to write {SUCCESS_MARKER_FILE_NAME} marker: {e}"))?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
