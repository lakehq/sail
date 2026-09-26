use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use serde::{Deserialize, Serialize};

use super::manifest_scan_exec::manifest_scan_schema;
use crate::spec::{DataContentType, DataFile, DataFileFormat};

/// Immutable data-file input; column metrics are unnecessary for an unfiltered rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct IcebergFileTask {
    pub path: String,
    pub size: u64,
    pub records: u64,
    pub spec_id: i32,
}

impl From<&DataFile> for IcebergFileTask {
    fn from(file: &DataFile) -> Self {
        Self {
            path: file.file_path.clone(),
            size: file.file_size_in_bytes,
            records: file.record_count,
            spec_id: file.partition_spec_id,
        }
    }
}

/// Each partition owns one rewrite group, including its readers and writer.
#[derive(Debug, Clone)]
pub struct IcebergFileTasksExec {
    groups: Arc<Vec<Vec<IcebergFileTask>>>,
    properties: Arc<PlanProperties>,
}

impl IcebergFileTasksExec {
    pub fn try_new(groups: Vec<Vec<IcebergFileTask>>) -> Result<Self> {
        if groups.is_empty() {
            return plan_err!("Iceberg file tasks require at least one group");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(manifest_scan_schema()),
            Partitioning::UnknownPartitioning(groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            groups: Arc::new(groups),
            properties,
        })
    }

    pub fn serialized_groups(&self) -> Result<String> {
        serde_json::to_string(self.groups.as_ref())
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))
    }

    pub fn try_from_serialized(groups: &str) -> Result<Self> {
        Self::try_new(
            serde_json::from_str(groups)
                .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?,
        )
    }
}

impl DisplayAs for IcebergFileTasksExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergFileTasksExec: groups={}, files={}",
            self.groups.len(),
            self.groups.iter().map(Vec::len).sum::<usize>()
        )
    }
}

impl ExecutionPlan for IcebergFileTasksExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("Iceberg file tasks cannot have children");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.groups.len() {
            return exec_err!("Invalid Iceberg file task partition {partition}");
        }
        let groups = Arc::clone(&self.groups);
        let schema = self.schema();
        let batch_schema = schema.clone();
        let batch_size = context.session_config().batch_size().max(1);
        let stream = async_stream::try_stream! {
            for files in groups[partition].chunks(batch_size) {
                yield RecordBatch::try_new(batch_schema.clone(), vec![
                    Arc::new(StringArray::from(files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(vec![DataFileFormat::Parquet.as_action_str(); files.len()])),
                    Arc::new(UInt64Array::from(files.iter().map(|file| file.records).collect::<Vec<_>>())),
                    Arc::new(UInt64Array::from(files.iter().map(|file| file.size).collect::<Vec<_>>())),
                    Arc::new(Int32Array::from(files.iter().map(|file| file.spec_id).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(vec![DataContentType::Data.as_action_str(); files.len()])),
                    Arc::new(BooleanArray::from(vec![false; files.len()])),
                ])?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
