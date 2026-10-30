use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
pub use crate::datasource::partition_defaults::IdentityPartitionDefaults;
use crate::spec::delete_index::DeleteFileRef;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Schema as IcebergSchema};

pub(crate) const COL_IDENTITY_PARTITION_DEFAULTS: &str = "identity_partition_defaults";
pub(crate) const COL_REWRITE_SCAN_CONTEXT: &str = "rewrite_scan_context";

/// Per-file state resolved against the snapshot pinned by the rewrite coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RewriteScanContext {
    pub read_schema: Schema,
    pub iceberg_schema: IcebergSchema,
    pub positional_deletes: Vec<DeleteFileRef>,
    pub equality_deletes: Vec<DeleteFileRef>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: i64,
    pub preserve_lineage: bool,
}

/// Immutable data-file input; column metrics are unnecessary for an unfiltered rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct IcebergFileTask {
    pub path: String,
    pub size: u64,
    pub records: u64,
    pub spec_id: i32,
    pub identity_partition_defaults: IdentityPartitionDefaults,
    /// JSON retains schema and delete descriptors (including Puffin blob ranges) in the task.
    #[serde(default)]
    pub rewrite_scan_context: Option<String>,
}

impl From<&DataFile> for IcebergFileTask {
    fn from(file: &DataFile) -> Self {
        Self {
            path: file.file_path.clone(),
            size: file.file_size_in_bytes,
            records: file.record_count,
            spec_id: file.partition_spec_id,
            identity_partition_defaults: Default::default(),
            rewrite_scan_context: None,
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
        let mut fields = manifest_scan_schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            COL_IDENTITY_PARTITION_DEFAULTS,
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            COL_REWRITE_SCAN_CONTEXT,
            DataType::Utf8,
            true,
        )));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::new(fields))),
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
                let defaults = files.iter().map(|file| serde_json::to_string(&file.identity_partition_defaults))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
                yield RecordBatch::try_new(batch_schema.clone(), vec![
                    Arc::new(StringArray::from(files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(vec![DataFileFormat::Parquet.as_action_str(); files.len()])),
                    Arc::new(UInt64Array::from(files.iter().map(|file| file.records).collect::<Vec<_>>())),
                    Arc::new(UInt64Array::from(files.iter().map(|file| file.size).collect::<Vec<_>>())),
                    Arc::new(Int32Array::from(files.iter().map(|file| file.spec_id).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(vec![DataContentType::Data.as_action_str(); files.len()])),
                    Arc::new(BooleanArray::from(vec![false; files.len()])),
                    Arc::new(StringArray::from(defaults)),
                    Arc::new(StringArray::from(files.iter().map(|file| file.rewrite_scan_context.as_deref()).collect::<Vec<_>>())),
                ])?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use futures::TryStreamExt;

    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Type};

    #[tokio::test]
    async fn rewrite_scan_context_roundtrips_schema_lineage_and_shared_puffin_ranges() {
        let iceberg_schema = IcebergSchema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "__sail_iceberg_equality_2",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("read schema");
        let read_schema =
            crate::datasource::type_converter::iceberg_schema_to_arrow(&iceberg_schema)
                .expect("Arrow schema");
        let read_schema =
            crate::row_lineage::append_lineage_fields(&read_schema).expect("lineage schema");
        let equality_file = serde_json::from_value(serde_json::json!({
            "content": DataContentType::EqualityDeletes, "file_path": "data/equality.parquet", "file_format": "PARQUET",
            "partition": [], "record_count": 1, "file_size_in_bytes": 100, "partition_spec_id": 0,
            "equality_ids": [2]
        })).expect("equality file");
        let equality_delete = DeleteFileRef {
            data_file: equality_file,
            data_sequence_number: 8,
            partition_spec_id: 0,
            is_unpartitioned_spec: true,
        };
        let contexts = [4, 36].into_iter().enumerate().map(|(index, offset)| {
            let file = serde_json::from_value(serde_json::json!({
                "content": DataContentType::PositionDeletes, "file_path": "data/shared.puffin", "file_format": "PUFFIN",
                "partition": [], "record_count": 1, "file_size_in_bytes": 256, "partition_spec_id": 0,
                "referenced_data_file": format!("data/{index}.parquet"),
                "content_offset": offset, "content_size_in_bytes": 32
            })).expect("deletion vector");
            RewriteScanContext {
                read_schema: read_schema.clone(), iceberg_schema: iceberg_schema.clone(),
                positional_deletes: vec![DeleteFileRef {
                    data_file: file, data_sequence_number: 8, partition_spec_id: 0, is_unpartitioned_spec: true,
                }],
                equality_deletes: vec![equality_delete.clone()],
                first_row_id: Some(100 + index as i64 * 10), data_sequence_number: 7, preserve_lineage: true,
            }
        }).collect::<Vec<_>>();
        let tasks = IcebergFileTasksExec::try_new(
            contexts
                .iter()
                .enumerate()
                .map(|(index, context)| {
                    vec![IcebergFileTask {
                        path: format!("data/{index}.parquet"),
                        size: 100,
                        records: 10,
                        spec_id: 0,
                        identity_partition_defaults: Default::default(),
                        rewrite_scan_context: Some(
                            serde_json::to_string(context).expect("context JSON"),
                        ),
                    }]
                })
                .collect(),
        )
        .expect("file tasks");
        let decoded = IcebergFileTasksExec::try_from_serialized(
            &tasks.serialized_groups().expect("groups JSON"),
        )
        .expect("decoded groups");
        for (partition, expected) in contexts.iter().enumerate() {
            let batches = decoded
                .execute(partition, Arc::new(TaskContext::default()))
                .expect("task stream")
                .try_collect::<Vec<_>>()
                .await
                .expect("task batches");
            assert_eq!(batches.len(), 1);
            let values = batches[0]
                .column_by_name(COL_REWRITE_SCAN_CONTEXT)
                .expect("context column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("JSON array");
            let context: RewriteScanContext =
                serde_json::from_str(values.value(0)).expect("typed context");
            assert_eq!(&context, expected);
            context.positional_deletes[0]
                .data_file
                .validate_deletion_vector()
                .expect("valid DV descriptor");
        }
    }
}
