use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, Result as DataFusionResult};

use crate::metadata::{file_info_schema, DuckLakeMetaStore, ListDataFilesRequest, PythonMetaStore};
use crate::spec::{FieldIndex, PartitionFilter, TableIndex};

/// Physical source node that streams DuckLake data file metadata as Arrow RecordBatches.
#[derive(Debug, Clone)]
pub struct DuckLakeMetadataScanExec {
    metastore_url: String,
    request: ListDataFilesRequest,
    batch_size: usize,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl DuckLakeMetadataScanExec {
    pub fn try_new(
        metastore_url: String,
        request: ListDataFilesRequest,
        batch_size: usize,
    ) -> DataFusionResult<Self> {
        let schema = file_info_schema()?;
        let cache = Self::compute_properties(schema.clone());
        Ok(Self {
            metastore_url,
            request,
            batch_size,
            schema,
            cache,
        })
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn metastore_url(&self) -> &str {
        &self.metastore_url
    }

    pub fn request(&self) -> &ListDataFilesRequest {
        &self.request
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn table_id(&self) -> TableIndex {
        self.request.table_id
    }

    pub fn snapshot_id(&self) -> Option<u64> {
        self.request.snapshot_id
    }

    pub fn partition_filters(&self) -> Option<&[PartitionFilter]> {
        self.request.partition_filters.as_deref()
    }

    pub fn required_column_stats(&self) -> Option<&[FieldIndex]> {
        self.request.required_column_stats.as_deref()
    }
}

#[async_trait]
impl ExecutionPlan for DuckLakeMetadataScanExec {
    fn name(&self) -> &'static str {
        "DuckLakeMetadataScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "DuckLakeMetadataScanExec can only be executed in a single partition"
            );
        }

        let meta_store = PythonMetaStore::new_sync(&self.metastore_url);
        meta_store.scan_data_files(self.request.clone(), self.batch_size)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DisplayAs for DuckLakeMetadataScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DuckLakeMetadataScanExec(url={}, table_id={})",
                self.metastore_url, self.request.table_id.0
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: ducklake")?;
                write!(
                    f,
                    "url={}, table_id={}",
                    self.metastore_url, self.request.table_id.0
                )
            }
        }
    }
}
