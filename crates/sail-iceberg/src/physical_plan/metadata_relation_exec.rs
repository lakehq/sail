use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
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
use url::Url;

use crate::metadata_relation::{IcebergMetadataRelationType, files};
use crate::spec::ManifestFile;
use crate::table::Table;
use crate::table::files::live_files;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetadataRelationScan {
    pub table_url: String,
    pub metadata_location: String,
    pub relation: IcebergMetadataRelationType,
    pub manifest_groups: Vec<Vec<ManifestFile>>,
    pub projection: Vec<usize>,
    pub limit: Option<usize>,
}

/// Reads immutable metadata on the executing worker; the plan contains no result rows.
#[derive(Debug, Clone)]
pub struct IcebergMetadataRelationExec {
    scan: Arc<MetadataRelationScan>,
    table_url: Url,
    original_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergMetadataRelationExec {
    pub(crate) fn try_new(original_schema: SchemaRef, scan: MetadataRelationScan) -> Result<Self> {
        if !scan.relation.is_supported()
            || scan.metadata_location.is_empty()
            || scan.manifest_groups.is_empty()
        {
            return plan_err!("Invalid Iceberg metadata relation scan");
        }
        if scan.relation != IcebergMetadataRelationType::Files && scan.manifest_groups.len() != 1 {
            return plan_err!("Static Iceberg metadata relations require one partition");
        }
        let table_url = Url::parse(&scan.table_url)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        let schema = Arc::new(original_schema.project(&scan.projection)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(scan.manifest_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            scan: Arc::new(scan),
            table_url,
            original_schema,
            properties,
        })
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn serialized_scan(&self) -> Result<String> {
        serde_json::to_string(self.scan.as_ref())
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))
    }

    pub fn try_from_serialized(schema: SchemaRef, scan: &str) -> Result<Self> {
        let scan = serde_json::from_str(scan)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        Self::try_new(schema, scan)
    }
}

impl DisplayAs for IcebergMetadataRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergMetadataRelationExec: relation={}, partitions={}, projection={:?}",
            self.scan.relation.name(),
            self.scan.manifest_groups.len(),
            self.scan.projection
        )
    }
}

impl ExecutionPlan for IcebergMetadataRelationExec {
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
            return plan_err!("Iceberg metadata scan cannot have children");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.scan.manifest_groups.len() {
            return exec_err!("Invalid Iceberg metadata scan partition {partition}");
        }
        let scan = Arc::clone(&self.scan);
        let table_url = self.table_url.clone();
        let schema = self.schema();
        let batch_size = context.session_config().batch_size().max(1);
        let stream = async_stream::try_stream! {
            if scan.limit != Some(0) {
                let table = Table::load_with_metadata_location(context.runtime_env().as_ref(), table_url,
                    Some(scan.metadata_location.clone())).await?;
                let mut remaining = scan.limit.unwrap_or(usize::MAX);
                if scan.relation == IcebergMetadataRelationType::Files {
                    let projection = files::FilesProjection::try_new(table.metadata(), &scan.projection)?;
                    'manifests: for manifest in &scan.manifest_groups[partition] {
                        let files = live_files(table.store_context(), manifest).await?;
                        for chunk in files.chunks(batch_size) {
                            let count = chunk.len().min(remaining);
                            let batch = projection.batch(&chunk[..count])?;
                            yield batch;
                            remaining -= count;
                            if remaining == 0 { break 'manifests; }
                        }
                    }
                } else {
                    let batch = scan.relation.record_batch(&table).await?.project(&scan.projection)?;
                    let count = batch.num_rows().min(remaining);
                    for offset in (0..count).step_by(batch_size) {
                        yield batch.slice(offset, batch_size.min(count - offset));
                    }
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
