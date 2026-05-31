use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::TryStreamExt;

pub const COL_PARTITION_SCAN: &str = "partition_scan";

/// Annotates the upstream metadata stream (from `IcebergManifestScanExec`) with a
/// boolean `partition_scan` column.
///
/// ```text
/// IcebergManifestScanExec
///   ↓ (file_path, file_format, record_count, ...)
/// IcebergDiscoveryExec          ← appends `partition_scan` bool column
///   ↓
/// RepartitionExec
///   ↓
/// IcebergScanByDataFilesExec
/// ```
#[derive(Debug)]
pub struct IcebergDiscoveryExec {
    /// Table URL for display and codec round-tripping.
    table_url: String,
    /// Snapshot ID for display and codec round-tripping.
    snapshot_id: i64,
    /// Upstream metadata plan (typically `IcebergManifestScanExec`).
    input: Arc<dyn ExecutionPlan>,
    /// Whether the upstream metadata originates from a partition-only scan.
    input_partition_scan: bool,
    /// Output schema (upstream schema + `partition_scan` boolean column).
    output_schema: SchemaRef,
    /// Cached properties.
    cache: Arc<PlanProperties>,
}

impl IcebergDiscoveryExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: String,
        snapshot_id: i64,
        partition_scan: bool,
    ) -> Result<Self> {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            COL_PARTITION_SCAN,
            DataType::Boolean,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table_url,
            snapshot_id,
            input,
            input_partition_scan: partition_scan,
            output_schema: schema,
            cache,
        })
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn input_partition_scan(&self) -> bool {
        self.input_partition_scan
    }
}

impl Clone for IcebergDiscoveryExec {
    fn clone(&self) -> Self {
        Self {
            table_url: self.table_url.clone(),
            snapshot_id: self.snapshot_id,
            input: Arc::clone(&self.input),
            input_partition_scan: self.input_partition_scan,
            output_schema: self.output_schema.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl DisplayAs for IcebergDiscoveryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergDiscoveryExec: table_url={}, snapshot_id={}, partition_scan={}",
                    self.table_url, self.snapshot_id, self.input_partition_scan,
                )
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergDiscoveryExec {
    fn name(&self) -> &str {
        "IcebergDiscoveryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergDiscoveryExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table_url.clone(),
            self.snapshot_id,
            self.input_partition_scan,
        )?))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, context)?;
        let partition_scan = self.input_partition_scan;

        let s = input_stream.try_filter_map(move |batch| {
            let schema = schema.clone();
            async move {
                if batch.num_rows() == 0 {
                    return Ok(None);
                }
                let scan_array =
                    Arc::new(BooleanArray::from(vec![partition_scan; batch.num_rows()]));
                let mut cols = batch.columns().to_vec();
                cols.push(scan_array);
                let out = RecordBatch::try_new(schema, cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(Some(out))
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            s,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_output_schema_appends_partition_scan() {
        let inner_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
        ]));

        let inner = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            inner_schema,
        ));
        let discovery =
            IcebergDiscoveryExec::new(inner, "file:///tmp/test_table".to_string(), 1234, false);
        assert!(discovery.is_ok());

        let schema = discovery.as_ref().map(|d| d.schema());
        assert!(schema.is_ok());
        let schema = schema.ok();
        assert!(schema.is_some());
        // upstream (2 fields) + partition_scan (1 field) = 3
        assert_eq!(schema.as_ref().map(|s| s.fields().len()), Some(3));
        assert!(schema
            .as_ref()
            .map(|s| s.field_with_name(COL_PARTITION_SCAN).is_ok())
            .is_some_and(|v| v));
    }
}
