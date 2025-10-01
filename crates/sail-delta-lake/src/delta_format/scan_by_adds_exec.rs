use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use deltalake::logstore::StorageConfig;
use futures::stream::{self, StreamExt, TryStreamExt};
use url::Url;

use crate::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use crate::table::open_table_with_object_store;

/// An ExecutionPlan that scans Delta files based on a stream of Add actions from its input.
/// This node acts as a bridge, consuming metadata (file list) and producing data.
#[derive(Debug, Clone)]
pub struct DeltaScanByAddsExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    table_schema: SchemaRef,
    cache: PlanProperties,
}

impl DeltaScanByAddsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url, table_schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(table_schema.clone());
        Self {
            input,
            table_url,
            table_schema,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    async fn create_scan_stream(
        &self,
        context: Arc<TaskContext>,
        candidate_adds: Vec<deltalake::kernel::Add>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table = open_table_with_object_store(
            self.table_url.clone(),
            object_store,
            StorageConfig::default(),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();

        let scan_config = DeltaScanConfigBuilder::new()
            .with_schema(self.table_schema.clone())
            .build(&snapshot)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table_provider = Arc::new(
            DeltaTableProvider::try_new(snapshot, table.log_store(), scan_config)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_files(candidate_adds),
        );

        let session_state = SessionStateBuilder::new()
            .with_runtime_env(context.runtime_env().clone())
            .build();

        let logical_plan =
            LogicalPlanBuilder::scan("scan_by_adds", provider_as_source(table_provider), None)?
                .build()?;

        let scan_plan = session_state.create_physical_plan(&logical_plan).await?;

        scan_plan.execute(0, context)
    }
}

#[async_trait]
impl ExecutionPlan for DeltaScanByAddsExec {
    fn name(&self) -> &'static str {
        "DeltaScanByAddsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaScanByAddsExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table_url.clone(),
            self.table_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaScanByAddsExec only supports a single partition");
        }

        let mut input_stream = self.input.execute(0, context.clone())?;
        let schema = self.schema();
        let schema_clone = schema.clone();
        let self_clone = self.clone();

        let stream_fut = async move {
            let mut candidate_adds = vec![];
            let mut partition_scan = true;

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }

                let scan_col = batch
                    .column_by_name("partition_scan")
                    .ok_or_else(|| {
                        DataFusionError::Internal("Missing partition_scan column".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "partition_scan column is not a BooleanArray".to_string(),
                        )
                    })?;
                partition_scan = scan_col.value(0);

                let adds_col = batch
                    .column_by_name("add")
                    .ok_or_else(|| DataFusionError::Internal("Missing add column".to_string()))?
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("add column is not a StringArray".to_string())
                    })?;
                for i in 0..adds_col.len() {
                    let add_json = adds_col.value(i);
                    let add: deltalake::kernel::Add = serde_json::from_str(add_json)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    candidate_adds.push(add);
                }
            }

            if partition_scan || candidate_adds.is_empty() {
                let empty_batch = RecordBatch::new_empty(schema_clone.clone());
                let stream = stream::once(async { Ok(empty_batch) });
                let adapter = RecordBatchStreamAdapter::new(schema_clone, stream);
                return Ok(Box::pin(adapter) as SendableRecordBatchStream);
            }

            self_clone.create_scan_stream(context, candidate_adds).await
        };

        let stream = futures::stream::once(stream_fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for DeltaScanByAddsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaScanByAddsExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanByAddsExec: table_path={}", self.table_url)
            }
        }
    }
}
