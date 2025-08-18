use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, Result};
use datafusion_physical_expr::EquivalenceProperties;
use deltalake::kernel::Action;
use deltalake::protocol::DeltaOperation;
use futures::stream::once;
use sail_common_datafusion::datasource::TableDeltaOptions;
use url::Url;

use crate::delta_format::DeltaDataSink;

/// Custom physical execution node for Delta Lake sink operations
#[derive(Debug)]
pub struct DeltaSinkExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    initial_actions: Vec<Action>,
    operation: Option<DeltaOperation>,
    table_exists: bool,
    cache: PlanProperties,
}

impl DeltaSinkExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        initial_actions: Vec<Action>,
        operation: Option<DeltaOperation>,
        table_exists: bool,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            options,
            partition_columns,
            initial_actions,
            operation,
            table_exists,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn options(&self) -> &TableDeltaOptions {
        &self.options
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn initial_actions(&self) -> &[Action] {
        &self.initial_actions
    }

    pub fn operation(&self) -> Option<&DeltaOperation> {
        self.operation.as_ref()
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

#[async_trait]
impl ExecutionPlan for DeltaSinkExec {
    fn name(&self) -> &'static str {
        "DeltaSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaSinkExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.initial_actions.clone(),
            self.operation.clone(),
            self.table_exists,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaSinkExec can only be executed in a single partition");
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let sink = DeltaDataSink::new(
            self.table_url.clone(),
            self.options.clone(),
            self.input.schema(),
            self.partition_columns.clone(),
            self.initial_actions.clone(),
            self.operation.clone(),
            self.table_exists,
        );

        let schema = self.schema();
        let future = async move {
            let count = sink.write_all(input_stream, &context).await?;
            let array = Arc::new(UInt64Array::from(vec![count]));
            let batch = RecordBatch::try_new(schema, vec![array])?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DeltaSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaSinkExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
