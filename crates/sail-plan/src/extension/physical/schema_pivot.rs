use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{exec_err, internal_datafusion_err, DataFusionError, Result};
use futures::{Stream, StreamExt};

use crate::utils::ItemTaker;

#[derive(Debug, Clone)]
pub struct SchemaPivotExec {
    input: Arc<dyn ExecutionPlan>,
    names: Vec<String>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SchemaPivotExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, names: Vec<String>, schema: SchemaRef) -> Self {
        let partitioning = match input.output_partitioning() {
            Partitioning::RoundRobinBatch(size) => Partitioning::RoundRobinBatch(*size),
            Partitioning::Hash(_phy_exprs, size) => Partitioning::UnknownPartitioning(*size),
            Partitioning::UnknownPartitioning(size) => Partitioning::UnknownPartitioning(*size),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            names,
            schema,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl DisplayAs for SchemaPivotExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "SchemaPivotExec")
    }
}

impl ExecutionPlan for SchemaPivotExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .one()
            .map_err(|_| internal_datafusion_err!("SchemaPivotExec should have one child"))?;
        Ok(Arc::new(Self {
            input,
            ..self.as_ref().clone()
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.properties().output_partitioning().partition_count() {
            return exec_err!("SchemaPivotExec: partition index out of range: {partition}");
        }
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(SchemaPivotStream::new(
            stream,
            self.names.clone(),
            self.schema().clone(),
        )))
    }
}

struct SchemaPivotStream {
    input: Option<SendableRecordBatchStream>,
    names: Vec<String>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    data: Vec<RecordBatch>,
}

enum SchemaPivotState {
    Continue,
    SchemaPivot,
    Error(DataFusionError),
    Stopped,
}

impl SchemaPivotStream {
    pub fn new(
        input: SendableRecordBatchStream,
        names: Vec<String>,
        output_schema: SchemaRef,
    ) -> Self {
        let input_schema = input.schema();
        Self {
            input: Some(input),
            names,
            input_schema,
            output_schema,
            data: vec![],
        }
    }

    fn schema_pivot(&self) -> Result<RecordBatch> {
        let input_fields = &self.names;
        let num_fields = input_fields.len();
        let mut seen_values = vec![false; num_fields];
        let batch = concat_batches(&self.input_schema, &self.data)?;
        let num_rows = batch.num_rows();

        for (i, seen) in seen_values.iter_mut().enumerate() {
            if !*seen {
                let column = batch.column(i);
                if !column.is_empty() && column.null_count() < num_rows {
                    *seen = true;
                }
            }
        }

        let column_names: StringArray = seen_values
            .iter()
            .enumerate()
            .filter_map(|(i, &has_values)| {
                if has_values {
                    Some(&input_fields[i])
                } else {
                    None
                }
            })
            .map(Some)
            .collect();

        let batch = RecordBatch::try_new(self.output_schema.clone(), vec![Arc::new(column_names)])?;
        Ok(batch)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<SchemaPivotState> {
        let input = match self.input.as_mut() {
            Some(x) => x,
            None => return Poll::Ready(SchemaPivotState::Stopped),
        };
        let poll = input.poll_next_unpin(cx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(SchemaPivotState::SchemaPivot),
            Poll::Ready(Some(Err(e))) => Poll::Ready(SchemaPivotState::Error(e)),
            Poll::Ready(Some(Ok(batch))) => {
                self.data.push(batch);
                Poll::Ready(SchemaPivotState::Continue)
            }
        }
    }
}

impl Stream for SchemaPivotStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.poll(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(SchemaPivotState::SchemaPivot) => {
                    self.input = None;
                    break Poll::Ready(Some(self.schema_pivot()));
                }
                Poll::Ready(SchemaPivotState::Error(e)) => {
                    self.input = None;
                    break Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(SchemaPivotState::Stopped) => {
                    break Poll::Ready(None);
                }
                Poll::Ready(SchemaPivotState::Continue) => continue,
            }
        }
    }
}

impl RecordBatchStream for SchemaPivotStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
