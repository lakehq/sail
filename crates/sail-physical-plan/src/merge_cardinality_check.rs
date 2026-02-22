use std::any::Any;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result, Statistics};
use futures::Stream;

#[derive(Debug)]
pub struct MergeCardinalityCheckExec {
    input: Arc<dyn ExecutionPlan>,
    target_row_id_col: String,
    target_present_col: String,
    source_present_col: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MergeCardinalityCheckExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        target_row_id_col: impl Into<String>,
        target_present_col: impl Into<String>,
        source_present_col: impl Into<String>,
    ) -> Result<Self> {
        let schema = input.schema();
        let properties = input.properties().clone();
        Ok(Self {
            input,
            target_row_id_col: target_row_id_col.into(),
            target_present_col: target_present_col.into(),
            source_present_col: source_present_col.into(),
            schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn target_row_id_col(&self) -> &str {
        &self.target_row_id_col
    }

    pub fn target_present_col(&self) -> &str {
        &self.target_present_col
    }

    pub fn source_present_col(&self) -> &str {
        &self.source_present_col
    }
}

impl DisplayAs for MergeCardinalityCheckExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MergeCardinalityCheckExec: row_id={}, target_present={}, source_present={}",
            self.target_row_id_col, self.target_present_col, self.source_present_col
        )
    }
}

impl ExecutionPlan for MergeCardinalityCheckExec {
    fn name(&self) -> &'static str {
        "MergeCardinalityCheckExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("MergeCardinalityCheckExec requires exactly one child");
        };
        Ok(Arc::new(MergeCardinalityCheckExec::new(
            Arc::clone(input),
            self.target_row_id_col.clone(),
            self.target_present_col.clone(),
            self.source_present_col.clone(),
        )?))
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        // Keep rows for the same target row id in one partition so local de-dup is globally valid.
        // Fall back to single partition if the expected column is missing to preserve correctness.
        let idx = match self
            .input
            .schema()
            .index_of(self.target_row_id_col.as_str())
        {
            Ok(i) => i,
            Err(_) => return vec![datafusion::physical_plan::Distribution::SinglePartition],
        };
        let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                self.target_row_id_col.as_str(),
                idx,
            ));
        vec![datafusion::physical_plan::Distribution::HashPartitioned(
            vec![expr],
        )]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema.clone();

        let row_id_idx = schema
            .index_of(self.target_row_id_col.as_str())
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        let target_present_idx = schema
            .index_of(self.target_present_col.as_str())
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        let source_present_idx = schema
            .index_of(self.source_present_col.as_str())
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

        let stream = MergeCardinalityCheckStream {
            schema,
            input,
            row_id_idx,
            target_present_idx,
            source_present_idx,
            seen: HashSet::new(),
        };
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }
}

struct MergeCardinalityCheckStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    row_id_idx: usize,
    target_present_idx: usize,
    source_present_idx: usize,
    seen: HashSet<RowIdKey>,
}

impl RecordBatchStream for MergeCardinalityCheckStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MergeCardinalityCheckStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let target_present_any = batch.column(self.target_present_idx);
                let source_present_any = batch.column(self.source_present_idx);
                let row_id_any = batch.column(self.row_id_idx);

                let target_present = target_present_any
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "expected BooleanArray for {}",
                            self.target_present_idx
                        ))
                    })?;
                let source_present = source_present_any
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "expected BooleanArray for {}",
                            self.source_present_idx
                        ))
                    })?;

                let row_id_values: RowIdView<'_> =
                    if let Some(a) = row_id_any.as_any().downcast_ref::<StringArray>() {
                        RowIdView::Utf8(a)
                    } else if let Some(a) = row_id_any.as_any().downcast_ref::<Int64Array>() {
                        RowIdView::Int64(a)
                    } else if let Some(a) = row_id_any.as_any().downcast_ref::<LargeStringArray>() {
                        RowIdView::LargeUtf8(a)
                    } else {
                        return Poll::Ready(Some(Err(DataFusionError::Internal(format!(
                            "expected Int64/Utf8/LargeUtf8 for target row id but got {:?}",
                            row_id_any.data_type()
                        )))));
                    };

                for i in 0..batch.num_rows() {
                    let matched = !target_present.is_null(i)
                        && target_present.value(i)
                        && !source_present.is_null(i)
                        && source_present.value(i);
                    if !matched {
                        continue;
                    }
                    if row_id_values.is_null(i) {
                        continue;
                    }
                    let id = row_id_values.key(i);
                    if !self.seen.insert(id.clone()) {
                        return Poll::Ready(Some(Err(DataFusionError::Execution(
                            format!(
                                "MERGE_CARDINALITY_VIOLATION: Multiple source rows matched target row '{}'",
                                id
                            ),
                        ))));
                    }
                }

                Poll::Ready(Some(Ok(batch)))
            }
        }
    }
}

enum RowIdView<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> RowIdView<'a> {
    fn is_null(&self, i: usize) -> bool {
        match self {
            RowIdView::Utf8(a) => a.is_null(i),
            RowIdView::Int64(a) => a.is_null(i),
            RowIdView::LargeUtf8(a) => a.is_null(i),
        }
    }

    fn key(&self, i: usize) -> RowIdKey {
        match self {
            RowIdView::Utf8(a) => RowIdKey::Utf8(a.value(i).to_string()),
            RowIdView::Int64(a) => RowIdKey::Int64(a.value(i)),
            RowIdView::LargeUtf8(a) => RowIdKey::Utf8(a.value(i).to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RowIdKey {
    Utf8(String),
    Int64(i64),
}

impl std::fmt::Display for RowIdKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowIdKey::Utf8(s) => f.write_str(s),
            RowIdKey::Int64(i) => write!(f, "{i}"),
        }
    }
}
