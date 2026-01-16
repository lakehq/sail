use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::TryStreamExt;

use crate::physical_plan::log_action_decode;

pub const COL_REPLAY_PATH: &str = "__sail_delta_replay_path";

const COL_ADD: &str = "add";
const COL_REMOVE: &str = "remove";

/// A unary node that materializes a top-level `replay_path` column from raw delta log actions.
///
/// For each row:
/// - If `add` is present, `replay_path = add.path`
/// - Else if `remove` is present, `replay_path = remove.path`
/// - Else `replay_path = NULL`
///
/// This column is used for hash repartitioning so that all actions for the same file path land in
/// the same partition, enabling parallel log replay.
#[derive(Debug, Clone)]
pub struct DeltaLogPathExtractExec {
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl DeltaLogPathExtractExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(COL_REPLAY_PATH, DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));

        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self { input, cache })
    }

    // Struct decoding helpers live in `physical_plan::log_action_decode`.
}

#[async_trait]
impl ExecutionPlan for DeltaLogPathExtractExec {
    fn name(&self) -> &'static str {
        "DeltaLogPathExtractExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaLogPathExtractExec expects exactly one child");
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let schema_for_stream = schema.clone();
        let input_stream = self.input.execute(partition, context)?;

        let s = input_stream.try_filter_map(move |batch| {
            let schema = schema_for_stream.clone();
            async move {
                if batch.num_rows() == 0 {
                    return Ok(Some(RecordBatch::new_empty(schema)));
                }

                let add = log_action_decode::get_struct_column(
                    &batch,
                    COL_ADD,
                    "DeltaLogPathExtractExec",
                )?;
                let remove = log_action_decode::get_struct_column(
                    &batch,
                    COL_REMOVE,
                    "DeltaLogPathExtractExec",
                )?;

                let mut b = StringBuilder::new();
                for row in 0..batch.num_rows() {
                    let mut p: Option<String> = None;
                    if let Some(add) = &add {
                        p = log_action_decode::parse_path_from_struct(
                            add.as_ref(),
                            row,
                            "DeltaLogPathExtractExec",
                        )?;
                    }
                    if p.is_none() {
                        if let Some(remove) = &remove {
                            p = log_action_decode::parse_path_from_struct(
                                remove.as_ref(),
                                row,
                                "DeltaLogPathExtractExec",
                            )?;
                        }
                    }
                    if let Some(v) = p {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }

                let mut cols = batch.columns().to_vec();
                cols.push(Arc::new(b.finish()) as ArrayRef);
                let out = RecordBatch::try_new(schema, cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(Some(out))
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

impl DisplayAs for DeltaLogPathExtractExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaLogPathExtractExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaLogPathExtractExec")
            }
        }
    }
}
