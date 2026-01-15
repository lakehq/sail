use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder, StructArray};
use datafusion::arrow::compute::cast;
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

pub const COL_REPLAY_PATH: &str = "__sail_delta_replay_path";

const COL_ADD: &str = "add";
const COL_REMOVE: &str = "remove";
const COL_PATH: &str = "path";

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

    fn get_struct_column(batch: &RecordBatch, col: &str) -> Result<Option<Arc<StructArray>>> {
        let Some(arr) = batch.column_by_name(col) else {
            return Ok(None);
        };
        let s = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogPathExtractExec input column '{col}' must be a Struct"
            ))
        })?;
        Ok(Some(Arc::new(s.clone())))
    }

    fn struct_field<'a>(s: &'a StructArray, name: &str) -> Option<&'a ArrayRef> {
        s.column_by_name(name)
    }

    fn parse_path_from_struct(s: &StructArray, row: usize) -> Result<Option<String>> {
        if s.is_null(row) {
            return Ok(None);
        }
        let path = Self::struct_field(s, COL_PATH).ok_or_else(|| {
            DataFusionError::Plan("DeltaLogPathExtractExec action missing 'path'".to_string())
        })?;
        let casted = cast(path.as_ref(), &DataType::Utf8)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let path = casted.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Plan("DeltaLogPathExtractExec action.path must be Utf8".to_string())
        })?;
        if path.is_null(row) {
            return Err(DataFusionError::Plan(
                "DeltaLogPathExtractExec action.path cannot be null".to_string(),
            ));
        }
        Ok(Some(path.value(row).to_string()))
    }
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

                let add = Self::get_struct_column(&batch, COL_ADD)?;
                let remove = Self::get_struct_column(&batch, COL_REMOVE)?;

                let mut b = StringBuilder::new();
                for row in 0..batch.num_rows() {
                    let mut p: Option<String> = None;
                    if let Some(add) = &add {
                        p = Self::parse_path_from_struct(add.as_ref(), row)?;
                    }
                    if p.is_none() {
                        if let Some(remove) = &remove {
                            p = Self::parse_path_from_struct(remove.as_ref(), row)?;
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

