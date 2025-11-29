use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;

pub fn extract_literal(
    expr: &Arc<dyn PhysicalExpr>,
) -> datafusion_common::Result<f64, DataFusionError> {
    fn dummy_batch() -> datafusion_common::Result<RecordBatch> {
        let fields: Vec<Field> = Vec::new();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::default().with_row_count(Some(1)),
        )
        .map_err(DataFusionError::from)
    }

    let batch = dummy_batch()?;
    let col_val = expr.evaluate(&batch)?;
    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
    };

    fn scalar_to_f64(sv: &ScalarValue) -> Option<f64> {
        match sv {
            ScalarValue::Float64(Some(v)) => Some(*v),
            ScalarValue::Float32(Some(v)) => Some(*v as f64),

            ScalarValue::Int64(Some(v)) => Some(*v as f64),
            ScalarValue::UInt64(Some(v)) => Some(*v as f64),
            ScalarValue::Int32(Some(v)) => Some(*v as f64),
            ScalarValue::UInt32(Some(v)) => Some(*v as f64),

            ScalarValue::Decimal128(Some(v), _precision, scale) => {
                Some((*v as f64) / 10f64.powi(*scale as i32))
            }

            _ => None,
        }
    }
    let percentile: f64 = scalar_to_f64(&scalar).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Cannot convert percentile literal {:?} to f64",
            scalar
        ))
    })?;
    Ok(percentile)
}
