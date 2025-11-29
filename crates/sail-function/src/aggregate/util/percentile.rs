use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::function::StateFieldsArgs;
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

pub fn state_fields(args: StateFieldsArgs) -> datafusion_common::Result<Vec<FieldRef>> {
    let value_type = args.input_fields[0].data_type().clone();

    let storage_type = match &value_type {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::Interval(_) | DataType::Duration(_) => DataType::Int64,
        _ => DataType::Float64,
    };

    let values_list_type = DataType::List(Arc::new(Field::new("item", storage_type, true)));

    Ok(vec![Field::new("values", values_list_type, true).into()])
}

pub fn return_type(arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
    match &arg_types[0] {
        DataType::Utf8 => Ok(DataType::Utf8),
        DataType::Utf8View => Ok(DataType::Utf8View),
        DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
        dt @ DataType::Interval(_) => Ok(dt.clone()),
        dt @ DataType::Duration(_) => Ok(dt.clone()),
        _ => Ok(DataType::Float64),
    }
}
