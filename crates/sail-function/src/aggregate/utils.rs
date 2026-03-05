use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, RecordBatch,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

/// Casts an array to the target type if it doesn't already match.
pub fn cast_to_type(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() != target_type {
        datafusion::arrow::compute::cast(array, target_type).map_err(DataFusionError::from)
    } else {
        Ok(Arc::clone(array))
    }
}

pub fn get_scalar_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    if let ColumnarValue::Scalar(s) = expr.evaluate(&batch)? {
        Ok(s)
    } else {
        Err(DataFusionError::Internal(
            "Didn't expect ColumnarValue::Array".to_string(),
        ))
    }
}

pub fn validate_percentile(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let scalar_value = get_scalar_value(expr).map_err(|_e| {
        DataFusionError::Plan(
            "Percentile value for 'PERCENTILE_DISC' must be a literal".to_string(),
        )
    })?;

    let percentile = match scalar_value {
        ScalarValue::Float32(Some(value)) => value as f64,
        ScalarValue::Float64(Some(value)) => value,
        sv => {
            return Err(DataFusionError::Plan(format!(
                "Percentile value for 'PERCENTILE_DISC' must be Float32 or Float64 literal (got data type {})",
                sv.data_type()
            )))
        }
    };

    if !(0.0..=1.0).contains(&percentile) {
        return Err(DataFusionError::Plan(format!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        )));
    }
    Ok(percentile)
}

pub fn filtered_null_mask(
    opt_filter: Option<&BooleanArray>,
    input: &dyn Array,
) -> Option<NullBuffer> {
    let opt_filter = opt_filter.and_then(|f| {
        let (filter_bools, filter_nulls) = f.clone().into_parts();
        let filter_bools = NullBuffer::from(filter_bools);
        NullBuffer::union(Some(&filter_bools), filter_nulls.as_ref())
    });
    NullBuffer::union(opt_filter.as_ref(), input.nulls())
}

/// Returns the value at the given discrete percentile.
///
/// Returns an actual value from the dataset (no interpolation). Specifically,
/// returns the first value whose cumulative distribution is >= the requested percentile.
pub fn calculate_percentile_disc<T: ArrowNumericType>(
    mut values: Vec<T::Native>,
    percentile: f64,
) -> Option<T::Native> {
    let len = values.len();
    if len == 0 {
        return None;
    }

    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    // percentile_disc: return actual value at the target index (no interpolation)
    // index = ceil(percentile * len) - 1, clamped to valid range
    let index = if percentile == 0.0 {
        0
    } else if percentile == 1.0 {
        len - 1
    } else {
        ((percentile * len as f64).ceil() as usize)
            .saturating_sub(1)
            .min(len - 1)
    };

    // select_nth_unstable_by is O(n) partial sort - finds the value at index
    let (_, value, _) = values.select_nth_unstable_by(index, cmp);
    Some(*value)
}
