use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, RecordBatch,
    RecordBatchOptions,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use sail_common_datafusion::literal::LiteralValue;

///
/// This is part of the shared contract for bitmap aggregate functions that
/// interoperate on the same binary bitmap representation.
pub(crate) const BITMAP_NUM_BYTES: usize = 4 * 1024;

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

/// Convert a `ScalarValue` to `f64`, supporting floats, integers, and Decimal128.
///
/// Used for extracting percentile literals from physical expressions where the
/// literal may arrive as any numeric scalar.
pub(crate) fn scalar_to_f64(scalar: &ScalarValue) -> Result<f64> {
    let percentile: f64 = match scalar {
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => *v as f64,
        ScalarValue::Float16(Some(v)) => f64::from(*v),
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => {
            let int_val = LiteralValue(scalar).try_to_i64().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Cannot convert percentile literal {scalar:?} to integer: {e}"
                ))
            })?;
            int_val as f64
        }
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            (*v as f64) / 10f64.powi(*scale as i32)
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Cannot convert percentile literal {scalar:?} to f64"
            )))
        }
    };
    Ok(percentile)
}

/// Extract a single `f64` percentile from a physical expression literal.
///
/// Validates that the value is within `[0.0, 1.0]`.
pub(crate) fn extract_percentile_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let fields: Vec<Field> = vec![];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;
    let col_val = expr.evaluate(&batch)?;
    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
    };
    if matches!(scalar, ScalarValue::List(_)) {
        return Err(DataFusionError::Plan(
            "expected a scalar percentile literal, got a list".into(),
        ));
    }
    let percentile = scalar_to_f64(&scalar)?;
    if !(0.0..=1.0).contains(&percentile) {
        return Err(DataFusionError::Execution(format!(
            "Percentile value {percentile} is out of range [0.0, 1.0]"
        )));
    }
    Ok(percentile)
}

/// Extract a vector of `f64` percentiles from a physical expression that
/// evaluates to a `List` of numeric scalars.
pub(crate) fn extract_percentiles_array(expr: &Arc<dyn PhysicalExpr>) -> Result<Vec<f64>> {
    let fields: Vec<Field> = vec![];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;

    let col_val = expr.evaluate(&batch).map_err(|e| {
        DataFusionError::Execution(format!("Failed to evaluate percentile expression: {e}"))
    })?;
    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => {
            if arr.is_empty() {
                return Err(DataFusionError::Execution(
                    "Cannot extract percentile from empty array".into(),
                ));
            }
            ScalarValue::try_from_array(arr.as_ref(), 0)?
        }
    };

    match scalar {
        ScalarValue::List(arr) => {
            // An empty array literal yields an empty Vec; callers decide what
            // to do with it (Spark's `percentile_disc(array())` returns NULL).
            let values = arr.values();
            let mut percentiles = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                // Spark treats NULL percentile-list elements as `0.0` (returns
                // the minimum value), rather than erroring. Mirror that
                // behavior so the WITHIN GROUP form aligns with Spark.
                let percentile = if values.is_null(i) {
                    0.0
                } else {
                    let val_scalar = ScalarValue::try_from_array(values.as_ref(), i)?;
                    scalar_to_f64(&val_scalar)?
                };
                if !(0.0..=1.0).contains(&percentile) {
                    return Err(DataFusionError::Execution(format!(
                        "Percentile value {percentile} is out of range [0.0, 1.0]"
                    )));
                }
                percentiles.push(percentile);
            }
            Ok(percentiles)
        }
        other => Err(DataFusionError::Execution(format!(
            "Expected List type for percentiles array, got: {other:?}"
        ))),
    }
}

/// Returns the value at the given discrete percentile.
///
/// Returns an actual value from the dataset (no interpolation). Specifically,
/// returns the first value whose cumulative distribution is >= the requested
/// percentile.
///
/// `descending=true` mirrors `ORDER BY x DESC` and selects the position from
/// the high end of the sorted data. Note that the `1 - percentile` shortcut
/// commonly used for `percentile_cont` does NOT work here, because
/// `percentile_disc`'s index formula `ceil(p * n) - 1` is not symmetric: e.g.
/// for `[1,2,3,4]` with `p=0.25 DESC`, the answer is `4` (first when sorted
/// descending), not `3` (which is what `(1 - 0.25)` on ASC would return).
pub fn calculate_percentile_disc<T: ArrowNumericType>(
    mut values: Vec<T::Native>,
    percentile: f64,
    descending: bool,
) -> Option<T::Native> {
    let len = values.len();
    if len == 0 {
        return None;
    }

    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    // For ASC: index = ceil(p * n) - 1, with p=0 mapped to 0 and p=1 to len-1.
    // For DESC: the equivalent ASC index is `len - ceil(p * n)`, with p=0
    // mapped to len-1 (max) and p=1 mapped to 0 (min). Both branches clamp
    // into the valid range.
    let index = if descending {
        if percentile == 0.0 {
            len - 1
        } else if percentile == 1.0 {
            0
        } else {
            len.saturating_sub((percentile * len as f64).ceil() as usize)
                .min(len - 1)
        }
    } else if percentile == 0.0 {
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
