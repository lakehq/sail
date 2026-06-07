use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, RecordBatch,
    RecordBatchOptions,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::CastOptions;
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
///
/// Uses a safe cast (invalid values become NULL).
pub fn cast_to_type(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    cast_to_type_with_safe(array, target_type, true)
}

/// Casts an array to the target type if it doesn't already match, choosing
/// between a safe cast (`safe = true`, invalid values become NULL) and a strict
/// cast (`safe = false`, invalid values raise an error).
///
/// This drives the ANSI-mode behavior of percentile aggregates: under
/// `spark.sql.ansi.enabled = false` a non-numeric string is coerced to NULL and
/// ignored, while under ANSI mode an invalid cast surfaces an error.
pub fn cast_to_type_with_safe(
    array: &ArrayRef,
    target_type: &DataType,
    safe: bool,
) -> Result<ArrayRef> {
    if array.data_type() != target_type {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        datafusion::arrow::compute::cast_with_options(array, target_type, &options)
            .map_err(DataFusionError::from)
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
                DataFusionError::Plan(format!(
                    "Cannot convert percentile literal {scalar:?} to integer: {e}"
                ))
            })?;
            int_val as f64
        }
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            (*v as f64) / 10f64.powi(*scale as i32)
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Cannot convert percentile literal {scalar:?} to f64"
            )))
        }
    };
    Ok(percentile)
}

/// Extract a single `f64` percentile from a physical expression literal.
///
/// Validates that the value is within `[0.0, 1.0]`. Errors with
/// `DataFusionError::Plan` for non-literal arguments and out-of-range values —
/// both are detectable at planning time.
pub(crate) fn extract_percentile_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let scalar = evaluate_percentile_literal(expr)?;
    if matches!(scalar, ScalarValue::List(_)) {
        return Err(DataFusionError::Plan(
            "expected a scalar percentile literal, got a list".into(),
        ));
    }
    let percentile = scalar_to_f64(&scalar)?;
    if !(0.0..=1.0).contains(&percentile) {
        return Err(DataFusionError::Plan(format!(
            "Percentile value {percentile} is out of range [0.0, 1.0]"
        )));
    }
    Ok(percentile)
}

/// Extract a vector of `f64` percentiles from a physical expression that
/// evaluates to a `List` of numeric scalars.
pub(crate) fn extract_percentiles_array(expr: &Arc<dyn PhysicalExpr>) -> Result<Vec<f64>> {
    let scalar = evaluate_percentile_literal(expr)?;
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
                    return Err(DataFusionError::Plan(format!(
                        "Percentile value {percentile} is out of range [0.0, 1.0]"
                    )));
                }
                percentiles.push(percentile);
            }
            Ok(percentiles)
        }
        other => Err(DataFusionError::Plan(format!(
            "Expected List type for percentiles array, got: {other:?}"
        ))),
    }
}

/// Evaluate `expr` as a constant percentile literal and return the scalar.
///
/// Rejects non-literal expressions (e.g. column references) with
/// `DataFusionError::Plan` so the failure is detected at planning time with a
/// clear message, rather than as a low-level evaluation error against a
/// synthetic empty `RecordBatch`.
fn evaluate_percentile_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    // Evaluate against an empty 1-row batch: any foldable constant (a literal, or
    // a cast/expression over literals like `CAST(1 AS DOUBLE)`) yields a value,
    // while a column-dependent expression errors because the batch has no columns.
    let schema = Arc::new(Schema::new(Vec::<Field>::new()));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;
    let col_val = expr.evaluate(&batch)?;
    Ok(match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
    })
}

/// ASC-sorted index for `percentile_disc(percentile)` over a population of
/// length `len`. `descending=true` returns the equivalent index for an
/// `ORDER BY ... DESC` selection performed on the same ASC-sorted slice.
///
/// `percentile_disc`'s index formula is asymmetric, so the `1 - percentile`
/// shortcut used for `percentile_cont` does NOT work here (e.g. for
/// `[1,2,3,4]` with `p=0.25 DESC` the answer is `4`, not `3`). Callers must
/// pass a non-empty `len` and a percentile already validated to `[0.0, 1.0]`.
pub fn percentile_disc_index(len: usize, percentile: f64, descending: bool) -> usize {
    debug_assert!(len > 0);
    if descending {
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
    }
}

/// Returns the value at the given discrete percentile.
///
/// Returns an actual value from the dataset (no interpolation). Specifically,
/// returns the first value whose cumulative distribution is >= the requested
/// percentile. See [`percentile_disc_index`] for the DESC semantics.
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
    let index = percentile_disc_index(len, percentile, descending);
    // select_nth_unstable_by is O(n) partial sort - finds the value at index
    let (_, value, _) = values.select_nth_unstable_by(index, cmp);
    Some(*value)
}
