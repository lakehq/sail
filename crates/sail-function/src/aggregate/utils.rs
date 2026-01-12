use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, PrimitiveArray, RecordBatch,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType, Schema};
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

pub fn accumulate<T, F>(
    group_indices: &[usize],
    values: &PrimitiveArray<T>,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowPrimitiveType + Send,
    F: FnMut(usize, T::Native) + Send,
{
    let data: &[T::Native] = values.values();
    assert_eq!(data.len(), group_indices.len());

    match (values.null_count() > 0, opt_filter) {
        (false, None) => {
            let iter = group_indices.iter().zip(data.iter());
            for (&group_index, &new_value) in iter {
                value_fn(group_index, new_value);
            }
        }
        (true, None) => {
            let nulls = values.nulls().unwrap();
            let group_indices_chunks = group_indices.chunks_exact(64);
            let data_chunks = data.chunks_exact(64);
            let bit_chunks = nulls.inner().bit_chunks();

            let group_indices_remainder = group_indices_chunks.remainder();
            let data_remainder = data_chunks.remainder();

            group_indices_chunks
                .zip(data_chunks)
                .zip(bit_chunks.iter())
                .for_each(|((group_index_chunk, data_chunk), mask)| {
                    let mut index_mask = 1;
                    group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                        |(&group_index, &new_value)| {
                            let is_valid = (mask & index_mask) != 0;
                            if is_valid {
                                value_fn(group_index, new_value);
                            }
                            index_mask <<= 1;
                        },
                    )
                });

            let remainder_bits = bit_chunks.remainder_bits();
            group_indices_remainder
                .iter()
                .zip(data_remainder.iter())
                .enumerate()
                .for_each(|(i, (&group_index, &new_value))| {
                    let is_valid = remainder_bits & (1 << i) != 0;
                    if is_valid {
                        value_fn(group_index, new_value);
                    }
                });
        }
        (false, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            group_indices
                .iter()
                .zip(data.iter())
                .zip(filter.iter())
                .for_each(|((&group_index, &new_value), filter_value)| {
                    if let Some(true) = filter_value {
                        value_fn(group_index, new_value);
                    }
                })
        }
        (true, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            filter
                .iter()
                .zip(group_indices.iter())
                .zip(values.iter())
                .for_each(|((filter_value, &group_index), new_value)| {
                    if let Some(true) = filter_value {
                        if let Some(new_value) = new_value {
                            value_fn(group_index, new_value)
                        }
                    }
                })
        }
    }
}

pub fn calculate_percentile_disc<T: ArrowNumericType>(
    mut values: Vec<T::Native>,
    percentile: f64,
) -> Option<T::Native> {
    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    let len = values.len();
    if len == 0 {
        None
    } else if len == 1 {
        Some(values[0])
    } else if percentile == 0.0 {
        Some(
            *values
                .iter()
                .min_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        )
    } else if percentile == 1.0 {
        Some(
            *values
                .iter()
                .max_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        )
    } else {
        let index = percentile * ((len - 1) as f64);
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            let (_, value, _) = values.select_nth_unstable_by(lower_index, cmp);
            Some(*value)
        } else {
            let (_, lower_value, _) = values.select_nth_unstable_by(lower_index, cmp);
            let lower_value = *lower_value;

            let (_, upper_value, _) = values.select_nth_unstable_by(upper_index, cmp);
            let upper_value = *upper_value;

            let fraction = index - (lower_index as f64);
            let diff = upper_value.sub_wrapping(lower_value);
            let interpolated = lower_value.add_wrapping(
                diff.mul_wrapping(T::Native::usize_as((fraction * 1_000_000_f64) as usize))
                    .div_wrapping(T::Native::usize_as(1_000_000)),
            );
            Some(interpolated)
        }
    }
}
