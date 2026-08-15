use std::sync::Arc;

use arrow::array::{NullArray, UInt64Array};
/// [Credit]: <https://github.com/apache/datafusion/blob/94d178ebe9674669b32ecd7896b5597f49e90791/datafusion/functions-nested/src/utils.rs>
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::take_arrays;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{ColumnarValue, LambdaArgument};

macro_rules! downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            ::datafusion_common::DataFusionError::Internal(format!(
                "could not cast to {}",
                ::std::any::type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! opt_downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{ $ARG.as_any().downcast_ref::<$ARRAY_TYPE>() }};
}

pub(crate) use downcast_arg;
pub(crate) use opt_downcast_arg;

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = inner(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

pub(crate) fn evaluate_lambda_rows(lambda: &LambdaArgument, rows: &[u64]) -> Result<ArrayRef> {
    let indices = UInt64Array::from(rows.to_vec());
    let dummy = || Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef);

    lambda
        .evaluate(&[&dummy], |arrays| Ok(take_arrays(arrays, &indices, None)?))?
        .into_array(rows.len())
}

pub(crate) fn evaluate_lambdas_until_null(
    lambdas: &[&LambdaArgument],
    number_rows: usize,
) -> Result<(Vec<ArrayRef>, Vec<u64>)> {
    let row_count = u64::try_from(number_rows)
        .map_err(|_| exec_datafusion_err!("sequence row count does not fit in u64"))?;
    let mut active_rows = (0..row_count).collect::<Vec<_>>();
    let mut values = Vec::with_capacity(lambdas.len());

    for lambda in lambdas {
        if active_rows.is_empty() {
            break;
        }
        let value = evaluate_lambda_rows(lambda, &active_rows)?;
        let mut retained_positions = Vec::with_capacity(value.len());
        let mut retained_rows = Vec::with_capacity(value.len());
        for (position, row) in active_rows.iter().copied().enumerate() {
            if !value.is_null(position) {
                retained_positions.push(
                    u64::try_from(position).map_err(|_| {
                        exec_datafusion_err!("sequence row index does not fit in u64")
                    })?,
                );
                retained_rows.push(row);
            }
        }

        values.push(value);
        if retained_rows.len() != active_rows.len() {
            let indices = UInt64Array::from(retained_positions);
            values = take_arrays(&values, &indices, None)?;
            active_rows = retained_rows;
        }
    }
    Ok((values, active_rows))
}

pub(crate) fn scatter_active_rows(
    value: ArrayRef,
    active_rows: &[u64],
    number_rows: usize,
) -> Result<ArrayRef> {
    if active_rows.len() == number_rows {
        return Ok(value);
    }

    let mut indices = vec![None::<u64>; number_rows];
    for (compact_index, row) in active_rows.iter().copied().enumerate() {
        let row = usize::try_from(row)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in usize"))?;
        let compact_index = u64::try_from(compact_index)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in u64"))?;
        let index = indices
            .get_mut(row)
            .ok_or_else(|| exec_datafusion_err!("sequence row index is out of bounds"))?;
        *index = Some(compact_index);
    }
    take_arrays(&[value], &UInt64Array::from(indices), None)?
        .pop()
        .ok_or_else(|| exec_datafusion_err!("sequence take returned no arrays"))
}
