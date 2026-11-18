// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, NullArray, UInt64Array};
use datafusion::arrow::compute::take_arrays;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_datafusion_err, plan_err};
use datafusion_expr::{LambdaArgument, ValueOrLambda};

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/f911d529a57b211eb44a98b253f97d839f60019f/datafusion/functions-nested/src/lambda_utils.rs
/// Extracts a `(value, lambda)` pair from a [`ValueOrLambda`] slice.
pub(crate) fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = take_function_args(name, args)?;

    let (ValueOrLambda::Value(value), ValueOrLambda::Lambda(lambda)) = (value, lambda) else {
        return plan_err!(
            "{name} expects a value followed by a lambda, got {value:?} and {lambda:?}"
        );
    };

    Ok((value, lambda))
}
// SPDX-SnippetEnd

pub(crate) fn evaluate_lambda_rows(lambda: &LambdaArgument, rows: &[u64]) -> Result<ArrayRef> {
    let indices = UInt64Array::from(rows.to_vec());
    let dummy = || Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef);

    lambda
        .evaluate(&[&dummy], |arrays| Ok(take_arrays(arrays, &indices, None)?))?
        .into_array(rows.len())
}

/// Like [`evaluate_lambda_rows`] for every row, without gathering the captures.
pub(crate) fn evaluate_lambda_all_rows(
    lambda: &LambdaArgument,
    number_rows: usize,
) -> Result<ArrayRef> {
    let dummy = || Ok(Arc::new(NullArray::new(number_rows)) as ArrayRef);
    lambda
        .evaluate(&[&dummy], |arrays| Ok(arrays.to_vec()))?
        .into_array(number_rows)
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
        let nulls = value.logical_nulls();
        let mut retained_positions = Vec::with_capacity(value.len());
        let mut retained_rows = Vec::with_capacity(value.len());
        for (position, row) in active_rows.iter().copied().enumerate() {
            if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(position)) {
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
