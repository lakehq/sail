//! Shared helpers for `(array, lambda)` higher-order array functions
//! (`filter`, `exists`, `forall`, ...).
//!
//! Mirrors the `pub(crate)` helpers in `datafusion-functions-nested`'s
//! `src/lambda_utils.rs`, kept here so every Spark HOF reuses the same
//! argument-unpacking and list-coercion logic instead of duplicating it.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{plan_err, Result};
use datafusion_expr::ValueOrLambda;

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

/// Coerces a single list argument for `(array, lambda)` style higher-order functions.
///
/// Normalises `ListView`/`FixedSizeList` → `List` and `LargeListView` → `LargeList`.
pub(crate) fn coerce_single_list_arg(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let list = if arg_types.len() == 1 {
        &arg_types[0]
    } else {
        return plan_err!(
            "{name} function requires 1 value argument, got {}",
            arg_types.len()
        );
    };

    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => return plan_err!("{name} expected a list as first argument, got {list}"),
    };

    Ok(vec![coerced])
}
