use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility,
};

/// Spark-compatible `filter(array, predicate)` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#filter>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayFilter {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

impl HigherOrderUDF for SparkArrayFilter {
    fn name(&self) -> &str {
        "spark_array_filter"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list] = arg_types else {
            return plan_err!(
                "{} requires exactly 1 value argument, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let coerced = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            _ => {
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    list
                );
            }
        };
        Ok(vec![coerced])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list_field, _lambda) = value_lambda_pair(self.name(), fields)?;
        let elem_field = match list_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
            _ => return plan_err!("expected list, got {list_field}"),
        };
        // Always expose 2 supported params (element, index) so both 1-arg and 2-arg lambdas work.
        // The physical planner zips lambda.params with these fields and stops at the shorter.
        let index_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        ));
        Ok(LambdaParametersProgress::Complete(vec![vec![
            elem_field,
            index_field,
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let (list_field, _lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            "",
            list_field.data_type().clone(),
            list_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair_args(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;

        // list_values handles sliced arrays correctly
        let flat_values = list_values(&list_array)?;
        let flat_len = flat_values.len();

        match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                let adjusted = adjust_offsets_for_slice(list);
                let offsets: &[i32] = adjusted.as_ref();

                let per_sublist_indices = build_per_sublist_indices_i32(offsets, list.len());
                let indices: ArrayRef = Arc::new(per_sublist_indices);
                let fv = Arc::clone(&flat_values);
                let mask = eval_lambda(lambda, fv, indices, flat_len)?;

                let field = match list.data_type() {
                    DataType::List(f) => Arc::clone(f),
                    _ => unreachable!(),
                };
                let (new_values, new_offsets) = apply_mask_i32(
                    &flat_values,
                    offsets,
                    list.len(),
                    |i| list.is_null(i),
                    &mask,
                )?;
                Ok(ColumnarValue::Array(Arc::new(ListArray::new(
                    field,
                    new_offsets,
                    new_values,
                    list.nulls().cloned(),
                ))))
            }
            DataType::LargeList(_) => {
                use datafusion::arrow::array::LargeListArray;
                let list = list_array.as_list::<i64>();
                let adjusted = adjust_offsets_for_slice(list);
                let offsets: &[i64] = adjusted.as_ref();

                let per_sublist_indices = build_per_sublist_indices_i64(offsets, list.len());
                let indices: ArrayRef = Arc::new(per_sublist_indices);
                let fv = Arc::clone(&flat_values);
                let mask = eval_lambda(lambda, fv, indices, flat_len)?;

                let field = match list.data_type() {
                    DataType::LargeList(f) => Arc::clone(f),
                    _ => unreachable!(),
                };
                let (new_values, new_offsets) = apply_mask_i64(
                    &flat_values,
                    offsets,
                    list.len(),
                    |i| list.is_null(i),
                    &mask,
                )?;
                Ok(ColumnarValue::Array(Arc::new(LargeListArray::new(
                    field,
                    new_offsets,
                    new_values,
                    list.nulls().cloned(),
                ))))
            }
            other => exec_err!("expected list, got {other}"),
        }
    }
}

/// Build an array of per-sublist element indices (0-based within each sublist).
/// Null rows contribute no elements.
fn build_per_sublist_indices_i32(offsets: &[i32], n_rows: usize) -> Int64Array {
    let mut indices = Vec::with_capacity(offsets.last().copied().unwrap_or(0) as usize);
    for row_idx in 0..n_rows {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        for sub_idx in 0..(end - start) {
            indices.push(sub_idx as i64);
        }
    }
    Int64Array::from(indices)
}

fn build_per_sublist_indices_i64(offsets: &[i64], n_rows: usize) -> Int64Array {
    let mut indices = Vec::with_capacity(offsets.last().copied().unwrap_or(0) as usize);
    for row_idx in 0..n_rows {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        for sub_idx in 0..(end - start) {
            indices.push(sub_idx as i64);
        }
    }
    Int64Array::from(indices)
}

fn eval_lambda(
    lambda: &LambdaArgument,
    flat_values: ArrayRef,
    indices: ArrayRef,
    flat_len: usize,
) -> Result<BooleanArray> {
    let elem_fn = || Ok(Arc::clone(&flat_values));
    let idx_fn = || Ok(Arc::clone(&indices));
    let mask_cv = lambda.evaluate(&[&elem_fn, &idx_fn])?;
    let mask_array = mask_cv.into_array(flat_len)?;

    mask_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "filter lambda must return Boolean".to_string(),
            )
        })
}

fn apply_mask_i32(
    flat_values: &ArrayRef,
    offsets: &[i32],
    n_rows: usize,
    is_null: impl Fn(usize) -> bool,
    mask: &BooleanArray,
) -> Result<(ArrayRef, OffsetBuffer<i32>)> {
    let mut new_offsets: Vec<i32> = Vec::with_capacity(n_rows + 1);
    let mut keep_indices: Vec<u64> = Vec::with_capacity(flat_values.len());
    new_offsets.push(0);

    for row_idx in 0..n_rows {
        if is_null(row_idx) {
            new_offsets.push(*new_offsets.last().unwrap_or(&0));
            continue;
        }
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut cnt = 0i32;
        for flat_idx in start..end {
            if mask.is_valid(flat_idx) && mask.value(flat_idx) {
                keep_indices.push(flat_idx as u64);
                cnt += 1;
            }
        }
        new_offsets.push(*new_offsets.last().unwrap_or(&0) + cnt);
    }

    let take_indices = datafusion::arrow::array::UInt64Array::from(keep_indices);
    let new_values = datafusion::arrow::compute::take(flat_values.as_ref(), &take_indices, None)?;
    Ok((new_values, OffsetBuffer::new(new_offsets.into())))
}

fn apply_mask_i64(
    flat_values: &ArrayRef,
    offsets: &[i64],
    n_rows: usize,
    is_null: impl Fn(usize) -> bool,
    mask: &BooleanArray,
) -> Result<(ArrayRef, OffsetBuffer<i64>)> {
    let mut new_offsets: Vec<i64> = Vec::with_capacity(n_rows + 1);
    let mut keep_indices: Vec<u64> = Vec::with_capacity(flat_values.len());
    new_offsets.push(0);

    for row_idx in 0..n_rows {
        if is_null(row_idx) {
            new_offsets.push(*new_offsets.last().unwrap_or(&0));
            continue;
        }
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut cnt = 0i64;
        for flat_idx in start..end {
            if mask.is_valid(flat_idx) && mask.value(flat_idx) {
                keep_indices.push(flat_idx as u64);
                cnt += 1;
            }
        }
        new_offsets.push(*new_offsets.last().unwrap_or(&0) + cnt);
    }

    let take_indices = datafusion::arrow::array::UInt64Array::from(keep_indices);
    let new_values = datafusion::arrow::compute::take(flat_values.as_ref(), &take_indices, None)?;
    Ok((new_values, OffsetBuffer::new(new_offsets.into())))
}

fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = args else {
        return plan_err!(
            "{name} expects exactly 2 arguments (value + lambda), got {}",
            args.len()
        );
    };
    match (value, lambda) {
        (ValueOrLambda::Value(v), ValueOrLambda::Lambda(l)) => Ok((v, l)),
        _ => plan_err!("{name} expects a value followed by a lambda"),
    }
}

fn value_lambda_pair_args<'a>(
    name: &str,
    args: &'a [ValueOrLambda<ColumnarValue, LambdaArgument>],
) -> Result<(&'a ColumnarValue, &'a LambdaArgument)> {
    let [value, lambda] = args else {
        return plan_err!(
            "{name} expects exactly 2 arguments (value + lambda), got {}",
            args.len()
        );
    };
    match (value, lambda) {
        (ValueOrLambda::Value(v), ValueOrLambda::Lambda(l)) => Ok((v, l)),
        _ => plan_err!("{name} expects a value followed by a lambda"),
    }
}
