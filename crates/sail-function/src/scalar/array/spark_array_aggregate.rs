/// Spark-compatible `aggregate(array, zero, merge[, finish])` higher-order function.
///
/// Spark semantics: see `ArrayAggregate` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`.
/// The merge lambda is a left fold over each array, the accumulator is always
/// typed nullable, a `NULL` array returns `NULL`, and the merge result must
/// structurally match the zero value type ignoring nested nullability.
use std::sync::Arc;

use datafusion::arrow::array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, AsArray, MutableArrayData,
    OffsetSizeTrait, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, take_function_args};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayAggregate {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayAggregate {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Lambda(()),
                    ValueOrLambda::Lambda(()),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkArrayAggregate {
    fn name(&self) -> &str {
        "aggregate"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list, zero, has_finish) = match fields {
            [ValueOrLambda::Value(list), ValueOrLambda::Value(zero), ValueOrLambda::Lambda(_merge)] => {
                (list, zero, false)
            }
            [ValueOrLambda::Value(list), ValueOrLambda::Value(zero), ValueOrLambda::Lambda(_merge), ValueOrLambda::Lambda(_finish)] => {
                (list, zero, true)
            }
            _ => {
                return plan_err!(
                    "{} expects value, value, lambda[, lambda] arguments",
                    self.name()
                );
            }
        };

        let element = list_element_field(self.name(), list.data_type())?;
        let acc = Arc::new(Field::new("", zero.data_type().clone(), true));
        let mut params = vec![vec![Arc::clone(&acc), element]];
        if has_finish {
            params.push(vec![acc]);
        }
        Ok(LambdaParametersProgress::Complete(params))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (list, zero, merge, finish) = aggregate_args(self.name(), args.arg_fields)?;
        list_element_field(self.name(), list.data_type())?;

        if !equals_structurally_ignore_nullability(zero.data_type(), merge.data_type()) {
            return plan_err!(
                "{} merge lambda result type must match zero value type, got {} and {}",
                self.name(),
                merge.data_type(),
                zero.data_type()
            );
        }

        Ok(Arc::new(Field::new(
            "",
            finish.data_type().clone(),
            list.is_nullable() || finish.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(args.return_type())));
        }

        let (list, zero, merge, finish) = aggregate_args(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;
        let zero_array = zero.to_array(args.number_rows)?;
        let values = list_values(&list_array)?;

        let output = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                aggregate_offsets(
                    self.name(),
                    args.number_rows,
                    list.nulls(),
                    &adjust_offsets_for_slice(list),
                    values,
                    zero_array,
                    merge,
                    finish,
                    args.return_type(),
                )?
            }
            DataType::LargeList(_) => {
                let list = list_array.as_list::<i64>();
                aggregate_offsets(
                    self.name(),
                    args.number_rows,
                    list.nulls(),
                    &adjust_offsets_for_slice(list),
                    values,
                    zero_array,
                    merge,
                    finish,
                    args.return_type(),
                )?
            }
            other => return exec_err!("{} expected list, got {other}", self.name()),
        };

        Ok(ColumnarValue::Array(output))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list, zero] = take_function_args(self.name(), arg_types)?;
        Ok(vec![coerce_list_type(self.name(), list)?, zero.clone()])
    }
}

fn aggregate_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L, &'a L)> {
    let [list, zero, merge, finish] = take_function_args(name, args)?;
    let (
        ValueOrLambda::Value(list),
        ValueOrLambda::Value(zero),
        ValueOrLambda::Lambda(merge),
        ValueOrLambda::Lambda(finish),
    ) = (list, zero, merge, finish)
    else {
        return plan_err!("{name} expects value, value, lambda, lambda arguments");
    };
    Ok((list, zero, merge, finish))
}

fn coerce_list_type(name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(_) | DataType::LargeList(_) => Ok(data_type.clone()),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            Ok(DataType::List(Arc::clone(field)))
        }
        DataType::LargeListView(field) => Ok(DataType::LargeList(Arc::clone(field))),
        _ => plan_err!("{name} expected a list as first argument, got {data_type}"),
    }
}

fn list_element_field(name: &str, data_type: &DataType) -> Result<FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Ok(Arc::clone(field)),
        other => plan_err!("{name} expected a list as first argument, got {other}"),
    }
}

fn equals_structurally_ignore_nullability(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size)) => {
            left_size == right_size
                && equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    equals_structurally_ignore_nullability(left.data_type(), right.data_type())
                })
        }
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        _ => left == right,
    }
}

#[expect(clippy::too_many_arguments)]
fn aggregate_offsets<O: OffsetSizeTrait>(
    name: &str,
    number_rows: usize,
    nulls: Option<&NullBuffer>,
    offsets: &OffsetBuffer<O>,
    values: ArrayRef,
    mut acc: ArrayRef,
    merge: &LambdaArgument,
    finish: &LambdaArgument,
    return_type: &DataType,
) -> Result<ArrayRef> {
    let max_len = (0..number_rows)
        .filter(|&row| !is_row_null(nulls, row))
        .map(|row| offsets[row + 1].as_usize() - offsets[row].as_usize())
        .max()
        .unwrap_or(0);

    for depth in 0..max_len {
        let mut rows = Vec::new();
        let mut value_indices = Vec::new();
        for row in 0..number_rows {
            if is_row_null(nulls, row) {
                continue;
            }
            let start = offsets[row].as_usize();
            let end = offsets[row + 1].as_usize();
            if start + depth < end {
                rows.push(row as u64);
                value_indices.push((start + depth) as u64);
            }
        }
        if rows.is_empty() {
            continue;
        }

        let row_indices = UInt64Array::from(rows.clone());
        let value_indices = UInt64Array::from(value_indices);
        let acc_param = take_one(&acc, &row_indices)?;
        let value_param = take_one(&values, &value_indices)?;
        let acc_arg = || Ok(Arc::clone(&acc_param));
        let value_arg = || Ok(Arc::clone(&value_param));
        let params: [&dyn Fn() -> Result<ArrayRef>; 2] = [&acc_arg, &value_arg];
        let next_acc = merge
            .evaluate(&params, |arrays| {
                Ok(take_arrays(arrays, &row_indices, None)?)
            })?
            .into_array(rows.len())?;

        acc = scatter_updates(name, &acc, &next_acc, &rows)?;
    }

    let non_null_rows = (0..number_rows)
        .filter(|&row| !is_row_null(nulls, row))
        .map(|row| row as u64)
        .collect::<Vec<_>>();
    if non_null_rows.is_empty() {
        return Ok(new_null_array(return_type, number_rows));
    }

    if non_null_rows.len() == number_rows {
        return evaluate_finish(finish, acc, number_rows, |arrays| Ok(arrays.to_vec()));
    }

    let row_indices = UInt64Array::from(non_null_rows.clone());
    let acc_param = take_one(&acc, &row_indices)?;
    let finished = evaluate_finish(finish, acc_param, non_null_rows.len(), |arrays| {
        Ok(take_arrays(arrays, &row_indices, None)?)
    })?;
    let nulls = new_null_array(return_type, number_rows);
    scatter_updates(name, &nulls, &finished, &non_null_rows)
}

fn evaluate_finish(
    finish: &LambdaArgument,
    acc: ArrayRef,
    number_rows: usize,
    spread_captures: impl FnOnce(&[ArrayRef]) -> Result<Vec<ArrayRef>>,
) -> Result<ArrayRef> {
    let acc_arg = || Ok(Arc::clone(&acc));
    let params: [&dyn Fn() -> Result<ArrayRef>; 1] = [&acc_arg];
    finish
        .evaluate(&params, spread_captures)?
        .into_array(number_rows)
}

fn take_one(array: &ArrayRef, indices: &UInt64Array) -> Result<ArrayRef> {
    let mut arrays = take_arrays(std::slice::from_ref(array), indices, None)?;
    arrays
        .pop()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("take returned no arrays"))
}

fn scatter_updates(
    name: &str,
    base: &ArrayRef,
    updates: &ArrayRef,
    rows: &[u64],
) -> Result<ArrayRef> {
    if updates.len() != rows.len() {
        return exec_err!(
            "{name} internal error: update length {} does not match row count {}",
            updates.len(),
            rows.len()
        );
    }

    let base_data = base.to_data();
    let update_data = if base.data_type() == updates.data_type() {
        updates.to_data()
    } else if equals_structurally_ignore_nullability(base.data_type(), updates.data_type()) {
        updates
            .to_data()
            .into_builder()
            .data_type(base.data_type().clone())
            .build()?
    } else {
        return exec_err!(
            "{name} internal error: cannot scatter updates of type {} into accumulator type {}",
            updates.data_type(),
            base.data_type()
        );
    };
    let mut mutable = MutableArrayData::new(vec![&base_data, &update_data], true, base.len());
    let mut base_start = 0usize;
    for (update_index, row) in rows.iter().enumerate() {
        let row = *row as usize;
        if row < base_start || row >= base.len() {
            return exec_err!("{name} internal error: invalid scatter row {row}");
        }
        if base_start < row {
            mutable.extend(0, base_start, row);
        }
        mutable.extend(1, update_index, update_index + 1);
        base_start = row + 1;
    }
    if base_start < base.len() {
        mutable.extend(0, base_start, base.len());
    }
    Ok(make_array(mutable.freeze()))
}

fn is_row_null(nulls: Option<&NullBuffer>, row: usize) -> bool {
    nulls.map(|n| n.is_null(row)).unwrap_or(false)
}
