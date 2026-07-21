/// Spark-compatible `exists(array, predicate)` higher-order function.
///
/// Spark semantics (`ArrayExists`, three-valued logic — the modern default):
/// - array `NULL` → `NULL`
/// - some element makes the predicate `true` → `true` (short-circuit)
/// - else if some element makes the predicate `NULL` → `NULL`
/// - else (including the empty array) → `false`
///
/// The predicate must return `BooleanType`. Unlike `filter`/`transform`, the
/// lambda takes exactly **one** parameter (the element); there is no index
/// parameter, so no `index_first` rewrite is needed.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, OffsetSizeTrait,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, list_values_row_number};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::{
    coerce_null_lambda_result, coerce_single_list_arg, short_circuit_boolean_reduce,
    value_lambda_pair,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayExists {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayExists {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayExists {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkArrayExists {
    fn name(&self) -> &str {
        "exists"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list, _lambda) = value_lambda_pair(self.name(), fields)?;
        let element = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            other => return plan_err!("{} expected a list, got {other}", self.name()),
        };
        Ok(LambdaParametersProgress::Complete(vec![vec![element]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (_list, _lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        // Boolean per row, nullable: three-valued logic can yield NULL even when
        // the input array column is not nullable.
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;
        let num_rows = list_array.len();

        let values = list_values(&list_array)?;

        let values_param = || Ok(Arc::clone(&values));
        let params: [&dyn Fn() -> Result<ArrayRef>; 1] = [&values_param];
        let predicate_output = match lambda.evaluate(&params, |arrays| {
            let indices = list_values_row_number(&list_array)?;
            Ok(take_arrays(arrays, &indices, None)?)
        }) {
            Ok(output) => output,
            // Evaluating every element at once can raise on an element that Spark
            // would never reach; retry in Spark's order before giving up.
            Err(_) => {
                let result =
                    short_circuit_boolean_reduce(self.name(), &list_array, &values, lambda, true)?;
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
        };

        let result = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                exists_reduce::<i32>(
                    num_rows,
                    list.nulls(),
                    &adjust_offsets_for_slice(list),
                    &predicate_output,
                    values.len(),
                )?
            }
            DataType::LargeList(_) => {
                let list = list_array.as_list::<i64>();
                exists_reduce::<i64>(
                    num_rows,
                    list.nulls(),
                    &adjust_offsets_for_slice(list),
                    &predicate_output,
                    values.len(),
                )?
            }
            other => return exec_err!("{} expected list, got {other}", self.name()),
        };

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
    }
}

/// Reduces the per-element predicate to one boolean per row using Spark's
/// three-valued logic (`true` wins via short-circuit; otherwise `NULL` if any
/// element was `NULL`; otherwise `false`). A `NULL` outer sublist yields `NULL`;
/// an empty sublist yields `false`.
fn exists_reduce<O: OffsetSizeTrait>(
    num_rows: usize,
    nulls: Option<&NullBuffer>,
    offsets: &OffsetBuffer<O>,
    predicate_output: &ColumnarValue,
    num_values: usize,
) -> Result<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(num_rows);

    // Scalar predicate short-circuit: the lambda body used the element in a way
    // that folds to a constant, e.g. `x -> true` / `x -> false` / `x -> NULL`.
    if let ColumnarValue::Scalar(ScalarValue::Boolean(b)) = predicate_output {
        for row in 0..num_rows {
            if is_row_null(nulls, row) {
                builder.append_null();
            } else if offsets[row + 1].as_usize() - offsets[row].as_usize() == 0 {
                builder.append_value(false);
            } else {
                match b {
                    Some(true) => builder.append_value(true),
                    Some(false) => builder.append_value(false),
                    None => builder.append_null(),
                }
            }
        }
        return Ok(builder.finish());
    }

    let predicate = coerce_null_lambda_result(predicate_output.clone().into_array(num_values)?);
    let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
        return exec_err!(
            "exists lambda must return boolean, got {}",
            predicate.data_type()
        );
    };

    for row in 0..num_rows {
        if is_row_null(nulls, row) {
            builder.append_null();
            continue;
        }
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let mut any_true = false;
        let mut any_null = false;
        for j in start..end {
            if predicate.is_null(j) {
                any_null = true;
            } else if predicate.value(j) {
                any_true = true;
                break;
            }
        }
        if any_true {
            builder.append_value(true);
        } else if any_null {
            builder.append_null();
        } else {
            builder.append_value(false);
        }
    }
    Ok(builder.finish())
}

fn is_row_null(nulls: Option<&NullBuffer>, row: usize) -> bool {
    nulls.map(|n| n.is_null(row)).unwrap_or(false)
}
