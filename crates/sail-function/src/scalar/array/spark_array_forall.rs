/// Spark-compatible `forall(array, predicate)` higher-order function.
///
/// Spark semantics (`ArrayForAll`, three-valued logic):
/// - array `NULL` → `NULL`
/// - some element makes the predicate `false` → `false` (short-circuit)
/// - else if some element makes the predicate `NULL` → `NULL`
/// - else (all `true`, **including the empty array**) → `true`
///
/// The predicate must return `BooleanType`. Like `exists`, the lambda takes
/// exactly **one** parameter (the element); no index parameter.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, OffsetSizeTrait,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, list_values_row_number};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::{coerce_single_list_arg, value_lambda_pair};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayForall {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayForall {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayForall {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkArrayForall {
    fn name(&self) -> &str {
        "forall"
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
        let predicate_output = lambda.evaluate(&params, |arrays| {
            let indices = list_values_row_number(&list_array)?;
            Ok(take_arrays(arrays, &indices, None)?)
        })?;

        let result = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                forall_reduce::<i32>(
                    num_rows,
                    list.nulls(),
                    &adjust_offsets_for_slice(list),
                    &predicate_output,
                    values.len(),
                )?
            }
            DataType::LargeList(_) => {
                let list = list_array.as_list::<i64>();
                forall_reduce::<i64>(
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
/// three-valued logic (`false` wins via short-circuit; otherwise `NULL` if any
/// element was `NULL`; otherwise `true`). A `NULL` outer sublist yields `NULL`;
/// an empty sublist yields `true` (vacuous truth).
fn forall_reduce<O: OffsetSizeTrait>(
    num_rows: usize,
    nulls: Option<&NullBuffer>,
    offsets: &OffsetBuffer<O>,
    predicate_output: &ColumnarValue,
    num_values: usize,
) -> Result<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(num_rows);

    // Scalar predicate short-circuit: the lambda body folds to a constant,
    // e.g. `x -> true` / `x -> false` / `x -> NULL`.
    if let ColumnarValue::Scalar(ScalarValue::Boolean(b)) = predicate_output {
        for row in 0..num_rows {
            if is_row_null(nulls, row) {
                builder.append_null();
            } else if offsets[row + 1].as_usize() - offsets[row].as_usize() == 0 {
                builder.append_value(true);
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

    let predicate = predicate_output.clone().into_array(num_values)?;
    let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
        return exec_err!(
            "forall lambda must return boolean, got {}",
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
        let mut any_false = false;
        let mut any_null = false;
        for j in start..end {
            if predicate.is_null(j) {
                any_null = true;
            } else if !predicate.value(j) {
                any_false = true;
                break;
            }
        }
        if any_false {
            builder.append_value(false);
        } else if any_null {
            builder.append_null();
        } else {
            builder.append_value(true);
        }
    }
    Ok(builder.finish())
}

fn is_row_null(nulls: Option<&NullBuffer>, row: usize) -> bool {
    nulls.map(|n| n.is_null(row)).unwrap_or(false)
}
