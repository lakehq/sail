/// [CREDIT]: https://github.com/apache/datafusion/blob/f911d529a57b211eb44a98b253f97d839f60019f/datafusion/functions-nested/src/array_filter.rs
///
/// Spark-compatible `filter(array, lambda)` higher-order function.
///
/// Adapted from `array_filter` in `datafusion-functions-nested` 54.0.0
/// (`src/array_filter.rs` and the `pub(crate)` helpers in `src/lambda_utils.rs`),
/// with one Spark-specific extension: the lambda receives an optional second
/// parameter holding the 0-based element index (`filter(arr, (x, i) -> ...)`),
/// typed as `Int32` to match Spark's `IntegerType`.
///
/// Spark semantics: see `ArrayFilter` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`
/// — `dataType` is the argument type unchanged, the lambda must return
/// `BooleanType`, `bindInternal` provides `(elementType, containsNull)` plus
/// `(IntegerType, false)` for two-parameter lambdas, and `nullSafeEval` keeps
/// an element only when the predicate evaluates to `true` (NULL is falsy).
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BooleanArray, LargeListArray, ListArray,
    OffsetBufferBuilder, OffsetSizeTrait,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{filter as arrow_filter, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values_row_number};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::scalar::array::lambda_utils::{
    coerce_single_list_arg, extract_list_values, index_array, value_lambda_pair, ListValuesResult,
};

/// The physical lambda evaluation batch is laid out as `[captures..., params...]`
/// with the body projected to the columns it actually uses, which only lines up
/// when the used parameters form a prefix of the declared ones. A lambda such as
/// `(x, i) -> i % 2 = 0` (element unused, index used) breaks that alignment, so
/// the planner must rewrite it to a single-parameter lambda over an
/// [`SparkArrayFilter::new_index_first`] instance, whose lambda parameters are
/// `[index, element]` instead of `[element, index]`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayFilter {
    signature: HigherOrderSignature,
    index_first: bool,
}

impl Default for SparkArrayFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            index_first: false,
        }
    }

    pub fn new_index_first() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            index_first: true,
        }
    }

    /// Whether this instance expects the lambda parameters in `[index, element]`
    /// order (an index-only lambda rewritten by the planner). Used by the
    /// serialization codec to reconstruct the correct variant.
    pub fn is_index_first(&self) -> bool {
        self.index_first
    }
}

impl HigherOrderUDFImpl for SparkArrayFilter {
    fn name(&self) -> &str {
        "filter"
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

        // Spark lambdas may declare one parameter (the element) or two (the
        // element and its 0-based index).
        let index = Arc::new(Field::new("", DataType::Int32, false));
        let params = if self.index_first {
            vec![index, element]
        } else {
            vec![element, index]
        };
        Ok(LambdaParametersProgress::Complete(vec![params]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (list, _lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            "",
            list.data_type().clone(),
            list.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;

        let list_values = match extract_list_values(&list_array, args.return_type())? {
            ListValuesResult::EarlyReturn(v) => return Ok(v),
            ListValuesResult::Values(v) => v,
        };

        let field = match args.return_field.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            _ => {
                return exec_err!(
                    "{} expected return_field to be a list, got {}",
                    self.name(),
                    args.return_field
                );
            }
        };

        let values_param = || Ok(Arc::clone(&list_values));
        let index_param = || index_array(self.name(), &list_array);
        let params: [&dyn Fn() -> Result<ArrayRef>; 2] = if self.index_first {
            [&index_param, &values_param]
        } else {
            [&values_param, &index_param]
        };
        let predicate_output = lambda.evaluate(&params, |arrays| {
            let indices = list_values_row_number(&list_array)?;
            Ok(take_arrays(arrays, &indices, None)?)
        })?;

        // Scalar predicate short-circuit: the lambda body used neither the
        // element nor the index, e.g. x -> true or x -> false/null.
        if let ColumnarValue::Scalar(ScalarValue::Boolean(b)) = &predicate_output {
            return match b {
                Some(true) => Ok(ColumnarValue::Array(list_array)),
                _ => Ok(ColumnarValue::Array(empty_filtered_list(
                    &list_array,
                    field,
                )?)),
            };
        }

        let predicate = predicate_output.into_array(list_values.len())?;
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!(
                "{} lambda must return boolean, got {}",
                self.name(),
                predicate.data_type()
            );
        };

        let filtered_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                let adjusted_offsets = adjust_offsets_for_slice(list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(ListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list::<i64>();
                let adjusted_offsets = adjust_offsets_for_slice(large_list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(LargeListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    large_list.nulls().cloned(),
                ))
            }
            other => exec_err!("expected list, got {other}")?,
        };

        Ok(ColumnarValue::Array(filtered_list))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
    }
}

/// Returns a list array with every non-null sublist emptied, preserving the null buffer.
/// Used for the `x -> false` / `x -> null` scalar predicate short-circuit.
fn empty_filtered_list(list_array: &ArrayRef, field: FieldRef) -> Result<ArrayRef> {
    let n = list_array.len();
    let empty_values = new_empty_array(field.data_type());
    Ok(match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32; n + 1])),
                empty_values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            Arc::new(LargeListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i64; n + 1])),
                empty_values,
                list.nulls().cloned(),
            ))
        }
        other => return exec_err!("expected list, got {other}"),
    })
}

/// Filters flat list values using a boolean predicate, returning filtered values and
/// recomputed per-sublist offsets. Null predicate values are treated as false.
fn filter_list_values<O: OffsetSizeTrait>(
    values: &ArrayRef,
    predicate: &BooleanArray,
    offsets: &OffsetBuffer<O>,
) -> Result<(ArrayRef, OffsetBuffer<O>)> {
    let num_sublists = offsets.len().saturating_sub(1);
    let mut builder = OffsetBufferBuilder::<O>::new(num_sublists);

    let has_nulls = predicate.null_count() > 0;
    for i in 0..num_sublists {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        let count = if has_nulls {
            (start..end)
                .filter(|&j| predicate.is_valid(j) && predicate.value(j))
                .count()
        } else {
            predicate
                .values()
                .slice(start, end - start)
                .count_set_bits()
        };
        builder.push_length(count);
    }

    let new_offsets = builder.finish();

    if new_offsets.last() == offsets.last() {
        return Ok((Arc::clone(values), offsets.clone()));
    }

    // arrow_filter treats null predicate values as false
    let filtered_values = arrow_filter(values.as_ref(), predicate)?;
    Ok((filtered_values, new_offsets))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::expr::{HigherOrderFunction, LambdaVariable};
    use datafusion_expr::{col, lambda, lit, Expr, HigherOrderUDF};
    use datafusion_physical_expr::create_physical_expr;

    use super::*;

    fn create_i32_list(
        values: impl Into<Int32Array>,
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    ) -> ListArray {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        ListArray::new(list_field, offsets, Arc::new(values.into()), nulls)
    }

    fn eval_filter_on_i32_list(
        list: impl Array + Clone + 'static,
        params: &[&str],
        lambda_body: Expr,
    ) -> Result<ArrayRef> {
        eval_with_func(SparkArrayFilter::new(), list, params, lambda_body)
    }

    fn eval_with_func(
        func: SparkArrayFilter,
        list: impl Array + Clone + 'static,
        params: &[&str],
        lambda_body: Expr,
    ) -> Result<ArrayRef> {
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new(
                "list",
                list.data_type().clone(),
                list.is_nullable(),
            )]
            .into(),
            HashMap::new(),
        )?;

        let func = Arc::new(HigherOrderUDF::new_from_impl(func));
        create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                func,
                vec![col("list"), lambda(params.iter().copied(), lambda_body)],
            )),
            &schema,
            &ExecutionProps::new(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(list.clone())],
        )?)?
        .into_array(list.len())
    }

    fn v() -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ))
    }

    fn i() -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            "i".to_string(),
            Some(Arc::new(Field::new("i", DataType::Int32, false))),
        ))
    }

    fn keep_greater_than_two(list: impl Array + Clone + 'static) -> Result<ArrayRef> {
        eval_filter_on_i32_list(list, &["v"], v().gt(lit(2i32)))
    }

    #[test]
    fn filter_basic() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            None,
        );

        let res = keep_greater_than_two(list)?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_multiple_sublists() -> Result<()> {
        let list = create_i32_list(
            vec![1, 5, 2, 4, 3],
            OffsetBuffer::<i32>::from_lengths(vec![2, 3]),
            None,
        );

        let res = keep_greater_than_two(list)?;
        let actual = res.as_list::<i32>();

        // [1,5] -> [5], [2,4,3] -> [4,3]
        let expected = create_i32_list(
            vec![5, 4, 3],
            OffsetBuffer::<i32>::from_lengths(vec![1, 2]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_sliced_list() -> Result<()> {
        // First sublist [0] is sliced away; sliced array covers sublists [1..3]
        let list = create_i32_list(
            vec![0, 1, 5, 2, 4, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 3]),
            None,
        )
        .slice(1, 2);

        let res = keep_greater_than_two(list)?;
        let actual = res.as_list::<i32>();

        // [1,5,2] -> [5], [4,3,7] -> [4,3,7]
        let expected = create_i32_list(
            vec![5, 4, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_null_sublists_not_evaluated() -> Result<()> {
        // The null sublist (index 1) contains values that would pass the
        // predicate if evaluated. They must not appear in the output.
        let list = create_i32_list(
            vec![1, 5, 99, 100, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let res = keep_greater_than_two(list)?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![5, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![1, 0, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_all_filtered_out() -> Result<()> {
        let list = create_i32_list(vec![1, 2], OffsetBuffer::<i32>::from_lengths(vec![2]), None);

        let res = keep_greater_than_two(list)?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![0i32; 0],
            OffsetBuffer::<i32>::from_lengths(vec![0]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_with_index_keeps_first_element_of_each_sublist() -> Result<()> {
        let list = create_i32_list(
            vec![10, 20, 30, 40, 50],
            OffsetBuffer::<i32>::from_lengths(vec![3, 2]),
            None,
        );

        // (v, i) -> i = 0 with the unused element parameter dropped, as the
        // planner rewrites it for the index-first instance
        let res = eval_with_func(
            SparkArrayFilter::new_index_first(),
            list,
            &["i"],
            i().eq(lit(0i32)),
        )?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![10, 40],
            OffsetBuffer::<i32>::from_lengths(vec![1, 1]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_with_index_and_element() -> Result<()> {
        let list = create_i32_list(
            vec![0, 5, 1, 10, 2],
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            None,
        );

        // (v, i) -> v > i
        let res = eval_filter_on_i32_list(list, &["v", "i"], v().gt(i()))?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![5, 10],
            OffsetBuffer::<i32>::from_lengths(vec![2]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn filter_with_index_restarts_per_sublist_when_sliced() -> Result<()> {
        let list = create_i32_list(
            vec![0, 10, 20, 30, 40, 50],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 2]),
            None,
        )
        .slice(1, 2);

        // (v, i) -> i = 0 over sublists [10,20,30] and [40,50]
        let res = eval_with_func(
            SparkArrayFilter::new_index_first(),
            list,
            &["i"],
            i().eq(lit(0i32)),
        )?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![10, 40],
            OffsetBuffer::<i32>::from_lengths(vec![1, 1]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn scalar_true_predicate_returns_original_list() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        let res = eval_filter_on_i32_list(list.clone(), &["v"], lit(true))?;
        assert_eq!(res.as_list::<i32>(), &list);
        Ok(())
    }

    #[test]
    fn scalar_false_predicate_returns_empty_sublists() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3, 4],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2]),
            None,
        );

        let res = eval_filter_on_i32_list(list, &["v"], lit(false))?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![0i32; 0],
            OffsetBuffer::<i32>::from_lengths(vec![0, 0]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }
}
