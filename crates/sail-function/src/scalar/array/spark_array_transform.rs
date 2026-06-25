/// [CREDIT]: https://github.com/apache/datafusion/blob/f911d529a57b211eb44a98b253f97d839f60019f/datafusion/functions-nested/src/array_transform.rs
///
/// Spark-compatible `transform(array, lambda)` higher-order function.
///
/// Adapted from `array_transform` in `datafusion-functions-nested` 54.0.0
/// (`src/array_transform.rs` and the `pub(crate)` helpers in `src/lambda_utils.rs`),
/// with one Spark-specific extension: the lambda receives an optional second
/// parameter holding the 0-based element index (`transform(arr, (x, i) -> ...)`),
/// typed as `Int32` to match Spark's `IntegerType`.
///
/// Spark semantics: see `ArrayTransform` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`
/// — the result is an array of the lambda's return type (element-wise map, the
/// sublist structure is preserved), `bindInternal` provides `(elementType,
/// containsNull)` plus `(IntegerType, false)` for two-parameter lambdas, and the
/// lambda is never evaluated on elements behind a null sublist.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, LargeListArray, ListArray};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values_row_number};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::scalar::array::lambda_utils::{
    coerce_single_list_arg, extract_list_values, index_array, value_lambda_pair, ListValuesResult,
};

/// Like [`super::spark_array_filter::SparkArrayFilter`], the physical lambda
/// evaluation batch is laid out as `[captures..., params...]` with the body
/// projected to the columns it actually uses, which only lines up when the used
/// parameters form a prefix of the declared ones. A lambda such as
/// `(x, i) -> i + 1` (element unused, index used) breaks that alignment, so the
/// planner rewrites it to a single-parameter lambda over a
/// [`SparkArrayTransform::new_index_first`] instance, whose lambda parameters are
/// `[index, element]` instead of `[element, index]`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayTransform {
    signature: HigherOrderSignature,
    index_first: bool,
}

impl Default for SparkArrayTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayTransform {
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

impl HigherOrderUDFImpl for SparkArrayTransform {
    fn name(&self) -> &str {
        "transform"
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
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        // The element type of the result is the lambda body's return type, not
        // the input element type — this is what distinguishes `transform` (map)
        // from `filter` (which keeps the input element type).
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));

        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            other => return plan_err!("{} expected a list, got {other}", self.name()),
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
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

        // By passing closures, `lambda.evaluate` only materialises the params the
        // lambda body actually references (the index closure is never built for
        // the common `x -> ...` case).
        let values_param = || Ok(Arc::clone(&list_values));
        let index_param = || index_array(self.name(), &list_array);
        let params: [&dyn Fn() -> Result<ArrayRef>; 2] = if self.index_first {
            [&index_param, &values_param]
        } else {
            [&values_param, &index_param]
        };
        let transformed_values = lambda
            .evaluate(&params, |arrays| {
                // If any column got captured, adjust it to the flattened values,
                // duplicating values of sublists with multiple elements and
                // dropping values of empty sublists.
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(list_values.len())?;

        let transformed_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                // `list_values` returns sliced values for a sliced list, so the
                // offsets must be adjusted to stay valid.
                let adjusted_offsets = adjust_offsets_for_slice(list);
                Arc::new(ListArray::new(
                    field,
                    adjusted_offsets,
                    transformed_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list::<i64>();
                let adjusted_offsets = adjust_offsets_for_slice(large_list);
                Arc::new(LargeListArray::new(
                    field,
                    adjusted_offsets,
                    transformed_values,
                    large_list.nulls().cloned(),
                ))
            }
            other => exec_err!("{} expected list, got {other}", self.name())?,
        };

        Ok(ColumnarValue::Array(transformed_list))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
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

    fn eval_with_func(
        func: SparkArrayTransform,
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

    fn eval_transform_on_i32_list(
        list: impl Array + Clone + 'static,
        params: &[&str],
        lambda_body: Expr,
    ) -> Result<ArrayRef> {
        eval_with_func(SparkArrayTransform::new(), list, params, lambda_body)
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

    #[test]
    fn transform_multiply_by_two() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        let res = eval_transform_on_i32_list(list, &["v"], v() * lit(2i32))?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![2, 4, 6],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn transform_multiple_sublists() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![2, 3]),
            None,
        );

        let res = eval_transform_on_i32_list(list, &["v"], v() + lit(10i32))?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![11, 12, 13, 14, 15],
            OffsetBuffer::<i32>::from_lengths(vec![2, 3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn transform_not_evaluated_on_unreachable_sliced_values() -> Result<()> {
        // The first sublist [0] is sliced away; if the lambda were evaluated on
        // it, `100 / 0` would error.
        let list = create_i32_list(
            vec![0, 4, 100, 25, 20, 5, 2, 1, 10],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 4, 1]),
            None,
        )
        .slice(1, 3);

        let res = eval_transform_on_i32_list(list, &["v"], lit(100i32) / v())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![25, 1, 4, 5, 20, 50, 100, 10],
            OffsetBuffer::<i32>::from_lengths(vec![3, 4, 1]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn transform_not_evaluated_on_values_behind_null() -> Result<()> {
        let list = create_i32_list(
            // 0 behind the null sublist would error if evaluated (divide by 0)
            vec![100, 20, 10, 0, 1, 2, 0, 1, 50],
            OffsetBuffer::<i32>::from_lengths(vec![3, 4, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let res = eval_transform_on_i32_list(list, &["v"], lit(100i32) / v())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![1, 5, 10, 100, 2],
            OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn transform_with_index_and_element() -> Result<()> {
        let list = create_i32_list(
            vec![10, 20, 30],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        // (v, i) -> v + i
        let res = eval_transform_on_i32_list(list, &["v", "i"], v() + i())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![10, 21, 32],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn transform_index_only_uses_index_first_instance() -> Result<()> {
        let list = create_i32_list(
            vec![100, 200, 300, 400, 500],
            OffsetBuffer::<i32>::from_lengths(vec![3, 2]),
            None,
        );

        // (v, i) -> i with the unused element parameter dropped, as the planner
        // rewrites it for the index-first instance: per-sublist 0-based indices.
        let res = eval_with_func(SparkArrayTransform::new_index_first(), list, &["i"], i())?;
        let actual = res.as_list::<i32>();

        // The index is non-nullable, so the result element field is non-nullable
        // too (the result nullability mirrors the lambda body's, matching Spark).
        let non_null_field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let expected = ListArray::new(
            non_null_field,
            OffsetBuffer::<i32>::from_lengths(vec![3, 2]),
            Arc::new(Int32Array::from(vec![0, 1, 2, 0, 1])),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }
}
