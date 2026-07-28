/// Spark-compatible `array_sort(array, comparator)` higher-order function.
///
/// Unlike `filter`/`transform` (element-wise maps evaluated once over the
/// flattened values), `array_sort`'s lambda is a *comparator* `(left, right) ->
/// int`: it must be invoked over pairs of elements and its result drives the
/// reordering of each sublist.
///
/// Spark semantics: see `ArraySort` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`
/// — the comparator receives both elements (NULLs included; the lambda decides
/// their order, there is no NULLS-LAST special-casing in this form), must return
/// `IntegerType`, and a `NULL` comparator result raises an error
/// (`comparatorReturnsNull`). The sort is stable (`java.util.Arrays.sort`) and
/// the result preserves the input array type.
///
/// The one-argument form `array_sort(array)` is handled by the planner via
/// DataFusion's built-in `array_sort`; only the comparator form reaches this UDF.
use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, LargeListArray, ListArray, UInt32Array,
};
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion_common::utils::adjust_offsets_for_slice;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::scalar::array::lambda_utils::{
    ListValuesResult, coerce_single_list_arg, extract_list_values, value_lambda_pair,
};

/// The physical lambda evaluation batch is laid out as `[captures..., params...]`
/// with the body projected to the columns it actually uses, which only lines up
/// when the used parameters form a prefix of the declared ones. A comparator that
/// uses only its second parameter (`(l, r) -> f(r)`, `l` unused) breaks that
/// alignment, so the planner rewrites it to a single-parameter lambda over a
/// [`SparkArraySort::new_swapped`] instance, which feeds the lambda the columns in
/// `[right, left]` order. See [`super::spark_array_filter::SparkArrayFilter`] for
/// the identical `index_first` workaround.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraySort {
    signature: HigherOrderSignature,
    swapped: bool,
}

impl Default for SparkArraySort {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraySort {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            swapped: false,
        }
    }

    pub fn new_swapped() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            swapped: true,
        }
    }

    /// Whether this instance feeds the lambda the comparison columns in
    /// `[right, left]` order (a right-only comparator rewritten by the planner).
    /// Used by the serialization codec to reconstruct the correct variant.
    pub fn is_swapped(&self) -> bool {
        self.swapped
    }
}

impl HigherOrderUDFImpl for SparkArraySort {
    fn name(&self) -> &str {
        "array_sort"
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

        // The comparator takes two elements of the array. Both parameters share
        // the element type, so the `[left, right]` / `[right, left]` ordering is
        // irrelevant for typing (it only matters for evaluation in `swapped`).
        let params = vec![Arc::clone(&element), element];
        Ok(LambdaParametersProgress::Complete(vec![params]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        // Spark requires the comparator to return exactly `IntegerType` (Int32);
        // bigint/double/etc. raise `UNEXPECTED_RETURN_TYPE` at analysis.
        if lambda.data_type() != &DataType::Int32 {
            return plan_err!(
                "{} comparator must return INT, got {}",
                self.name(),
                lambda.data_type()
            );
        }

        // `array_sort` preserves the input array type (element type and
        // nullability are unchanged by sorting).
        match list.data_type() {
            DataType::List(_) | DataType::LargeList(_) => Ok(Arc::new(Field::new(
                "",
                list.data_type().clone(),
                list.is_nullable(),
            ))),
            other => plan_err!("{} expected a list, got {other}", self.name()),
        }
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

        // Per-sublist boundaries into the (null-aware) flattened values. Null
        // sublists contribute a zero-length window, so they yield no pairs and
        // no output elements, matching `extract_list_values`/`list_values`.
        let offsets: Vec<usize> = match list_array.data_type() {
            DataType::List(_) => adjust_offsets_for_slice(list_array.as_list::<i32>())
                .iter()
                .map(|o| o.as_usize())
                .collect(),
            DataType::LargeList(_) => adjust_offsets_for_slice(list_array.as_list::<i64>())
                .iter()
                .map(|o| o.as_usize())
                .collect(),
            other => return exec_err!("{} expected list, got {other}", self.name()),
        };

        // Sort each sublist independently with a stable comparison sort,
        // evaluating the comparator lambda once per comparison — the same model
        // as Spark's `java.util.Arrays.sort` + per-pair `f.eval`. This keeps the
        // extra memory O(1) and the comparisons O(n log n) per sublist.
        //
        // TODO(perf): each comparison builds a one-row `RecordBatch` and walks
        // the lambda's physical expression tree, so the per-comparison constant
        // is high (heavier than Spark's codegen). It never materialises the
        // O(n^2) pair matrix (the previous approach, which could OOM), but for
        // very large arrays the per-call overhead dominates. If this becomes a
        // hot path, batch the comparisons across rows with an oblivious sorting
        // network (Batcher odd-even) or a synchronised merge sort: O(log^2 n)
        // (resp. O(n)) lambda evaluations over wide batches instead of one per
        // comparison. Revisit the complexity here before optimising elsewhere.
        let compare = |row: usize, pa: usize, pb: usize| -> Result<Ordering> {
            let left = list_values.slice(pa, 1);
            let right = list_values.slice(pb, 1);
            let left_param = || Ok(Arc::clone(&left));
            let right_param = || Ok(Arc::clone(&right));
            let params: [&dyn Fn() -> Result<ArrayRef>; 2] = if self.swapped {
                [&right_param, &left_param]
            } else {
                [&left_param, &right_param]
            };
            let result = lambda
                .evaluate(&params, |captures| {
                    // The eval batch is this comparison's single outer row, so
                    // captured columns collapse to that row's value.
                    let index = UInt32Array::from(vec![row as u32]);
                    Ok(take_arrays(captures, &index, None)?)
                })?
                .into_array(1)?;
            // `return_field_from_args` already rejected non-`Int32` comparators.
            let result = result
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("{} comparator must return INT", self.name())
                })?;
            if result.is_null(0) {
                return exec_err!(
                    "{} comparator returned NULL; the comparator must return a non-null integer",
                    self.name()
                );
            }
            Ok(result.value(0).cmp(&0))
        };

        let mut perm: Vec<u32> = Vec::with_capacity(list_values.len());
        for (row, &[start, end]) in offsets.array_windows::<2>().enumerate() {
            let n = end - start;
            if n == 0 {
                continue;
            }
            // Stable sort to mirror Spark's `java.util.Arrays.sort` (TimSort).
            let mut local: Vec<usize> = (0..n).collect();
            let mut error: Option<DataFusionError> = None;
            local.sort_by(|&a, &b| {
                if error.is_some() {
                    return Ordering::Equal;
                }
                match compare(row, start + a, start + b) {
                    Ok(ordering) => ordering,
                    Err(e) => {
                        error = Some(e);
                        Ordering::Equal
                    }
                }
            });
            if let Some(e) = error {
                return Err(e);
            }
            perm.extend(local.into_iter().map(|l| (start + l) as u32));
        }

        let perm_array = UInt32Array::from(perm);
        let sorted_values = take(list_values.as_ref(), &perm_array, None)?;

        let sorted_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                Arc::new(ListArray::new(
                    field,
                    adjust_offsets_for_slice(list),
                    sorted_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list::<i64>();
                Arc::new(LargeListArray::new(
                    field,
                    adjust_offsets_for_slice(large_list),
                    sorted_values,
                    large_list.nulls().cloned(),
                ))
            }
            other => return exec_err!("{} expected list, got {other}", self.name()),
        };

        Ok(ColumnarValue::Array(sorted_list))
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
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::expr::{HigherOrderFunction, LambdaVariable};
    use datafusion_expr::{Case, Expr, HigherOrderUDF, col, lambda, lit};
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
        func: SparkArraySort,
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

    fn lvar(name: &str) -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            name.to_string(),
            Some(Arc::new(Field::new(name, DataType::Int32, true))),
        ))
    }

    /// Builds a standard `(l, r) -> case when l < r then -1 when l > r then 1
    /// else 0 end` comparator body (ascending).
    fn asc_comparator() -> Expr {
        let l = lvar("l");
        let r = lvar("r");
        Expr::Case(Case::new(
            None,
            vec![
                (Box::new(l.clone().lt(r.clone())), Box::new(lit(-1i32))),
                (Box::new(l.gt(r)), Box::new(lit(1i32))),
            ],
            Some(Box::new(lit(0i32))),
        ))
    }

    #[test]
    fn sort_ascending() -> Result<()> {
        let list = create_i32_list(
            vec![5, 6, 1],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        let res = eval_with_func(SparkArraySort::new(), list, &["l", "r"], asc_comparator())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![1, 5, 6],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn sort_descending() -> Result<()> {
        let l = lvar("l");
        let r = lvar("r");
        // (l, r) -> r - l  (descending)
        let body = r - l;
        let list = create_i32_list(
            vec![3, 1, 2],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        let res = eval_with_func(SparkArraySort::new(), list, &["l", "r"], body)?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![3, 2, 1],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn sort_multiple_sublists() -> Result<()> {
        let list = create_i32_list(
            vec![3, 1, 2, 9, 8, 7],
            OffsetBuffer::<i32>::from_lengths(vec![3, 3]),
            None,
        );

        let res = eval_with_func(SparkArraySort::new(), list, &["l", "r"], asc_comparator())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![1, 2, 3, 7, 8, 9],
            OffsetBuffer::<i32>::from_lengths(vec![3, 3]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn sort_preserves_null_sublists() -> Result<()> {
        let list = create_i32_list(
            vec![3, 1, 2, 0, 0, 9, 8],
            OffsetBuffer::<i32>::from_lengths(vec![3, 2, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let res = eval_with_func(SparkArraySort::new(), list, &["l", "r"], asc_comparator())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![1, 2, 3, 8, 9],
            OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn sort_single_and_empty_sublists_unchanged() -> Result<()> {
        let list = create_i32_list(
            vec![42],
            OffsetBuffer::<i32>::from_lengths(vec![1, 0]),
            None,
        );

        let res = eval_with_func(SparkArraySort::new(), list, &["l", "r"], asc_comparator())?;
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![42],
            OffsetBuffer::<i32>::from_lengths(vec![1, 0]),
            None,
        );

        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn sort_null_comparator_result_errors() {
        // (l, r) -> NULL :: int  → a comparator that always returns NULL.
        let comparator = Expr::Literal(ScalarValue::Int32(None), None);
        let list = create_i32_list(vec![2, 1], OffsetBuffer::<i32>::from_lengths(vec![2]), None);

        let result = eval_with_func(SparkArraySort::new(), list, &["l", "r"], comparator);
        assert!(result.is_err(), "null comparator result should error");
        if let Err(err) = result {
            assert!(err.to_string().contains("comparator returned NULL"));
        }
    }

    #[test]
    fn sort_swapped_right_only_comparator() -> Result<()> {
        // A degenerate comparator `(l, r) -> r` that uses only the right element.
        // The planner would route this to the swapped instance with a
        // single-parameter lambda; here we exercise the instance directly.
        let list = create_i32_list(
            vec![3, 1, 2],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        // Single param bound to the right element (swapped feeds `[right, left]`).
        let res = eval_with_func(SparkArraySort::new_swapped(), list, &["r"], lvar("r"))?;
        // cmp(a, b) = value(b); the resulting order is implementation-defined but
        // must be a valid permutation of the input.
        let actual = res.as_list::<i32>();
        assert_eq!(actual.value_length(0), 3);
        Ok(())
    }
}
