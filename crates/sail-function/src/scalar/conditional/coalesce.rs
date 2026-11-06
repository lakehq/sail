// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/core/coalesce.rs

use std::sync::Arc;

use arrow::array::{BooleanArray, new_null_array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::conditional_expressions::CaseBuilder;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use itertools::Itertools;
use sail_common_datafusion::metadata::coerced_fields_metadata;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::conditional::coalesce_lazy::CoalesceLazy;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CoalesceFunc {
    pub(super) signature: Signature,
}

impl Default for CoalesceFunc {
    fn default() -> Self {
        CoalesceFunc::new()
    }
}

impl CoalesceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoalesceFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "coalesce"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // If any the arguments in coalesce is non-null, the result is non-null
        let nullable = args.arg_fields.iter().all(|f| f.is_nullable());
        let Some(return_type) = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .find_or_first(|d| !d.is_null())
            .cloned()
        else {
            return plan_err!("coalesce must have at least one argument");
        };
        // Propagate field metadata (e.g. Arrow extension types) from the arguments that can
        // supply a value; see `coerced_fields_metadata` for the merge semantics.
        let metadata = coerced_fields_metadata(args.arg_fields.iter().map(|f| f.as_ref()))?;
        Ok(metadata
            .add_to_field(Field::new(self.name(), return_type, nullable))
            .into())
    }

    /// coalesce evaluates to the first value which is not NULL
    ///
    /// Only reached when [`Self::simplify`] keeps the call because an argument
    /// is volatile; evaluates each argument exactly once.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!(
                "coalesce was called with {} arguments. It requires at least 1.",
                args.len()
            );
        }

        let return_type = args[0].data_type();
        let mut return_array = args.iter().filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        if let Some(size) = return_array.next() {
            // start with nulls as default output
            let mut current_value = new_null_array(&return_type, size);
            let mut remainder = BooleanArray::from(vec![true; size]);

            for arg in args {
                match arg {
                    ColumnarValue::Array(ref array) => {
                        let to_apply = and(&remainder, &is_not_null(array.as_ref())?)?;
                        current_value = zip(&to_apply, array, &current_value)?;
                        remainder = and(&remainder, &is_null(array)?)?;
                    }
                    ColumnarValue::Scalar(value) => {
                        if value.is_null() {
                            continue;
                        } else {
                            let last_value = value.to_scalar()?;
                            current_value = zip(&remainder, &last_value, &current_value)?;
                            break;
                        }
                    }
                }
                if remainder.true_count() == 0 {
                    break;
                }
            }
            Ok(ColumnarValue::Array(current_value))
        } else {
            let result = args
                .iter()
                .find_map(|x| match x {
                    ColumnarValue::Scalar(s) if !s.is_null() => Some(x.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| args[0].clone());
            Ok(result)
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return plan_err!("coalesce must have at least one argument");
        }

        if args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(args.one()?));
        }

        // `coalesce(a, b)` is rewritten to the (lazy, fast)
        // `CASE WHEN a IS NOT NULL THEN a ELSE b END`, which names every argument
        // but the last one twice. A volatile argument named twice is evaluated
        // twice, so the rewrite stops at the first volatile non-final argument
        // and the rest is left to the `invoke_with_args` kernel, which evaluates
        // each argument once. See https://github.com/apache/datafusion/issues/25477
        let (init, last) = args.split_at(args.len() - 1);
        let (prefix, rest) = match init.iter().position(|arg| arg.is_volatile()) {
            None => (init, last[0].clone()),
            // Sail: the kernel evaluates every argument eagerly, so an argument
            // after a non-NULL volatile one would still be evaluated (and could
            // fail), unlike Spark's lazy `Coalesce`. Use `CoalesceLazy` for the
            // rest unless every later argument is a column or a literal, which
            // is cheap and cannot fail (this also keeps the plan shape of
            // `coalesce(input_file_name(), '')`).
            Some(i) if !args[i + 1..].iter().all(is_column_or_literal) => {
                if i == 0 {
                    return Ok(ExprSimplifyResult::Simplified(CoalesceLazy::call(args)?));
                }
                (&init[..i], CoalesceLazy::call(args[i..].to_vec())?)
            }
            Some(0) => return Ok(ExprSimplifyResult::Original(args)),
            Some(i) => (
                &init[..i],
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::new(ScalarUDF::new_from_impl(CoalesceFunc::new())),
                    args[i..].to_vec(),
                )),
            ),
        };

        let whens = prefix.iter().map(|x| x.clone().is_not_null()).collect();
        Ok(ExprSimplifyResult::Simplified(
            CaseBuilder::new(None, whens, prefix.to_vec(), Some(Box::new(rest))).end()?,
        ))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let eager = vec![&args[0]];
        let lazy = args[1..].iter().collect();
        Some((eager, lazy))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("coalesce must have at least one argument");
        }

        try_type_union_resolution(arg_types)
    }
}

// Sail: see `CoalesceFunc::simplify`.
fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(..))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{Expr, ScalarUDF, cast, col, lit, when};

    use super::CoalesceFunc;
    use crate::scalar::math::random::Random;
    use crate::scalar::misc::raise_error::RaiseError;

    fn coalesce(args: Vec<Expr>) -> Expr {
        ScalarUDF::from(CoalesceFunc::new()).call(args)
    }

    fn rand() -> Expr {
        ScalarUDF::from(Random::new()).call(vec![])
    }

    fn raise_error(message: Expr) -> Expr {
        cast(
            ScalarUDF::from(RaiseError::new()).call(vec![message]),
            DataType::Float64,
        )
    }

    fn table(columns: Vec<(&str, ArrayRef)>) -> Result<DataFrame> {
        let batch = RecordBatch::try_from_iter(columns)?;
        SessionContext::new().read_batch(batch)
    }

    fn nullable_doubles() -> Result<DataFrame> {
        table(vec![(
            "a",
            Arc::new(Float64Array::from(vec![None, Some(1.0)])) as ArrayRef,
        )])
    }

    /// Each probe: (name, dataframe with a single output column `v`).
    fn probes() -> Result<Vec<(&'static str, DataFrame)>> {
        let three = table(vec![(
            "m",
            Arc::new(StringArray::from(vec!["boom"; 3])) as ArrayRef,
        )])?;
        let many = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10_000)) as ArrayRef,
        )])?;
        let sometimes_null =
            when(rand().lt(lit(0.5)), lit(ScalarValue::Int64(None))).otherwise(lit(1_i64))?;
        Ok(vec![
            // Volatile first argument that is never null: later arguments
            // must not be evaluated, so the error is never raised.
            (
                "volatile_first_literal_error",
                three.clone().select(vec![
                    coalesce(vec![rand(), raise_error(lit("boom"))]).alias("v"),
                ])?,
            ),
            // The error message comes from a column.
            (
                "volatile_first_column_error",
                three.select(vec![
                    coalesce(vec![rand(), raise_error(col("m"))]).alias("v"),
                ])?,
            ),
            // Volatile argument in the middle: only the rest after the
            // non-volatile prefix stays a `coalesce` call.
            (
                "volatile_middle_error",
                nullable_doubles()?.select(vec![
                    coalesce(vec![col("a"), rand(), raise_error(lit("boom"))]).alias("v"),
                ])?,
            ),
            // Volatile last argument: only evaluated where `a` is null.
            (
                "volatile_last",
                nullable_doubles()?.select(vec![coalesce(vec![col("a"), rand()]).alias("v")])?,
            ),
            // A volatile, sometimes-null first argument must be evaluated once:
            // the result is never null when the last argument is non-null.
            (
                "volatile_sometimes_null",
                many.select(vec![coalesce(vec![sometimes_null, lit(2_i64)]).alias("v")])?,
            ),
        ])
    }

    /// Spark semantics: every probe succeeds and `v` is never null.
    async fn null_count(df: DataFrame) -> Result<usize> {
        let batches = df.collect().await?;
        Ok(batches.iter().map(|b| b.column(0).null_count()).sum())
    }

    /// Later arguments of the lazy path are evaluated on the rows that are
    /// still NULL, and each row takes the first non-NULL value.
    #[tokio::test]
    async fn coalesce_lazy_takes_the_first_non_null_value() -> Result<()> {
        let x = || col("x");
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                // Volatile, and NULL where x % 3 = 0.
                when(
                    rand().lt(lit(2.0)).and((x() % lit(3_i64)).eq(lit(0_i64))),
                    lit(ScalarValue::Int64(None)),
                )
                .otherwise(x())?,
                // NULL where x % 2 = 0.
                when(
                    (x() % lit(2_i64)).eq(lit(0_i64)),
                    lit(ScalarValue::Int64(None)),
                )
                .otherwise(x() * lit(10_i64))?,
                x() * lit(100_i64),
            ])
            .alias("v"),
        ])?;
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        let expected = (0..10_i64)
            .map(|x| match (x % 3, x % 2) {
                (0, 0) => Some(x * 100),
                (0, _) => Some(x * 10),
                _ => Some(x),
            })
            .collect::<Vec<_>>();
        assert_eq!(values, expected);
        Ok(())
    }

    /// When the first argument is NULL on every row, the next argument is
    /// evaluated on the whole batch.
    #[tokio::test]
    async fn coalesce_lazy_first_argument_null_on_every_row() -> Result<()> {
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                when(rand().lt(lit(2.0)), lit(ScalarValue::Int64(None))).end()?,
                col("x") * lit(10_i64),
            ])
            .alias("v"),
        ])?;
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            (0..10_i64).map(|x| Some(x * 10)).collect::<Vec<_>>()
        );
        Ok(())
    }

    async fn collect_int64(df: DataFrame) -> Result<Vec<Option<i64>>> {
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect())
    }

    /// Covers a lazy argument evaluated on every row whose body uses no
    /// columns, an all-rows argument followed by a partial one, and rows that
    /// stay NULL after every argument.
    #[tokio::test]
    async fn coalesce_lazy_mixed_pending_rows_and_trailing_nulls() -> Result<()> {
        let x = || col("x");
        let null = || lit(ScalarValue::Int64(None));
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                // NULL on every row.
                when(rand().lt(lit(2.0)), null()).end()?,
                // Evaluated on every row, uses no columns, NULL on every row.
                when(rand().lt(lit(2.0)), null()).end()?,
                // Evaluated on every row; NULL where x is even.
                when((x() % lit(2_i64)).eq(lit(0_i64)), null()).otherwise(x() * lit(10_i64))?,
                // Evaluated on the even rows; NULL where x % 3 = 0.
                when((x() % lit(3_i64)).eq(lit(0_i64)), null()).otherwise(x() * lit(100_i64))?,
            ])
            .alias("v"),
        ])?;
        let expected = (0..10_i64)
            .map(|x| match (x % 2, x % 3) {
                (1, _) => Some(x * 10),
                (_, 0) => None,
                _ => Some(x * 100),
            })
            .collect::<Vec<_>>();
        assert_eq!(collect_int64(df).await?, expected);
        Ok(())
    }

    /// The lazy path with a struct return type.
    #[tokio::test]
    async fn coalesce_lazy_struct_values() -> Result<()> {
        use datafusion::functions::core::expr_fn::{get_field, named_struct};

        let x = || col("x");
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            get_field(
                coalesce(vec![
                    when(
                        rand().lt(lit(2.0)).and((x() % lit(2_i64)).eq(lit(0_i64))),
                        lit(ScalarValue::Null),
                    )
                    .otherwise(named_struct(vec![lit("a"), x()]))?,
                    named_struct(vec![lit("a"), x() * lit(10_i64)]),
                ]),
                "a",
            )
            .alias("v"),
        ])?;
        let expected = (0..10_i64)
            .map(|x| Some(if x % 2 == 0 { x * 10 } else { x }))
            .collect::<Vec<_>>();
        assert_eq!(collect_int64(df).await?, expected);
        Ok(())
    }

    /// Like Spark's `Coalesce`, arguments are evaluated lazily and at most
    /// once per row, also when an argument is volatile.
    #[tokio::test]
    async fn coalesce_volatile_arguments_follow_spark_semantics() -> Result<()> {
        let mut failures = vec![];
        for (name, df) in probes()? {
            match null_count(df).await {
                Ok(0) => {}
                Ok(n) => failures.push(format!("{name}: {n} null results")),
                Err(e) => failures.push(format!("{name}: {e}")),
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
        Ok(())
    }
}
