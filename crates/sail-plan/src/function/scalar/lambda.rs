use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, Expr, ScalarUDF};
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_filter_expr::SparkArrayFilterExpr;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    LambdaFunction, LambdaFunctionInput, ScalarFunction, ScalarFunctionInput,
};

/// Spark's array_sort always puts NULLs last, regardless of sort direction
/// https://spark.apache.org/docs/latest/api/sql/index.html#array_sort
fn array_sort_spark(array: expr::Expr, asc: expr::Expr) -> PlanResult<expr::Expr> {
    let (sort, nulls) = match asc {
        expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _metadata) => (
            lit(ScalarValue::Utf8(Some("ASC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS LAST".to_string()))),
        ),
        expr::Expr::Literal(ScalarValue::Boolean(Some(false)), _metadata) => (
            lit(ScalarValue::Utf8(Some("DESC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS LAST".to_string()))),
        ),
        _ => {
            return Err(PlanError::invalid(format!(
                "Invalid asc value for array_sort_spark: {asc}"
            )))
        }
    };
    Ok(expr_fn::array_sort(array, sort, nulls))
}

fn array_sort(input: ScalarFunctionInput) -> PlanResult<datafusion_expr::Expr> {
    let (array, rest) = input.arguments.at_least_one()?;

    // Check if there's a second argument (lambda comparator)
    if !rest.is_empty() {
        // array_sort with lambda comparator is not yet implemented
        return Err(crate::error::PlanError::todo(
            "array_sort with lambda comparator is not yet implemented",
        ));
    }

    // array_sort(array) without lambda - sorts in ascending order with NULLs last (Spark behavior)
    array_sort_spark(array, lit(true))
}

/// Scalar functions that may or may not use lambdas.
/// These are called when NO lambda is present (e.g., array_sort without comparator).
pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aggregate", F::unknown("aggregate")),
        ("array_sort", F::custom(array_sort)),
        ("exists", F::unknown("exists")),
        ("forall", F::unknown("forall")),
        ("map_filter", F::unknown("map_filter")),
        ("map_zip_with", F::unknown("map_zip_with")),
        ("reduce", F::unknown("reduce")),
        ("transform", F::unknown("transform")),
        ("transform_keys", F::unknown("transform_keys")),
        ("transform_values", F::unknown("transform_values")),
        ("zip_with", F::unknown("zip_with")),
    ]
}

/// Handler for filter(array, lambda) - filters array elements using a predicate lambda.
fn filter_lambda(input: LambdaFunctionInput) -> PlanResult<Expr> {
    let LambdaFunctionInput {
        array_expr,
        resolved_lambda,
        element_type,
        element_column_name,
        index_column_name,
        outer_columns,
        outer_column_exprs,
    } = input;

    // Build UDF arguments: array + outer column expressions
    let mut udf_args = vec![array_expr];
    udf_args.extend(outer_column_exprs);

    // Create the filter UDF with appropriate constructor based on what's present
    let filter_udf = if outer_columns.is_empty() {
        if let Some(idx_name) = index_column_name {
            SparkArrayFilterExpr::with_index_column(
                resolved_lambda,
                element_type,
                element_column_name,
                idx_name,
            )
        } else {
            SparkArrayFilterExpr::with_column_name(
                resolved_lambda,
                element_type,
                element_column_name,
            )
        }
    } else {
        SparkArrayFilterExpr::with_outer_columns(
            resolved_lambda,
            element_type,
            element_column_name,
            index_column_name,
            outer_columns,
        )
    };

    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(filter_udf)),
        args: udf_args,
    }))
}

/// Higher-order (lambda) function handlers.
/// These are called when a lambda IS present in the function call.
pub fn list_built_in_higher_order_functions() -> Vec<(&'static str, LambdaFunction)> {
    use crate::function::common::LambdaFunctionBuilder as F;

    vec![
        ("aggregate", F::unknown("aggregate")),
        ("array_sort", F::unknown("array_sort with comparator")),
        ("exists", F::unknown("exists")),
        ("filter", F::custom(filter_lambda)),
        ("forall", F::unknown("forall")),
        ("map_filter", F::unknown("map_filter")),
        ("map_zip_with", F::unknown("map_zip_with")),
        ("reduce", F::unknown("reduce")),
        ("transform", F::unknown("transform")),
        ("transform_keys", F::unknown("transform_keys")),
        ("transform_values", F::unknown("transform_values")),
        ("zip_with", F::unknown("zip_with")),
    ]
}
