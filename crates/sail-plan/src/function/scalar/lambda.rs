use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::expr::{self, HigherOrderFunction, Lambda, LambdaVariable};
use datafusion_expr::{lit, Expr, ScalarUDF};
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::higher_order::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_filter_expr::{
    build_batch_schema, SparkArrayFilterExpr,
};

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
///
/// Uses DataFusion's native HigherOrderUDF when no outer-column captures are needed.
/// Falls back to SparkArrayFilterExpr (ScalarUDF) for outer-column capture until DF
/// resolves https://github.com/apache/datafusion/issues/21172.
fn filter_lambda(input: LambdaFunctionInput) -> PlanResult<Expr> {
    let LambdaFunctionInput {
        array_expr,
        resolved_lambda,
        element_type,
        element_column_name,
        element_var_name,
        index_column_name,
        index_var_name,
        outer_columns,
        outer_column_exprs,
        function_context,
    } = input;

    // Native HigherOrderUDF path — no outer column capture needed and element type is known
    if outer_columns.is_empty() && element_type != DataType::Null {
        let body = replace_synthetic_columns_with_lambda_vars(
            resolved_lambda,
            &element_column_name,
            &element_var_name,
            &element_type,
            index_column_name.as_deref(),
            index_var_name.as_deref(),
        )?;

        let mut params = vec![element_var_name];
        if let Some(idx_name) = index_var_name {
            params.push(idx_name);
        }

        let lambda_expr = Expr::Lambda(Lambda::new(params, body));
        return Ok(Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(SparkArrayFilter::new()),
            vec![array_expr, lambda_expr],
        )));
    }

    // Fallback: ScalarUDF path for lambdas with outer-column captures.
    // Compile the PhysicalExpr once at planning time using the session state.
    let schema = build_batch_schema(
        &element_column_name,
        &element_type,
        index_column_name.as_deref(),
        &outer_columns,
    );
    let df_schema = DFSchema::try_from(Arc::clone(&schema))
        .map_err(|e| PlanError::internal(format!("filter lambda: failed to build schema: {e}")))?;
    let physical_expr = function_context
        .session_context
        .state()
        .create_physical_expr(resolved_lambda.clone(), &df_schema)
        .map_err(|e| PlanError::internal(format!("filter lambda: compile failed: {e}")))?;

    let mut udf_args = vec![array_expr];
    udf_args.extend(outer_column_exprs);

    let filter_udf = SparkArrayFilterExpr::with_precompiled(
        resolved_lambda,
        physical_expr,
        element_type,
        element_column_name,
        index_column_name,
        outer_columns,
    );

    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(filter_udf)),
        args: udf_args,
    }))
}

/// Walk `expr` replacing `Column(synthetic_id)` references with `LambdaVariable` nodes.
fn replace_synthetic_columns_with_lambda_vars(
    expr: Expr,
    elem_id: &str,
    elem_name: &str,
    elem_type: &DataType,
    idx_id: Option<&str>,
    idx_name: Option<&str>,
) -> PlanResult<Expr> {
    let elem_field = Arc::new(Field::new(elem_name, elem_type.clone(), true));
    let idx_field = idx_name.map(|name| Arc::new(Field::new(name, DataType::Int64, false)));

    let result = expr.transform(|e| match &e {
        Expr::Column(Column {
            name,
            relation: None,
            ..
        }) if name.as_str() == elem_id => Ok(Transformed::yes(Expr::LambdaVariable(
            LambdaVariable::new(elem_name.to_string(), Some(Arc::clone(&elem_field))),
        ))),
        Expr::Column(Column {
            name,
            relation: None,
            ..
        }) if idx_id.is_some_and(|id| name.as_str() == id) => {
            let field = idx_field
                .as_ref()
                .and_then(|_f| idx_name.map(|n| Arc::new(Field::new(n, DataType::Int64, false))));
            Ok(Transformed::yes(Expr::LambdaVariable(LambdaVariable::new(
                idx_name.unwrap_or("i").to_string(),
                field,
            ))))
        }
        _ => Ok(Transformed::no(e)),
    })?;
    Ok(result.data)
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
