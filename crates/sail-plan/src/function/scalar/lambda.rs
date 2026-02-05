use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{expr, lit};
use datafusion_functions_nested::expr_fn;
use sail_common::spec;
use sail_common::spec::Literal;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_filter::{FilterOp, SparkArrayFilter};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction as ScalarFunctionType, ScalarFunctionInput};

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

/// Parse a lambda expression to extract the filter predicate.
///
/// Supports patterns like:
/// - `x > 2` -> (GreaterThan, Literal(2), false)
/// - `2 < x` -> (GreaterThan, Literal(2), false) (normalized)
/// - `x == 5` -> (Equal, Literal(5), false)
fn parse_lambda_predicate(
    function: &spec::Expr,
    lambda_var_names: &[String],
) -> PlanResult<(FilterOp, ScalarValue, bool)> {
    // The lambda body should be an UnresolvedFunction with a comparison operator
    let spec::Expr::UnresolvedFunction(func) = function else {
        return Err(PlanError::unsupported(format!(
            "Lambda body must be a function call, got: {:?}",
            function
        )));
    };

    // Get the operator name
    let op_name = func
        .function_name
        .parts()
        .iter()
        .map(|p| p.as_ref())
        .collect::<Vec<_>>()
        .join(".");

    let op = FilterOp::from_str(&op_name).ok_or_else(|| {
        PlanError::unsupported(format!(
            "Unsupported lambda operator: {}. Supported: >, >=, <, <=, =, ==, !=, <>",
            op_name
        ))
    })?;

    // We expect exactly 2 arguments
    if func.arguments.len() != 2 {
        return Err(PlanError::invalid(format!(
            "Comparison operator expects 2 arguments, got {}",
            func.arguments.len()
        )));
    }

    let (left, right) = (&func.arguments[0], &func.arguments[1]);

    // Helper to check if an expression is a lambda variable reference
    // This handles both:
    // - UnresolvedNamedLambdaVariable (from DataFrame API)
    // - UnresolvedAttribute (from SQL parser)
    let is_lambda_var = |expr: &spec::Expr| -> Option<bool> {
        match expr {
            spec::Expr::UnresolvedNamedLambdaVariable(v) => {
                let var_name: Vec<String> = v.name.clone().into();
                if lambda_var_names.iter().any(|n| var_name.contains(n)) {
                    Some(true)
                } else {
                    None
                }
            }
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id: _,
                is_metadata_column: _,
            } => {
                let var_name: Vec<String> = name.clone().into();
                if lambda_var_names.iter().any(|n| var_name.contains(n)) {
                    Some(true)
                } else {
                    None
                }
            }
            _ => None,
        }
    };

    // Determine which argument is the variable and which is the literal
    let (var_is_left, literal_expr) = match (is_lambda_var(left), is_lambda_var(right)) {
        (Some(true), None) => (true, right),
        (None, Some(true)) => (false, left),
        (Some(true), Some(true)) => {
            return Err(PlanError::unsupported(
                "Lambda predicate cannot compare two variables",
            ))
        }
        _ => {
            return Err(PlanError::unsupported(
                "Lambda predicate must compare a variable with a literal",
            ))
        }
    };

    // Extract the literal value
    let scalar_value = match literal_expr {
        spec::Expr::Literal(lit) => literal_to_scalar_value(lit)?,
        _ => {
            return Err(PlanError::unsupported(format!(
                "Lambda predicate must compare with a literal, got: {:?}",
                literal_expr
            )))
        }
    };

    // If the variable is on the right side, we need to reverse the operator
    // e.g., `2 < x` becomes `x > 2`
    let (final_op, reversed) = if var_is_left {
        (op, false)
    } else {
        // Reverse the comparison direction
        let reversed_op = match op {
            FilterOp::GreaterThan => FilterOp::LessThan,
            FilterOp::GreaterThanOrEqual => FilterOp::LessThanOrEqual,
            FilterOp::LessThan => FilterOp::GreaterThan,
            FilterOp::LessThanOrEqual => FilterOp::GreaterThanOrEqual,
            FilterOp::Equal => FilterOp::Equal,
            FilterOp::NotEqual => FilterOp::NotEqual,
        };
        (reversed_op, false)
    };

    Ok((final_op, scalar_value, reversed))
}

fn literal_to_scalar_value(lit: &Literal) -> PlanResult<ScalarValue> {
    match lit {
        Literal::Int8 { value: Some(v) } => Ok(ScalarValue::Int8(Some(*v))),
        Literal::Int16 { value: Some(v) } => Ok(ScalarValue::Int16(Some(*v))),
        Literal::Int32 { value: Some(v) } => Ok(ScalarValue::Int32(Some(*v))),
        Literal::Int64 { value: Some(v) } => Ok(ScalarValue::Int64(Some(*v))),
        Literal::Float32 { value: Some(v) } => Ok(ScalarValue::Float32(Some(*v))),
        Literal::Float64 { value: Some(v) } => Ok(ScalarValue::Float64(Some(*v))),
        Literal::Utf8 { value: Some(v) } => Ok(ScalarValue::Utf8(Some(v.clone()))),
        Literal::Boolean { value: Some(v) } => Ok(ScalarValue::Boolean(Some(*v))),
        Literal::Null => Ok(ScalarValue::Null),
        other => Err(PlanError::unsupported(format!(
            "Unsupported literal type in lambda: {:?}",
            other
        ))),
    }
}

/// Create an array filter expression from a lambda predicate.
///
/// This is called by the resolver when it encounters `filter(array, lambda)`.
pub fn create_array_filter_expr(
    array_expr: expr::Expr,
    lambda_function: &spec::Expr,
    lambda_arguments: &[spec::UnresolvedNamedLambdaVariable],
) -> PlanResult<expr::Expr> {
    // Extract lambda variable names
    let var_names: Vec<String> = lambda_arguments
        .iter()
        .flat_map(|v| {
            let names: Vec<String> = v.name.clone().into();
            names
        })
        .collect();

    // Parse the lambda body to extract the predicate
    let (op, scalar_value, reversed) = parse_lambda_predicate(lambda_function, &var_names)?;

    // Create the SparkArrayFilter UDF with the extracted predicate
    let filter_udf = SparkArrayFilter::new(op, scalar_value, reversed);
    let udf = Arc::new(datafusion_expr::ScalarUDF::new_from_impl(filter_udf));

    Ok(expr::Expr::ScalarFunction(ScalarFunction {
        func: udf,
        args: vec![array_expr],
    }))
}

pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunctionType)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aggregate", F::unknown("aggregate")),
        ("array_sort", F::custom(array_sort)),
        ("exists", F::unknown("exists")),
        ("filter", F::unknown("filter")), // Handled specially in the resolver
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
