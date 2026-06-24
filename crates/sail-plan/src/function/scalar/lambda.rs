use std::sync::{Arc, LazyLock};

use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion_expr::{expr, lit, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda};
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

static SPARK_ARRAY_FILTER_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new())));

static SPARK_ARRAY_AGGREGATE_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayAggregate::new())));

static SPARK_ARRAY_FILTER_INDEX_FIRST_UDF: LazyLock<Arc<HigherOrderUDF>> = LazyLock::new(|| {
    Arc::new(HigherOrderUDF::new_from_impl(
        SparkArrayFilter::new_index_first(),
    ))
});

static SPARK_ARRAY_EXISTS_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayExists::new())));

static SPARK_ARRAY_FORALL_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayForall::new())));

static SPARK_ARRAY_TRANSFORM_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())));

static SPARK_ARRAY_TRANSFORM_INDEX_FIRST_UDF: LazyLock<Arc<HigherOrderUDF>> = LazyLock::new(|| {
    Arc::new(HigherOrderUDF::new_from_impl(
        SparkArrayTransform::new_index_first(),
    ))
});

pub(crate) fn is_higher_order_function(name: &str) -> bool {
    matches!(
        name,
        "aggregate" | "reduce" | "filter" | "transform" | "exists" | "forall"
    )
}

/// Returns the lambda parameter fields of a built-in higher-order function, one
/// set per lambda argument, given the fields of its arguments (`None` for the
/// lambda arguments themselves). Used by the resolver to type lambda variables
/// before resolving lambda bodies.
pub(crate) fn get_lambda_parameters(
    function_name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> PlanResult<Vec<Vec<FieldRef>>> {
    let udf = match function_name {
        "aggregate" | "reduce" => &SPARK_ARRAY_AGGREGATE_UDF,
        "filter" => &SPARK_ARRAY_FILTER_UDF,
        "transform" => &SPARK_ARRAY_TRANSFORM_UDF,
        "exists" => &SPARK_ARRAY_EXISTS_UDF,
        "forall" => &SPARK_ARRAY_FORALL_UDF,
        other => {
            return Err(PlanError::internal(format!(
                "not a higher-order function: {other}"
            )))
        }
    };
    match udf.lambda_parameters(0, fields)? {
        LambdaParametersProgress::Complete(params) => Ok(params),
        LambdaParametersProgress::Partial(_) => Err(PlanError::internal(format!(
            "unresolved lambda parameters for function: {function_name}"
        ))),
    }
}

/// Returns whether the lambda body references the given lambda parameter,
/// ignoring occurrences shadowed by a nested lambda that redeclares it.
fn lambda_body_uses_param(body: &expr::Expr, param: &str) -> PlanResult<bool> {
    let mut found = false;
    body.apply(|e| {
        Ok(match e {
            expr::Expr::Lambda(inner) if inner.params.iter().any(|p| p == param) => {
                TreeNodeRecursion::Jump
            }
            expr::Expr::LambdaVariable(variable) if variable.name == param => {
                found = true;
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}

/// Builds a `(array, lambda)` higher-order function expression supporting Spark's
/// optional 0-based index parameter `(x, i) -> ...`.
///
/// The physical lambda evaluation batch contains all declared parameters, while
/// the body is projected to the columns it actually uses; the two only line up
/// when the used parameters form a prefix of the declared ones. An index-only
/// lambda `(x, i) -> p(i)` is therefore rewritten to `i -> p(i)` over the
/// `udf_index_first` instance, whose lambda parameters are `[index, element]`.
fn array_lambda_with_index(
    name: &str,
    input: ScalarFunctionInput,
    udf: &LazyLock<Arc<HigherOrderUDF>>,
    udf_index_first: &LazyLock<Arc<HigherOrderUDF>>,
) -> PlanResult<expr::Expr> {
    let (array, lambda) = input.arguments.two()?;
    let expr::Expr::Lambda(lambda) = lambda else {
        return Err(PlanError::AnalysisError(format!(
            "`{name}` expects a lambda function as its second argument"
        )));
    };
    if lambda.params.len() > 2 {
        return Err(PlanError::AnalysisError(format!(
            "`{name}` expects a lambda function with 1 or 2 parameters, got {}",
            lambda.params.len()
        )));
    }
    let (func, lambda) = if lambda.params.len() == 2
        && !lambda_body_uses_param(&lambda.body, &lambda.params[0])?
        && lambda_body_uses_param(&lambda.body, &lambda.params[1])?
    {
        let Lambda { params, body } = lambda;
        let (_element, index) = params.two()?;
        (
            Arc::clone(udf_index_first),
            Lambda {
                params: vec![index],
                body,
            },
        )
    } else {
        (Arc::clone(udf), lambda)
    };
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        func,
        vec![array, expr::Expr::Lambda(lambda)],
    )))
}

fn filter(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    array_lambda_with_index(
        "filter",
        input,
        &SPARK_ARRAY_FILTER_UDF,
        &SPARK_ARRAY_FILTER_INDEX_FIRST_UDF,
    )
}

fn transform(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    array_lambda_with_index(
        "transform",
        input,
        &SPARK_ARRAY_TRANSFORM_UDF,
        &SPARK_ARRAY_TRANSFORM_INDEX_FIRST_UDF,
    )
}

fn exists(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, lambda) = input.arguments.two()?;
    let expr::Expr::Lambda(lambda) = lambda else {
        return Err(PlanError::AnalysisError(
            "`exists` expects a lambda function as its second argument".to_string(),
        ));
    };
    if lambda.params.len() != 1 {
        return Err(PlanError::AnalysisError(format!(
            "`exists` expects a lambda function with 1 parameter, got {}",
            lambda.params.len()
        )));
    }
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_ARRAY_EXISTS_UDF),
        vec![array, expr::Expr::Lambda(lambda)],
    )))
}

fn forall(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, lambda) = input.arguments.two()?;
    let expr::Expr::Lambda(lambda) = lambda else {
        return Err(PlanError::AnalysisError(
            "`forall` expects a lambda function as its second argument".to_string(),
        ));
    };
    if lambda.params.len() != 1 {
        return Err(PlanError::AnalysisError(format!(
            "`forall` expects a lambda function with 1 parameter, got {}",
            lambda.params.len()
        )));
    }
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_ARRAY_FORALL_UDF),
        vec![array, expr::Expr::Lambda(lambda)],
    )))
}

fn aggregate(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;
    let (array, zero, merge, finish) = match args.len() {
        3 => {
            let (array, zero, merge) = args.three()?;
            let acc = match &merge {
                expr::Expr::Lambda(lambda) => lambda
                    .params
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "acc".to_string()),
                _ => "acc".to_string(),
            };
            let finish = expr::Expr::Lambda(Lambda::new(
                vec![acc.clone()],
                expr::Expr::LambdaVariable(LambdaVariable::new(acc, None)),
            ));
            (array, zero, merge, finish)
        }
        4 => args.four()?,
        n => {
            return Err(PlanError::AnalysisError(format!(
                "`aggregate` expects 3 or 4 arguments, got {n}"
            )));
        }
    };

    let expr::Expr::Lambda(merge) = merge else {
        return Err(PlanError::AnalysisError(
            "`aggregate` expects a merge lambda as its third argument".to_string(),
        ));
    };
    if merge.params.len() != 2 {
        return Err(PlanError::AnalysisError(format!(
            "`aggregate` expects a merge lambda with 2 parameters, got {}",
            merge.params.len()
        )));
    }

    let expr::Expr::Lambda(finish) = finish else {
        return Err(PlanError::AnalysisError(
            "`aggregate` expects a finish lambda as its fourth argument".to_string(),
        ));
    };
    if finish.params.len() != 1 {
        return Err(PlanError::AnalysisError(format!(
            "`aggregate` expects a finish lambda with 1 parameter, got {}",
            finish.params.len()
        )));
    }

    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_ARRAY_AGGREGATE_UDF),
        vec![
            array,
            zero,
            expr::Expr::Lambda(merge),
            expr::Expr::Lambda(finish),
        ],
    )))
}

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

pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aggregate", F::custom(aggregate)),
        ("array_sort", F::custom(array_sort)),
        ("exists", F::custom(exists)),
        ("filter", F::custom(filter)),
        ("forall", F::custom(forall)),
        ("map_filter", F::unknown("map_filter")),
        ("map_zip_with", F::unknown("map_zip_with")),
        ("reduce", F::custom(aggregate)),
        ("transform", F::custom(transform)),
        ("transform_keys", F::unknown("transform_keys")),
        ("transform_values", F::unknown("transform_values")),
        ("zip_with", F::unknown("zip_with")),
    ]
}
