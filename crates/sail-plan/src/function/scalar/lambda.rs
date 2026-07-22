use std::sync::{Arc, LazyLock};

use datafusion_common::ScalarValue;
use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion_expr::{
    HigherOrderTypeSignature, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda, expr, lit,
};
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_sort::SparkArraySort;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

static SPARK_ARRAY_FILTER_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new())));

static SPARK_ARRAY_AGGREGATE_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayAggregate::new())));

static SPARK_ARRAY_AGGREGATE_ELEMENT_FIRST_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| {
        Arc::new(HigherOrderUDF::new_from_impl(
            SparkArrayAggregate::new_element_first(),
        ))
    });

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

static SPARK_ARRAY_SORT_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArraySort::new())));

static SPARK_ARRAY_SORT_SWAPPED_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArraySort::new_swapped())));

/// The single name-to-UDF lookup for built-in higher-order functions.
///
/// This is the only place a function name is matched: the resolver runs before
/// the UDF is looked up in the catalog, so the parsed name is the only key
/// available at that point. Everything else about a higher-order function
/// (whether it is one, which arguments are lambdas) is derived from the UDF this
/// returns, so those facts are declared once, on the UDF itself.
fn higher_order_udf(name: &str) -> Option<&'static LazyLock<Arc<HigherOrderUDF>>> {
    Some(match name.trim().to_lowercase().as_str() {
        "aggregate" | "reduce" => &SPARK_ARRAY_AGGREGATE_UDF,
        "filter" => &SPARK_ARRAY_FILTER_UDF,
        "transform" => &SPARK_ARRAY_TRANSFORM_UDF,
        "exists" => &SPARK_ARRAY_EXISTS_UDF,
        "forall" => &SPARK_ARRAY_FORALL_UDF,
        "array_sort" => &SPARK_ARRAY_SORT_UDF,
        _ => return None,
    })
}

pub(crate) fn is_higher_order_function(name: &str) -> bool {
    higher_order_udf(name).is_some()
}

/// Returns the argument positions where a built-in higher-order function expects
/// a lambda, for a call with `arity` arguments.
///
/// Spark accepts a plain expression in any of these positions and wraps it in a
/// lambda whose parameters go unused, so `exists(a, true)` means
/// `exists(a, x -> true)`. The positions come straight from the UDF's own
/// signature — the slots it declares as [`ValueOrLambda::Lambda`] — rather than
/// a hand-written table. Positions at or beyond `arity` are dropped so that a
/// shorter overload (e.g. `array_sort(a)` or `aggregate` without the optional
/// finish lambda) keeps resolving as an ordinary call.
pub(crate) fn lambda_argument_positions(function_name: &str, arity: usize) -> Vec<usize> {
    let Some(udf) = higher_order_udf(function_name) else {
        return vec![];
    };
    let HigherOrderTypeSignature::Exact(slots) = &udf.signature().type_signature else {
        return vec![];
    };
    slots
        .iter()
        .enumerate()
        .filter(|(index, slot)| *index < arity && matches!(slot, ValueOrLambda::Lambda(())))
        .map(|(index, _)| index)
        .collect()
}

/// Returns how many parameters to declare when wrapping a bare (non-lambda)
/// expression in a lambda for `function_name`, given the `available` parameters
/// the function's lambda supports.
///
/// The wrapped body references no parameters, so only the minimum arity Spark
/// declares is needed. Spark wraps the element-wise higher-order functions with
/// a single hidden element parameter (see `higherOrderFunctions.scala`);
/// declaring the optional extras (e.g. `transform`/`filter`'s index) would only
/// force DataFusion to materialize an unused per-element column. `array_sort`
/// and `aggregate` genuinely require their full comparator/accumulator arity, so
/// they keep every available parameter.
pub(crate) fn wrapped_lambda_param_count(function_name: &str, available: usize) -> usize {
    match function_name.trim().to_lowercase().as_str() {
        "transform" | "filter" | "exists" | "forall" => 1,
        _ => available,
    }
}

/// Returns the lambda parameter fields of a built-in higher-order function, one
/// set per lambda argument, given the fields of its arguments (`None` for the
/// lambda arguments themselves). Used by the resolver to type lambda variables
/// before resolving lambda bodies.
pub(crate) fn get_lambda_parameters(
    function_name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> PlanResult<Vec<Vec<FieldRef>>> {
    let udf = higher_order_udf(function_name).ok_or_else(|| {
        PlanError::internal(format!("not a higher-order function: {function_name}"))
    })?;
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
    let (func, lambda) = match lambda {
        expr::Expr::Lambda(lambda) if lambda.params.len() > 2 => {
            return Err(PlanError::AnalysisError(format!(
                "`{name}` expects a lambda function with 1 or 2 parameters, got {}",
                lambda.params.len()
            )));
        }
        expr::Expr::Lambda(lambda)
            if lambda.params.len() == 2
                && !lambda_body_uses_param(&lambda.body, &lambda.params[0])?
                && lambda_body_uses_param(&lambda.body, &lambda.params[1])? =>
        {
            let Lambda { params, body } = lambda;
            let (_element, index) = params.two()?;
            (
                Arc::clone(udf_index_first),
                expr::Expr::Lambda(Lambda {
                    params: vec![index],
                    body,
                }),
            )
        }
        lambda => (Arc::clone(udf), lambda),
    };
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        func,
        vec![array, lambda],
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
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_ARRAY_EXISTS_UDF),
        vec![array, lambda],
    )))
}

fn forall(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, lambda) = input.arguments.two()?;
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_ARRAY_FORALL_UDF),
        vec![array, lambda],
    )))
}

/// Enforces the exact lambda arity Spark requires for a higher-order
/// `aggregate`/`reduce` argument, but only on a direct `Expr::Lambda` match.
///
/// The UDF binding only rejects lambdas with too many parameters, so a `merge`
/// lambda with fewer than 2 parameters would otherwise bind silently to a prefix
/// and return a wrong result instead of erroring like Spark
/// (`INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH`).
///
/// Direct expr matching is unreliable for aliased or otherwise-wrapped lambdas,
/// so anything that is not a bare `Expr::Lambda` falls back to the lenient path
/// (no arity check here) until expr matching is improved more broadly.
fn expect_lambda_arity(role: &str, expr: &expr::Expr, arity: usize) -> PlanResult<()> {
    if let expr::Expr::Lambda(lambda) = expr
        && lambda.params.len() != arity
    {
        // Mirrors Spark's `INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH`
        // wording, naming no function (`aggregate`/`reduce` share this builder).
        return Err(PlanError::AnalysisError(format!(
            "Invalid lambda function call. The {role} lambda function expects {arity} arguments, but got {}",
            lambda.params.len()
        )));
    }
    Ok(())
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
    expect_lambda_arity("merge", &merge, 2)?;
    expect_lambda_arity("finish", &finish, 1)?;
    let (func, merge) = match merge {
        expr::Expr::Lambda(lambda)
            if lambda.params.len() == 2
                && !lambda_body_uses_param(&lambda.body, &lambda.params[0])?
                && lambda_body_uses_param(&lambda.body, &lambda.params[1])? =>
        {
            let Lambda { params, body } = lambda;
            let (_acc, element) = params.two()?;
            (
                Arc::clone(&SPARK_ARRAY_AGGREGATE_ELEMENT_FIRST_UDF),
                expr::Expr::Lambda(Lambda {
                    params: vec![element],
                    body,
                }),
            )
        }
        merge => (Arc::clone(&SPARK_ARRAY_AGGREGATE_UDF), merge),
    };
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        func,
        vec![array, zero, merge, finish],
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
            )));
        }
    };
    Ok(expr_fn::array_sort(array, sort, nulls))
}

/// Builds `array_sort(array)` (no comparator) and the comparator form
/// `array_sort(array, (left, right) -> int)`.
///
/// Expectations (a 2-parameter comparator, plus the swapped rewrite below) are
/// only enforced on a direct `Expr::Lambda` match; any other shape (e.g. an
/// aliased or otherwise wrapped expression) is passed through to the UDF
/// unchanged rather than rejected, mirroring `aggregate`/`array_lambda_with_index`.
///
/// The physical lambda evaluation batch contains all declared parameters while
/// the body is projected to the columns it actually uses; the two only line up
/// when the used parameters form a prefix of the declared ones. A comparator that
/// uses only its second parameter (`(l, r) -> f(r)`, `l` unused) is therefore
/// rewritten to `r -> f(r)` over the `SPARK_ARRAY_SORT_SWAPPED_UDF` instance,
/// which feeds the lambda the columns in `[right, left]` order. This mirrors the
/// index-first rewrite in [`array_lambda_with_index`].
fn array_sort(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, rest) = input.arguments.at_least_one()?;

    if rest.is_empty() {
        // array_sort(array) without lambda - ascending order, NULLs last (Spark behavior).
        return array_sort_spark(array, lit(true));
    }

    let comparator = rest.one()?;
    let (func, comparator) = match comparator {
        expr::Expr::Lambda(lambda) if lambda.params.len() != 2 => {
            return Err(PlanError::AnalysisError(format!(
                "`array_sort` expects a comparator lambda with 2 parameters, got {}",
                lambda.params.len()
            )));
        }
        expr::Expr::Lambda(lambda)
            if !lambda_body_uses_param(&lambda.body, &lambda.params[0])?
                && lambda_body_uses_param(&lambda.body, &lambda.params[1])? =>
        {
            let Lambda { params, body } = lambda;
            let (_left, right) = params.two()?;
            (
                Arc::clone(&SPARK_ARRAY_SORT_SWAPPED_UDF),
                expr::Expr::Lambda(Lambda {
                    params: vec![right],
                    body,
                }),
            )
        }
        comparator => (Arc::clone(&SPARK_ARRAY_SORT_UDF), comparator),
    };
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        func,
        vec![array, comparator],
    )))
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
