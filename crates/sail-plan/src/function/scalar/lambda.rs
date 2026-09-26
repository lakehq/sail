use std::sync::{Arc, LazyLock};

use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::arrow::compute::can_cast_types;
use datafusion_common::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchema, ScalarValue, plan_err};
use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion_expr::{
    ExprSchemable, HigherOrderUDF, LambdaParametersProgress, ScalarUDF, ValueOrLambda, cast, expr,
    lit,
};
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_sort::SparkArraySort;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::array::spark_zip_with::SparkZipWith;
use sail_function::scalar::map::spark_map_filter::SparkMapFilter;
use sail_function::scalar::map::utils::map_type_from_key_value_types;
use sail_function::scalar::spark_struct_rename::SparkStructRename;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput, expr_contains_python_udf};
use crate::resolver::build_rename_target_type;

static SPARK_ARRAY_FILTER_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new())));

static SPARK_MAP_FILTER_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkMapFilter::new())));

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

pub(crate) fn is_higher_order_function(name: &str) -> bool {
    matches!(
        name.trim().to_lowercase().as_str(),
        "aggregate"
            | "reduce"
            | "filter"
            | "map_filter"
            | "transform"
            | "exists"
            | "forall"
            | "array_sort"
            | "zip_with"
            | "map_zip_with"
    )
}

/// Returns the lambda parameter fields of a built-in higher-order function, one
/// set per lambda argument, given the fields of its arguments (`None` for the
/// lambda arguments themselves). Used by the resolver to type lambda variables
/// before resolving lambda bodies.
pub(crate) fn get_lambda_parameters(
    function_name: &str,
    ansi_mode: bool,
    case_sensitive: bool,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> PlanResult<Vec<Vec<FieldRef>>> {
    let zip_udf;
    let udf: &Arc<HigherOrderUDF> = match function_name.trim().to_lowercase().as_str() {
        "aggregate" | "reduce" => &SPARK_ARRAY_AGGREGATE_UDF,
        "filter" => &SPARK_ARRAY_FILTER_UDF,
        "map_filter" => &SPARK_MAP_FILTER_UDF,
        "transform" => &SPARK_ARRAY_TRANSFORM_UDF,
        "exists" => &SPARK_ARRAY_EXISTS_UDF,
        "forall" => &SPARK_ARRAY_FORALL_UDF,
        "array_sort" => &SPARK_ARRAY_SORT_UDF,
        name @ ("zip_with" | "map_zip_with") => {
            zip_udf = Arc::new(HigherOrderUDF::new_from_impl(SparkZipWith::new(
                name == "map_zip_with",
                ansi_mode,
                case_sensitive,
            )));
            &zip_udf
        }
        other => {
            return Err(PlanError::internal(format!(
                "not a higher-order function: {other}"
            )));
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

pub(super) fn lambda_with_fresh_parameter(body: expr::Expr, base: &str) -> PlanResult<expr::Expr> {
    let mut parameter = base.to_owned();
    while lambda_body_uses_param(&body, &parameter)? {
        parameter.push('_');
    }
    Ok(expr::Expr::Lambda(Lambda::new(vec![parameter], body)))
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

fn map_filter(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    // TODO: Defer shared literal-zero division validation so NullType operands
    // containing 1 / 0 can reach the coercions below (see the sail-bug tests).
    let (mut map, predicate) = input.arguments.two()?;
    // Spark binds an ordinary expression as a hidden lambda whose parameters
    // are unused. Avoid capturing variables from any enclosing lambda.
    let mut predicate = if matches!(predicate, expr::Expr::Lambda(_)) {
        predicate
    } else {
        // Spark coerces a null map only when the predicate is already resolved.
        // An explicit lambda remains unresolved and must still reject this input.
        if map.get_type(input.function_context.schema)? == DataType::Null {
            validate_map_filter_null_expr(&map, input.function_context.schema)?;
            map = lit(ScalarValue::try_new_null(&map_type_from_key_value_types(
                &DataType::Null,
                &DataType::Null,
            ))?);
        }
        let mut params = Vec::with_capacity(2);
        for base in ["__map_key", "__map_value"] {
            let mut name = base.to_string();
            while lambda_body_uses_param(&predicate, &name)? {
                name.push('_');
            }
            params.push(name);
        }
        expr::Expr::Lambda(Lambda::new(params, predicate))
    };
    expect_lambda_arity("map_filter", &predicate, 2)?;
    if let expr::Expr::Lambda(lambda) = &mut predicate
        && lambda.body.get_type(input.function_context.schema)? == DataType::Null
    {
        // Preserve validation of lambdas outside higher-order function arguments.
        let has_bare_lambda = matches!(lambda.body.as_ref(), expr::Expr::Lambda(_))
            || lambda.body.exists(|expression| {
                if matches!(expression, expr::Expr::HigherOrderFunction(_)) {
                    return Ok(false);
                }
                let mut has_lambda_child = false;
                expression.apply_children(|child| {
                    has_lambda_child |= matches!(child, expr::Expr::Lambda(_));
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(has_lambda_child)
            })?;
        if !has_bare_lambda {
            validate_map_filter_null_expr(&lambda.body, input.function_context.schema)?;
            // Spark replaces NullType predicates with Boolean NULL before evaluation.
            *lambda.body = lit(ScalarValue::Boolean(None));
        }
    }
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::clone(&SPARK_MAP_FILTER_UDF),
        vec![map, predicate],
    )))
}

fn validate_map_filter_null_expr(expression: &expr::Expr, schema: &DFSchema) -> PlanResult<()> {
    let mut rewriter = TypeCoercionRewriter::new(schema);
    expression.clone().transform_up(|expression| {
        // Spark may discard a nested higher-order function before checking its
        // return type. Only run coercion when its input fields can be inferred.
        let mut inputs_resolved = true;
        expression.apply_children(|child| {
            let child = match child {
                expr::Expr::Lambda(lambda) => lambda.body.as_ref(),
                child => child,
            };
            inputs_resolved &= child.to_field(schema).is_ok();
            Ok(TreeNodeRecursion::Continue)
        })?;
        if inputs_resolved {
            // Type coercion does not validate explicit casts. Preserve their
            // type errors before replacing a NullType operand with a literal.
            if let expr::Expr::Cast(expr::Cast { expr, field })
            | expr::Expr::TryCast(expr::TryCast { expr, field }) = &expression
            {
                let from = expr.get_type(schema)?;
                if !can_cast_types(&from, field.data_type()) {
                    return plan_err!("cannot cast {from} to {}", field.data_type());
                }
            }
            rewriter.f_up(expression)
        } else {
            Ok(Transformed::no(expression))
        }
    })?;
    Ok(())
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

/// Enforces the exact lambda arity Spark requires for a higher-order function
/// argument, but only on a direct `Expr::Lambda` match.
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
            let finish_field = Arc::new(Field::new(
                acc.clone(),
                zero.get_type(input.function_context.schema)?,
                true,
            ));
            let finish = expr::Expr::Lambda(Lambda::new(
                vec![acc.clone()],
                expr::Expr::LambdaVariable(LambdaVariable::new(acc, Some(finish_field))),
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

fn zip_with(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    zip_collections(input, false)
}

fn map_zip_with(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    zip_collections(input, true)
}

fn zip_collections(input: ScalarFunctionInput, map: bool) -> PlanResult<expr::Expr> {
    let (mut left, mut right, function) = input.arguments.three()?;
    let name = if map { "map_zip_with" } else { "zip_with" };
    expect_lambda_arity(name, &function, if map { 3 } else { 2 })?;
    let udf = SparkZipWith::new(
        map,
        input.function_context.plan_config.ansi_mode,
        input.function_context.plan_config.case_sensitive,
    );
    let schema = input.function_context.schema;
    if !matches!(function, expr::Expr::Lambda(_)) {
        // Spark can coerce NullType collections once an ordinary (hidden lambda)
        // body is resolved. An explicit lambda with an untyped NULL still errors.
        for argument in [&mut left, &mut right] {
            if argument.get_type(schema)? == DataType::Null {
                let data_type = if map {
                    map_type_from_key_value_types(&DataType::Null, &DataType::Null)
                } else {
                    DataType::List(Arc::new(Field::new_list_field(DataType::Null, true)))
                };
                *argument = cast(argument.clone(), data_type);
            }
        }
    }
    if expr_contains_python_udf(&function)? {
        return Err(PlanError::AnalysisError(format!(
            "Lambda function with Python UDF is not supported in {name}"
        )));
    }
    let types = udf.coerce_collection_types(&[left.get_type(schema)?, right.get_type(schema)?])?;
    // TODO: Extract Python UDF subexpressions from collection arguments before
    // null short-circuiting while leaving their surrounding native expressions lazy.
    let mut arguments = [left, right]
        .into_iter()
        .zip(types)
        .map(|(argument, data_type)| {
            // Spark aligns struct key fields positionally, including names that
            // differ only in case. Rename before Arrow's name-based key cast.
            let source_type = argument.get_type(schema)?;
            let renamed_type = build_rename_target_type(&source_type, &data_type);
            let argument = if source_type == renamed_type {
                argument
            } else {
                ScalarUDF::from(SparkStructRename::new(renamed_type)).call(vec![argument])
            };
            let argument = if argument.get_type(schema)? == data_type {
                argument
            } else {
                // TODO: Match Spark floating-point/timestamp string formatting and DST
                // resolution once shared nested map casts support them
                // (map_zip_with_deferred_casts.feature).
                cast(argument, data_type)
            };
            lambda_with_fresh_parameter(argument, "__zip_collection")
        })
        .collect::<PlanResult<Vec<_>>>()?;
    arguments.push(if matches!(function, expr::Expr::Lambda(_)) {
        function
    } else {
        lambda_with_fresh_parameter(function, "__zip_element")?
    });
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(udf)),
        arguments,
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
        ("map_filter", F::custom(map_filter)),
        ("map_zip_with", F::custom(map_zip_with)),
        ("reduce", F::custom(aggregate)),
        ("transform", F::custom(transform)),
        ("transform_keys", F::unknown("transform_keys")),
        ("transform_values", F::unknown("transform_values")),
        ("zip_with", F::custom(zip_with)),
    ]
}
