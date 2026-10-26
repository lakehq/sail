use std::sync::Arc;

use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::HigherOrderUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_sort::SparkArraySort;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::map::spark_map_filter::SparkMapFilter;
use sail_function::scalar::map::spark_transform_keys::SparkTransformKeys;
use sail_function::scalar::map::spark_transform_values::SparkTransformValues;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind, ExprType, FunctionArg, QuerySchema};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

pub(crate) fn get_higher_order_function(
    name: &str,
    config: &crate::config::PlanConfig,
) -> Option<Arc<HigherOrderUDF>> {
    Some(Arc::new(match name {
        "transform" => HigherOrderUDF::new_from_impl(SparkArrayTransform::new()),
        "filter" => HigherOrderUDF::new_from_impl(SparkArrayFilter::new()),
        "exists" => HigherOrderUDF::new_from_impl(SparkArrayExists::new()),
        "forall" => HigherOrderUDF::new_from_impl(SparkArrayForall::new()),
        "aggregate" | "reduce" => HigherOrderUDF::new_from_impl(SparkArrayAggregate::new()),
        "array_sort" => HigherOrderUDF::new_from_impl(SparkArraySort::new()),
        "map_filter" => HigherOrderUDF::new_from_impl(SparkMapFilter::new()),
        "transform_values" => HigherOrderUDF::new_from_impl(SparkTransformValues::new()),
        "transform_keys" => HigherOrderUDF::new_from_impl(SparkTransformKeys::new(
            config.map_key_dedup_policy == crate::config::MapKeyDedupPolicy::LastWin,
        )),
        _ => return None,
    }))
}

pub(crate) fn lazy_arguments(
    context: &mut FunctionContextInput<'_>,
    function: Arc<HigherOrderUDF>,
    arguments: Vec<Expr>,
) -> PlanResult<Expr> {
    mixed_lazy_arguments(
        context,
        function,
        arguments
            .into_iter()
            .map(datafusion_expr::ValueOrLambda::Lambda)
            .collect(),
    )
}

pub(crate) fn mixed_lazy_arguments(
    context: &mut FunctionContextInput<'_>,
    function: Arc<HigherOrderUDF>,
    arguments: Vec<datafusion_expr::ValueOrLambda<Expr, Expr>>,
) -> PlanResult<Expr> {
    use datafusion_expr::{LambdaParametersProgress, ValueOrLambda};
    let fields = arguments
        .iter()
        .map(|a| {
            Ok(match a {
                ValueOrLambda::Value(value) => {
                    ValueOrLambda::Value(value.result_type.resolved("lazy value")?.to_field(""))
                }
                ValueOrLambda::Lambda(body) => {
                    ValueOrLambda::Lambda(body.result_type.resolved("lazy body")?.to_field(""))
                }
            })
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let inference = fields
        .iter()
        .map(|f| match f {
            ValueOrLambda::Value(f) => ValueOrLambda::Value(f.clone()),
            ValueOrLambda::Lambda(f) => ValueOrLambda::Lambda(Some(f.clone())),
        })
        .collect::<Vec<_>>();
    let LambdaParametersProgress::Complete(parameters) =
        function.lambda_parameters(0, &inference)?
    else {
        return Err(PlanError::internal(
            "lazy argument parameter inference did not complete",
        ));
    };
    let lambda_count = arguments
        .iter()
        .filter(|a| matches!(a, ValueOrLambda::Lambda(_)))
        .count();
    crate::analyzer::typing::coercion::validate_arity(
        function.name(),
        parameters.len(),
        lambda_count,
    )?;
    let literals = arguments
        .iter()
        .map(|a| match a {
            ValueOrLambda::Value(value) => match &value.kind {
                Deferred::Resolved(ExprKind::Literal(value)) => Some(value),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    let typing = crate::analyzer::typing::higher_order::resolve(&function, &fields, &literals)?;
    let mut parameters = parameters.into_iter();
    let arguments = arguments
        .into_iter()
        .zip(typing.arguments)
        .map(|(argument, target)| {
            Ok(match (argument, target) {
                (ValueOrLambda::Value(value), ValueOrLambda::Value(target)) => {
                    FunctionArg::Value(Box::new(context.cast(value, target.data_type().clone())?))
                }
                (ValueOrLambda::Lambda(body), ValueOrLambda::Lambda(_)) => {
                    let fields = parameters
                        .next()
                        .ok_or_else(|| PlanError::internal("missing lazy parameters"))?;
                    let parameters = QuerySchema {
                        attributes: fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                context.attribute(format!("lazy_{i}"), ExprType::from_field(f))
                            })
                            .collect(),
                    };
                    FunctionArg::Lambda {
                        id: context.ids.expression(),
                        parameters,
                        body: Box::new(body),
                    }
                }
                _ => return Err(PlanError::internal("lazy coercion changed argument kind")),
            })
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Expr {
        id: context.ids.expression(),
        result_type: Deferred::Resolved(ExprType::from_field(&typing.result)),
        kind: Deferred::Resolved(ExprKind::HigherOrderFunction {
            function,
            arguments,
        }),
    })
}

/// Higher-order functions resolved together with their lambda scopes.
pub(crate) fn list_built_in_higher_order_functions() -> Vec<&'static str> {
    vec![
        "aggregate",
        "array_sort",
        "exists",
        "filter",
        "forall",
        "map_filter",
        "map_zip_with",
        "reduce",
        "transform",
        "transform_keys",
        "transform_values",
        "zip_with",
    ]
}

fn array_sort(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    match input.arguments.len() {
        1 => {
            let array = input.arguments.one()?;
            let direction = input.context.literal(ScalarValue::Utf8(Some("ASC".into())));
            let nulls = input
                .context
                .literal(ScalarValue::Utf8(Some("NULLS LAST".into())));
            input.context.scalar_udf(
                functions_nested::sort::array_sort_udf(),
                vec![array, direction, nulls],
            )
        }
        _ => input
            .context
            .scalar_udf(functions_nested::sort::array_sort_udf(), input.arguments),
    }
}

pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array_sort", F::custom(array_sort)),
        ("map_zip_with", F::unknown("map_zip_with")),
        ("zip_with", F::unknown("zip_with")),
    ]
}
