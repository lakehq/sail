use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, first_last, grouping, median, min_max, regr, stddev, sum,
    variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion::sql::sqlparser::ast::NullTreatment;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::{expr, lit, when, AggregateUDF, ExprSchemable};
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::kurtosis::KurtosisFunction;
use crate::extension::function::max_min_by::{MaxByFunction, MinByFunction};
use crate::extension::function::mode::ModeFunction;
use crate::extension::function::skewness::SkewnessFunc;
use crate::function::common::{get_null_treatment, AggFunction, AggFunctionInput};
use crate::function::transform_count_star_wildcard_expr;
use crate::utils::ItemTaker;

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggFunction> =
        HashMap::from_iter(list_built_in_aggregate_functions());
}

fn get_arguments_and_null_treatment(
    args: Vec<expr::Expr>,
    ignore_nulls: Option<bool>,
) -> PlanResult<(Vec<expr::Expr>, Option<NullTreatment>)> {
    if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], get_null_treatment(ignore_nulls)))
    } else if args.len() == 2 {
        if ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "first/last value arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (expr, ignore_nulls) = args.two()?;
        let null_treatment = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls)), _metadata) => {
                if ignore_nulls {
                    Some(NullTreatment::IgnoreNulls)
                } else {
                    Some(NullTreatment::RespectNulls)
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "first/last value requires a boolean literal as the second argument",
                ))
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid(
            "first/last value requires 1 or 2 arguments",
        ))
    }
}

fn avg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    if args
        .first()
        .map(|arg| arg.get_type(input.function_context.schema))
        .transpose()?
        == Some(DataType::Null)
    {
        return Ok(lit(ScalarValue::Null));
    }
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: average::avg_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn first_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::first_value_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn last_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::last_value_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn kurtosis(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input
        .arguments
        .into_iter()
        .map(|arg| {
            expr::Expr::Cast(expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Float64,
            })
        })
        .collect();
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(KurtosisFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn max_by(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(MaxByFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn min_by(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(MinByFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn mode(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ModeFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn skewness(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input
        .arguments
        .into_iter()
        .map(|arg| {
            expr::Expr::Cast(expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Float64,
            })
        })
        .collect();
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(SkewnessFunc::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn count(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let AggFunctionInput {
        arguments,
        distinct,
        ignore_nulls,
        filter,
        order_by,
        function_context: _,
    } = input;
    let null_treatment = get_null_treatment(ignore_nulls);
    let args = transform_count_star_wildcard_expr(arguments);
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: count::count_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        },
    }))
}

fn count_if(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    match input.arguments.len() {
        1 => Ok(expr::Expr::AggregateFunction(AggregateFunction {
            func: count::count_udaf(),
            params: AggregateFunctionParams {
                args: input.arguments.clone(),
                distinct: input.distinct,
                order_by: input.order_by,
                filter: Some(Box::new(
                    input
                        .arguments
                        .first()
                        .ok_or_else(|| PlanError::invalid("`count_if` requires 1 argument"))?
                        .clone(),
                )),
                null_treatment: get_null_treatment(input.ignore_nulls),
            },
        })),
        _ => Err(PlanError::invalid("`count_if` requires 1 argument")),
    }
}

fn collect_set(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: input.arguments.clone(),
            distinct: true,
            order_by: input.order_by,
            filter: input.filter,
            null_treatment: get_null_treatment(Some(true)),
        },
    }))
}

fn array_agg_compacted(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: input.arguments.clone(),
            distinct: input.distinct,
            order_by: input.order_by,
            filter: input.filter,
            null_treatment: get_null_treatment(Some(true)),
        },
    }))
}

fn listagg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (agg_col, other_args) = input.arguments.at_least_one()?;
    if agg_col.get_type(schema)? == DataType::Null {
        return Ok(lit(ScalarValue::Null));
    }
    let delim = other_args.first().cloned().unwrap_or_else(|| lit(""));

    let agg = expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: vec![agg_col.clone()],
            distinct: input.distinct,
            order_by: if input.distinct {
                vec![agg_col.clone().sort(true, true)]
            } else {
                input.order_by
            },
            filter: input.filter,
            null_treatment: get_null_treatment(Some(true)),
        },
    });

    let string_agg = array_to_string(
        agg.cast_to(
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            schema,
        )?,
        delim.cast_to(&DataType::Utf8, schema)?,
    );

    let casted_agg = match agg_col.get_type(schema)? {
        DataType::Binary | DataType::BinaryView => string_agg.cast_to(&DataType::Binary, schema)?,
        DataType::LargeBinary => string_agg.cast_to(&DataType::LargeBinary, schema)?,
        _ => string_agg,
    };

    Ok(when(casted_agg.clone().is_not_null(), casted_agg)
        .when(lit(true), lit(ScalarValue::Null))
        .end()?)
}

fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggFunction)> {
    use crate::function::common::AggFunctionBuilder as F;

    vec![
        ("any", F::default(bool_and_or::bool_or_udaf)),
        ("any_value", F::custom(first_value)),
        (
            "approx_count_distinct",
            F::default(approx_distinct::approx_distinct_udaf),
        ),
        (
            "approx_percentile",
            F::default(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("array_agg", F::custom(array_agg_compacted)),
        ("avg", F::custom(avg)),
        ("bit_and", F::default(bit_and_or_xor::bit_and_udaf)),
        ("bit_or", F::default(bit_and_or_xor::bit_or_udaf)),
        ("bit_xor", F::default(bit_and_or_xor::bit_xor_udaf)),
        ("bitmap_construct_agg", F::unknown("bitmap_construct_agg")),
        ("bitmap_or_agg", F::unknown("bitmap_or_agg")),
        ("bool_and", F::default(bool_and_or::bool_and_udaf)),
        ("bool_or", F::default(bool_and_or::bool_or_udaf)),
        ("collect_list", F::default(array_agg::array_agg_udaf)),
        ("collect_set", F::custom(collect_set)),
        ("corr", F::default(correlation::corr_udaf)),
        ("count", F::custom(count)),
        ("count_if", F::custom(count_if)),
        ("count_min_sketch", F::unknown("count_min_sketch")),
        ("covar_pop", F::default(covariance::covar_pop_udaf)),
        ("covar_samp", F::default(covariance::covar_samp_udaf)),
        ("every", F::default(bool_and_or::bool_and_udaf)),
        ("first", F::custom(first_value)),
        ("first_value", F::custom(first_value)),
        ("grouping", F::default(grouping::grouping_udaf)),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::custom(kurtosis)),
        ("last", F::custom(last_value)),
        ("last_value", F::custom(last_value)),
        ("listagg", F::custom(listagg)),
        ("max", F::default(min_max::max_udaf)),
        ("max_by", F::custom(max_by)),
        ("mean", F::default(average::avg_udaf)),
        ("median", F::default(median::median_udaf)),
        ("min", F::default(min_max::min_udaf)),
        ("min_by", F::custom(min_by)),
        ("mode", F::custom(mode)),
        ("percentile", F::unknown("percentile")),
        (
            "percentile_approx",
            F::default(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("regr_avgx", F::default(regr::regr_avgx_udaf)),
        ("regr_avgy", F::default(regr::regr_avgy_udaf)),
        ("regr_count", F::default(regr::regr_count_udaf)),
        ("regr_intercept", F::default(regr::regr_intercept_udaf)),
        ("regr_r2", F::default(regr::regr_r2_udaf)),
        ("regr_slope", F::default(regr::regr_slope_udaf)),
        ("regr_sxx", F::default(regr::regr_sxx_udaf)),
        ("regr_sxy", F::default(regr::regr_sxy_udaf)),
        ("regr_syy", F::default(regr::regr_syy_udaf)),
        ("skewness", F::custom(skewness)),
        ("some", F::default(bool_and_or::bool_or_udaf)),
        ("std", F::default(stddev::stddev_udaf)),
        ("stddev", F::default(stddev::stddev_udaf)),
        ("stddev_pop", F::default(stddev::stddev_pop_udaf)),
        ("stddev_samp", F::default(stddev::stddev_udaf)),
        ("string_agg", F::custom(listagg)),
        ("sum", F::default(sum::sum_udaf)),
        ("try_avg", F::unknown("try_avg")),
        ("try_sum", F::unknown("try_sum")),
        ("var_pop", F::default(variance::var_pop_udaf)),
        ("var_samp", F::default(variance::var_samp_udaf)),
        ("variance", F::default(variance::var_samp_udaf)),
    ]
}

pub(crate) fn get_built_in_aggregate_function(name: &str) -> PlanResult<AggFunction> {
    Ok(BUILT_IN_AGGREGATE_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown aggregate function: {name}")))?
        .clone())
}
