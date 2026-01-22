use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, first_last, grouping, median, min_max, percentile_cont, regr,
    stddev, sum, variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::{cast, expr, lit, when, AggregateUDF, ExprSchemable, ScalarUDF};
use lazy_static::lazy_static;
use sail_common::spec::SAIL_LIST_FIELD_NAME;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::percentile_disc::percentile_disc_udaf;
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::aggregate::try_sum::TrySumFunction;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    get_arguments_and_null_treatment, get_null_treatment, AggFunction, AggFunctionInput,
};
use crate::function::transform_count_star_wildcard_expr;

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggFunction> =
        HashMap::from_iter(list_built_in_aggregate_functions());
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

/// Builds a percentile_cont aggregate expression from WITHIN GROUP syntax.
///
/// DataFusion's percentile_cont expects args = [column, percentile], but Spark's
/// SQL syntax `percentile_cont(0.5) WITHIN GROUP (ORDER BY col)` puts the column
/// in order_by and the percentile in arguments. This function combines them.
fn percentile_cont_expr(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // Extract the single column expression from ORDER BY (error if multiple)
    let sort = input.order_by.clone().one()?;
    let column = sort.expr;

    // Get the percentile value from arguments
    let percentile = input.arguments.one()?;

    // Combine: [column, percentile] as DataFusion expects
    let args = vec![column, percentile];

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: percentile_cont::percentile_cont_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

/// Builds a percentile_disc aggregate expression from WITHIN GROUP syntax.
fn percentile_disc_expr(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let sort = input.order_by.clone().one()?;
    let column = sort.expr;
    let percentile = input.arguments.one()?;
    let args = vec![column, percentile];

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: percentile_disc_udaf(),
        params: AggregateFunctionParams {
            args,
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

fn try_sum(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(TrySumFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn try_avg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(TryAvgFunction::new())),
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
    // TODO: remove StructFunction call when count distinct from multiple arguments is implemented
    // https://github.com/apache/datafusion/blob/58ddf0d4390c770bc571f3ac2727c7de77aa25ab/datafusion/functions-aggregate/src/count.rs#L333
    let args = if distinct && (args.len() > 1) {
        vec![ScalarUDF::from(StructFunction::new(
            (0..args.len()).map(|i| format!("col{i}")).collect(),
        ))
        .call(args)]
    } else {
        args
    };
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
    // Spark's collect_set ignores NULLs by default
    let ignore_nulls = input.ignore_nulls.or(Some(true));

    // WORKAROUND: DataFusion's array_agg doesn't properly handle null_treatment when distinct=true
    // So we need to add an explicit filter for NULLs
    let (args, filter, null_treatment) = if ignore_nulls == Some(true) {
        let arg = input.arguments.one()?;
        let null_filter = arg.clone().is_not_null();
        let combined_filter = match input.filter {
            Some(existing) => Some(Box::new(existing.as_ref().clone().and(null_filter))),
            None => Some(Box::new(null_filter)),
        };
        (vec![arg], combined_filter, None) // Don't use null_treatment when we have explicit filter
    } else {
        (
            input.arguments,
            input.filter,
            get_null_treatment(ignore_nulls),
        )
    };

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: true,
            order_by: input.order_by,
            filter,
            null_treatment,
        },
    }))
}

fn array_agg_compacted(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // Spark's collect_list ignores NULLs by default
    let ignore_nulls = input.ignore_nulls.or(Some(true));

    // WORKAROUND: DataFusion's array_agg doesn't properly handle null_treatment when distinct=true
    // So we need to add an explicit filter for NULLs when both distinct and ignore_nulls are true
    let (args, filter, null_treatment) = if input.distinct && ignore_nulls == Some(true) {
        let arg = input.arguments.one()?;
        let null_filter = arg.clone().is_not_null();
        let combined_filter = match input.filter {
            Some(existing) => Some(Box::new(existing.as_ref().clone().and(null_filter))),
            None => Some(Box::new(null_filter)),
        };
        (vec![arg], combined_filter, None) // Don't use null_treatment when we have explicit filter
    } else {
        (
            input.arguments,
            input.filter,
            get_null_treatment(ignore_nulls),
        )
    };

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            order_by: input.order_by,
            filter,
            null_treatment,
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
            &DataType::List(Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                DataType::Utf8,
                true,
            ))),
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

fn median(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(cast(
        expr::Expr::AggregateFunction(AggregateFunction {
            func: median::median_udaf(),
            params: AggregateFunctionParams {
                args: input.arguments.clone(),
                distinct: input.distinct,
                order_by: input.order_by,
                filter: input.filter,
                null_treatment: get_null_treatment(input.ignore_nulls),
            },
        }),
        DataType::Float64,
    ))
}

fn approx_count_distinct(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(cast(
        expr::Expr::AggregateFunction(AggregateFunction {
            func: approx_distinct::approx_distinct_udaf(),
            params: AggregateFunctionParams {
                args: input.arguments.clone(),
                distinct: input.distinct,
                order_by: input.order_by,
                filter: input.filter,
                null_treatment: get_null_treatment(input.ignore_nulls),
            },
        }),
        DataType::Int64,
    ))
}

/// Creates a list of built-in aggregate functions.
/// This is used to create a hashmap that the resolver uses to look up
/// aggregate functions by name.
fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggFunction)> {
    use crate::function::common::AggFunctionBuilder as F;

    vec![
        ("any", F::default(bool_and_or::bool_or_udaf)),
        ("any_value", F::custom(first_value)),
        ("approx_count_distinct", F::custom(approx_count_distinct)),
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
        ("collect_list", F::custom(array_agg_compacted)),
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
        ("median", F::custom(median)),
        ("min", F::default(min_max::min_udaf)),
        ("min_by", F::custom(min_by)),
        ("mode", F::custom(mode)),
        ("percentile", F::unknown("percentile")),
        (
            "percentile_approx",
            F::default(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("percentile_cont", F::custom(percentile_cont_expr)),
        ("percentile_disc", F::custom(percentile_disc_expr)),
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
        ("try_avg", F::custom(try_avg)),
        ("try_sum", F::custom(try_sum)),
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
