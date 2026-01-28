use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, grouping, median, min_max, regr, stddev, sum, variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    cast, expr, lit, when, AggregateUDF, ExprSchemable, WindowFunctionDefinition,
};
use lazy_static::lazy_static;
use sail_common::spec::SAIL_LIST_FIELD_NAME;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::aggregate::try_sum::TrySumFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    get_arguments_and_null_treatment, get_null_treatment, WinFunction, WinFunctionInput,
};
use crate::function::transform_count_star_wildcard_expr;

lazy_static! {
    static ref BUILT_IN_WINDOW_FUNCTIONS: HashMap<&'static str, WinFunction> =
        HashMap::from_iter(list_built_in_window_functions());
}

fn nth_value(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let mut args = arguments;
    let null_treatment = get_null_treatment(if args.len() == 3 {
        args.pop()
            .map(|e| match e {
                expr::Expr::Literal(ScalarValue::Boolean(value), _) => Ok(value),
                _ => Err(PlanError::InvalidArgument(
                    "nth_value third argument should be boolean scalar".to_string(),
                )),
            })
            .transpose()?
            .flatten()
    } else {
        ignore_nulls
    });
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(nth_value_udwf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment,
            distinct,
        },
    })))
}

fn avg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let (args, null_treatment) = get_arguments_and_null_treatment(arguments, ignore_nulls)?;
    if args
        .first()
        .map(|arg| arg.get_type(input.function_context.schema))
        .transpose()?
        == Some(DataType::Null)
    {
        return Ok(lit(ScalarValue::Null));
    }
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(average::avg_udaf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment,
            distinct,
        },
    })))
}

fn first_value(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let (args, null_treatment) = get_arguments_and_null_treatment(arguments, ignore_nulls)?;
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(first_value_udwf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment,
            distinct,
        },
    })))
}

fn last_value(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let (args, null_treatment) = get_arguments_and_null_treatment(arguments, ignore_nulls)?;
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(last_value_udwf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment,
            distinct,
        },
    })))
}

fn kurtosis(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = arguments
        .into_iter()
        .map(|arg| {
            expr::Expr::Cast(expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Float64,
            })
        })
        .collect();
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(Arc::new(AggregateUDF::from(
            KurtosisFunction::new(),
        ))),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    })))
}

fn skewness(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = arguments
        .into_iter()
        .map(|arg| {
            expr::Expr::Cast(expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Float64,
            })
        })
        .collect();
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(Arc::new(AggregateUDF::from(
            SkewnessFunc::new(),
        ))),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    })))
}

fn count(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = transform_count_star_wildcard_expr(arguments);
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(count::count_udaf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    })))
}

fn count_if(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    match arguments.len() {
        1 => Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(count::count_udaf()),
            params: WindowFunctionParams {
                args: arguments,
                partition_by,
                order_by,
                window_frame,
                filter: None,
                null_treatment: get_null_treatment(ignore_nulls),
                distinct,
            },
        }))),
        _ => Err(PlanError::invalid("`count_if` requires 1 argument")),
    }
}

fn collect_set(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    // Spark's collect_set always returns distinct values and ignores NULLs.
    // WORKAROUND: DataFusion's array_agg doesn't properly handle null_treatment
    // when distinct=true, so we add an explicit IS NOT NULL filter
    // (same workaround as the aggregate version in aggregate.rs).
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls: _,
        distinct: _,
        function_context: _,
    } = input;
    let arg = arguments.one()?;
    let null_filter = Some(Box::new(arg.clone().is_not_null()));
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(array_agg::array_agg_udaf()),
        params: WindowFunctionParams {
            args: vec![arg],
            partition_by,
            order_by,
            window_frame,
            filter: null_filter,
            null_treatment: None,
            distinct: true,
        },
    })))
}

fn array_agg_compacted(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls: _,
        distinct,
        function_context: _,
    } = input;
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(array_agg::array_agg_udaf()),
        params: WindowFunctionParams {
            args: arguments,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(Some(true)),
            distinct,
        },
    })))
}

fn listagg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls: _,
        distinct,
        function_context,
    } = input;
    let schema = function_context.schema;
    let (agg_col, other_args) = arguments.at_least_one()?;
    if agg_col.get_type(schema)? == DataType::Null {
        return Ok(lit(ScalarValue::Null));
    }
    let delim = other_args.first().cloned().unwrap_or_else(|| lit(""));

    let agg = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(array_agg::array_agg_udaf()),
        params: WindowFunctionParams {
            args: vec![agg_col.clone()],
            partition_by,
            order_by,
            // order_by: if input.distinct {
            //     vec![agg_col.clone().sort(true, true)]
            // } else {
            //     input.order_by
            // },
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(Some(true)),
            distinct,
        },
    }));

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

fn median(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    Ok(cast(
        expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(median::median_udaf()),
            params: WindowFunctionParams {
                args: arguments,
                partition_by,
                order_by,
                window_frame,
                filter: None,
                null_treatment: get_null_treatment(ignore_nulls),
                distinct,
            },
        })),
        DataType::Float64,
    ))
}

fn approx_count_distinct(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    Ok(cast(
        expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(approx_distinct::approx_distinct_udaf()),
            params: WindowFunctionParams {
                args: arguments,
                partition_by,
                order_by,
                window_frame,
                filter: None,
                null_treatment: get_null_treatment(ignore_nulls),
                distinct,
            },
        })),
        DataType::Int64,
    ))
}

fn list_built_in_window_functions() -> Vec<(&'static str, WinFunction)> {
    use crate::function::common::WinFunctionBuilder as F;
    vec![
        // Window
        ("cume_dist", F::window(cume_dist_udwf)),
        ("dense_rank", F::window(dense_rank_udwf)),
        ("first", F::window(first_value_udwf)),
        ("first_value", F::window(first_value_udwf)),
        ("lag", F::window(lag_udwf)),
        ("last", F::window(last_value_udwf)),
        ("last_value", F::window(last_value_udwf)),
        ("lead", F::window(lead_udwf)),
        ("nth_value", F::custom(nth_value)),
        ("ntile", F::window(ntile_udwf)),
        ("rank", F::window(rank_udwf)),
        ("row_number", F::window(row_number_udwf)),
        ("percent_rank", F::window(percent_rank_udwf)),
        // Aggregate
        ("any", F::aggregate(bool_and_or::bool_or_udaf)),
        ("any_value", F::custom(first_value)),
        ("approx_count_distinct", F::custom(approx_count_distinct)),
        (
            "approx_percentile",
            F::aggregate(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("array_agg", F::custom(array_agg_compacted)),
        ("array_join", F::custom(listagg)),
        ("avg", F::custom(avg)),
        ("bit_and", F::aggregate(bit_and_or_xor::bit_and_udaf)),
        ("bit_or", F::aggregate(bit_and_or_xor::bit_or_udaf)),
        ("bit_xor", F::aggregate(bit_and_or_xor::bit_xor_udaf)),
        ("bitmap_construct_agg", F::unknown("bitmap_construct_agg")),
        ("bitmap_or_agg", F::unknown("bitmap_or_agg")),
        ("bool_and", F::aggregate(bool_and_or::bool_and_udaf)),
        ("bool_or", F::aggregate(bool_and_or::bool_or_udaf)),
        ("collect_list", F::aggregate(array_agg::array_agg_udaf)),
        ("collect_set", F::custom(collect_set)),
        ("corr", F::aggregate(correlation::corr_udaf)),
        ("count", F::custom(count)),
        ("count_if", F::custom(count_if)),
        ("count_min_sketch", F::unknown("count_min_sketch")),
        ("covar_pop", F::aggregate(covariance::covar_pop_udaf)),
        ("covar_samp", F::aggregate(covariance::covar_samp_udaf)),
        ("every", F::aggregate(bool_and_or::bool_and_udaf)),
        ("first", F::custom(first_value)),
        ("first_value", F::custom(first_value)),
        ("grouping", F::aggregate(grouping::grouping_udaf)),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::custom(kurtosis)),
        ("last", F::custom(last_value)),
        ("last_value", F::custom(last_value)),
        ("listagg", F::custom(listagg)),
        ("max", F::aggregate(min_max::max_udaf)),
        (
            "max_by",
            F::aggregate(|| Arc::new(AggregateUDF::from(MaxByFunction::new()))),
        ),
        ("mean", F::aggregate(average::avg_udaf)),
        ("median", F::custom(median)),
        ("min", F::aggregate(min_max::min_udaf)),
        (
            "min_by",
            F::aggregate(|| Arc::new(AggregateUDF::from(MinByFunction::new()))),
        ),
        (
            "mode",
            F::aggregate(|| Arc::new(AggregateUDF::from(ModeFunction::new()))),
        ),
        ("percentile", F::unknown("percentile")),
        (
            "percentile_approx",
            F::aggregate(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("percentile_cont", F::unknown("percentile_cont")),
        ("percentile_disc", F::unknown("percentile_disc")),
        ("regr_avgx", F::aggregate(regr::regr_avgx_udaf)),
        ("regr_avgy", F::aggregate(regr::regr_avgy_udaf)),
        ("regr_count", F::aggregate(regr::regr_count_udaf)),
        ("regr_intercept", F::aggregate(regr::regr_intercept_udaf)),
        ("regr_r2", F::aggregate(regr::regr_r2_udaf)),
        ("regr_slope", F::aggregate(regr::regr_slope_udaf)),
        ("regr_sxx", F::aggregate(regr::regr_sxx_udaf)),
        ("regr_sxy", F::aggregate(regr::regr_sxy_udaf)),
        ("regr_syy", F::aggregate(regr::regr_syy_udaf)),
        ("skewness", F::custom(skewness)),
        ("some", F::aggregate(bool_and_or::bool_or_udaf)),
        ("std", F::aggregate(stddev::stddev_udaf)),
        ("stddev", F::aggregate(stddev::stddev_udaf)),
        ("stddev_pop", F::aggregate(stddev::stddev_pop_udaf)),
        ("stddev_samp", F::aggregate(stddev::stddev_udaf)),
        ("string_agg", F::custom(listagg)),
        ("sum", F::aggregate(sum::sum_udaf)),
        (
            "try_avg",
            F::aggregate(|| Arc::new(AggregateUDF::from(TryAvgFunction::new()))),
        ),
        (
            "try_sum",
            F::aggregate(|| Arc::new(AggregateUDF::from(TrySumFunction::new()))),
        ),
        ("var_pop", F::aggregate(variance::var_pop_udaf)),
        ("var_samp", F::aggregate(variance::var_samp_udaf)),
        ("variance", F::aggregate(variance::var_samp_udaf)),
    ]
}

pub(crate) fn get_built_in_window_function(name: &str) -> PlanResult<WinFunction> {
    Ok(BUILT_IN_WINDOW_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown window function: {name}")))?
        .clone())
}
