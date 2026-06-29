use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, grouping, median, min_max, stddev, sum, variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    cast, expr, lit, when, AggregateUDF, ExprSchemable, WindowFrame, WindowFunctionDefinition,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use lazy_static::lazy_static;
use sail_common::spec::SAIL_LIST_FIELD_NAME;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::bitmap_and_agg::BitmapAndAggFunction;
use sail_function::aggregate::bitmap_construct_agg::BitmapConstructAggFunction;
use sail_function::aggregate::bitmap_or_agg::BitmapOrAggFunction;
use sail_function::aggregate::count_min_sketch::CountMinSketchFunction;
use sail_function::aggregate::histogram_numeric::HistogramNumericFunction;
use sail_function::aggregate::hll_sketch::{HllSketchAggFunction, HllUnionAggFunction};
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::percentile::PercentileFunction;
use sail_function::aggregate::product::ProductFunction;
use sail_function::aggregate::regr::{Regr, RegrType};
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::theta_sketch::{
    ThetaIntersectionAggFunction, ThetaSketchAggFunction, ThetaUnionAggFunction,
};
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::window::spark_ntile_udwf;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    count_min_sketch_args, get_arguments_and_null_treatment, get_null_treatment,
    hll_args_with_default_lg, hll_union_args_with_default_allow_different_lg,
    theta_args_with_default_lg, WinFunction, WinFunctionInput,
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
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
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

fn aggregate_udf_window_expr(
    func: Arc<AggregateUDF>,
    args: Vec<expr::Expr>,
    partition_by: Vec<expr::Expr>,
    order_by: Vec<expr::Sort>,
    window_frame: WindowFrame,
    ignore_nulls: Option<bool>,
    distinct: bool,
) -> expr::Expr {
    expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(func),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    }))
}

fn product(input: WinFunctionInput) -> PlanResult<expr::Expr> {
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
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
        .collect();
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(ProductFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
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
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
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

fn hll_sketch_agg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = hll_args_with_default_lg(arguments, "hll_sketch_agg")?;
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(HllSketchAggFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
}

fn hll_union_agg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = hll_union_args_with_default_allow_different_lg(arguments)?;
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(HllUnionAggFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
}

fn count_min_sketch(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = count_min_sketch_args(arguments)?;
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(CountMinSketchFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
}

fn theta_sketch_agg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = theta_args_with_default_lg(arguments, "theta_sketch_agg")?;
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(ThetaSketchAggFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
}

fn theta_union_agg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    let args = theta_args_with_default_lg(arguments, "theta_union_agg")?;
    Ok(aggregate_udf_window_expr(
        Arc::new(AggregateUDF::from(ThetaUnionAggFunction::new())),
        args,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
    ))
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
        1 => {
            let filter = arguments
                .first()
                .ok_or_else(|| PlanError::invalid("`count_if` requires 1 argument"))?
                .clone();
            Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(count::count_udaf()),
                params: WindowFunctionParams {
                    args: vec![lit(0)],
                    partition_by,
                    order_by,
                    window_frame,
                    filter: Some(Box::new(filter)),
                    null_treatment: get_null_treatment(ignore_nulls),
                    distinct,
                },
            })))
        }
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

fn percentile_exact_agg(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(Arc::new(AggregateUDF::from(
            PercentileFunction::new(),
        ))),
        params: WindowFunctionParams {
            args: arguments,
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    })))
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

fn rank_like_udwf(
    func: impl Fn() -> Arc<datafusion_expr::WindowUDF>,
    input: WinFunctionInput,
) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments: _,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        distinct,
        function_context: _,
    } = input;
    Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(func()),
        params: WindowFunctionParams {
            args: vec![],
            partition_by,
            order_by,
            window_frame,
            filter: None,
            null_treatment: get_null_treatment(ignore_nulls),
            distinct,
        },
    })))
}

fn list_built_in_window_functions() -> Vec<(&'static str, WinFunction)> {
    use crate::function::common::WinFunctionBuilder as F;
    vec![
        // Window
        ("cume_dist", F::window(cume_dist_udwf)),
        (
            "dense_rank",
            F::custom(|input| rank_like_udwf(dense_rank_udwf, input)),
        ),
        ("first", F::window(first_value_udwf)),
        ("first_value", F::window(first_value_udwf)),
        ("lag", F::window(lag_udwf)),
        ("last", F::window(last_value_udwf)),
        ("last_value", F::window(last_value_udwf)),
        ("lead", F::window(lead_udwf)),
        ("nth_value", F::custom(nth_value)),
        ("ntile", F::window(spark_ntile_udwf)),
        ("rank", F::custom(|input| rank_like_udwf(rank_udwf, input))),
        ("row_number", F::window(row_number_udwf)),
        (
            "percent_rank",
            F::custom(|input| rank_like_udwf(percent_rank_udwf, input)),
        ),
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
        (
            "bitmap_and_agg",
            F::aggregate(|| Arc::new(AggregateUDF::from(BitmapAndAggFunction::new()))),
        ),
        (
            "bitmap_construct_agg",
            F::aggregate(|| Arc::new(AggregateUDF::from(BitmapConstructAggFunction::new()))),
        ),
        (
            "bitmap_or_agg",
            F::aggregate(|| Arc::new(AggregateUDF::from(BitmapOrAggFunction::new()))),
        ),
        ("bool_and", F::aggregate(bool_and_or::bool_and_udaf)),
        ("bool_or", F::aggregate(bool_and_or::bool_or_udaf)),
        ("collect_list", F::aggregate(array_agg::array_agg_udaf)),
        ("collect_set", F::custom(collect_set)),
        ("corr", F::aggregate(correlation::corr_udaf)),
        ("count", F::custom(count)),
        ("count_if", F::custom(count_if)),
        ("count_min_sketch", F::custom(count_min_sketch)),
        ("covar_pop", F::aggregate(covariance::covar_pop_udaf)),
        ("covar_samp", F::aggregate(covariance::covar_samp_udaf)),
        ("every", F::aggregate(bool_and_or::bool_and_udaf)),
        ("first", F::custom(first_value)),
        ("first_value", F::custom(first_value)),
        ("grouping", F::aggregate(grouping::grouping_udaf)),
        ("grouping_id", F::unknown("grouping_id")),
        (
            "histogram_numeric",
            F::aggregate(|| Arc::new(AggregateUDF::from(HistogramNumericFunction::new()))),
        ),
        ("hll_sketch_agg", F::custom(hll_sketch_agg)),
        ("hll_union_agg", F::custom(hll_union_agg)),
        (
            "theta_intersection_agg",
            F::aggregate(|| Arc::new(AggregateUDF::from(ThetaIntersectionAggFunction::new()))),
        ),
        ("theta_sketch_agg", F::custom(theta_sketch_agg)),
        ("theta_union_agg", F::custom(theta_union_agg)),
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
        ("percentile", F::custom(percentile_exact_agg)),
        (
            "percentile_approx",
            F::aggregate(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("percentile_cont", F::unknown("percentile_cont")),
        ("percentile_disc", F::unknown("percentile_disc")),
        ("product", F::custom(product)),
        (
            "regr_avgx",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgX, "regr_avgx")))),
        ),
        (
            "regr_avgy",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgY, "regr_avgy")))),
        ),
        (
            "regr_count",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Count, "regr_count")))),
        ),
        (
            "regr_intercept",
            F::aggregate(|| {
                Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Intercept,
                    "regr_intercept",
                )))
            }),
        ),
        (
            "regr_r2",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::R2, "regr_r2")))),
        ),
        (
            "regr_slope",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Slope, "regr_slope")))),
        ),
        (
            "regr_sxx",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxx, "regr_sxx")))),
        ),
        (
            "regr_sxy",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxy, "regr_sxy")))),
        ),
        (
            "regr_syy",
            F::aggregate(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Syy, "regr_syy")))),
        ),
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
            F::aggregate(|| Arc::new(AggregateUDF::from(SparkTrySum::new()))),
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
