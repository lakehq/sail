use std::collections::HashMap;

use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, first_last, grouping, kurtosis_pop, median, min_max, regr,
    stddev, sum, variance,
};
use datafusion_common::ScalarValue;
use datafusion_expr::expr;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::sqlparser::ast::NullTreatment;
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::function::common::AggFunction;
use crate::utils::ItemTaker;

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggFunction> =
        HashMap::from_iter(list_built_in_aggregate_functions());
}

fn min_max_by(args: Vec<expr::Expr>, distinct: bool, asc: bool) -> PlanResult<expr::Expr> {
    let (args, order_by, filter) = if args.len() == 2 {
        let (first, second) = args.two()?;
        Ok((vec![first], second, None))
    } else if args.len() == 3 {
        let (first, second, third) = args.three()?;
        Ok((vec![first], third, Some(Box::new(second))))
    } else {
        Err(PlanError::invalid("max_by requires 2 or 3 arguments"))
    }?;
    let order_by = Some(vec![expr::Sort::new(order_by, asc, false)]);
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::first_value_udaf(),
        args,
        distinct,
        filter,
        order_by,
        null_treatment: None,
    }))
}

fn first_value(args: Vec<expr::Expr>, distinct: bool) -> PlanResult<expr::Expr> {
    let (args, ignore_nulls) = if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], NullTreatment::RespectNulls))
    } else if args.len() == 2 {
        let (expr, ignore_nulls) = args.two()?;
        let ignore_nulls = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls))) => {
                if ignore_nulls {
                    NullTreatment::IgnoreNulls
                } else {
                    NullTreatment::RespectNulls
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "any_value requires a boolean literal as the second argument",
                ))
            }
        };
        Ok((vec![expr], ignore_nulls))
    } else {
        Err(PlanError::invalid("any_value requires 1 or s arguments"))
    }?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::first_value_udaf(),
        args,
        distinct,
        filter: None,
        order_by: None,
        null_treatment: Some(ignore_nulls),
    }))
}

fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggFunction)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("any", F::default_agg(bool_and_or::bool_or_udaf)),
        ("any_value", F::custom_agg(first_value)),
        (
            "approx_count_distinct",
            F::default_agg(approx_distinct::approx_distinct_udaf),
        ),
        (
            "approx_percentile",
            F::default_agg(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("array_agg", F::default_agg(array_agg::array_agg_udaf)),
        ("avg", F::default_agg(average::avg_udaf)),
        ("bit_and", F::default_agg(bit_and_or_xor::bit_and_udaf)),
        ("bit_or", F::default_agg(bit_and_or_xor::bit_or_udaf)),
        ("bit_xor", F::default_agg(bit_and_or_xor::bit_xor_udaf)),
        (
            "bitmap_construct_agg",
            F::unknown_agg("bitmap_construct_agg"),
        ),
        ("bitmap_or_agg", F::unknown_agg("bitmap_or_agg")),
        ("bool_and", F::default_agg(bool_and_or::bool_and_udaf)),
        ("bool_or", F::default_agg(bool_and_or::bool_or_udaf)),
        ("collect_list", F::default_agg(array_agg::array_agg_udaf)),
        ("collect_set", F::unknown_agg("collect_set")),
        ("corr", F::default_agg(correlation::corr_udaf)),
        ("count", F::default_agg(count::count_udaf)),
        ("count_if", F::unknown_agg("count_if")),
        ("count_min_sketch", F::unknown_agg("count_min_sketch")),
        ("covar_pop", F::default_agg(covariance::covar_pop_udaf)),
        ("covar_samp", F::default_agg(covariance::covar_samp_udaf)),
        ("every", F::default_agg(bool_and_or::bool_and_udaf)),
        ("first", F::custom_agg(first_value)),
        ("first_value", F::custom_agg(first_value)),
        ("grouping", F::default_agg(grouping::grouping_udaf)),
        ("grouping_id", F::unknown_agg("grouping_id")),
        ("histogram_numeric", F::unknown_agg("histogram_numeric")),
        ("hll_sketch_agg", F::unknown_agg("hll_sketch_agg")),
        ("hll_union_agg", F::unknown_agg("hll_union_agg")),
        ("kurtosis", F::default_agg(kurtosis_pop::kurtosis_pop_udaf)),
        ("last", F::default_agg(first_last::last_value_udaf)),
        ("last_value", F::default_agg(first_last::last_value_udaf)),
        ("max", F::default_agg(min_max::max_udaf)),
        (
            "max_by",
            F::custom_agg(|args, distinct| min_max_by(args, distinct, false)),
        ),
        ("mean", F::default_agg(average::avg_udaf)),
        ("median", F::default_agg(median::median_udaf)),
        ("min", F::default_agg(min_max::min_udaf)),
        (
            "min_by",
            F::custom_agg(|args, distinct| min_max_by(args, distinct, true)),
        ),
        ("mode", F::unknown_agg("mode")),
        ("percentile", F::unknown_agg("percentile")),
        (
            "percentile_approx",
            F::default_agg(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
        ("regr_avgx", F::default_agg(regr::regr_avgx_udaf)),
        ("regr_avgy", F::default_agg(regr::regr_avgy_udaf)),
        ("regr_count", F::default_agg(regr::regr_count_udaf)),
        ("regr_intercept", F::default_agg(regr::regr_intercept_udaf)),
        ("regr_r2", F::default_agg(regr::regr_r2_udaf)),
        ("regr_slope", F::default_agg(regr::regr_slope_udaf)),
        ("regr_sxx", F::default_agg(regr::regr_sxx_udaf)),
        ("regr_sxy", F::default_agg(regr::regr_sxy_udaf)),
        ("regr_syy", F::default_agg(regr::regr_syy_udaf)),
        ("skewness", F::unknown_agg("skewness")),
        ("some", F::default_agg(bool_and_or::bool_or_udaf)),
        ("std", F::default_agg(stddev::stddev_udaf)),
        ("stddev", F::default_agg(stddev::stddev_udaf)),
        ("stddev_pop", F::default_agg(stddev::stddev_pop_udaf)),
        ("stddev_samp", F::default_agg(stddev::stddev_udaf)),
        ("sum", F::default_agg(sum::sum_udaf)),
        ("try_avg", F::unknown_agg("try_avg")),
        ("try_sum", F::unknown_agg("try_sum")),
        ("var_pop", F::default_agg(variance::var_pop_udaf)),
        ("var_samp", F::default_agg(variance::var_samp_udaf)),
        ("variance", F::default_agg(variance::var_samp_udaf)),
    ]
}

pub(crate) fn get_built_in_aggregate_function(name: &str) -> PlanResult<AggFunction> {
    let name = name.to_lowercase();
    Ok(BUILT_IN_AGGREGATE_FUNCTIONS
        .get(name.as_str())
        .ok_or_else(|| PlanError::unsupported(format!("unknown aggregate function: {name}")))?
        .clone())
}
