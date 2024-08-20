use std::collections::HashMap;
use std::sync::Arc;

use datafusion::functions_aggregate::{
    array_agg, average, bit_and_or_xor, bool_and_or, correlation, count, covariance, first_last,
    grouping, median, min_max, regr, stddev, sum, variance,
};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{expr, AggregateUDF};
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, Arc<AggregateUDF>> =
        HashMap::from_iter(list_built_in_aggregate_functions());
}

struct AggregateFunctionBuilder;

impl AggregateFunctionBuilder {
    fn unknown(_name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }
}

fn list_built_in_aggregate_functions() -> Vec<(&'static str, Arc<AggregateUDF>)> {
    use AggregateFunctionBuilder as F;

    vec![
        ("any", Some(bool_and_or::bool_or_udaf())),
        ("any_value", F::unknown("any_value")),
        ("approx_count_distinct", F::unknown("approx_count_distinct")),
        ("approx_percentile", F::unknown("approx_percentile")),
        ("array_agg", Some(array_agg::array_agg_udaf())),
        ("avg", Some(average::avg_udaf())),
        ("bit_and", Some(bit_and_or_xor::bit_and_udaf())),
        ("bit_or", Some(bit_and_or_xor::bit_or_udaf())),
        ("bit_xor", Some(bit_and_or_xor::bit_xor_udaf())),
        ("bitmap_construct_agg", F::unknown("bitmap_construct_agg")),
        ("bitmap_or_agg", F::unknown("bitmap_or_agg")),
        ("bool_and", Some(bool_and_or::bool_and_udaf())),
        ("bool_or", Some(bool_and_or::bool_or_udaf())),
        ("collect_list", Some(array_agg::array_agg_udaf())),
        ("collect_set", F::unknown("collect_set")),
        ("corr", Some(correlation::corr_udaf())),
        ("count", Some(count::count_udaf())),
        ("count_if", F::unknown("count_if")),
        ("count_min_sketch", F::unknown("count_min_sketch")),
        ("covar_pop", Some(covariance::covar_pop_udaf())),
        ("covar_samp", Some(covariance::covar_samp_udaf())),
        ("every", Some(bool_and_or::bool_and_udaf())),
        ("first", Some(first_last::first_value_udaf())),
        ("first_value", Some(first_last::first_value_udaf())),
        ("grouping", Some(grouping::grouping_udaf())),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::unknown("kurtosis")),
        ("last", Some(first_last::last_value_udaf())),
        ("last_value", Some(first_last::last_value_udaf())),
        ("max", Some(min_max::max_udaf())),
        ("max_by", F::unknown("max_by")),
        ("mean", Some(average::avg_udaf())),
        ("median", Some(median::median_udaf())),
        ("min", Some(min_max::min_udaf())),
        ("min_by", F::unknown("min_by")),
        ("mode", F::unknown("mode")),
        ("percentile", F::unknown("percentile")),
        ("percentile_approx", F::unknown("percentile_approx")),
        ("regr_avgx", Some(regr::regr_avgx_udaf())),
        ("regr_avgy", Some(regr::regr_avgy_udaf())),
        ("regr_count", Some(regr::regr_count_udaf())),
        ("regr_intercept", Some(regr::regr_intercept_udaf())),
        ("regr_r2", Some(regr::regr_r2_udaf())),
        ("regr_slope", Some(regr::regr_slope_udaf())),
        ("regr_sxx", Some(regr::regr_sxx_udaf())),
        ("regr_sxy", Some(regr::regr_sxy_udaf())),
        ("regr_syy", Some(regr::regr_syy_udaf())),
        ("skewness", F::unknown("skewness")),
        ("some", Some(bool_and_or::bool_or_udaf())),
        ("std", Some(stddev::stddev_udaf())),
        ("stddev", Some(stddev::stddev_udaf())),
        ("stddev_pop", Some(stddev::stddev_pop_udaf())),
        ("stddev_samp", Some(stddev::stddev_udaf())),
        ("sum", Some(sum::sum_udaf())),
        ("try_avg", F::unknown("try_avg")),
        ("try_sum", F::unknown("try_sum")),
        ("var_pop", Some(variance::var_pop_udaf())),
        ("var_samp", Some(variance::var_samp_udaf())),
        ("variance", Some(variance::var_samp_udaf())),
    ]
    .into_iter()
    .filter_map(|(name, f)| f.map(|f| (name, f)))
    .collect()
}

pub(crate) fn get_built_in_aggregate_function(
    name: &str,
    args: Vec<expr::Expr>,
    distinct: bool,
) -> PlanResult<expr::Expr> {
    let name = name.to_lowercase();
    let f = BUILT_IN_AGGREGATE_FUNCTIONS
        .get(name.as_str())
        .ok_or_else(|| PlanError::unsupported(format!("unknown aggregate function: {name}")))?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: f.clone(),
        args,
        distinct,
        filter: None,
        order_by: None,
        null_treatment: None,
    }))
}
