use std::collections::HashMap;
use std::sync::Arc;

use datafusion::functions_aggregate::{
    average, bit_and_or_xor, bool_and_or, correlation, count, covariance, first_last, grouping,
    median, regr, stddev, sum, variance,
};
use datafusion_expr::expr::AggregateFunctionDefinition;
use datafusion_expr::{expr, AggregateFunction, AggregateUDF};
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggregateFunctionDefinition> =
        HashMap::from_iter(list_built_in_aggregate_functions());
}

struct AggregateFunctionBuilder;

impl AggregateFunctionBuilder {
    fn unknown(_name: &str) -> Option<AggregateFunctionDefinition> {
        None
    }

    fn agg(f: AggregateFunction) -> Option<AggregateFunctionDefinition> {
        Some(AggregateFunctionDefinition::BuiltIn(f))
    }

    fn udaf(f: Arc<AggregateUDF>) -> Option<AggregateFunctionDefinition> {
        Some(AggregateFunctionDefinition::UDF(f))
    }
}

fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggregateFunctionDefinition)> {
    use AggregateFunctionBuilder as F;

    vec![
        ("any", F::udaf(bool_and_or::bool_or_udaf())),
        ("any_value", F::unknown("any_value")),
        ("approx_count_distinct", F::unknown("approx_count_distinct")),
        ("approx_percentile", F::unknown("approx_percentile")),
        ("array_agg", F::agg(AggregateFunction::ArrayAgg)),
        ("avg", F::udaf(average::avg_udaf())),
        ("bit_and", F::udaf(bit_and_or_xor::bit_and_udaf())),
        ("bit_or", F::udaf(bit_and_or_xor::bit_or_udaf())),
        ("bit_xor", F::udaf(bit_and_or_xor::bit_xor_udaf())),
        ("bitmap_construct_agg", F::unknown("bitmap_construct_agg")),
        ("bitmap_or_agg", F::unknown("bitmap_or_agg")),
        ("bool_and", F::udaf(bool_and_or::bool_and_udaf())),
        ("bool_or", F::udaf(bool_and_or::bool_or_udaf())),
        ("collect_list", F::agg(AggregateFunction::ArrayAgg)),
        ("collect_set", F::unknown("collect_set")),
        ("corr", F::udaf(correlation::corr_udaf())),
        ("count", F::udaf(count::count_udaf())),
        ("count_if", F::unknown("count_if")),
        ("count_min_sketch", F::unknown("count_min_sketch")),
        ("covar_pop", F::udaf(covariance::covar_pop_udaf())),
        ("covar_samp", F::udaf(covariance::covar_samp_udaf())),
        ("every", F::udaf(bool_and_or::bool_and_udaf())),
        ("first", F::udaf(first_last::first_value_udaf())),
        ("first_value", F::udaf(first_last::first_value_udaf())),
        ("grouping", F::udaf(grouping::grouping_udaf())),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::unknown("kurtosis")),
        ("last", F::udaf(first_last::last_value_udaf())),
        ("last_value", F::udaf(first_last::last_value_udaf())),
        ("max", F::agg(AggregateFunction::Max)),
        ("max_by", F::unknown("max_by")),
        ("mean", F::udaf(average::avg_udaf())),
        ("median", F::udaf(median::median_udaf())),
        ("min", F::agg(AggregateFunction::Min)),
        ("min_by", F::unknown("min_by")),
        ("mode", F::unknown("mode")),
        ("percentile", F::unknown("percentile")),
        ("percentile_approx", F::unknown("percentile_approx")),
        ("regr_avgx", F::udaf(regr::regr_avgx_udaf())),
        ("regr_avgy", F::udaf(regr::regr_avgy_udaf())),
        ("regr_count", F::udaf(regr::regr_count_udaf())),
        ("regr_intercept", F::udaf(regr::regr_intercept_udaf())),
        ("regr_r2", F::udaf(regr::regr_r2_udaf())),
        ("regr_slope", F::udaf(regr::regr_slope_udaf())),
        ("regr_sxx", F::udaf(regr::regr_sxx_udaf())),
        ("regr_sxy", F::udaf(regr::regr_sxy_udaf())),
        ("regr_syy", F::udaf(regr::regr_syy_udaf())),
        ("skewness", F::unknown("skewness")),
        ("some", F::udaf(bool_and_or::bool_or_udaf())),
        ("std", F::udaf(stddev::stddev_udaf())),
        ("stddev", F::udaf(stddev::stddev_udaf())),
        ("stddev_pop", F::udaf(stddev::stddev_pop_udaf())),
        ("stddev_samp", F::udaf(stddev::stddev_udaf())),
        ("sum", F::udaf(sum::sum_udaf())),
        ("try_avg", F::unknown("try_avg")),
        ("try_sum", F::unknown("try_sum")),
        ("var_pop", F::udaf(variance::var_pop_udaf())),
        ("var_samp", F::udaf(variance::var_samp_udaf())),
        ("variance", F::udaf(variance::var_samp_udaf())),
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
    Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
        func_def: f.clone(),
        args,
        distinct,
        filter: None,
        order_by: None,
        null_treatment: None,
    }))
}
