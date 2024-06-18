use std::collections::HashMap;
use std::sync::Arc;

use datafusion::functions_aggregate::{covariance, first_last, median, variance};
use datafusion_expr::expr::AggregateFunctionDefinition;
use datafusion_expr::{expr, AggregateFunction, AggregateUDF};
use futures::StreamExt;
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
        ("any", F::agg(AggregateFunction::BoolOr)),
        ("any_value", F::unknown("any_value")),
        ("approx_count_distinct", F::unknown("approx_count_distinct")),
        ("approx_percentile", F::unknown("approx_percentile")),
        ("array_agg", F::agg(AggregateFunction::ArrayAgg)),
        ("avg", F::agg(AggregateFunction::Avg)),
        ("bit_and", F::agg(AggregateFunction::BitAnd)),
        ("bit_or", F::agg(AggregateFunction::BitOr)),
        ("bit_xor", F::agg(AggregateFunction::BitXor)),
        ("bitmap_construct_agg", F::unknown("bitmap_construct_agg")),
        ("bitmap_or_agg", F::unknown("bitmap_or_agg")),
        ("bool_and", F::agg(AggregateFunction::BoolAnd)),
        ("bool_or", F::agg(AggregateFunction::BoolOr)),
        ("collect_list", F::agg(AggregateFunction::ArrayAgg)),
        ("collect_set", F::unknown("collect_set")),
        ("corr", F::agg(AggregateFunction::Correlation)),
        ("count", F::agg(AggregateFunction::Count)),
        ("count_if", F::unknown("count_if")),
        ("count_min_sketch", F::unknown("count_min_sketch")),
        ("covar_pop", F::udaf(covariance::covar_pop_udaf())),
        ("covar_samp", F::udaf(covariance::covar_samp_udaf())),
        ("every", F::agg(AggregateFunction::BoolAnd)),
        ("first", F::udaf(first_last::first_value_udaf())),
        ("first_value", F::udaf(first_last::first_value_udaf())),
        ("grouping", F::agg(AggregateFunction::Grouping)),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::unknown("kurtosis")),
        ("last", F::udaf(first_last::last_value_udaf())),
        ("last_value", F::udaf(first_last::last_value_udaf())),
        ("max", F::agg(AggregateFunction::Max)),
        ("max_by", F::unknown("max_by")),
        ("mean", F::agg(AggregateFunction::Avg)),
        ("median", F::udaf(median::median_udaf())),
        ("min", F::agg(AggregateFunction::Min)),
        ("min_by", F::unknown("min_by")),
        ("mode", F::unknown("mode")),
        ("percentile", F::unknown("percentile")),
        ("percentile_approx", F::unknown("percentile_approx")),
        ("regr_avgx", F::agg(AggregateFunction::RegrAvgx)),
        ("regr_avgy", F::agg(AggregateFunction::RegrAvgy)),
        ("regr_count", F::agg(AggregateFunction::RegrCount)),
        ("regr_intercept", F::agg(AggregateFunction::RegrIntercept)),
        ("regr_r2", F::agg(AggregateFunction::RegrR2)),
        ("regr_slope", F::agg(AggregateFunction::RegrSlope)),
        ("regr_sxx", F::agg(AggregateFunction::RegrSXX)),
        ("regr_sxy", F::agg(AggregateFunction::RegrSXY)),
        ("regr_syy", F::agg(AggregateFunction::RegrSYY)),
        ("skewness", F::unknown("skewness")),
        ("some", F::agg(AggregateFunction::BoolOr)),
        ("std", F::agg(AggregateFunction::Stddev)),
        ("stddev", F::agg(AggregateFunction::Stddev)),
        ("stddev_pop", F::agg(AggregateFunction::StddevPop)),
        ("stddev_samp", F::agg(AggregateFunction::Stddev)),
        ("sum", F::agg(AggregateFunction::Sum)),
        ("try_avg", F::unknown("try_avg")),
        ("try_sum", F::unknown("try_sum")),
        ("var_pop", F::agg(AggregateFunction::VariancePop)),
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
