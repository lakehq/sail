use crate::error::{PlanError, PlanResult};
use datafusion_expr::expr::AggregateFunctionDefinition;
use datafusion_expr::{expr, AggregateFunction};
use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggregateFunctionDefinition> = {
        let mut m = HashMap::new();
        for (name, func) in list_built_in_aggregate_functions() {
            m.insert(name, func);
        }
        m
    };
}

struct AggregateFunctionBuilder;

impl AggregateFunctionBuilder {
    fn unknown(name: &str) -> AggregateFunctionDefinition {
        AggregateFunctionDefinition::Name(name.into())
    }

    fn agg(f: AggregateFunction) -> AggregateFunctionDefinition {
        AggregateFunctionDefinition::BuiltIn(f)
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
        ("covar_pop", F::agg(AggregateFunction::CovariancePop)),
        ("covar_samp", F::unknown("covar_samp")),
        ("every", F::agg(AggregateFunction::BoolAnd)),
        ("first", F::agg(AggregateFunction::FirstValue)),
        ("first_value", F::agg(AggregateFunction::FirstValue)),
        ("grouping", F::agg(AggregateFunction::Grouping)),
        ("grouping_id", F::unknown("grouping_id")),
        ("histogram_numeric", F::unknown("histogram_numeric")),
        ("hll_sketch_agg", F::unknown("hll_sketch_agg")),
        ("hll_union_agg", F::unknown("hll_union_agg")),
        ("kurtosis", F::unknown("kurtosis")),
        ("last", F::agg(AggregateFunction::LastValue)),
        ("last_value", F::agg(AggregateFunction::LastValue)),
        ("max", F::agg(AggregateFunction::Max)),
        ("max_by", F::unknown("max_by")),
        ("mean", F::agg(AggregateFunction::Avg)),
        ("median", F::agg(AggregateFunction::Median)),
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
        ("var_samp", F::agg(AggregateFunction::Variance)),
        ("variance", F::agg(AggregateFunction::Variance)),
    ]
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
