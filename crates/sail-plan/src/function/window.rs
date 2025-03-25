use std::collections::HashMap;

use datafusion::functions_aggregate::{average, count, min_max, sum};
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{expr, WindowFunctionDefinition};
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{get_null_treatment, WinFunction, WinFunctionInput};
use crate::function::transform_count_star_wildcard_expr;

lazy_static! {
    static ref BUILT_IN_WINDOW_FUNCTIONS: HashMap<&'static str, WinFunction> =
        HashMap::from_iter(list_built_in_window_functions());
}

fn count(input: WinFunctionInput) -> PlanResult<expr::Expr> {
    let WinFunctionInput {
        arguments,
        partition_by,
        order_by,
        window_frame,
        ignore_nulls,
        function_context: _,
    } = input;
    let null_treatment = get_null_treatment(ignore_nulls);
    let args = transform_count_star_wildcard_expr(arguments);
    Ok(expr::Expr::WindowFunction(expr::WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(count::count_udaf()),
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            null_treatment,
        },
    }))
}

fn list_built_in_window_functions() -> Vec<(&'static str, WinFunction)> {
    use crate::function::common::WinFunctionBuilder as F;
    vec![
        ("avg", F::aggregate(average::avg_udaf)),
        ("min", F::aggregate(min_max::min_udaf)),
        ("max", F::aggregate(min_max::max_udaf)),
        ("sum", F::aggregate(sum::sum_udaf)),
        ("count", F::custom(count)),
        ("cume_dist", F::window(cume_dist_udwf)),
        ("dense_rank", F::window(dense_rank_udwf)),
        ("first", F::window(first_value_udwf)),
        ("first_value", F::window(first_value_udwf)),
        ("lag", F::window(lag_udwf)),
        ("last", F::window(last_value_udwf)),
        ("last_value", F::window(last_value_udwf)),
        ("lead", F::window(lead_udwf)),
        ("nth_value", F::window(nth_value_udwf)),
        ("ntile", F::window(ntile_udwf)),
        ("rank", F::window(rank_udwf)),
        ("row_number", F::window(row_number_udwf)),
        ("percent_rank", F::window(percent_rank_udwf)),
    ]
}

pub(crate) fn get_built_in_window_function(name: &str) -> PlanResult<WinFunction> {
    Ok(BUILT_IN_WINDOW_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown window function: {name}")))?
        .clone())
}
