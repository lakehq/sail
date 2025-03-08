use datafusion::functions_aggregate::{average, count, min_max, sum};
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_expr::expr;

use crate::error::{PlanError, PlanResult};

pub(crate) fn get_built_in_window_function(
    name: &str,
) -> PlanResult<expr::WindowFunctionDefinition> {
    match name {
        "avg" => Ok(expr::WindowFunctionDefinition::AggregateUDF(
            average::avg_udaf(),
        )),
        "min" => Ok(expr::WindowFunctionDefinition::AggregateUDF(
            min_max::min_udaf(),
        )),
        "max" => Ok(expr::WindowFunctionDefinition::AggregateUDF(
            min_max::max_udaf(),
        )),
        "sum" => Ok(expr::WindowFunctionDefinition::AggregateUDF(sum::sum_udaf())),
        "count" => Ok(expr::WindowFunctionDefinition::AggregateUDF(
            count::count_udaf(),
        )),
        "cume_dist" => Ok(expr::WindowFunctionDefinition::WindowUDF(cume_dist_udwf())),
        "dense_rank" => Ok(expr::WindowFunctionDefinition::WindowUDF(dense_rank_udwf())),
        "first" | "first_value" => {
            Ok(expr::WindowFunctionDefinition::WindowUDF(first_value_udwf()))
        }
        "lag" => Ok(expr::WindowFunctionDefinition::WindowUDF(lag_udwf())),
        "last" | "last_value" => Ok(expr::WindowFunctionDefinition::WindowUDF(last_value_udwf())),
        "lead" => Ok(expr::WindowFunctionDefinition::WindowUDF(lead_udwf())),
        "nth_value" => Ok(expr::WindowFunctionDefinition::WindowUDF(nth_value_udwf())),
        "ntile" => Ok(expr::WindowFunctionDefinition::WindowUDF(ntile_udwf())),
        "rank" => Ok(expr::WindowFunctionDefinition::WindowUDF(rank_udwf())),
        "row_number" => Ok(expr::WindowFunctionDefinition::WindowUDF(row_number_udwf())),
        "percent_rank" => Ok(expr::WindowFunctionDefinition::WindowUDF(
            percent_rank_udwf(),
        )),
        s => Err(PlanError::invalid(format!("unknown window function: {s}",))),
    }
}
