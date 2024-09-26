use datafusion::functions_aggregate::{average, count, min_max, sum};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_expr::{expr, BuiltInWindowFunction};

use crate::error::{PlanError, PlanResult};

pub(crate) fn get_built_in_window_function(
    name: &str,
) -> PlanResult<expr::WindowFunctionDefinition> {
    let name = name.to_lowercase();
    match name.as_str() {
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
        "cume_dist" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::CumeDist,
        )),
        "dense_rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::DenseRank,
        )),
        "first_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::FirstValue,
        )),
        "lag" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Lag,
        )),
        "last_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::LastValue,
        )),
        "lead" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Lead,
        )),
        "nth_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::NthValue,
        )),
        "ntile" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Ntile,
        )),
        "rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Rank,
        )),
        "row_number" => Ok(expr::WindowFunctionDefinition::WindowUDF(row_number_udwf())),
        "percent_rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::PercentRank,
        )),
        s => Err(PlanError::invalid(format!("unknown window function: {s}",))),
    }
}
