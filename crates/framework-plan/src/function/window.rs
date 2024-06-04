use crate::error::{PlanError, PlanResult};
use datafusion_expr::{expr, AggregateFunction, BuiltInWindowFunction};

pub(crate) fn get_built_in_window_function(
    name: &str,
) -> PlanResult<expr::WindowFunctionDefinition> {
    let name = name.to_lowercase();
    match name.as_str() {
        "avg" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Avg,
        )),
        "min" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Min,
        )),
        "max" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Max,
        )),
        "sum" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Sum,
        )),
        "count" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Count,
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
        "row_number" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::RowNumber,
        )),
        "percent_rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::PercentRank,
        )),
        s => Err(PlanError::invalid(format!(
            "unknown window function: {}",
            s
        ))),
    }
}