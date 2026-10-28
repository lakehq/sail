#[expect(clippy::disallowed_types)]
use datafusion_expr::{LogicalPlan, ResetVariable, SetVariable, Statement};
use sail_sql_analyzer::parser::parse_native_config_value;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        let value = parse_native_config_value(&value).unwrap_or(value);
        let statement = Statement::SetVariable(SetVariable { variable, value });

        Ok(LogicalPlan::Statement(statement))
    }

    pub(super) fn resolve_command_reset_variable(&self, variable: String) -> LogicalPlan {
        LogicalPlan::Statement(Statement::ResetVariable(ResetVariable { variable }))
    }
}
