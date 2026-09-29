#[expect(clippy::disallowed_types)]
use datafusion_expr::{LogicalPlan, SetVariable, Statement};
use sail_common::spec;
use sail_sql_analyzer::expression::from_ast_expression;
use sail_sql_analyzer::parser::parse_expression;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        // Native DataFusion settings retain SQL string-literal decoding.
        let value = match parse_expression(&value).and_then(from_ast_expression) {
            Ok(spec::Expr::Literal(spec::Literal::Utf8 { value: Some(value) })) => value,
            _ => value,
        };
        let variable = if variable.eq_ignore_ascii_case("timezone")
            || variable.eq_ignore_ascii_case("time.zone")
        {
            "datafusion.execution.time_zone".to_string()
        } else {
            variable
        };
        let statement = Statement::SetVariable(SetVariable { variable, value });

        Ok(LogicalPlan::Statement(statement))
    }
}
