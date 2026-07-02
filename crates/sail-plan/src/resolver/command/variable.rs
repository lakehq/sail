use datafusion_common::{DFSchema, DFSchemaRef};
#[expect(
    clippy::disallowed_types,
    reason = "DataFusion exposes SET variable plans through LogicalPlan::Statement."
)]
use datafusion_expr::logical_plan::Statement;
use datafusion_expr::{EmptyRelation, LogicalPlan, SetVariable};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        if variable.eq_ignore_ascii_case("spark.sql.parser.escapedStringLiterals") {
            // TODO: Thread this parser flag through SQL parsing once Sail supports
            // config-sensitive string literal escaping.
            return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }));
        }
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
