#[expect(clippy::disallowed_types)]
use datafusion_common::{DFSchema, DFSchemaRef};
#[expect(clippy::disallowed_types)]
use datafusion_expr::{EmptyRelation, LogicalPlan, SetVariable, Statement};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        if variable.eq_ignore_ascii_case("spark.sql.legacy.allowNegativeScaleOfDecimal") {
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
