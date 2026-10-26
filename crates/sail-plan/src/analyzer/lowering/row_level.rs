//! Native row-level operations; format-specific planning is a lowering step.
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource::{
    DataSourceRegistry, DeleteInfo, DeltaCheckConstraintExpr, UpdateAssignment, UpdateInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::analyzer::row_level::*;
use crate::error::PlanResult;

impl RowOperation {
    pub(crate) async fn lower(self, session: &SessionContext) -> PlanResult<LogicalPlan> {
        let schema = self.input.schema.resolved("row-level input")?.clone();
        let resolved_target_field_names =
            schema.attributes.iter().map(|a| a.name.clone()).collect();
        let target_plan = Arc::new(crate::analyzer::lowering::lower_query(self.input)?.plan);
        let input_schema = Arc::clone(target_plan.schema());
        let condition = self
            .condition
            .map(|c| -> PlanResult<_> {
                Ok(ExprWithSource::new(
                    super::checked_expression(&c.expression, &schema)?,
                    c.source,
                ))
            })
            .transpose()?;
        let format = self.target.format.clone();
        let operation = match self.action {
            RowAction::Delete => RowLevelOperation::Delete(Box::new(DeleteInfo {
                target_plan,
                target: self.target,
                condition,
                input_schema,
                resolved_target_field_names,
                case_sensitive: self.case_sensitive,
            })),
            RowAction::Update {
                assignments,
                generated,
                constraints,
            } => {
                let assignments = assignments
                    .into_iter()
                    .map(|(column, expression)| {
                        Ok(UpdateAssignment {
                            column,
                            value: super::checked_expression(&expression, &schema)?,
                        })
                    })
                    .collect::<PlanResult<_>>()?;
                let generated_column_exprs = generated
                    .into_iter()
                    .map(|(name, expression)| {
                        Ok((name, super::checked_expression(&expression, &schema)?))
                    })
                    .collect::<PlanResult<_>>()?;
                let check_constraint_exprs = constraints
                    .into_iter()
                    .map(|c| {
                        Ok(DeltaCheckConstraintExpr {
                            name: c.name,
                            expression: c.source,
                            expr: super::checked_expression(&c.expression, &schema)?,
                            violation: c.violation,
                        })
                    })
                    .collect::<PlanResult<_>>()?;
                RowLevelOperation::Update(Box::new(UpdateInfo {
                    target_plan,
                    target: self.target,
                    condition,
                    assignments,
                    input_schema,
                    resolved_target_field_names,
                    case_sensitive: self.case_sensitive,
                    generated_column_exprs,
                    check_constraint_exprs,
                }))
            }
        };
        Ok(session
            .extension::<DataSourceRegistry>()?
            .get_lake_source(&format)?
            .plan_row_level_operation(&session.state(), operation)
            .await?)
    }
}
