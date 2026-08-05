use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::datasource::{DeleteInfo, SourceRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves the DELETE command.
    pub(super) async fn resolve_command_delete(
        &self,
        delete: spec::Delete,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Delete {
            table,
            table_alias,
            condition,
        } = delete;
        let target = self.resolve_row_level_target(&table).await?;
        let target_format = target.format.clone();
        let mut target_plan = self.resolve_row_level_table_plan(table, state).await?;
        if let Some(alias) = table_alias {
            target_plan = self.apply_row_level_table_alias(target_plan, alias.as_ref())?;
        }
        let input_schema = target_plan.schema().clone();
        let resolved_target_field_names = Self::get_field_names(&input_schema, state)?;
        let condition = if let Some(condition) = condition {
            Some(ExprWithSource::new(
                self.resolve_expression(condition.expr, &input_schema, state)
                    .await?,
                condition.source,
            ))
        } else {
            None
        };

        let delete_info = DeleteInfo {
            target_plan: Arc::new(target_plan),
            target,
            condition,
            input_schema,
            resolved_target_field_names,
        };

        let registry = self.ctx.extension::<SourceRegistry>()?;
        registry
            .get_lake_source(&target_format)?
            .plan_row_level_operation(
                &self.ctx.state(),
                RowLevelOperation::Delete(Box::new(delete_info)),
            )
            .await
            .map_err(PlanError::from)
    }
}
