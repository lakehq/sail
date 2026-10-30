use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::datasource::{DataSourceRegistry, DeleteInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_lineage::{LineageDataset, LineageOperation};

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
        let condition = match condition {
            Some(condition) => {
                let expression = self
                    .resolve_expression(condition.expr, &input_schema, state)
                    .await?;
                self.validate_row_level_condition("DELETE", &expression)?;
                Some(ExprWithSource::new(expression, condition.source))
            }
            None => None,
        };

        self.record_lineage_output(
            LineageOperation::Delete,
            LineageDataset {
                identifier: None,
                location: Some(target.location.clone()),
                table: target
                    .lakehouse_table
                    .as_ref()
                    // Path-based targets (e.g. `delta.`/path``) are not catalog tables.
                    .map(|x| x.catalog_table().to_vec()),
                format: Some(target_format.clone()),
                schema: None,
                lifecycle: None,
            },
        );
        let delete_info = DeleteInfo {
            target_plan: Arc::new(target_plan),
            target,
            condition,
            input_schema,
            resolved_target_field_names,
            case_sensitive: self.config.case_sensitive,
        };

        let registry = self.ctx.extension::<DataSourceRegistry>()?;
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
