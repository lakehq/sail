use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::ToDFSchema;
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_logical_plan::file_update::{FileUpdateNode, FileUpdateOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the UPDATE command.
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::ExprWithSource>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let (location, format, table_schema) = self
            .get_row_level_target_schema(&table_status, "UPDATE")
            .await?;

        let field_ids = state.register_fields(table_schema.fields());
        let original_arrow_schema = Arc::new(table_schema.as_arrow().clone());
        let schema_for_resolution = rename_schema(&original_arrow_schema, &field_ids)?;
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        let resolved_condition = if let Some(condition) = condition {
            let expr = self
                .resolve_expression(condition.expr, &df_schema_for_resolution, state)
                .await?;
            let rewritten =
                expression_before_rename(&expr, &field_ids, &original_arrow_schema, true)?;
            Some(ExprWithSource::new(rewritten, condition.source))
        } else {
            None
        };

        let alias_str = table_alias.as_ref().map(|id| id.as_ref().to_string());
        let mut seen_columns: HashSet<String> = HashSet::new();
        let mut resolved_assignments: Vec<(String, ExprWithSource)> = Vec::new();

        for (target, value) in assignments {
            let parts: Vec<String> = target.into();
            let column_name = match parts.as_slice() {
                [column] => column.clone(),
                [qualifier, column] => {
                    let matches_alias = alias_str
                        .as_deref()
                        .map(|a| a.eq_ignore_ascii_case(qualifier))
                        .unwrap_or(false);
                    let matches_table = table
                        .parts()
                        .last()
                        .map(|id| id.as_ref().eq_ignore_ascii_case(qualifier))
                        .unwrap_or(false);
                    if !matches_alias && !matches_table {
                        return Err(PlanError::invalid(format!(
                            "UPDATE assignment qualifier `{qualifier}` does not match target table"
                        )));
                    }
                    column.clone()
                }
                _ => {
                    return Err(PlanError::invalid(
                        "UPDATE assignment target must be an unqualified or table-qualified column",
                    ));
                }
            };

            let canonical = table_schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(&column_name))
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    PlanError::invalid(format!("unknown column `{column_name}` in UPDATE target"))
                })?;

            if !seen_columns.insert(canonical.to_ascii_lowercase()) {
                return Err(PlanError::invalid(format!(
                    "column `{canonical}` assigned multiple times in UPDATE"
                )));
            }

            let source_text = spec_expr_source(&value);
            let resolved_value = self
                .resolve_expression(value, &df_schema_for_resolution, state)
                .await?;
            let rewritten_value = expression_before_rename(
                &resolved_value,
                &field_ids,
                &original_arrow_schema,
                true,
            )?;
            resolved_assignments
                .push((canonical, ExprWithSource::new(rewritten_value, source_text)));
        }

        let file_update_options = FileUpdateOptions {
            table_name: table.into(),
            path: location,
            format,
            assignments: resolved_assignments,
            condition: resolved_condition,
            options: vec![],
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileUpdateNode::new(file_update_options)),
        }))
    }
}

fn spec_expr_source(_expr: &spec::Expr) -> Option<String> {
    // Spec expressions do not carry source text for assignment RHS today.
    // We leave this None to preserve parity with DELETE; any source capture
    // would require changing the parser to emit `ExprWithSource` here too.
    None
}
