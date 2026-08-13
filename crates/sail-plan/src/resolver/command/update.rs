use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, lit};
use sail_common::spec;
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{SourceRegistry, UpdateAssignment, UpdateInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::config::StoreAssignmentPolicy;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::ExprWithSource>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if self.config.store_assignment_policy == StoreAssignmentPolicy::Legacy {
            return Err(PlanError::AnalysisError(
                "LEGACY store assignment policy is disallowed in Spark data source V2. Please set the configuration spark.sql.storeAssignmentPolicy to other values."
                    .to_string(),
            ));
        }
        let target = self.resolve_row_level_target(&table).await?;
        let target_format = target.format.clone();
        let mut target_plan = self.resolve_row_level_table_plan(table, state).await?;
        let target_alias = table_alias.as_ref().map(|alias| alias.as_ref().to_string());
        if let Some(alias) = table_alias {
            target_plan = self.apply_row_level_table_alias(target_plan, alias.as_ref())?;
        }

        let input_schema = target_plan.schema().clone();
        let resolved_target_field_names = Self::get_field_names(&input_schema, state)?;
        let assignments = self.normalize_update_assignment_targets(
            assignments,
            target_alias.as_deref(),
            &input_schema,
            &resolved_target_field_names,
        )?;

        let mut seen_columns = HashSet::new();
        let mut resolved_assignments = Vec::with_capacity(assignments.len());
        for (column, value) in assignments {
            let column_expr = spec::Expr::UnresolvedAttribute {
                name: column,
                plan_id: None,
                is_metadata_column: false,
            };
            let column = match self
                .resolve_expression(column_expr, &input_schema, state)
                .await?
            {
                Expr::Column(column) => state
                    .get_field_info(&column.name)
                    .map(|info| info.name().to_string())
                    .unwrap_or(column.name),
                _ => {
                    return Err(PlanError::invalid(
                        "UPDATE assignments must reference columns only",
                    ));
                }
            };
            let column_key = if self.config.case_sensitive {
                column.clone()
            } else {
                column.to_ascii_lowercase()
            };
            if !seen_columns.insert(column_key) {
                return Err(PlanError::invalid(format!(
                    "UPDATE assigns column '{column}' more than once"
                )));
            }
            let mut value = self.resolve_expression(value, &input_schema, state).await?;
            let target_index = resolved_target_field_names
                .iter()
                .position(|name| {
                    if self.config.case_sensitive {
                        name == &column
                    } else {
                        name.eq_ignore_ascii_case(&column)
                    }
                })
                .ok_or_else(|| {
                    PlanError::invalid(format!("Cannot resolve UPDATE target column `{column}`"))
                })?;
            let target_field = input_schema.fields().get(target_index).ok_or_else(|| {
                PlanError::invalid("UPDATE target field is missing during assignment validation")
            })?;
            if target_format.eq_ignore_ascii_case("delta")
                && ColumnFeatures::from_map(target_field.metadata())
                    .identity()
                    .is_some()
            {
                return Err(PlanError::unsupported(format!(
                    "UPDATE cannot assign Delta identity column '{column}'"
                )));
            }
            if Self::expr_contains_default_column_value(&value)? {
                if !Self::is_standalone_default_column_value_expr(&value) {
                    return Err(PlanError::invalid(
                        "DEFAULT must be a standalone UPDATE assignment value",
                    ));
                }
                value = if let Some(default) =
                    ColumnFeatures::from_field(target_field).current_default()
                {
                    self.resolve_column_default_expression(
                        &default,
                        &Arc::new(DFSchema::empty()),
                        state,
                    )
                    .await?
                } else if !target_field.is_nullable()
                    || ColumnFeatures::from_field(target_field).is_not_null_constraint()
                {
                    return Err(PlanError::AnalysisError(format!(
                        "[NO_DEFAULT_COLUMN_VALUE_AVAILABLE] Can't determine the default value for `{column}` since it is not nullable and it has no default value."
                    )));
                } else {
                    lit(ScalarValue::try_from(target_field.data_type())?)
                };
            }
            let write_type = value.get_type(&input_schema)?;
            self.validate_store_assignment_type(&write_type, target_field.data_type(), &column)?;
            resolved_assignments.push(UpdateAssignment { column, value });
        }

        let condition = match condition {
            Some(condition) => {
                let expression = self
                    .resolve_expression(condition.expr, &input_schema, state)
                    .await?;
                if Self::expr_contains_default_column_value(&expression)? {
                    return Err(PlanError::invalid(
                        "DEFAULT is not allowed in an UPDATE condition",
                    ));
                }
                self.validate_row_level_condition("UPDATE", &expression)?;
                Some(ExprWithSource::new(expression, condition.source))
            }
            None => None,
        };
        let generated_column_exprs = self
            .resolve_delta_update_generated_column_exprs(&input_schema, state)
            .await?;
        let check_constraint_exprs = self
            .resolve_delta_row_level_check_constraints(
                &target.format,
                &target.options,
                &input_schema,
                state,
            )
            .await?;

        let registry = self.ctx.extension::<SourceRegistry>()?;
        registry
            .get_lake_source(&target_format)?
            .plan_row_level_operation(
                &self.ctx.state(),
                RowLevelOperation::Update(Box::new(UpdateInfo {
                    target_plan: Arc::new(target_plan),
                    target,
                    condition,
                    assignments: resolved_assignments,
                    input_schema,
                    resolved_target_field_names,
                    case_sensitive: self.config.case_sensitive,
                    generated_column_exprs,
                    check_constraint_exprs,
                })),
            )
            .await
            .map_err(PlanError::from)
    }

    fn normalize_update_assignment_targets(
        &self,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        target_alias: Option<&str>,
        input_schema: &DFSchemaRef,
        resolved_target_field_names: &[String],
    ) -> PlanResult<Vec<(spec::ObjectName, spec::Expr)>> {
        let names_equal = |left: &str, right: &str| {
            if self.config.case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };
        let mut paths = Vec::<Vec<String>>::new();
        let mut grouped = Vec::<(String, spec::Expr)>::new();

        for (column, value) in assignments {
            let mut parts = column
                .parts()
                .iter()
                .map(|part| part.as_ref().to_string())
                .collect::<Vec<_>>();
            let has_target_qualifier = parts.len() > 1
                && (target_alias.is_some_and(|alias| names_equal(alias, &parts[0]))
                    || (!resolved_target_field_names
                        .iter()
                        .any(|name| names_equal(name, &parts[0]))
                        && resolved_target_field_names
                            .iter()
                            .any(|name| names_equal(name, &parts[1]))));
            if has_target_qualifier {
                parts.remove(0);
            }
            let Some(root_index) = parts.first().and_then(|root| {
                resolved_target_field_names
                    .iter()
                    .position(|name| names_equal(name, root))
            }) else {
                return Err(PlanError::invalid(format!(
                    "Cannot resolve UPDATE target column `{}`",
                    parts.join(".")
                )));
            };
            let root_name = resolved_target_field_names[root_index].clone();
            let mut canonical_path = vec![root_name.clone()];
            let mut data_type = input_schema.fields()[root_index].data_type().clone();
            for requested in &parts[1..] {
                let DataType::Struct(fields) = &data_type else {
                    return Err(PlanError::invalid(format!(
                        "Cannot update nested field `{}` because `{}` is not a struct",
                        parts.join("."),
                        canonical_path.join(".")
                    )));
                };
                let Some(field) = fields
                    .iter()
                    .find(|field| names_equal(field.name(), requested))
                else {
                    return Err(PlanError::invalid(format!(
                        "Cannot resolve UPDATE target field `{}`",
                        parts.join(".")
                    )));
                };
                canonical_path.push(field.name().clone());
                data_type = field.data_type().clone();
            }
            if paths.iter().any(|existing| existing == &canonical_path) {
                return Err(PlanError::invalid(format!(
                    "UPDATE assigns column '{}' more than once",
                    canonical_path.join(".")
                )));
            }
            if paths.iter().any(|existing| {
                canonical_path.starts_with(existing) || existing.starts_with(&canonical_path)
            }) {
                return Err(PlanError::invalid(format!(
                    "UPDATE assigns overlapping target path '{}' more than once",
                    canonical_path.join(".")
                )));
            }
            paths.push(canonical_path.clone());

            if canonical_path.len() == 1 {
                grouped.push((root_name, value));
                continue;
            }
            let previous = grouped
                .iter()
                .position(|(root, _)| names_equal(root, &root_name))
                .map(|index| grouped.remove(index).1)
                .unwrap_or_else(|| spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(root_name.clone()),
                    plan_id: None,
                    is_metadata_column: false,
                });
            let updated = spec::Expr::UpdateFields {
                struct_expression: Box::new(previous),
                field_name: spec::ObjectName::from(canonical_path[1..].to_vec()),
                value_expression: Some(Box::new(value)),
            };
            grouped.push((root_name, updated));
        }

        Ok(grouped
            .into_iter()
            .map(|(column, value)| (spec::ObjectName::bare(column), value))
            .collect())
    }
}
