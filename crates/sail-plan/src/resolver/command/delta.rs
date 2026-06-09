use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchemaRef};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{
    col, lit, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::AlterTableOptions;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableColumnStatus, TableKind};
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{
    find_path_in_options, OptionLayer, SourceInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::check_constraints::{
    apply_delta_check_constraint_filter, DeltaCheckConstraintExpr, DeltaConstraintViolation,
};
use sail_logical_plan::file_write::FileWriteOptions;

use super::merge::merge_disambiguate_unqualified_plan_ids;
use super::write::{TableInfo, WriteColumnMatch};
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

const DELTA_FORMAT: &str = "delta";
const DELTA_CHECK_CONSTRAINT_PREFIX: &str = "delta.constraints.";
const DELTA_INVARIANTS_METADATA_KEY: &str = "delta.invariants";

#[derive(Clone, Debug)]
struct DeltaCheckConstraint {
    name: String,
    expression: String,
    violation: DeltaConstraintViolation,
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_delta_alter_table_or_catalog(
        &self,
        table: spec::ObjectName,
        if_exists: bool,
        operation: spec::AlterTableOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if let Some(key) = delta_constraint_property_mutation_key(&operation) {
            if self.alter_table_target_is_delta(&table, if_exists).await? {
                return Err(PlanError::invalid(format!(
                    "[DELTA_ADD_CONSTRAINTS] Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints. Invalid property: {key}"
                )));
            }
        }
        match operation {
            spec::AlterTableOperation::AddCheckConstraint { name, expression } => {
                self.resolve_delta_add_check_constraint(
                    table,
                    if_exists,
                    name.into(),
                    expression,
                    state,
                )
                .await
            }
            other => {
                self.resolve_catalog_alter_table(table, if_exists, other, state)
                    .await
            }
        }
    }

    async fn alter_table_target_is_delta(
        &self,
        table: &spec::ObjectName,
        if_exists: bool,
    ) -> PlanResult<bool> {
        match self
            .ctx
            .extension::<CatalogManager>()?
            .get_table(table.parts())
            .await
        {
            Ok(status) => match status.kind {
                TableKind::Table { format, .. } => Ok(is_delta_format(&format)),
                _ => Ok(false),
            },
            Err(CatalogError::NotFound(_, _)) if if_exists => Ok(false),
            Err(CatalogError::NotFound(_, _)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) async fn rewrite_data_source_delta_table_features(
        &self,
        input: LogicalPlan,
        options: &FileWriteOptions,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !is_delta_format(&options.format) {
            return Ok(input);
        }
        let Some(path) = find_path_in_options(&options.options) else {
            return Ok(input);
        };

        let registry = self.ctx.extension::<TableFormatRegistry>().map_err(|e| {
            PlanError::invalid(format!(
                "failed to access table format registry for Delta path `{path}`: {e}",
            ))
        })?;
        let table_format = registry.get(&options.format).map_err(|e| {
            PlanError::invalid(format!(
                "failed to resolve table format `{}` for Delta path `{path}`: {e}",
                options.format
            ))
        })?;
        let info = SourceInfo {
            paths: vec![path.clone()],
            catalog_table: None,
            schema: None,
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![],
        };
        let metadata = match table_format.infer_metadata(&self.ctx.state(), info).await {
            Ok(metadata) => metadata,
            Err(e) => {
                log::debug!(
                    "skipping Delta table feature rewrite for path `{path}` because existing table metadata could not be loaded: {e}"
                );
                return Ok(input);
            }
        };

        let columns = metadata
            .schema
            .fields()
            .iter()
            .map(|field| {
                let features = ColumnFeatures::from_field(field);
                TableColumnStatus {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment: None,
                    default: features.current_default(),
                    generated_always_as: features.generation_expression(),
                    identity: features.identity(),
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }
            })
            .collect::<Vec<_>>();
        let constraints = delta_constraints_from_schema_and_properties(
            metadata.schema.fields().iter().map(|field| field.as_ref()),
            &metadata.properties,
        );
        let has_column_expressions = columns.iter().any(|column| {
            column.generated_always_as.is_some()
                || column.default.is_some()
                || column.identity.is_some()
        });
        if !has_column_expressions
            && !Self::plan_has_default_column_value(&input)?
            && constraints.is_empty()
        {
            return Ok(input);
        }

        let info = TableInfo {
            catalog_table: options.catalog_table.clone(),
            columns,
            location: Some(path),
            format: options.format.clone(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            properties: metadata.properties,
        };
        let input = self
            .rewrite_write_input(input, WriteColumnMatch::ByName, &info, state)
            .await?;
        self.apply_delta_check_constraints(input, constraints, state)
            .await
    }

    async fn resolve_delta_add_check_constraint(
        &self,
        table: spec::ObjectName,
        if_exists: bool,
        name: String,
        expression: spec::ExprWithSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let status = match self
            .ctx
            .extension::<CatalogManager>()?
            .get_table(table.parts())
            .await
        {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _)) if if_exists => {
                return Ok(LogicalPlanBuilder::empty(false).build()?);
            }
            Err(CatalogError::NotFound(_, _)) => {
                return Err(PlanError::invalid(format!(
                    "table does not exist: {table:?}"
                )));
            }
            Err(e) => return Err(e.into()),
        };

        let TableKind::Table {
            columns: _,
            location,
            format,
            properties,
            ..
        } = status.kind
        else {
            return Err(PlanError::invalid(
                "ALTER TABLE ADD CONSTRAINT is only supported for tables",
            ));
        };
        if !is_delta_format(&format) {
            return Err(PlanError::unsupported(
                "ALTER TABLE ADD CONSTRAINT is only supported for Delta Lake tables",
            ));
        }
        let location = location.ok_or_else(|| {
            PlanError::invalid(format!(
                "Delta table `{table:?}` does not have a storage location"
            ))
        })?;
        if delta_check_constraints_from_properties(&properties)
            .iter()
            .any(|constraint| constraint.name.eq_ignore_ascii_case(&name))
        {
            return Err(PlanError::invalid(format!(
                "Delta constraint `{name}` already exists"
            )));
        }

        let spec::ExprWithSource {
            expr,
            source: expression_source,
        } = expression;
        let expression_source = expression_source.ok_or_else(|| {
            PlanError::invalid(format!(
                "ALTER TABLE ADD CONSTRAINT `{name}` requires SQL source text for the CHECK expression"
            ))
        })?;

        let scan = self
            .resolve_query_plan(
                spec::QueryPlan {
                    node: spec::QueryNode::Read {
                        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
                            format: Some(DELTA_FORMAT.to_string()),
                            schema: None,
                            options: vec![],
                            paths: vec![location.clone()],
                            predicates: vec![],
                        })),
                        is_streaming: false,
                    },
                    plan_id: None,
                },
                state,
            )
            .await?;
        let schema = scan.schema().clone();
        let resolved_expr = self.resolve_expression(expr, &schema, state).await?;
        let data_type = resolved_expr.get_type(&schema)?;
        if data_type != DataType::Boolean {
            return Err(PlanError::invalid(format!(
                "Delta CHECK constraint `{name}` expression `{expression_source}` must evaluate to BOOLEAN, got {data_type}"
            )));
        }

        let validation_error = ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
            "[DELTA_NEW_CHECK_CONSTRAINT_VIOLATION] CHECK constraint `{}` \
             (expression: {}) would be violated by existing data.",
            name, expression_source
        ))]);
        let validation = LogicalPlanBuilder::new(scan)
            .filter(Expr::IsNotTrue(Box::new(resolved_expr)))?
            .limit(0, Some(1))?
            .project(vec![validation_error.alias("__delta_constraint_validation")])?
            .build()?;

        let command = self.resolve_catalog_command(CatalogCommand::AlterTable {
            table: table.into(),
            if_exists,
            options: AlterTableOptions::AddCheckConstraint {
                name,
                expression: expression_source,
            },
        })?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(BarrierNode::new(
                vec![Arc::new(validation)],
                Arc::new(command),
            )),
        }))
    }

    pub(super) async fn rewrite_delta_check_constraints_from_options(
        &self,
        input: LogicalPlan,
        options: &FileWriteOptions,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !is_delta_format(&options.format) {
            return Ok(input);
        }
        let constraints = delta_check_constraints_from_option_layers(&options.options);
        self.apply_delta_check_constraints(input, constraints, state)
            .await
    }

    pub(super) async fn apply_delta_table_constraints(
        &self,
        input: LogicalPlan,
        info: &TableInfo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !is_delta_format(&info.format) {
            return Ok(input);
        }
        let constraints = self.delta_constraints_from_table_info(info).await?;
        self.apply_delta_check_constraints(input, constraints, state)
            .await
    }

    pub(super) async fn resolve_delta_merge_check_constraints(
        &self,
        format: &str,
        options: &[OptionLayer],
        target_schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<DeltaCheckConstraintExpr>> {
        if !is_delta_format(format) {
            return Ok(vec![]);
        }
        let mut properties = Vec::new();
        for layer in options {
            if let OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } =
                layer
            {
                properties.extend(items.clone());
            }
        }
        let constraints = delta_constraints_from_schema_and_properties(
            target_schema.fields().iter().map(|field| field.as_ref()),
            &properties,
        );
        self.resolve_delta_check_constraints(constraints, target_schema, state)
            .await
    }

    pub(super) async fn resolve_delta_merge_generated_column_exprs(
        &self,
        target_schema: &DFSchemaRef,
        source_schema: &DFSchemaRef,
        merge_schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, Expr)>> {
        let mut out = Vec::new();
        for field in target_schema.fields() {
            let Some(expr_str) = ColumnFeatures::from_field(field).generation_expression() else {
                continue;
            };
            let actual_name = state
                .get_field_info(field.name())
                .map(|info| info.name().to_string())
                .unwrap_or_else(|_| field.name().clone());
            let spec_expr = parse_delta_generation_expr(&expr_str)?;
            let disambiguated = merge_disambiguate_unqualified_plan_ids(
                spec_expr,
                state,
                target_schema,
                source_schema,
            );
            let resolved = self
                .resolve_expression(disambiguated, merge_schema, state)
                .await?;
            out.push((actual_name, resolved));
        }
        Ok(out)
    }

    async fn apply_delta_check_constraints(
        &self,
        input: LogicalPlan,
        constraints: Vec<DeltaCheckConstraint>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if constraints.is_empty() {
            return Ok(input);
        }
        let original_schema = input.schema().clone();
        let field_ids = original_schema
            .fields()
            .iter()
            .map(|field| state.register_field_name(field.name().clone()))
            .collect::<Vec<_>>();
        let intermediate_exprs = input
            .schema()
            .columns()
            .into_iter()
            .zip(field_ids.iter())
            .map(|(column, field_id)| col(column).alias(field_id.clone()))
            .collect::<Vec<_>>();
        let intermediate = LogicalPlanBuilder::new(input)
            .project(intermediate_exprs)?
            .build()?;
        let constraints = self
            .resolve_delta_check_constraints(constraints, intermediate.schema(), state)
            .await?;
        let filtered = apply_delta_check_constraint_filter(intermediate, &constraints, None)?;
        let output_exprs = original_schema
            .fields()
            .iter()
            .zip(field_ids.iter())
            .map(|(field, field_id)| {
                let expr = col(Column::from_name(field_id));
                if field.metadata().is_empty() {
                    expr.alias(field.name().clone())
                } else {
                    expr.alias_with_metadata(
                        field.name().clone(),
                        Some(FieldMetadata::from(field.metadata().clone())),
                    )
                }
            })
            .collect::<Vec<_>>();
        Ok(LogicalPlanBuilder::new(filtered)
            .project(output_exprs)?
            .build()?)
    }

    async fn resolve_delta_check_constraints(
        &self,
        constraints: Vec<DeltaCheckConstraint>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<DeltaCheckConstraintExpr>> {
        let mut out = Vec::with_capacity(constraints.len());
        for constraint in constraints {
            let spec_expr =
                parse_delta_check_constraint_expr(&constraint.name, &constraint.expression)?;
            let expr = self.resolve_expression(spec_expr, schema, state).await?;
            let data_type = expr.get_type(schema)?;
            if data_type != DataType::Boolean {
                return Err(PlanError::invalid(format!(
                    "Delta CHECK constraint `{}` expression `{}` must evaluate to BOOLEAN, got {data_type}",
                    constraint.name, constraint.expression
                )));
            }
            out.push(DeltaCheckConstraintExpr {
                name: constraint.name,
                expression: constraint.expression,
                expr,
                violation: constraint.violation,
            });
        }
        Ok(out)
    }

    async fn delta_constraints_from_table_info(
        &self,
        info: &TableInfo,
    ) -> PlanResult<Vec<DeltaCheckConstraint>> {
        if let Some(metadata) = self.infer_existing_delta_metadata(info).await? {
            return Ok(delta_constraints_from_schema_and_properties(
                metadata.schema.fields().iter().map(|field| field.as_ref()),
                &metadata.properties,
            ));
        }
        let schema = info
            .columns
            .iter()
            .map(|column| column.field())
            .collect::<Vec<_>>();
        Ok(delta_constraints_from_schema_and_properties(
            schema.iter(),
            &info.properties,
        ))
    }

    async fn infer_existing_delta_metadata(
        &self,
        info: &TableInfo,
    ) -> PlanResult<Option<sail_common_datafusion::datasource::TableFormatMetadata>> {
        let Some(location) = info.location.as_ref() else {
            return Ok(None);
        };
        let registry = self.ctx.extension::<TableFormatRegistry>().map_err(|e| {
            PlanError::invalid(format!(
                "failed to access table format registry for Delta table `{location}`: {e}"
            ))
        })?;
        let table_format = registry.get(&info.format).map_err(|e| {
            PlanError::invalid(format!(
                "failed to resolve table format `{}` for Delta table `{location}`: {e}",
                info.format
            ))
        })?;
        let source = SourceInfo {
            paths: vec![location.clone()],
            catalog_table: info.catalog_table.clone(),
            schema: None,
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![OptionLayer::TablePropertyList {
                items: info.properties.clone(),
            }],
        };
        match table_format.infer_metadata(&self.ctx.state(), source).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                log::debug!(
                    "skipping Delta metadata constraint inference for `{location}` because metadata could not be loaded: {e}"
                );
                Ok(None)
            }
        }
    }
}

fn delta_check_constraints_from_option_layers(
    options: &[OptionLayer],
) -> Vec<DeltaCheckConstraint> {
    let mut constraints = Vec::new();
    for layer in options {
        match layer {
            OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                upsert_delta_check_constraints(&mut constraints, items);
            }
            _ => {}
        }
    }
    constraints.sort_by_key(|a| a.name.to_lowercase());
    constraints
}

fn delta_check_constraints_from_properties(
    properties: &[(String, String)],
) -> Vec<DeltaCheckConstraint> {
    let mut constraints = Vec::new();
    upsert_delta_check_constraints(&mut constraints, properties);
    constraints.sort_by_key(|a| a.name.to_lowercase());
    constraints
}

fn delta_constraints_from_schema_and_properties<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
    properties: &[(String, String)],
) -> Vec<DeltaCheckConstraint> {
    let mut constraints = delta_check_constraints_from_properties(properties);
    for field in fields {
        let column_name = field.name().clone();
        if !field.is_nullable() {
            constraints.push(DeltaCheckConstraint {
                name: format!("{column_name} NOT NULL"),
                expression: format!("{} IS NOT NULL", quote_delta_identifier(&column_name)),
                violation: DeltaConstraintViolation::NotNull {
                    column: column_name.clone(),
                },
            });
        }
        if let Some(expression) = delta_invariant_expression(field) {
            constraints.push(DeltaCheckConstraint {
                name: format!("{column_name} INVARIANT"),
                expression,
                violation: DeltaConstraintViolation::Invariant {
                    column: column_name,
                },
            });
        }
    }
    constraints.sort_by_key(|a| a.name.to_lowercase());
    constraints
}

fn upsert_delta_check_constraints(
    constraints: &mut Vec<DeltaCheckConstraint>,
    properties: &[(String, String)],
) {
    for (key, value) in properties {
        let Some(name) = strip_delta_check_constraint_prefix(key) else {
            continue;
        };
        if let Some(existing) = constraints
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            existing.name = name.to_string();
            existing.expression = value.clone();
            existing.violation = DeltaConstraintViolation::Check;
        } else {
            constraints.push(DeltaCheckConstraint {
                name: name.to_string(),
                expression: value.clone(),
                violation: DeltaConstraintViolation::Check,
            });
        }
    }
}

fn strip_delta_check_constraint_prefix(key: &str) -> Option<&str> {
    if key.len() <= DELTA_CHECK_CONSTRAINT_PREFIX.len() {
        return None;
    }
    let prefix = key.get(..DELTA_CHECK_CONSTRAINT_PREFIX.len())?;
    if !prefix.eq_ignore_ascii_case(DELTA_CHECK_CONSTRAINT_PREFIX) {
        return None;
    }
    let name = key.get(DELTA_CHECK_CONSTRAINT_PREFIX.len()..)?;
    (!name.is_empty()).then_some(name)
}

fn is_delta_format(format: &str) -> bool {
    format.eq_ignore_ascii_case(DELTA_FORMAT)
}

fn delta_constraint_property_mutation_key(operation: &spec::AlterTableOperation) -> Option<&str> {
    match operation {
        spec::AlterTableOperation::SetTableProperties { properties } => properties
            .iter()
            .map(|(key, _)| key.as_str())
            .find(|key| strip_delta_check_constraint_prefix(key).is_some()),
        spec::AlterTableOperation::UnsetTableProperties { keys, .. } => keys
            .iter()
            .map(|key| key.as_str())
            .find(|key| strip_delta_check_constraint_prefix(key).is_some()),
        _ => None,
    }
}

fn quote_delta_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn delta_invariant_expression(field: &Field) -> Option<String> {
    fn expression_from_value(value: &serde_json::Value) -> Option<String> {
        if let serde_json::Value::String(inner) = value {
            return serde_json::from_str::<serde_json::Value>(inner)
                .ok()
                .and_then(|value| expression_from_value(&value))
                .or_else(|| Some(inner.clone()));
        }
        value
            .get("expression")?
            .get("expression")?
            .as_str()
            .map(ToString::to_string)
    }

    field
        .metadata()
        .get(DELTA_INVARIANTS_METADATA_KEY)
        .and_then(|raw| {
            serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|value| expression_from_value(&value))
        })
}

pub(super) fn parse_delta_generation_expr(gen_expr_str: &str) -> PlanResult<spec::Expr> {
    let ast_expr = sail_sql_analyzer::parser::parse_expression(gen_expr_str).map_err(|e| {
        PlanError::invalid(format!(
            "failed to parse generation expression `{gen_expr_str}`: {e}"
        ))
    })?;
    sail_sql_analyzer::expression::from_ast_expression(ast_expr).map_err(|e| {
        PlanError::invalid(format!(
            "failed to analyze generation expression `{gen_expr_str}`: {e}"
        ))
    })
}

fn parse_delta_check_constraint_expr(name: &str, expression: &str) -> PlanResult<spec::Expr> {
    let ast_expr = sail_sql_analyzer::parser::parse_expression(expression).map_err(|e| {
        PlanError::invalid(format!(
            "failed to parse Delta CHECK constraint `{name}` expression `{expression}`: {e}"
        ))
    })?;
    sail_sql_analyzer::expression::from_ast_expression(ast_expr).map_err(|e| {
        PlanError::invalid(format!(
            "failed to analyze Delta CHECK constraint `{name}` expression `{expression}`: {e}"
        ))
    })
}
