use arrow_schema::{DataType, Schema};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::AlterTableOptions;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::write_constraints::strip_delta_check_constraint_prefix;
use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};
impl Resolver<'_, '_> {
    pub(super) async fn resolve_delta_alter_table_or_catalog(
        &mut self,
        table: spec::ObjectName,
        if_exists: bool,
        operation: spec::AlterTableOperation,
    ) -> PlanResult<CommandKind> {
        if let Some(key) = delta_constraint_property_mutation_key(&operation)
            && self.alter_table_target_is_delta(&table, if_exists).await?
        {
            return Err(PlanError::invalid(format!(
                "[DELTA_ADD_CONSTRAINTS] Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints. Invalid property: {key}"
            )));
        }
        match operation {
            spec::AlterTableOperation::AddCheckConstraint { name, expression } => {
                self.resolve_delta_add_check_constraint(table, if_exists, name.into(), expression)
                    .await
            }
            other => {
                self.resolve_catalog_alter_table(table, if_exists, other)
                    .await
            }
        }
    }

    async fn alter_table_target_is_delta(
        &mut self,
        table: &spec::ObjectName,
        if_exists: bool,
    ) -> PlanResult<bool> {
        match self
            .context
            .session
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

    pub(super) async fn validate_delta_replacement_generated_columns(
        &mut self,
        table: &spec::ObjectName,
        requested: &Schema,
    ) -> PlanResult<()> {
        if !self.alter_table_target_is_delta(table, false).await? {
            return Ok(());
        }
        let Some(info) = self.resolve_table_info(table).await? else {
            return Ok(());
        };
        let generated = requested
            .fields()
            .iter()
            .filter_map(|field| {
                info.columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(field.name()))
                    .and_then(|column| column.generated_always_as.as_deref())
                    .map(|source| (field, source))
            })
            .collect::<Vec<_>>();
        let attributes = requested
            .fields()
            .iter()
            .filter(|field| {
                !generated
                    .iter()
                    .any(|(generated, _)| generated.name() == field.name())
            })
            .map(|field| self.attribute(field.name().clone(), ExprType::from_field(field)))
            .collect();
        let schema = QuerySchema { attributes };
        for (field, source) in generated {
            let expression = self
                .expression(super::write_input::parse_expression(source)?, &schema)
                .await?
                .expression;
            let actual = &expression
                .result_type
                .resolved("generated replacement type")?
                .data_type;
            if !same_generated_column_type(field.data_type(), actual) {
                return Err(PlanError::invalid(format!(
                    "[DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH] Generated column '{}' has type {}, but its expression has type {actual}",
                    field.name(),
                    field.data_type()
                )));
            }
        }
        Ok(())
    }

    async fn resolve_delta_add_check_constraint(
        &mut self,
        table: spec::ObjectName,
        if_exists: bool,
        name: String,
        request: spec::ExprWithSource,
    ) -> PlanResult<CommandKind> {
        let status = match self
            .context
            .session
            .extension::<CatalogManager>()?
            .get_table(table.parts())
            .await
        {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _)) if if_exists => {
                return Ok(CommandKind::Query(Self::empty(false)));
            }
            Err(error) => return Err(error.into()),
        };
        let TableKind::Table {
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
        if properties.iter().any(|(key, _)| {
            key.to_lowercase() == format!("delta.constraints.{name}").to_lowercase()
        }) {
            return Err(PlanError::invalid(format!(
                "Delta constraint `{name}` already exists"
            )));
        }
        let source = request.source.ok_or_else(|| PlanError::invalid(format!("ALTER TABLE ADD CONSTRAINT `{name}` requires SQL source text for the CHECK expression")))?;
        let input = self
            .data_source(spec::ReadDataSource {
                format: Some("delta".into()),
                schema: None,
                options: vec![],
                paths: vec![location],
                predicates: vec![],
            })
            .await?;
        let schema = input
            .schema
            .resolved("constraint validation input")?
            .clone();
        let expression = self.expression(request.expr, &schema).await?.expression;
        let data_type = &expression
            .result_type
            .resolved("constraint type")?
            .data_type;
        if data_type != &DataType::Boolean {
            return Err(PlanError::invalid(format!(
                "Delta CHECK constraint `{name}` expression `{source}` must evaluate to BOOLEAN, got {data_type}"
            )));
        }
        let condition = ExprNode {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::TruthTest {
                expression: Box::new(expression),
                value: Some(true),
                negated: true,
            }),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        };
        let input = QueryNode {
            kind: Deferred::Resolved(QueryKind::Filter {
                input: Box::new(input),
                condition: Box::new(condition),
            }),
            schema: Deferred::Resolved(schema),
        };
        let input = self.limit(input, 0, 1);
        let message = format!(
            "[DELTA_NEW_CHECK_CONSTRAINT_VIOLATION] CHECK constraint `{name}` (expression: {source}) would be violated by existing data."
        );
        let expression = self.write_error(message)?;
        let validation = self.project(
            input,
            vec![NamedExpression {
                name: "constraint_validation".into(),
                expression,
            }],
        )?;
        let command = CommandKind::Catalog(CatalogCommand::AlterTable {
            table: table.into(),
            if_exists,
            options: AlterTableOptions::AddCheckConstraint {
                name,
                expression: source,
            },
        });
        Ok(CommandKind::Barrier {
            preconditions: vec![CommandKind::Query(validation).into()],
            command: Box::new(command.into()),
        })
    }
}

fn is_delta_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("delta")
}
fn same_generated_column_type(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    left.name() == right.name()
                        && same_generated_column_type(left.data_type(), right.data_type())
                })
        }
        (
            DataType::List(left) | DataType::LargeList(left),
            DataType::List(right) | DataType::LargeList(right),
        )
        | (DataType::Map(left, _), DataType::Map(right, _)) => {
            same_generated_column_type(left.data_type(), right.data_type())
        }
        (
            DataType::Timestamp(left_unit, left_zone),
            DataType::Timestamp(right_unit, right_zone),
        ) => left_unit == right_unit && left_zone.is_some() == right_zone.is_some(),
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => true,
        _ => expected == actual,
    }
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
