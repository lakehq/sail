use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::catalog::TableColumnStatus;
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{
    DataSourceRegistry, DeltaConstraintViolation, OptionLayer, SourceInfo, find_path_in_options,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::Resolver;
use super::write::{TableInfo, WriteColumnMatch};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::analysis::writer::WriteOperation;
use crate::error::{PlanError, PlanResult};
const DELTA_FORMAT: &str = "delta";
const DELTA_CHECK_CONSTRAINT_PREFIX: &str = "delta.constraints.";
const DELTA_INVARIANTS_METADATA_KEY: &str = "delta.invariants";
#[derive(Clone, Debug)]
struct DeltaCheckConstraint {
    name: String,
    expression: String,
    violation: DeltaConstraintViolation,
}
impl Resolver<'_, '_> {
    pub(super) async fn rewrite_data_source_delta_table_features(
        &mut self,
        input: QueryNode,
        format: &str,
        info: &WriteOperation,
    ) -> PlanResult<QueryNode> {
        if !is_delta_format(format) {
            return Ok(input);
        }
        let Some(path) = find_path_in_options(&info.options) else {
            return Ok(input);
        };

        let registry = self
            .context
            .session
            .extension::<DataSourceRegistry>()
            .map_err(|e| {
                PlanError::invalid(format!(
                    "failed to access lake source registry for Delta path `{path}`: {e}",
                ))
            })?;
        let lake_source = registry.get_lake_source(format).map_err(|e| {
            PlanError::invalid(format!(
                "failed to resolve lake source `{}` for Delta path `{path}`: {e}",
                format
            ))
        })?;
        let source_info = SourceInfo {
            paths: vec![path.clone()],
            lakehouse_table: None,
            schema: None,
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![],
            read_case_sensitive: self.context.config.case_sensitive,
        };
        let metadata = match lake_source
            .infer_metadata(&self.context.session.state(), source_info)
            .await
        {
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
            default_values: Default::default(),
            lakehouse_table: info.lakehouse_table.clone(),
            columns,
            location: Some(path),
            format: format.to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            properties: metadata.properties,
        };
        let input = self
            .rewrite_write_input(input, WriteColumnMatch::ByName, &info)
            .await?;
        self.apply_delta_check_constraints(input, constraints).await
    }

    pub(super) async fn rewrite_delta_check_constraints_from_options(
        &mut self,
        input: QueryNode,
        format: &str,
        info: &WriteOperation,
    ) -> PlanResult<QueryNode> {
        if !is_delta_format(format) {
            return Ok(input);
        }
        let constraints = delta_check_constraints_from_option_layers(&info.options);
        self.apply_delta_check_constraints(input, constraints).await
    }

    pub(super) async fn apply_delta_table_constraints(
        &mut self,
        input: QueryNode,
        info: &TableInfo,
    ) -> PlanResult<QueryNode> {
        if !is_delta_format(&info.format) {
            return Ok(input);
        }
        let constraints = self.delta_constraints_from_table_info(info).await?;
        self.apply_delta_check_constraints(input, constraints).await
    }

    async fn delta_constraints_from_table_info(
        &mut self,
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
        &mut self,
        info: &TableInfo,
    ) -> PlanResult<Option<sail_common_datafusion::lakesource::LakeSourceMetadata>> {
        let Some(location) = info.location.as_ref() else {
            return Ok(None);
        };
        let registry = self
            .context
            .session
            .extension::<DataSourceRegistry>()
            .map_err(|e| {
                PlanError::invalid(format!(
                    "failed to access lake source registry for Delta table `{location}`: {e}"
                ))
            })?;
        let lake_source = registry.get_lake_source(&info.format).map_err(|e| {
            PlanError::invalid(format!(
                "failed to resolve lake source `{}` for Delta table `{location}`: {e}",
                info.format
            ))
        })?;
        let source = SourceInfo {
            paths: vec![location.clone()],
            lakehouse_table: info.lakehouse_table.clone(),
            schema: None,
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![OptionLayer::TablePropertyList {
                items: info.properties.clone(),
            }],
            read_case_sensitive: self.context.config.case_sensitive,
        };
        match lake_source
            .infer_metadata(&self.context.session.state(), source)
            .await
        {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                log::debug!(
                    "skipping Delta metadata constraint inference for `{location}` because metadata could not be loaded: {e}"
                );
                Ok(None)
            }
        }
    }
    async fn apply_delta_check_constraints(
        &mut self,
        input: QueryNode,
        constraints: Vec<DeltaCheckConstraint>,
    ) -> PlanResult<QueryNode> {
        if constraints.is_empty() {
            return Ok(input);
        }
        let schema = input.schema.resolved("constraint input")?.clone();
        let mut predicates = vec![];
        for constraint in constraints {
            let request =
                parse_delta_check_constraint_expr(&constraint.name, &constraint.expression)?;
            let expression = self.expression(request, &schema).await?.expression;
            let data_type = &expression
                .result_type
                .resolved("constraint type")?
                .data_type;
            if data_type != &DataType::Boolean {
                return Err(PlanError::invalid(format!(
                    "Delta CHECK constraint `{}` expression `{}` must evaluate to BOOLEAN, got {data_type}",
                    constraint.name, constraint.expression
                )));
            }
            predicates.push((constraint, expression));
        }
        let mut validation = self.literal(ScalarValue::Int8(Some(1)));
        for (constraint, expression) in predicates.into_iter().rev() {
            let message = match constraint.violation {
                DeltaConstraintViolation::Check => format!(
                    "[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint {} ({}) violated.",
                    constraint.name, constraint.expression
                ),
                DeltaConstraintViolation::NotNull { column } => format!(
                    "[DELTA_NOT_NULL_CONSTRAINT_VIOLATED] NOT NULL constraint violated for column: {column}"
                ),
                DeltaConstraintViolation::Invariant { column } => format!(
                    "[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint {column} ({}) violated.",
                    constraint.expression
                ),
            };
            let error = self.write_error(message)?;
            let condition = ExprNode {
                id: self.context.ids.expression(),
                result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
                kind: Deferred::Resolved(ExprKind::TruthTest {
                    expression: Box::new(expression),
                    value: Some(true),
                    negated: false,
                }),
            };
            validation = self.case(vec![(condition, validation)], Some(error))?;
        }
        let valid = self.literal(ScalarValue::Int8(Some(1)));
        let condition = self.binary(validation, Operator::Eq, valid)?;
        Ok(QueryNode {
            kind: Deferred::Resolved(QueryKind::Filter {
                input: Box::new(input),
                condition: Box::new(condition),
            }),
            schema: Deferred::Resolved(schema),
        })
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
            .find(|constraint| delta_constraint_names_equal(&constraint.name, name))
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

fn delta_constraint_names_equal(left: &str, right: &str) -> bool {
    left.to_lowercase() == right.to_lowercase()
}

pub(super) fn strip_delta_check_constraint_prefix(key: &str) -> Option<&str> {
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

impl Resolver<'_, '_> {
    pub(super) async fn row_level_constraints(
        &mut self,
        format: &str,
        options: &[OptionLayer],
        schema: &QuerySchema,
    ) -> PlanResult<Vec<crate::analysis::row_level::CheckConstraint>> {
        if !is_delta_format(format) {
            return Ok(vec![]);
        }
        let properties = options
            .iter()
            .flat_map(|layer| match layer {
                OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                    items.clone()
                }
                _ => vec![],
            })
            .collect::<Vec<_>>();
        let arrow_schema = schema.arrow_schema()?;
        let constraints = delta_constraints_from_schema_and_properties(
            arrow_schema.fields().iter().map(AsRef::as_ref),
            &properties,
        );
        let mut result = vec![];
        for constraint in constraints {
            let expression = self
                .expression(
                    parse_delta_check_constraint_expr(&constraint.name, &constraint.expression)?,
                    schema,
                )
                .await?
                .expression;
            if expression
                .result_type
                .resolved("constraint type")?
                .data_type
                != DataType::Boolean
            {
                return Err(PlanError::invalid(format!(
                    "Delta CHECK constraint `{}` must evaluate to BOOLEAN",
                    constraint.name
                )));
            }
            result.push(crate::analysis::row_level::CheckConstraint {
                name: constraint.name,
                source: constraint.expression,
                expression,
                violation: constraint.violation,
            });
        }
        Ok(result)
    }
}
