use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::core;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_function::scalar::struct_function::StructFunction;

use crate::analyzer::model::*;
use crate::analyzer::row_level::{RowAction, RowOperation};
use crate::config::StoreAssignmentPolicy;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
enum UpdateAssignmentValue {
    Expression(spec::Expr),
    Struct(Vec<(String, UpdateAssignmentValue)>),
}

fn rebuild_update_struct(
    original: spec::Expr,
    data_type: &DataType,
    assignments: &[(Vec<String>, spec::Expr)],
) -> PlanResult<UpdateAssignmentValue> {
    if assignments.is_empty() {
        return Ok(UpdateAssignmentValue::Expression(original));
    }
    if let Some((_, value)) = assignments.iter().find(|(path, _)| path.is_empty()) {
        return Ok(UpdateAssignmentValue::Expression(value.clone()));
    }
    let DataType::Struct(fields) = data_type else {
        return Err(PlanError::invalid(
            "nested UPDATE assignment requires a struct",
        ));
    };
    let children = fields
        .iter()
        .map(|field| {
            let child = spec::Expr::UnresolvedExtractValue {
                child: Box::new(original.clone()),
                extraction: Box::new(spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(field.name().clone()),
                })),
            };
            let child_assignments = assignments
                .iter()
                .filter(|(path, _)| path.first() == Some(field.name()))
                .map(|(path, value)| (path[1..].to_vec(), value.clone()))
                .collect::<Vec<_>>();
            Ok((
                field.name().clone(),
                rebuild_update_struct(child, field.data_type(), &child_assignments)?,
            ))
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(UpdateAssignmentValue::Struct(children))
}

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_command_update(
        &mut self,
        table: spec::ObjectName,
        table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::ExprWithSource>,
    ) -> PlanResult<CommandKind> {
        if self.context.config.store_assignment_policy == StoreAssignmentPolicy::Legacy {
            return Err(PlanError::analysis(
                "LEGACY store assignment policy is disallowed in Spark data source V2. Please set the configuration spark.sql.storeAssignmentPolicy to other values.",
            ));
        }
        let target = self.resolve_row_level_target(&table).await?;
        let identifier = if target.lakehouse_table.is_some() {
            self.context
                .session
                .extension::<CatalogManager>()?
                .resolve_fully_qualified_object_name(table.parts())?
        } else {
            table.clone().into()
        };
        let mut input = self.resolve_row_level_table_plan(table).await?;
        if let Some(alias) = &table_alias {
            self.row_level_alias(&mut input, alias.as_ref())?;
        }
        let schema = input.schema.resolved("UPDATE target")?.clone();
        let names = schema
            .attributes
            .iter()
            .map(|a| a.name.clone())
            .collect::<Vec<_>>();
        let assignments = self.normalize_update_assignment_targets(
            assignments,
            table_alias.as_ref().map(AsRef::as_ref),
            &identifier,
            &schema.arrow_schema()?,
            &names,
        )?;
        let mut values = vec![];
        for (name, request) in assignments {
            let name = name
                .parts()
                .first()
                .ok_or_else(|| PlanError::internal("empty UPDATE column"))?
                .as_ref()
                .to_string();
            let attribute = schema
                .attributes
                .iter()
                .find(|a| self.name_key(&a.name) == self.name_key(&name))
                .ok_or_else(|| {
                    PlanError::invalid(format!("Cannot resolve UPDATE target column `{name}`"))
                })?;
            let field = attribute
                .result_type
                .resolved("UPDATE column")?
                .to_field(&name);
            let features = ColumnFeatures::from_field(&field);
            if target.format.eq_ignore_ascii_case("delta") && features.identity().is_some() {
                return Err(PlanError::unsupported(format!(
                    "UPDATE cannot assign Delta identity column '{name}'"
                )));
            }
            let value = match request {
                UpdateAssignmentValue::Expression(spec::Expr::DefaultColumnValue) => {
                    if let Some(value) = features.current_default_value()? {
                        self.context.function_context().literal(value)
                    } else if let Some(source) = features.current_default() {
                        self.resolve_column_default_expression(&source).await?
                    } else if !field.is_nullable() || features.is_not_null_constraint() {
                        return Err(PlanError::analysis(format!(
                            "[NO_DEFAULT_COLUMN_VALUE_AVAILABLE] Can't determine the default value for `{name}` since it is not nullable and it has no default value."
                        )));
                    } else {
                        self.context
                            .function_context()
                            .literal(ScalarValue::try_from(field.data_type())?)
                    }
                }
                value => self.resolve_update_assignment_value(value, &schema).await?,
            };
            self.validate_store_assignment_type(
                &value.result_type.resolved("UPDATE assignment")?.data_type,
                field.data_type(),
                &name,
            )?;
            let value = self.align_store_value(value, field.data_type(), false)?;
            values.push((name, value));
        }
        let condition = self
            .row_level_condition("UPDATE", condition, &schema)
            .await?;
        let mut generated = vec![];
        for attribute in &schema.attributes {
            let field = attribute
                .result_type
                .resolved("UPDATE generated field")?
                .to_field(&attribute.name);
            if let Some(source) = ColumnFeatures::from_field(&field).generation_expression() {
                let expression = self
                    .resolve_named_expression(
                        crate::resolver::command::write_input::parse_expression(&source)?,
                        &schema,
                    )
                    .await?
                    .expression;
                let expression = self.align_store_value(expression, field.data_type(), false)?;
                generated.push((attribute.name.clone(), expression));
            }
        }
        let constraints = self
            .row_level_constraints(&target.format, &target.options, &schema)
            .await?;
        Ok(CommandKind::RowOperation(Box::new(RowOperation {
            input,
            target,
            condition,
            case_sensitive: self.context.config.case_sensitive,
            action: RowAction::Update {
                assignments: values,
                generated,
                constraints,
            },
        })))
    }

    #[async_recursion::async_recursion]
    async fn resolve_update_assignment_value(
        &mut self,
        value: UpdateAssignmentValue,
        schema: &QuerySchema,
    ) -> PlanResult<Expr> {
        match value {
            UpdateAssignmentValue::Expression(expression) => {
                let expression = self
                    .resolve_named_expression(expression, schema)
                    .await?
                    .expression;
                // Any remaining deferred node here is an illegal nested DEFAULT.
                expression.any(&|_| false)?;
                Ok(expression)
            }
            UpdateAssignmentValue::Struct(children) => {
                let mut names = vec![];
                let mut values = vec![];
                for (name, value) in children {
                    names.push(name);
                    values.push(self.resolve_update_assignment_value(value, schema).await?);
                }
                self.context.function_context().scalar_udf(
                    Arc::new(ScalarUDF::from(StructFunction::new(names))),
                    values,
                )
            }
        }
    }

    pub(in crate::resolver) fn validate_store_assignment_type(
        &self,
        source: &DataType,
        target: &DataType,
        name: &str,
    ) -> PlanResult<()> {
        if crate::resolver::command::merge::store_assignment_compatible(
            source,
            target,
            self.context.config.store_assignment_policy,
            self.context.config.case_sensitive,
        ) {
            Ok(())
        } else {
            Err(PlanError::analysis(format!(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table: Cannot safely cast `{name}` \"{source}\" to \"{target}\"."
            )))
        }
    }

    pub(in crate::resolver) fn align_store_value(
        &mut self,
        value: Expr,
        target: &DataType,
        fill_missing: bool,
    ) -> PlanResult<Expr> {
        let source = value
            .result_type
            .resolved("assignment value")?
            .data_type
            .clone();
        if &source == target {
            return Ok(value);
        }
        let (DataType::Struct(source_fields), DataType::Struct(target_fields)) = (&source, target)
        else {
            return self.context.function_context().cast(value, target.clone());
        };
        let mut children = vec![];
        for target_field in target_fields {
            let mut matching = source_fields
                .iter()
                .filter(|field| self.name_key(field.name()) == self.name_key(target_field.name()));
            let source_field = matching.next();
            if matching.next().is_some() {
                return Err(PlanError::invalid(format!(
                    "ambiguous source struct field '{}' during row-level assignment",
                    target_field.name()
                )));
            }
            let child = match source_field {
                Some(field) => {
                    let child = self.context.function_context().copy_expression(&value)?;
                    let name = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Utf8(Some(field.name().clone())));
                    let child = self
                        .context
                        .function_context()
                        .scalar_udf(core::get_field(), vec![child, name])?;
                    self.align_store_value(child, target_field.data_type(), fill_missing)?
                }
                None if fill_missing => self
                    .context
                    .function_context()
                    .literal(ScalarValue::try_from(target_field.data_type())?),
                None => {
                    return Err(PlanError::invalid(format!(
                        "source struct is missing target field '{}' during row-level assignment",
                        target_field.name()
                    )));
                }
            };
            children.push(child);
        }
        let names = target_fields
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let rebuilt = self.context.function_context().scalar_udf(
            Arc::new(ScalarUDF::from(StructFunction::new(names))),
            children,
        )?;
        let rebuilt = self
            .context
            .function_context()
            .cast(rebuilt, target.clone())?;
        let is_null = self.context.function_context().is_null(value);
        let null = self
            .context
            .function_context()
            .literal(ScalarValue::try_from(target)?);
        self.context
            .function_context()
            .case(vec![(is_null, null)], Some(rebuilt))
    }
    fn normalize_update_assignment_targets(
        &self,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        target_alias: Option<&str>,
        target_identifier: &[String],
        input_schema: &arrow_schema::Schema,
        resolved_target_field_names: &[String],
    ) -> PlanResult<Vec<(spec::ObjectName, UpdateAssignmentValue)>> {
        let names_equal = |left: &str, right: &str| {
            if self.context.config.case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };
        let mut paths = Vec::<Vec<String>>::new();
        let mut grouped = Vec::<(String, DataType, Vec<(Vec<String>, spec::Expr)>)>::new();

        for (column, value) in assignments {
            let mut parts = column
                .parts()
                .iter()
                .map(|part| part.as_ref().to_string())
                .collect::<Vec<_>>();
            let has_alias_qualifier =
                parts.len() > 1 && target_alias.is_some_and(|alias| names_equal(alias, &parts[0]));
            if has_alias_qualifier {
                parts.remove(0);
            } else if let Some(root_position) = parts.iter().position(|part| {
                resolved_target_field_names
                    .iter()
                    .any(|name| names_equal(name, part))
            }) && root_position > 0
            {
                let qualifier = &parts[..root_position];
                let qualifier_matches_target = qualifier.len() <= target_identifier.len()
                    && qualifier
                        .iter()
                        .zip(&target_identifier[target_identifier.len() - qualifier.len()..])
                        .all(|(left, right)| names_equal(left, right));
                if qualifier_matches_target {
                    parts.drain(..root_position);
                }
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

            if let Some((_, _, assignments)) =
                grouped.iter_mut().find(|(root, _, _)| root == &root_name)
            {
                assignments.push((canonical_path[1..].to_vec(), value));
            } else {
                grouped.push((
                    root_name,
                    input_schema.fields()[root_index].data_type().clone(),
                    vec![(canonical_path[1..].to_vec(), value)],
                ));
            }
        }

        grouped
            .into_iter()
            .map(|(column, data_type, assignments)| {
                let original = spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(column.clone()),
                    plan_id: None,
                    is_metadata_column: false,
                };
                Ok((
                    spec::ObjectName::bare(column),
                    rebuild_update_struct(original, &data_type, &assignments)?,
                ))
            })
            .collect()
    }
}
