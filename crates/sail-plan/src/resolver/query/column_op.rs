use std::collections::HashSet;

use datafusion::functions::core;
use indexmap::IndexMap;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::scope::ResolutionScope;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn expand_regex(
        &mut self,
        pattern: &str,
        plan_id: Option<i64>,
        schema: &QuerySchema,
    ) -> PlanResult<Vec<NamedExpression>> {
        let schema = self.recovery_local_schema(schema).clone();
        let quoted = pattern
            .strip_prefix('`')
            .and_then(|p| p.strip_suffix('`'))
            .filter(|p| !p.is_empty());
        let plan_id = if quoted.is_some() { None } else { plan_id };
        let pattern = quoted.unwrap_or_else(|| pattern.trim_matches('`'));
        let regex = regex::Regex::new(&format!("^{pattern}$")).map_err(|error| {
            PlanError::invalid(format!("invalid regex pattern '{pattern}': {error}"))
        })?;
        schema
            .attributes
            .iter()
            .filter(|a| {
                !a.hidden
                    && regex.is_match(&a.name)
                    && plan_id.is_none_or(|id| a.plan_ids.contains(&id))
            })
            .map(|a| {
                Ok(NamedExpression {
                    name: a.name.clone(),
                    expression: self.reference(a)?,
                })
            })
            .collect()
    }

    pub(in crate::resolver) fn visible_output(&mut self, input: Query) -> PlanResult<Query> {
        let schema = input.schema.resolved("visible output")?;
        if schema.attributes.iter().all(|a| !a.hidden) {
            return Ok(input);
        }
        let values = schema
            .attributes
            .iter()
            .filter(|a| !a.hidden)
            .map(|a| {
                Ok(NamedExpression {
                    name: a.name.clone(),
                    expression: self.reference(a)?,
                })
            })
            .collect::<PlanResult<_>>()?;
        self.project(input, values)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn expand_star(
        &mut self,
        target: Option<spec::ObjectName>,
        plan_id: Option<i64>,
        options: spec::WildcardOptions,
        schema: &QuerySchema,
    ) -> PlanResult<Vec<NamedExpression>> {
        let local = self.recovery_local_schema(schema).clone();
        let schema = &local;
        let qualifier = target.map(Vec::<String>::from).unwrap_or_default();
        let mut candidates = schema
            .attributes
            .iter()
            .filter(|attribute| {
                (!attribute.hidden || !qualifier.is_empty())
                    && plan_id.is_none_or(|id| attribute.plan_ids.contains(&id))
                    && attribute.qualifier.len() >= qualifier.len()
                    && attribute.qualifier[attribute.qualifier.len() - qualifier.len()..]
                        .iter()
                        .zip(&qualifier)
                        .all(|(a, b)| self.name_key(a) == self.name_key(b))
            })
            .collect::<Vec<_>>();
        if !qualifier.is_empty() {
            candidates.sort_by_key(|a| a.qualified_position);
        }
        if candidates.is_empty() && !qualifier.is_empty() {
            let id = self.context.ids.expression();
            let root = self.resolve_attribute(id, &qualifier, plan_id, schema)?;
            let arrow_schema::DataType::Struct(fields) = root
                .expression
                .result_type
                .resolved("struct wildcard")?
                .data_type
                .clone()
            else {
                return Err(PlanError::analysis("wildcard target must be a struct"));
            };
            let mut expanded = QuerySchema::default();
            let mut values = std::collections::HashMap::new();
            for field in fields.iter() {
                let source = self
                    .context
                    .function_context()
                    .copy_expression(&root.expression)?;
                let key =
                    self.context
                        .function_context()
                        .literal(datafusion_common::ScalarValue::Utf8(Some(
                            field.name().clone(),
                        )));
                let value = self
                    .context
                    .function_context()
                    .scalar_udf(core::get_field(), vec![source, key])?;
                let attribute = self.context.function_context().attribute(
                    field.name().clone(),
                    value.result_type.resolved("wildcard field")?.clone(),
                );
                values.insert(attribute.id, value);
                expanded.attributes.push(attribute);
            }
            let mut output = self.expand_star(None, None, options, &expanded).await?;
            for named in &mut output {
                named.expression =
                    named
                        .expression
                        .clone()
                        .transform_up::<PlanError>(&mut |expression| {
                            if let crate::analyzer::deferred::Deferred::Resolved(
                                ExprKind::Attribute(id),
                            ) = expression.kind
                                && let Some(value) = values.get(&id)
                            {
                                return self.context.function_context().copy_expression(value);
                            }
                            Ok(expression)
                        })?;
            }
            return Ok(output);
        }
        if candidates.is_empty() && plan_id.is_some() {
            return Err(PlanError::analysis("wildcard target is not in scope"));
        }
        let mut exclusions = HashSet::new();
        for name in options
            .exclude_columns
            .into_iter()
            .flatten()
            .chain(options.except_columns.into_iter().flatten())
        {
            let key = self.name_key(name.as_ref());
            if !candidates.iter().any(|a| self.name_key(&a.name) == key) {
                return Err(PlanError::analysis(format!(
                    "wildcard exclusion does not exist: {}",
                    name.as_ref()
                )));
            }
            exclusions.insert(key);
        }
        let pattern = options
            .ilike_pattern
            .map(|pattern| {
                let pattern = pattern
                    .chars()
                    .map(|c| match c {
                        '%' => ".*".into(),
                        '_' => ".".into(),
                        c => regex::escape(&c.to_string()),
                    })
                    .collect::<String>();
                regex::Regex::new(&format!("(?is)^{pattern}$"))
                    .map_err(|e| PlanError::analysis(e.to_string()))
            })
            .transpose()?;
        let mut replacements = IndexMap::new();
        for replacement in options.replace_columns.unwrap_or_default() {
            let key = self.name_key(replacement.column_name.as_ref());
            if exclusions.contains(&key)
                || !candidates.iter().any(|a| self.name_key(&a.name) == key)
                || replacements.insert(key, replacement).is_some()
            {
                return Err(PlanError::analysis("invalid wildcard replacement"));
            }
        }
        let mut renames = IndexMap::new();
        for rename in options.rename_columns.unwrap_or_default() {
            let key = self.name_key(rename.identifier.as_ref());
            if exclusions.contains(&key)
                || !candidates.iter().any(|a| self.name_key(&a.name) == key)
                || renames.insert(key, rename.alias).is_some()
            {
                return Err(PlanError::analysis("invalid wildcard rename"));
            }
        }
        let mut output = vec![];
        for attribute in candidates {
            let key = self.name_key(&attribute.name);
            if exclusions.contains(&key)
                || pattern
                    .as_ref()
                    .is_some_and(|pattern| !pattern.is_match(&attribute.name))
            {
                continue;
            }
            let expression = if let Some(replacement) = replacements.get(&key) {
                self.resolve_named_expression((*replacement.expression).clone(), schema)
                    .await?
                    .expression
            } else {
                self.reference(attribute)?
            };
            let name = renames
                .get(&key)
                .map(|name| name.as_ref().to_string())
                .unwrap_or_else(|| attribute.name.clone());
            output.push(NamedExpression { name, expression });
        }
        Ok(output)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_with_columns(
        &mut self,
        mut input: Query,
        aliases: Vec<spec::Expr>,
    ) -> PlanResult<Query> {
        let original = input.schema.resolved("WITH COLUMNS input")?.clone();
        let previous = self.expression_replacements.len();
        let mut generators = vec![];
        for alias in &aliases {
            crate::resolver::tree::explode::nested_generators(alias, &mut generators);
        }
        for request in generators {
            if self
                .expression_replacements
                .iter()
                .any(|(r, _)| r == &request)
            {
                continue;
            }
            let (next, mut values) = self.generator(input, request.clone()).await?;
            input = next;
            if values.len() != 1 {
                return Err(PlanError::analysis(
                    "WITH COLUMNS generator requires one output",
                ));
            }
            self.expression_replacements
                .push((request, values.remove(0)));
        }
        let schema = input.schema.resolved("WITH COLUMNS generated input")?;
        let mut replacements = IndexMap::new();
        for alias in aliases {
            let spec::Expr::Alias { name, .. } = &alias else {
                return Err(PlanError::analysis("WITH COLUMNS requires aliases"));
            };
            let name = name.clone().one()?;
            let key = self.name_key(name.as_ref());
            if replacements.insert(key, alias).is_some() {
                return Err(PlanError::analysis("duplicate WITH COLUMNS output name"));
            }
        }
        let mut used = HashSet::new();
        let mut expressions = vec![];
        for attribute in &original.attributes {
            let key = self.name_key(&attribute.name);
            let named = if let Some(request) = replacements.get(&key) {
                used.insert(key);
                // Each duplicate output occurrence owns its expression IDs.
                self.resolve_named_expression(request.clone(), schema)
                    .await?
            } else {
                NamedExpression {
                    name: attribute.name.clone(),
                    expression: self.reference(attribute)?,
                }
            };
            expressions.push(named);
        }
        for (key, request) in replacements {
            if !used.contains(&key) {
                expressions.push(self.resolve_named_expression(request, schema).await?);
            }
        }
        self.expression_replacements.truncate(previous);
        self.project_preserving_hidden(input, expressions)
    }

    pub(in crate::resolver) fn resolve_query_drop_columns(
        &mut self,
        input: Query,
        columns: Vec<spec::Expr>,
        names: Vec<spec::Identifier>,
    ) -> PlanResult<Query> {
        let schema = input.schema.resolved("DROP input")?;
        let scope = ResolutionScope::new(schema, [])
            .with_case_sensitive(self.context.config.case_sensitive);
        let mut excluded = HashSet::new();
        for column in columns {
            let spec::Expr::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column: false,
            } = column
            else {
                return Err(PlanError::analysis("DROP requires column references"));
            };
            let parts: Vec<String> = name.into();
            self.validate_source_binding(&parts, plan_id)?;
            if let Some(bound) = scope.resolve_optional(&parts, plan_id)? {
                // Duplicate references to the same origin are one binding.
                let origin = schema.attribute(bound.id)?.origin;
                excluded.extend(
                    schema
                        .attributes
                        .iter()
                        .filter(|a| a.origin == origin)
                        .map(|a| a.id),
                );
            }
        }
        let names = names
            .iter()
            .map(|name| self.name_key(name.as_ref()))
            .collect::<HashSet<_>>();
        let mut expressions = vec![];
        for attribute in &schema.attributes {
            if !excluded.contains(&attribute.id) && !names.contains(&self.name_key(&attribute.name))
            {
                expressions.push(NamedExpression {
                    name: attribute.name.clone(),
                    expression: self.reference(attribute)?,
                });
            }
        }
        self.project_preserving_hidden(input, expressions)
    }

    fn project_preserving_hidden(
        &mut self,
        input: Query,
        values: Vec<NamedExpression>,
    ) -> PlanResult<Query> {
        let hidden = input
            .schema
            .resolved("hidden inputs")?
            .attributes
            .iter()
            .filter(|a| a.hidden)
            .map(|a| a.origin)
            .collect::<HashSet<_>>();
        let mut output = self.project(input, values)?;
        if let crate::analyzer::deferred::Deferred::Resolved(QueryKind::Project {
            expressions,
            ..
        }) = &mut output.kind
        {
            for projection in expressions.iter_mut() {
                projection.attribute.hidden = hidden.contains(&projection.attribute.origin);
            }
            output.schema = crate::analyzer::deferred::Deferred::Resolved(QuerySchema {
                attributes: expressions.iter().map(|p| p.attribute.clone()).collect(),
            });
        }
        Ok(output)
    }

    pub(in crate::resolver) fn reconcile_schema(
        &mut self,
        input: Query,
        target: spec::Schema,
    ) -> PlanResult<Query> {
        let target = self.types().resolve_schema(target)?;
        let schema = input.schema.resolved("TO SCHEMA input")?;
        let mut expressions = vec![];
        for field in target.fields() {
            let bound = ResolutionScope::new(schema, [])
                .with_case_sensitive(self.context.config.case_sensitive)
                .resolve(std::slice::from_ref(field.name()), None)?;
            let value = self.reference(schema.attribute(bound.id)?)?;
            expressions.push(NamedExpression {
                name: field.name().clone(),
                expression: typing::coerce(value, field.data_type(), &mut self.context.ids)?,
            });
        }
        self.project(input, expressions)
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn query_columns(
        &mut self,
        node: spec::QueryNode,
    ) -> PlanResult<Query> {
        Ok(match node {
            spec::QueryNode::ToDf {
                input,
                column_names,
            } => {
                let input = self.resolve_query_plan(*input).await?;
                self.rename(input, column_names)?
            }
            spec::QueryNode::WithColumns { input, aliases } => {
                let input = self.resolve_query_plan(*input).await?;
                self.resolve_query_with_columns(input, aliases).await?
            }
            spec::QueryNode::Drop {
                input,
                columns,
                column_names,
            } => {
                let input = self.resolve_query_plan(*input).await?;
                self.resolve_query_drop_columns(input, columns, column_names)?
            }
            spec::QueryNode::ToSchema { input, schema } => {
                let input = self.resolve_query_plan(*input).await?;
                self.reconcile_schema(input, schema)?
            }
            spec::QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                let input = self.resolve_query_plan(*input).await?;
                let schema = input.schema.resolved("rename input")?;
                let mut values = vec![];
                for attribute in &schema.attributes {
                    let rename = rename_columns_map.iter().find(|(from, _)| {
                        self.name_key(from.as_ref()) == self.name_key(&attribute.name)
                    });
                    let mut expression = self.reference(attribute)?;
                    let name = if let Some((_, to)) = rename {
                        expression = Expr {
                            id: self.context.ids.expression(),
                            result_type: expression.result_type.clone(),
                            kind: Deferred::Resolved(ExprKind::WithMetadata(Box::new(expression))),
                        };
                        to.as_ref().to_string()
                    } else {
                        attribute.name.clone()
                    };
                    values.push(NamedExpression { name, expression });
                }
                self.project(input, values)?
            }
            spec::QueryNode::FillNa {
                input,
                columns,
                values,
            } => self.resolve_query_fill_na(*input, columns, values).await?,
            spec::QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => {
                self.resolve_query_drop_na(*input, columns, min_non_nulls)
                    .await?
            }
            spec::QueryNode::Replace {
                input,
                columns,
                replacements,
            } => {
                self.resolve_query_replace(*input, columns, replacements)
                    .await?
            }
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }
}
