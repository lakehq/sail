use std::collections::HashSet;

use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::column_features::ColumnFeatures;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::merge::{MergeAction, MergeClause, MergeOperation};
use crate::analyzer::model::*;
use crate::analyzer::row_level::Condition;
use crate::config::StoreAssignmentPolicy;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_command_merge_into(
        &mut self,
        request: spec::MergeInto,
    ) -> PlanResult<CommandKind> {
        validate_clause_order(&request.clauses)?;
        if self.context.config.store_assignment_policy == StoreAssignmentPolicy::Legacy {
            return Err(PlanError::analysis(
                "LEGACY store assignment policy is disallowed in Spark data source V2. Please set the configuration spark.sql.storeAssignmentPolicy to other values.",
            ));
        }
        let metadata = self.resolve_row_level_target(&request.target).await?;
        let mut target = self.resolve_row_level_table_plan(request.target).await?;
        let target_alias = request.target_alias.map(|a| a.as_ref().to_string());
        if let Some(alias) = &target_alias {
            self.row_level_alias(&mut target, alias)?;
        }
        let (mut source, source_alias) = match request.source {
            spec::MergeSource::Table { name, alias } => (
                self.resolve_row_level_table_plan(name).await?,
                alias.map(|a| a.as_ref().to_string()),
            ),
            spec::MergeSource::Query { input, alias } => (
                self.command_query(*input).await?,
                alias.map(|a| a.as_ref().to_string()),
            ),
        };
        if let Some(alias) = &source_alias {
            self.row_level_alias(&mut source, alias)?;
        }
        let evolves = request.with_schema_evolution
            && request.clauses.iter().any(|clause| {
                matches!(
                    clause,
                    spec::MergeClause::Matched(spec::MergeMatchedClause {
                        action: spec::MergeMatchedAction::UpdateAll,
                        ..
                    }) | spec::MergeClause::NotMatchedByTarget(
                        spec::MergeNotMatchedByTargetClause {
                            action: spec::MergeNotMatchedByTargetAction::InsertAll,
                            ..
                        }
                    )
                )
            });
        if evolves {
            target = self.evolve_merge_target(target, source.schema.resolved("MERGE source")?)?;
        }
        let target_schema = target.schema.resolved("MERGE target")?.clone();
        let source_schema = source.schema.resolved("MERGE source")?.clone();
        for attribute in &target_schema.attributes {
            if ColumnFeatures::from_map(
                &attribute
                    .result_type
                    .resolved("MERGE target field")?
                    .metadata,
            )
            .identity()
            .is_some()
            {
                return Err(PlanError::unsupported(
                    "MERGE INTO tables with Delta identity columns is not yet supported",
                ));
            }
        }
        // Unqualified names present on both sides bind to the target. Qualified
        // names and explicit Connect plan IDs continue to see both relations.
        let mut combined = target_schema.clone();
        for attribute in &source_schema.attributes {
            let mut attribute = attribute.clone();
            attribute.hidden |= target_schema
                .attributes
                .iter()
                .any(|a| self.name_key(&a.name) == self.name_key(&attribute.name));
            combined.attributes.push(attribute);
        }
        let condition = self
            .merge_condition(Some(request.on_condition), &combined)
            .await?
            .ok_or_else(|| PlanError::internal("missing MERGE condition"))?;
        let (join_keys, residual, target_predicates) =
            self.merge_join_keys(&condition.expression, &target_schema, &source_schema)?;
        let mut matched = vec![];
        let mut source_missing = vec![];
        let mut target_missing = vec![];
        for clause in request.clauses {
            match clause {
                spec::MergeClause::Matched(clause) => {
                    let condition = self.merge_condition(clause.condition, &combined).await?;
                    let action = match clause.action {
                        spec::MergeMatchedAction::Delete => MergeAction::Delete,
                        spec::MergeMatchedAction::UpdateAll => {
                            self.validate_merge_star(
                                &target_schema,
                                &source_schema,
                                request.with_schema_evolution,
                            )?;
                            MergeAction::Update(self.merge_star_assignments(
                                &target_schema,
                                &source_schema,
                                false,
                            )?)
                        }
                        spec::MergeMatchedAction::UpdateSet(assignments) => MergeAction::Update(
                            self.merge_assignments(assignments, &target_schema, &combined)
                                .await?,
                        ),
                    };
                    matched.push(MergeClause { condition, action });
                }
                spec::MergeClause::NotMatchedBySource(clause) => {
                    let condition = self
                        .merge_condition(clause.condition, &target_schema)
                        .await?;
                    let action = match clause.action {
                        spec::MergeNotMatchedBySourceAction::Delete => MergeAction::Delete,
                        spec::MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                            MergeAction::Update(
                                self.merge_assignments(assignments, &target_schema, &target_schema)
                                    .await?,
                            )
                        }
                    };
                    source_missing.push(MergeClause { condition, action });
                }
                spec::MergeClause::NotMatchedByTarget(clause) => {
                    let condition = self
                        .merge_condition(clause.condition, &source_schema)
                        .await?;
                    let action = match clause.action {
                        spec::MergeNotMatchedByTargetAction::InsertAll => {
                            self.validate_merge_star(
                                &target_schema,
                                &source_schema,
                                request.with_schema_evolution,
                            )?;
                            MergeAction::Insert(self.merge_star_assignments(
                                &target_schema,
                                &source_schema,
                                true,
                            )?)
                        }
                        spec::MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                            if columns.len() != values.len() {
                                return Err(PlanError::invalid(
                                    "MERGE INSERT column and value counts do not match",
                                ));
                            }
                            MergeAction::Insert(
                                self.merge_assignments(
                                    columns.into_iter().zip(values).collect(),
                                    &target_schema,
                                    &source_schema,
                                )
                                .await?,
                            )
                        }
                    };
                    target_missing.push(MergeClause { condition, action });
                }
            }
        }
        let mut generated = vec![];
        let mut defaults = vec![];
        for attribute in &target_schema.attributes {
            let field = attribute
                .result_type
                .resolved("MERGE column")?
                .to_field(&attribute.name);
            let features = ColumnFeatures::from_field(&field);
            if let Some(source) = features.generation_expression() {
                let expression = self
                    .resolve_named_expression(
                        crate::resolver::command::write_input::parse_expression(&source)?,
                        &target_schema,
                    )
                    .await?
                    .expression;
                let expression = self.align_store_value(expression, field.data_type(), false)?;
                generated.push((attribute.name.clone(), expression));
            }
            let default = if let Some(value) = features.current_default_value()? {
                Some(self.context.function_context().literal(value))
            } else if let Some(source) = features.current_default() {
                Some(self.resolve_column_default_expression(&source).await?)
            } else {
                Some(
                    self.context
                        .function_context()
                        .literal(ScalarValue::try_from(field.data_type())?),
                )
            };
            if let Some(default) = default {
                defaults.push((
                    attribute.name.clone(),
                    self.align_store_value(default, field.data_type(), false)?,
                ));
            }
        }
        let constraints = self
            .row_level_constraints(&metadata.format, &metadata.options, &target_schema)
            .await?;
        Ok(CommandKind::Merge(Box::new(MergeOperation {
            target,
            source,
            metadata,
            target_alias,
            source_alias,
            with_schema_evolution: request.with_schema_evolution,
            case_sensitive: self.context.config.case_sensitive,
            condition,
            matched,
            source_missing,
            target_missing,
            generated,
            defaults,
            constraints,
            join_keys,
            residual,
            target_predicates,
        })))
    }

    fn evolve_merge_target(&mut self, input: Query, source: &QuerySchema) -> PlanResult<Query> {
        let schema = input.schema.resolved("MERGE evolution target")?;
        let mut expressions = vec![];
        for attribute in &schema.attributes {
            let mut output = attribute.clone();
            let field = attribute
                .result_type
                .resolved("MERGE evolution field")?
                .to_field(&attribute.name);
            let evolved = source
                .attributes
                .iter()
                .find(|a| self.name_key(&a.name) == self.name_key(&attribute.name))
                .map(|a| {
                    Ok::<_, PlanError>(
                        crate::analyzer::typing::store_assignment::evolve_row_level_field(
                            &field,
                            &a.result_type
                                .resolved("MERGE evolution source")?
                                .to_field(&a.name),
                            self.context.config.case_sensitive,
                        ),
                    )
                })
                .transpose()?
                .unwrap_or_else(|| field.as_ref().clone());
            let expression = self.reference(attribute)?;
            let expression = self.align_store_value(expression, evolved.data_type(), true)?;
            output.id = self.context.ids.attribute();
            output.origin = output.id;
            output.result_type = Deferred::Resolved(ExprType::from_field(&evolved));
            expressions.push(Projection {
                attribute: output,
                expression,
            });
        }
        for attribute in &source.attributes {
            if schema
                .attributes
                .iter()
                .any(|a| self.name_key(&a.name) == self.name_key(&attribute.name))
            {
                continue;
            }
            use sail_logical_plan::merge::{
                MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN, SOURCE_PRESENT_COLUMN,
                TARGET_PRESENT_COLUMN, TARGET_ROW_ID_COLUMN,
            };
            if [
                SOURCE_PRESENT_COLUMN,
                TARGET_PRESENT_COLUMN,
                TARGET_ROW_ID_COLUMN,
                OPERATION_COLUMN,
                MERGE_SOURCE_METRIC_COLUMN,
            ]
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&attribute.name))
            {
                return Err(PlanError::invalid(format!(
                    "MERGE schema evolution cannot add reserved internal column '{}'",
                    attribute.name
                )));
            }
            let mut result_type = attribute
                .result_type
                .resolved("MERGE added column")?
                .clone();
            result_type.nullable = true;
            let expression = self
                .context
                .function_context()
                .literal(ScalarValue::try_from(&result_type.data_type)?);
            let mut output = self
                .context
                .function_context()
                .attribute(attribute.name.clone(), result_type);
            output.qualifier = schema
                .attributes
                .first()
                .map(|a| a.qualifier.clone())
                .unwrap_or_default();
            expressions.push(Projection {
                attribute: output,
                expression,
            });
        }
        let schema = QuerySchema {
            attributes: expressions.iter().map(|p| p.attribute.clone()).collect(),
        };
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        })
    }

    fn merge_star_assignments(
        &mut self,
        target: &QuerySchema,
        source: &QuerySchema,
        inserting: bool,
    ) -> PlanResult<Vec<(AttributeId, Expr)>> {
        let mut values = vec![];
        for attribute in &target.attributes {
            let field = attribute.result_type.resolved("MERGE star target")?;
            let value = if let Some(source) = source
                .attributes
                .iter()
                .find(|a| self.name_key(&a.name) == self.name_key(&attribute.name))
            {
                self.reference(source)?
            } else if inserting
                || ColumnFeatures::from_map(&field.metadata)
                    .generation_expression()
                    .is_some()
            {
                // Missing insert/generated fields are populated by their typed expressions.
                continue;
            } else {
                self.reference(attribute)?
            };
            values.push((
                attribute.id,
                self.align_store_value(value, &field.data_type, true)?,
            ));
        }
        Ok(values)
    }

    async fn merge_condition(
        &mut self,
        request: Option<spec::ExprWithSource>,
        schema: &QuerySchema,
    ) -> PlanResult<Option<Condition>> {
        let condition = self.row_level_condition("MERGE", request, schema).await?;
        if let Some(condition) = &condition {
            if condition.expression.any(&|node| {
                matches!(
                    node,
                    ExprKind::ScalarSubquery(_)
                        | ExprKind::Exists { .. }
                        | ExprKind::InSubquery { .. }
                )
            })? {
                return Err(PlanError::analysis(
                    "Subqueries are not allowed in MERGE conditions",
                ));
            }
            if condition.expression.any(&|node| {
                matches!(
                    node,
                    ExprKind::AggregateFunction { .. } | ExprKind::WindowFunction { .. }
                )
            })? {
                return Err(PlanError::analysis(
                    "Aggregate or window expressions are not allowed in MERGE conditions",
                ));
            }
        }
        Ok(condition)
    }

    async fn merge_assignments(
        &mut self,
        requests: Vec<(spec::ObjectName, spec::Expr)>,
        target: &QuerySchema,
        scope: &QuerySchema,
    ) -> PlanResult<Vec<(AttributeId, Expr)>> {
        let mut seen = HashSet::new();
        let mut result = vec![];
        for (name, request) in requests {
            let parts = Vec::<String>::from(name);
            let expression_id = self.context.ids.expression();
            let column = self.resolve_attribute(expression_id, &parts, None, target)?;
            let ExprKind::Attribute(id) = column
                .expression
                .kind
                .into_resolved("MERGE target column")?
            else {
                return Err(PlanError::invalid(
                    "MERGE assignments must reference columns only",
                ));
            };
            let column = target.attribute(id)?;
            if !seen.insert(id) {
                return Err(PlanError::invalid(format!(
                    "Multiple assignments for MERGE target column `{}`",
                    column.name
                )));
            }
            let field = column
                .result_type
                .resolved("MERGE assignment target")?
                .to_field(&column.name);
            let value = if matches!(request, spec::Expr::DefaultColumnValue) {
                let features = ColumnFeatures::from_field(&field);
                if let Some(value) = features.current_default_value()? {
                    self.context.function_context().literal(value)
                } else if let Some(source) = features.current_default() {
                    self.resolve_column_default_expression(&source).await?
                } else if !field.is_nullable() || features.is_not_null_constraint() {
                    return Err(PlanError::analysis(format!(
                        "[NO_DEFAULT_COLUMN_VALUE_AVAILABLE] Can't determine the default value for `{}` since it is not nullable and it has no default value.",
                        column.name
                    )));
                } else {
                    self.context
                        .function_context()
                        .literal(ScalarValue::try_from(field.data_type())?)
                }
            } else {
                self.resolve_named_expression(request, scope)
                    .await?
                    .expression
            };
            value.any(&|_| false)?;
            self.validate_store_assignment_type(
                &value
                    .result_type
                    .resolved("MERGE assignment value")?
                    .data_type,
                field.data_type(),
                &column.name,
            )?;
            result.push((id, self.align_store_value(value, field.data_type(), false)?));
        }
        Ok(result)
    }

    fn validate_merge_star(
        &self,
        target: &QuerySchema,
        source: &QuerySchema,
        evolution: bool,
    ) -> PlanResult<()> {
        for attribute in &target.attributes {
            let field = attribute
                .result_type
                .resolved("MERGE star target")?
                .to_field(&attribute.name);
            let source = source
                .attributes
                .iter()
                .find(|a| self.name_key(&a.name) == self.name_key(&attribute.name));
            let Some(source) = source else {
                if evolution
                    || ColumnFeatures::from_field(&field)
                        .generation_expression()
                        .is_some()
                {
                    continue;
                }
                return Err(PlanError::invalid(format!(
                    "Cannot resolve source column `{}` for MERGE * action without schema evolution",
                    attribute.name
                )));
            };
            let source = source
                .result_type
                .resolved("MERGE star source")?
                .to_field(&source.name);
            let target = if evolution {
                crate::analyzer::typing::store_assignment::evolve_row_level_field(
                    &field,
                    &source,
                    self.context.config.case_sensitive,
                )
            } else {
                field.as_ref().clone()
            };
            self.validate_store_assignment_type(
                source.data_type(),
                target.data_type(),
                &attribute.name,
            )?;
        }
        Ok(())
    }

    #[expect(clippy::type_complexity)]
    fn merge_join_keys(
        &mut self,
        expression: &Expr,
        target: &QuerySchema,
        source: &QuerySchema,
    ) -> PlanResult<(Vec<(Expr, Expr)>, Vec<Expr>, Vec<Expr>)> {
        let mut predicates = vec![];
        split_and(expression, &mut predicates)?;
        let mut keys = vec![];
        let mut residual = vec![];
        let mut target_predicates = vec![];
        for predicate in predicates {
            if let ExprKind::Binary {
                left,
                operator: Operator::Eq,
                right,
            } = predicate.kind.resolved("MERGE equality")?
            {
                let left_domain = domain(left, target, source)?;
                let right_domain = domain(right, target, source)?;
                if left_domain == 1 && right_domain == 2 {
                    keys.push((
                        self.context.function_context().copy_expression(left)?,
                        self.context.function_context().copy_expression(right)?,
                    ));
                    continue;
                }
                if left_domain == 2 && right_domain == 1 {
                    keys.push((
                        self.context.function_context().copy_expression(right)?,
                        self.context.function_context().copy_expression(left)?,
                    ));
                    continue;
                }
            }
            if domain(predicate, target, source)? == 1 {
                target_predicates.push(self.context.function_context().copy_expression(predicate)?);
            }
            residual.push(self.context.function_context().copy_expression(predicate)?);
        }
        Ok((keys, residual, target_predicates))
    }
}

fn domain(expression: &Expr, target: &QuerySchema, source: &QuerySchema) -> PlanResult<u8> {
    let mut domain = 0;
    expression
        .clone()
        .transform_up::<PlanError>(&mut |expression| {
            if let ExprKind::Attribute(id) = expression.kind.resolved("MERGE expression domain")? {
                domain |= if target.attribute(*id).is_ok() {
                    1
                } else if source.attribute(*id).is_ok() {
                    2
                } else {
                    3
                };
            }
            Ok(expression)
        })?;
    Ok(domain)
}

fn split_and<'a>(expression: &'a Expr, output: &mut Vec<&'a Expr>) -> PlanResult<()> {
    if let ExprKind::Binary {
        left,
        operator: Operator::And,
        right,
    } = expression.kind.resolved("MERGE predicate")?
    {
        split_and(left, output)?;
        split_and(right, output)?;
    } else {
        output.push(expression);
    }
    Ok(())
}

fn validate_clause_order(clauses: &[spec::MergeClause]) -> PlanResult<()> {
    let mut matched_unconditional = false;
    let mut not_matched_by_source_unconditional = false;
    let mut not_matched_by_target_unconditional = false;

    for clause in clauses {
        match clause {
            spec::MergeClause::Matched(clause) => {
                if matched_unconditional {
                    return Err(PlanError::invalid(
                        "[NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION] When there are more than one MATCHED clauses in a MERGE statement, only the last MATCHED clause can omit the condition.",
                    ));
                }
                matched_unconditional = clause.condition.is_none();
            }
            spec::MergeClause::NotMatchedBySource(clause) => {
                if not_matched_by_source_unconditional {
                    return Err(PlanError::invalid(
                        "[NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION] When there are more than one NOT MATCHED BY SOURCE clauses in a MERGE statement, only the last NOT MATCHED BY SOURCE clause can omit the condition.",
                    ));
                }
                not_matched_by_source_unconditional = clause.condition.is_none();
            }
            spec::MergeClause::NotMatchedByTarget(clause) => {
                if not_matched_by_target_unconditional {
                    return Err(PlanError::invalid(
                        "[NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION] When there are more than one NOT MATCHED [BY TARGET] clauses in a MERGE statement, only the last NOT MATCHED [BY TARGET] clause can omit the condition.",
                    ));
                }
                not_matched_by_target_unconditional = clause.condition.is_none();
            }
        }
    }
    Ok(())
}
