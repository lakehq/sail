use std::sync::Arc;

use arrow_schema::DataType;
use async_recursion::async_recursion;
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::ScalarValue;
use datafusion_expr::TableSource;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, read_record_batches,
};

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion]
    pub(super) async fn query(&mut self, request: spec::QueryPlan) -> PlanResult<LogicalPlan> {
        let plan_id = request.plan_id;
        let mut plan = match request.node {
            node @ (spec::QueryNode::Empty { .. }
            | spec::QueryNode::Values(_)
            | spec::QueryNode::Range(_)
            | spec::QueryNode::LocalRelation { .. }
            | spec::QueryNode::Read { .. }
            | spec::QueryNode::CachedRemoteRelation { .. }) => {
                self.query_source(node, plan_id).await?
            }
            node @ (spec::QueryNode::Project { .. }
            | spec::QueryNode::Aggregate(_)
            | spec::QueryNode::Filter { .. }
            | spec::QueryNode::Sort { .. }
            | spec::QueryNode::Limit { .. }) => self.query_projection(node).await?,
            node @ (spec::QueryNode::SubqueryAlias { .. }
            | spec::QueryNode::TableAlias { .. }
            | spec::QueryNode::NamedWindows { .. }
            | spec::QueryNode::WithCtes { .. }
            | spec::QueryNode::WithRelations { .. }
            | spec::QueryNode::WithParameters { .. }) => self.query_scope(node).await?,
            node @ (spec::QueryNode::ToDf { .. }
            | spec::QueryNode::WithColumns { .. }
            | spec::QueryNode::Drop { .. }
            | spec::QueryNode::ToSchema { .. }
            | spec::QueryNode::WithColumnsRenamed { .. }
            | spec::QueryNode::FillNa { .. }
            | spec::QueryNode::DropNa { .. }
            | spec::QueryNode::Replace { .. }) => self.query_columns(node).await?,
            node @ (spec::QueryNode::LateralView { .. }
            | spec::QueryNode::Repartition { .. }
            | spec::QueryNode::RepartitionByExpression { .. }
            | spec::QueryNode::Deduplicate(_)
            | spec::QueryNode::Hint { .. }
            | spec::QueryNode::Sample(_)
            | spec::QueryNode::TableSample { .. }
            | spec::QueryNode::SetOperation(_)
            | spec::QueryNode::LateralJoin { .. }
            | spec::QueryNode::Join(_)) => self.query_relations(node).await?,
            node @ (spec::QueryNode::CommonInlineUserDefinedTableFunction(_)
            | spec::QueryNode::GroupMap(_)
            | spec::QueryNode::CoGroupMap(_)
            | spec::QueryNode::MapPartitions { .. }
            | spec::QueryNode::Pivot(_)
            | spec::QueryNode::Unpivot(_)
            | spec::QueryNode::Tail { .. }
            | spec::QueryNode::StatCrosstab { .. }
            | spec::QueryNode::StatCov { .. }
            | spec::QueryNode::StatCorr { .. }
            | spec::QueryNode::StatSummary { .. }
            | spec::QueryNode::StatDescribe { .. }
            | spec::QueryNode::StatSampleBy { .. }) => self.query_other(node).await?,
            spec::QueryNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            spec::QueryNode::CollectMetrics { .. } => {
                return Err(PlanError::todo("collect metrics"));
            }
            spec::QueryNode::Parse(_) => return Err(PlanError::todo("parse")),
            spec::QueryNode::WithWatermark(_) => return Err(PlanError::todo("with watermark")),
            spec::QueryNode::ApplyInPandasWithState(_) => {
                return Err(PlanError::todo("apply in pandas with state"));
            }
            spec::QueryNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            spec::QueryNode::StatFreqItems { .. } => return Err(PlanError::todo("freq items")),
        };
        if let Some(id) = plan_id {
            let mut schema = plan.schema.into_resolved("query schema")?;
            for attribute in &mut schema.attributes {
                if !attribute.plan_ids.contains(&id) {
                    attribute.plan_ids.push(id);
                }
            }
            self.bindings.entry(id).or_default().push(schema.clone());
            plan.schema = Deferred::Resolved(schema);
        }
        Ok(plan)
    }

    #[async_recursion]
    async fn query_source(
        &mut self,
        node: spec::QueryNode,
        plan_id: Option<i64>,
    ) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::Empty { produce_one_row } => Self::empty(produce_one_row),
            spec::QueryNode::Values(rows) => self.values(rows).await?,
            spec::QueryNode::Range(range) => {
                let partitions = range.num_partitions.unwrap_or(1);
                if partitions == 0 || range.step == 0 {
                    return Err(PlanError::analysis(
                        "range step must not be 0 and partition count must be positive",
                    ));
                }
                let attribute =
                    self.attribute("id".into(), ExpressionType::new(DataType::Int64, false));
                LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Range {
                        start: range.start.unwrap_or(0),
                        end: range.end,
                        step: range.step,
                        partitions,
                    }),
                    schema: Deferred::Resolved(PlanSchema {
                        attributes: vec![attribute],
                    }),
                }
            }
            spec::QueryNode::LocalRelation { data, schema } => {
                let batches = data
                    .map(|data| read_record_batches(&data))
                    .transpose()?
                    .unwrap_or_default();
                let (schema, batches) = if let Some(schema) = schema {
                    let schema = Arc::new(self.types().resolve_schema(schema)?);
                    let batches = batches
                        .into_iter()
                        .map(|batch| cast_record_batch_positionally(batch, Arc::clone(&schema)))
                        .collect::<Result<Vec<_>, _>>()?;
                    (schema, batches)
                } else if let Some(batch) = batches.first() {
                    (batch.schema(), batches)
                } else {
                    return Err(PlanError::analysis("missing schema for local relation"));
                };
                let provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
                self.scan(
                    "local_relation".into(),
                    provider_as_source(provider),
                    vec![],
                )?
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::DataSource(source),
                is_streaming: _,
            } => self.data_source(*source).await?,
            spec::QueryNode::Read {
                read_type: spec::ReadType::NamedTable(table),
                is_streaming: _,
            } => {
                let name: Vec<String> = table.name.clone().into();
                if let Some(request) = self.ctes.get(&self.name_key(&name.join("."))).cloned() {
                    if table.temporal.is_some() || !table.options.is_empty() {
                        return Err(PlanError::analysis(
                            "CTE references do not accept table options",
                        ));
                    }
                    let origins = if request.parameter {
                        &mut self.parameter_origins
                    } else {
                        &mut self.cte_origins
                    };
                    let output = request.plan.schema.resolved("CTE definition")?;
                    let renew = output
                        .attributes
                        .iter()
                        .any(|a| origins.contains(&a.origin));
                    if !renew {
                        origins.extend(output.attributes.iter().map(|a| a.origin));
                    }
                    let mut plan = request.plan.instantiate_reference(
                        &mut self.context.ids,
                        renew,
                        request.parameter || !renew,
                    )?;
                    let mut schema = plan.schema.into_resolved("CTE schema")?;
                    for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                        attribute.qualifier = name.clone();
                        attribute.qualified_position = position;
                    }
                    plan.schema = Deferred::Resolved(schema);
                    let plan = LogicalPlan {
                        schema: plan.schema.clone(),
                        node: Deferred::Resolved(PlanNode::Scope {
                            input: Box::new(plan),
                        }),
                    };
                    if let Some(sample) = table.sample {
                        self.table_sample(plan, sample).await?
                    } else {
                        plan
                    }
                } else {
                    self.named_table(*table).await?
                }
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::DynamicTable(table),
                is_streaming: _,
            } => {
                let name = self.identifier(table.name).await?;
                self.query(spec::QueryPlan {
                    plan_id: None,
                    node: spec::QueryNode::Read {
                        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                            name,
                            temporal: None,
                            sample: table.sample,
                            options: table.options,
                        })),
                        is_streaming: false,
                    },
                })
                .await?
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::Udtf(function),
                is_streaming: _,
            } => {
                if !function.options.is_empty() {
                    return Err(PlanError::todo("ReadType::UDTF options"));
                }
                if matches!(function.name.parts(), [name] if name.as_ref().eq_ignore_ascii_case("range"))
                    && self.registered_function("range")?.is_none()
                {
                    if !function.named_arguments.is_empty() {
                        return Err(PlanError::invalid("range does not accept named arguments"));
                    }
                    let mut args = vec![];
                    for argument in function.arguments {
                        let value = self
                            .expression(argument, &PlanSchema::default())
                            .await?
                            .expression;
                        args.push(crate::analysis::lowering::evaluate_constant(&value)?);
                    }
                    use sail_common_datafusion::literal::LiteralValue;
                    let (start, end, step, partitions) = match args.as_slice() {
                        [end] => (0, LiteralValue(end).try_to_i64()?, 1, 1),
                        [start, end] => (
                            LiteralValue(start).try_to_i64()?,
                            LiteralValue(end).try_to_i64()?,
                            1,
                            1,
                        ),
                        [start, end, step] => (
                            LiteralValue(start).try_to_i64()?,
                            LiteralValue(end).try_to_i64()?,
                            LiteralValue(step).try_to_i64()?,
                            1,
                        ),
                        [start, end, step, partitions] => (
                            LiteralValue(start).try_to_i64()?,
                            LiteralValue(end).try_to_i64()?,
                            LiteralValue(step).try_to_i64()?,
                            LiteralValue(partitions).try_to_usize()?,
                        ),
                        _ => return Err(PlanError::invalid("range requires 1 to 4 arguments")),
                    };
                    return self
                        .query(spec::QueryPlan {
                            plan_id,
                            node: spec::QueryNode::Range(spec::Range {
                                start: Some(start),
                                end,
                                step,
                                num_partitions: Some(partitions),
                            }),
                        })
                        .await;
                }
                let request = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: function.name,
                    arguments: function.arguments,
                    named_arguments: function.named_arguments,
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                });
                let (input, generated) = self.generator(Self::empty(true), request).await?;
                self.project(input, generated)?
            }
            spec::QueryNode::CachedRemoteRelation { relation_id } => {
                use sail_common_datafusion::extension::SessionExtensionAccessor;
                let registry =
                    self.context
                        .session
                        .extension::<sail_cache::remote_checkpoint::RemoteCheckpointRegistry>()?;
                let descriptor = registry.get(&relation_id)?.ok_or_else(|| {
                    PlanError::analysis(format!(
                        "remote checkpoint relation does not exist: {relation_id}"
                    ))
                })?;
                let attributes = descriptor
                    .logical_schema
                    .fields()
                    .iter()
                    .map(|f| self.attribute(f.name().clone(), ExpressionType::from_field(f)))
                    .collect();
                LogicalPlan {
                    schema: Deferred::Resolved(PlanSchema { attributes }),
                    node: Deferred::Resolved(PlanNode::RemoteCheckpoint { relation_id }),
                }
            }
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    #[async_recursion]
    async fn query_projection(&mut self, node: spec::QueryNode) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::Project { input, expressions } => {
                let mut input = match input {
                    Some(input) => self.query(*input).await?,
                    None => Self::empty(true),
                };
                let mut resolved = vec![];
                let replacements_start = self.expression_replacements.len();
                let schema = input.schema.resolved("project input")?.clone();
                for expression in expressions {
                    if super::generators::is_generator(&expression) {
                        let (next, generated) = self.generator(input, expression).await?;
                        input = next;
                        resolved.extend(generated);
                    } else if let spec::Expr::UnresolvedRegex { col_name, plan_id } = expression {
                        resolved.extend(self.expand_regex(&col_name, plan_id, &schema)?);
                    } else if let spec::Expr::UnresolvedStar {
                        target,
                        plan_id,
                        wildcard_options,
                    } = expression
                    {
                        resolved.extend(
                            self.expand_star(target, plan_id, wildcard_options, &schema)
                                .await?,
                        );
                    } else {
                        let mut generators = vec![];
                        super::generators::nested_generators(&expression, &mut generators);
                        for request in generators {
                            let (next, mut generated) =
                                self.generator(input, request.clone()).await?;
                            input = next;
                            if generated.len() != 1 {
                                return Err(PlanError::analysis(
                                    "nested generator must produce exactly one column",
                                ));
                            }
                            let generated = generated
                                .pop()
                                .ok_or_else(|| PlanError::internal("missing generator column"))?;
                            self.expression_replacements.push((request, generated));
                        }
                        resolved.push(
                            self.expression(expression, input.schema.resolved("project input")?)
                                .await?,
                        );
                    }
                }
                self.expression_replacements.truncate(replacements_start);
                let has_aggregate = resolved
                    .iter()
                    .map(|named| {
                        Ok(named.expression.any(&|node| {
                            matches!(node, ExpressionNode::AggregateFunction { .. })
                        })?)
                    })
                    .collect::<PlanResult<Vec<_>>>()?
                    .into_iter()
                    .any(|value| value);
                if has_aggregate {
                    self.aggregate(input, vec![], resolved, None)?
                } else {
                    self.project(input, resolved)?
                }
            }
            spec::QueryNode::Aggregate(aggregate) => self.aggregate_request(aggregate).await?,
            spec::QueryNode::Filter { input, condition } => {
                let input = self.query(*input).await?;
                let output = input.schema.resolved("filter output")?.clone();
                let (input, condition) = self.recover_expression(input, condition, false).await?;
                let condition = typing::coerce(
                    condition.expression,
                    &DataType::Boolean,
                    &mut self.context.ids,
                )?;
                let schema = input.schema.clone();
                let result = LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Filter {
                        input: Box::new(input),
                        condition: Box::new(condition),
                    }),
                    schema,
                };
                self.restore_output(result, output)?
            }
            spec::QueryNode::Sort {
                input,
                order,
                is_global,
            } => {
                let mut input = self.query(*input).await?;
                let output = input.schema.resolved("sort output")?.clone();
                let visible = output
                    .attributes
                    .iter()
                    .filter(|a| !a.hidden)
                    .collect::<Vec<_>>();
                let mut resolved = vec![];
                for sort in order {
                    let ascending = !matches!(sort.direction, spec::SortDirection::Descending);
                    let expression = if let Some(position) =
                        super::aggregate_request::ordinal(&sort.child)
                    {
                        if position < 1 || position as usize > visible.len() {
                            return Err(PlanError::invalid(format!(
                                "Cannot resolve column position {position}. Valid positions are 1 to {}.",
                                visible.len()
                            )));
                        }
                        self.reference(visible[position as usize - 1])?
                    } else {
                        let (next, expression) =
                            self.recover_expression(input, *sort.child, true).await?;
                        input = next;
                        expression.expression
                    };
                    let expression = if matches!(
                        expression.node,
                        Deferred::Resolved(ExpressionNode::Attribute(_))
                    ) {
                        expression
                    } else {
                        let (next, attribute) =
                            self.append(input, expression, "sort_key".into())?;
                        input = next;
                        self.reference(&attribute)?
                    };
                    resolved.push(SortExpression {
                        expression,
                        ascending,
                        nulls_first: match sort.null_ordering {
                            spec::NullOrdering::NullsFirst => true,
                            spec::NullOrdering::NullsLast => false,
                            spec::NullOrdering::Unspecified => ascending,
                        },
                    });
                }
                let schema = input.schema.clone();
                let result = LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Sort {
                        input: Box::new(input),
                        order: resolved,
                        global: is_global,
                        required: false,
                    }),
                    schema,
                };
                self.restore_output(result, output)?
            }
            spec::QueryNode::Limit { input, skip, limit } => {
                let input = self.query(*input).await?;
                let schema = input.schema.resolved("limit input")?.clone();
                let skip = self.limit_expression(skip).await?;
                let fetch = self.limit_expression(limit).await?;
                LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Limit {
                        input: Box::new(input),
                        skip,
                        fetch,
                    }),
                    schema: Deferred::Resolved(schema),
                }
            }
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    #[async_recursion]
    async fn query_scope(&mut self, node: spec::QueryNode) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => {
                let input = self.query(*input).await?;
                let mut input = self.visible_output(input)?;
                let mut qualifier: Vec<String> = qualifier.into_iter().map(String::from).collect();
                qualifier.push(alias.into());
                let mut schema = input.schema.into_resolved("alias input")?;
                for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                    attribute.qualifier.clone_from(&qualifier);
                    attribute.qualified_position = position;
                }
                input.schema = Deferred::Resolved(schema);
                LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Scope {
                        input: Box::new(input),
                    }),
                }
            }
            spec::QueryNode::TableAlias {
                input,
                name,
                columns,
            } => {
                let input = self.query(*input).await?;
                let input = self.visible_output(input)?;
                let mut input = if columns.is_empty() {
                    input
                } else {
                    self.rename(input, columns)?
                };
                let mut schema = input.schema.into_resolved("table alias input")?;
                for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                    attribute.qualifier = vec![name.as_ref().into()];
                    attribute.qualified_position = position;
                }
                input.schema = Deferred::Resolved(schema);
                LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Scope {
                        input: Box::new(input),
                    }),
                }
            }
            spec::QueryNode::NamedWindows { input, windows } => {
                let previous = self.windows.clone();
                let mut names = std::collections::HashSet::new();
                for (name, window) in windows {
                    let name = name.as_ref().to_ascii_lowercase();
                    if !names.insert(name.clone()) {
                        return Err(PlanError::analysis(format!(
                            "Name {name} is used more than once in WINDOW clause"
                        )));
                    }
                    self.windows.insert(name, window);
                }
                let result = self.query(*input).await;
                self.windows = previous;
                result?
            }
            spec::QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => {
                if recursive
                    && ctes
                        .iter()
                        .any(|(_, plan)| matches!(plan.node, spec::QueryNode::SetOperation { .. }))
                {
                    return Err(PlanError::unsupported("recursive CTE resolution"));
                }
                let previous = self.ctes.clone();
                for (name, plan) in ctes {
                    let key = self.name_key(name.as_ref());
                    let plan = self.query(plan).await?;
                    let definition = super::CteDefinition {
                        plan,
                        parameter: false,
                    };
                    self.ctes.insert(key, Arc::new(definition));
                }
                let result = self.query(*input).await;
                let mut input = result?;
                let mut schema = input.schema.into_resolved("CTE output")?;
                for attribute in &mut schema.attributes {
                    for definition in self.ctes.values().filter(|d| !d.parameter) {
                        for source in &definition
                            .plan
                            .schema
                            .resolved("CTE definition")?
                            .attributes
                        {
                            if source.origin == attribute.origin {
                                for id in &source.plan_ids {
                                    if !attribute.plan_ids.contains(id) {
                                        attribute.plan_ids.push(*id);
                                    }
                                }
                            }
                        }
                    }
                }
                self.ctes = previous;
                input.schema = Deferred::Resolved(schema);
                LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Scope {
                        input: Box::new(input),
                    }),
                }
            }
            spec::QueryNode::WithRelations { root, references } => {
                self.with_relations(*root, references).await?
            }
            spec::QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                // Bind against the enclosing parameter scope and an empty row scope.
                // A parameter is a value, never a delayed column reference.
                let mut bindings = Vec::new();
                for (name, argument) in positional_arguments
                    .into_iter()
                    .enumerate()
                    .map(|(i, argument)| (format!("${}", i + 1), argument))
                    .chain(named_arguments)
                {
                    let expression = self.expression(argument, &PlanSchema::default()).await?;
                    let value =
                        crate::analysis::lowering::evaluate_constant(&expression.expression)?;
                    bindings.push((name, value));
                }
                let previous = self.parameters.clone();
                self.parameters.extend(bindings);
                let result = self.query(*input).await;
                self.parameters = previous;
                result?
            }
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    #[async_recursion]
    async fn query_columns(&mut self, node: spec::QueryNode) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::ToDf {
                input,
                column_names,
            } => {
                let input = self.query(*input).await?;
                self.rename(input, column_names)?
            }
            spec::QueryNode::WithColumns { input, aliases } => {
                let input = self.query(*input).await?;
                self.with_columns(input, aliases).await?
            }
            spec::QueryNode::Drop {
                input,
                columns,
                column_names,
            } => {
                let input = self.query(*input).await?;
                self.drop_columns(input, columns, column_names)?
            }
            spec::QueryNode::ToSchema { input, schema } => {
                let input = self.query(*input).await?;
                self.reconcile_schema(input, schema)?
            }
            spec::QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                let input = self.query(*input).await?;
                let schema = input.schema.resolved("rename input")?;
                let mut values = vec![];
                for attribute in &schema.attributes {
                    let rename = rename_columns_map.iter().find(|(from, _)| {
                        self.name_key(from.as_ref()) == self.name_key(&attribute.name)
                    });
                    let mut expression = self.reference(attribute)?;
                    let name = if let Some((_, to)) = rename {
                        expression = LogicalExpression {
                            id: self.context.ids.expression(),
                            result_type: expression.result_type.clone(),
                            node: Deferred::Resolved(ExpressionNode::WithMetadata(Box::new(
                                expression,
                            ))),
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
            } => self.fill_na(*input, columns, values).await?,
            spec::QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => self.drop_na(*input, columns, min_non_nulls).await?,
            spec::QueryNode::Replace {
                input,
                columns,
                replacements,
            } => self.replace(*input, columns, replacements).await?,
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    #[async_recursion]
    async fn query_relations(&mut self, node: spec::QueryNode) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => {
                let input = match input {
                    Some(input) => self.query(*input).await?,
                    None => Self::empty(true),
                };
                let original = input
                    .schema
                    .resolved("lateral view input")?
                    .attributes
                    .clone();
                let function = if outer {
                    spec::ObjectName::bare(
                        match Vec::<String>::from(function)
                            .join(".")
                            .to_ascii_lowercase()
                            .as_str()
                        {
                            "explode" => "explode_outer".to_string(),
                            "posexplode" => "posexplode_outer".to_string(),
                            "inline" => "inline_outer".to_string(),
                            name => name.to_string(),
                        },
                    )
                } else {
                    function
                };
                let mut request = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: function,
                    arguments,
                    named_arguments,
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                });
                if let Some(name) = column_aliases {
                    request = spec::Expr::Alias {
                        expr: Box::new(request),
                        name,
                        metadata: None,
                    };
                }
                let (input, generated) = self.generator(input, request).await?;
                let mut projections = original
                    .iter()
                    .map(|a| {
                        let a = input
                            .schema
                            .resolved("lateral passthrough")?
                            .attribute(a.id)?;
                        Ok(Projection {
                            expression: self.reference(a)?,
                            attribute: a.clone(),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                for named in generated {
                    let mut projection =
                        self.projection(named, input.schema.resolved("lateral output")?)?;
                    if let Some(qualifier) = &table_alias {
                        projection.attribute.qualifier = qualifier.clone().into();
                    }
                    projections.push(projection);
                }
                let schema = PlanSchema {
                    attributes: projections.iter().map(|p| p.attribute.clone()).collect(),
                };
                LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Project {
                        input: Box::new(input),
                        expressions: projections,
                    }),
                    schema: Deferred::Resolved(schema),
                }
            }
            spec::QueryNode::Repartition {
                input,
                num_partitions,
                shuffle,
            } => {
                if num_partitions == 0 {
                    return Err(PlanError::analysis("partition count must be positive"));
                }
                let input = self.query(*input).await?;
                let schema = input.schema.clone();
                LogicalPlan {
                    node: Deferred::Resolved(PlanNode::Repartition {
                        input: Box::new(input),
                        partitioning: if shuffle {
                            Partitioning::RoundRobin(num_partitions)
                        } else {
                            Partitioning::Coalesce(num_partitions)
                        },
                    }),
                    schema,
                }
            }
            spec::QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                let mut input = self.query(*input).await?;
                let schema = input.schema.resolved("repartition input")?.clone();
                let mut expressions = vec![];
                for expression in partition_expressions {
                    let (next, value) = self.recover_expression(input, expression, false).await?;
                    input = next;
                    expressions.push(value.expression);
                }
                let partitioning = if let Some(partitions) = num_partitions {
                    if partitions == 0 {
                        return Err(PlanError::analysis("partition count must be positive"));
                    }
                    Partitioning::Hash(expressions, partitions)
                } else {
                    Partitioning::Distribute(expressions)
                };
                let partitioned = LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Repartition {
                        input: Box::new(input),
                        partitioning,
                    }),
                };
                self.restore_output(partitioned, schema)?
            }
            spec::QueryNode::Deduplicate(dedup) => self.deduplicate(dedup).await?,
            spec::QueryNode::Hint {
                input,
                name,
                parameters,
            } => self.hint(*input, name, parameters).await?,
            spec::QueryNode::Sample(sample) => self.sample(sample).await?,
            spec::QueryNode::TableSample { input, sample } => {
                let input = self.query(*input).await?;
                self.table_sample(input, sample).await?
            }
            spec::QueryNode::SetOperation(set) => self.set_operation(set).await?,
            spec::QueryNode::LateralJoin {
                left,
                right,
                join_condition,
                join_type,
            } => {
                self.join(
                    spec::Join {
                        left,
                        right,
                        join_type,
                        join_criteria: join_condition.map(spec::JoinCriteria::On),
                        join_data_type: None,
                    },
                    true,
                )
                .await?
            }
            spec::QueryNode::Join(join) => self.join(join, false).await?,
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    #[async_recursion]
    async fn query_other(&mut self, node: spec::QueryNode) -> PlanResult<LogicalPlan> {
        Ok(match node {
            spec::QueryNode::CommonInlineUserDefinedTableFunction(function) => {
                self.inline_python_table(function).await?
            }
            spec::QueryNode::GroupMap(map) => self.group_map(map).await?,
            spec::QueryNode::CoGroupMap(map) => self.co_group_map(map).await?,
            spec::QueryNode::MapPartitions {
                input,
                function,
                is_barrier: _,
            } => self.map_partitions(*input, function).await?,
            spec::QueryNode::Pivot(pivot) => self.pivot(pivot).await?,
            spec::QueryNode::Unpivot(unpivot) => self.unpivot(unpivot).await?,
            spec::QueryNode::Tail { input, limit } => self.tail(*input, limit).await?,
            spec::QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => self.crosstab(*input, left_column, right_column).await?,
            spec::QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => {
                self.pair_statistic(*input, left_column, right_column, "covar_samp", "cov")
                    .await?
            }
            spec::QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => {
                if !method.eq_ignore_ascii_case("pearson") {
                    return Err(PlanError::unsupported(format!(
                        "unsupported correlation method: {method}"
                    )));
                }
                self.pair_statistic(*input, left_column, right_column, "corr", "corr")
                    .await?
            }
            spec::QueryNode::StatSummary { input, statistics } => {
                self.summary(*input, vec![], statistics).await?
            }
            spec::QueryNode::StatDescribe { input, columns } => {
                self.summary(
                    *input,
                    columns,
                    ["count", "mean", "stddev", "min", "max"]
                        .map(String::from)
                        .to_vec(),
                )
                .await?
            }
            spec::QueryNode::StatSampleBy {
                input,
                column,
                fractions,
                seed,
            } => self.stat_sample_by(*input, column, fractions, seed).await?,
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }

    pub(super) fn empty(produce_one_row: bool) -> LogicalPlan {
        LogicalPlan {
            node: Deferred::Resolved(PlanNode::Empty { produce_one_row }),
            schema: Deferred::Resolved(PlanSchema::default()),
        }
    }

    pub(super) fn scan(
        &mut self,
        name: String,
        source: Arc<dyn TableSource>,
        qualifier: Vec<String>,
    ) -> PlanResult<LogicalPlan> {
        let attributes = source
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| {
                let mut attribute =
                    self.attribute(field.name().clone(), ExpressionType::from_field(field));
                attribute.qualifier.clone_from(&qualifier);
                attribute.qualified_position = position;
                attribute
            })
            .collect();
        Ok(LogicalPlan {
            node: Deferred::Resolved(PlanNode::Scan {
                name,
                source,
                projection: None,
            }),
            schema: Deferred::Resolved(PlanSchema { attributes }),
        })
    }

    pub(super) fn rename(
        &mut self,
        input: LogicalPlan,
        names: Vec<spec::Identifier>,
    ) -> PlanResult<LogicalPlan> {
        let schema = input.schema.resolved("rename input")?;
        if names.len() != schema.attributes.len() {
            return Err(PlanError::analysis(
                "column alias count does not match input width",
            ));
        }
        let expressions = schema
            .attributes
            .iter()
            .zip(names)
            .map(|(attribute, name)| {
                Ok(NamedExpression {
                    name: name.into(),
                    expression: LogicalExpression {
                        id: self.context.ids.expression(),
                        result_type: attribute.result_type.clone(),
                        node: Deferred::Resolved(ExpressionNode::WithMetadata(Box::new(
                            self.reference(attribute)?,
                        ))),
                    },
                })
            })
            .collect::<PlanResult<_>>()?;
        self.project(input, expressions)
    }

    #[async_recursion::async_recursion]
    async fn limit_expression(
        &mut self,
        expression: Option<spec::Expr>,
    ) -> PlanResult<Option<Box<LogicalExpression>>> {
        let Some(expression) = expression else {
            return Ok(None);
        };
        let expression = self
            .expression(expression, &PlanSchema::default())
            .await?
            .expression;
        if let ExpressionNode::Literal(value) = expression.node.resolved("limit expression")? {
            let value = value.cast_to(&DataType::Int64)?;
            if !matches!(value, ScalarValue::Int64(Some(value)) if value >= 0) {
                return Err(PlanError::analysis("limit must be a nonnegative integer"));
            }
        }
        Ok(Some(Box::new(typing::coerce(
            expression,
            &DataType::Int64,
            &mut self.context.ids,
        )?)))
    }

    #[async_recursion::async_recursion]
    async fn values(&mut self, rows: Vec<Vec<spec::Expr>>) -> PlanResult<LogicalPlan> {
        let width = rows
            .first()
            .map(Vec::len)
            .ok_or_else(|| PlanError::analysis("VALUES requires at least one row"))?;
        if rows.iter().any(|row| row.len() != width) {
            return Err(PlanError::analysis("VALUES rows have different widths"));
        }
        let mut resolved = vec![];
        for row in rows {
            let mut values = vec![];
            for value in row {
                values.push(
                    self.expression(value, &PlanSchema::default())
                        .await?
                        .expression,
                );
            }
            resolved.push(values);
        }
        self.typed_values(resolved)
    }

    pub(super) fn typed_values(
        &mut self,
        resolved: Vec<Vec<LogicalExpression>>,
    ) -> PlanResult<LogicalPlan> {
        let width = resolved
            .first()
            .map(Vec::len)
            .ok_or_else(|| PlanError::analysis("VALUES requires at least one row"))?;
        let mut attributes = vec![];
        for index in 0..width {
            let types = resolved
                .iter()
                .map(|row| Ok(row[index].result_type.resolved("VALUES element type")?))
                .collect::<PlanResult<Vec<_>>>()?;
            let nullable = types.iter().any(|field| field.nullable);
            let mut common = sail_type_system::coercion::common_type(
                &types
                    .iter()
                    .map(|field| field.data_type.clone())
                    .collect::<Vec<_>>(),
            )?;
            let has_nan = resolved.iter().any(|row| row[index].any(&|node| matches!(node,
                ExpressionNode::Literal(ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) | ScalarValue::Utf8View(Some(value))) if value.eq_ignore_ascii_case("nan"))
                || matches!(node, ExpressionNode::Literal(ScalarValue::Float32(Some(value))) if value.is_nan())
                || matches!(node, ExpressionNode::Literal(ScalarValue::Float64(Some(value))) if value.is_nan())).unwrap_or(false));
            if has_nan
                && types
                    .iter()
                    .all(|t| t.data_type.is_numeric() || t.data_type.is_null())
            {
                common = if types.iter().any(|t| {
                    matches!(
                        t.data_type,
                        DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..)
                    )
                }) {
                    DataType::Float64
                } else {
                    DataType::Float32
                };
            }
            let mut metadata = types
                .iter()
                .find(|t| !t.data_type.is_null())
                .map(|t| t.metadata.clone())
                .unwrap_or_default();
            let mut interval: Option<spec::SparkIntervalMetadata> = None;
            for field in &types {
                if let Some(value) = field.metadata.get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
                    let value = spec::SparkIntervalMetadata::from_json(value)?;
                    interval = Some(match interval {
                        None => value,
                        Some(current) => current.wider(value).ok_or_else(|| {
                            PlanError::invalid("incompatible interval types in VALUES column")
                        })?,
                    });
                }
            }
            if let Some(interval) = interval {
                metadata.insert(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    interval.to_json()?,
                );
            }
            attributes.push(self.attribute(
                format!("col{}", index + 1),
                ExpressionType {
                    data_type: common,
                    nullable,
                    metadata,
                },
            ));
        }
        let rows = resolved
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .zip(&attributes)
                    .map(|(value, attribute)| {
                        let target = attribute.result_type.resolved("VALUES target type")?;
                        let value =
                            typing::coerce(value, &target.data_type, &mut self.context.ids)?;
                        let mut result_type =
                            value.result_type.resolved("VALUES coerced value")?.clone();
                        if result_type.metadata == target.metadata {
                            return Ok(value);
                        }
                        result_type.metadata.clone_from(&target.metadata);
                        Ok(LogicalExpression {
                            id: self.context.ids.expression(),
                            node: Deferred::Resolved(ExpressionNode::WithMetadata(Box::new(value))),
                            result_type: Deferred::Resolved(result_type),
                        })
                    })
                    .collect::<PlanResult<_>>()
            })
            .collect::<PlanResult<_>>()?;
        Ok(LogicalPlan {
            node: Deferred::Resolved(PlanNode::Values { rows }),
            schema: Deferred::Resolved(PlanSchema { attributes }),
        })
    }

    #[async_recursion::async_recursion]
    async fn join(&mut self, join: spec::Join, lateral: bool) -> PlanResult<LogicalPlan> {
        use datafusion_expr::JoinType as J;
        let explicit_cross = matches!(join.join_type, spec::JoinType::Cross);
        if explicit_cross && join.join_criteria.is_some() {
            return Err(PlanError::invalid("cross join with join criteria"));
        }
        if join.join_criteria.is_none() && join.join_data_type.is_some() {
            return Err(PlanError::invalid("cross join with join data type"));
        }
        if !lateral
            && !explicit_cross
            && join.join_criteria.is_none()
            && !self.context.config.cross_join_enabled
        {
            return Err(PlanError::analysis(IMPLICIT_CARTESIAN_PRODUCT_MSG));
        }
        let kind = if !lateral && join.join_criteria.is_none() {
            J::Inner
        } else {
            match join.join_type {
                spec::JoinType::Inner | spec::JoinType::Cross => J::Inner,
                spec::JoinType::LeftOuter => J::Left,
                spec::JoinType::RightOuter => J::Right,
                spec::JoinType::FullOuter => J::Full,
                spec::JoinType::LeftSemi => J::LeftSemi,
                spec::JoinType::RightSemi => J::RightSemi,
                spec::JoinType::LeftAnti => J::LeftAnti,
                spec::JoinType::RightAnti => J::RightAnti,
            }
        };
        let left = self.query(*join.left).await?;
        if lateral {
            self.outer
                .push(left.schema.resolved("lateral left input")?.clone());
        }
        let right = self.query(*join.right).await;
        if lateral {
            self.outer.pop();
        }
        let right = right?;
        let left_schema = left.schema.resolved("join left")?;
        let right_schema = right.schema.resolved("join right")?;
        let merged = PlanSchema {
            attributes: left_schema
                .attributes
                .iter()
                .chain(&right_schema.attributes)
                .cloned()
                .collect(),
        };
        let using = match &join.join_criteria {
            Some(spec::JoinCriteria::Using(names)) => Some(
                names
                    .iter()
                    .map(|name| name.as_ref().to_string())
                    .collect::<Vec<_>>(),
            ),
            Some(spec::JoinCriteria::Natural) => Some(
                left_schema
                    .attributes
                    .iter()
                    .filter(|left| {
                        !left.hidden
                            && right_schema.attributes.iter().any(|right| {
                                !right.hidden
                                    && self.name_key(&left.name) == self.name_key(&right.name)
                            })
                    })
                    .map(|attribute| attribute.name.clone())
                    .collect(),
            ),
            _ => None,
        };
        let mut keys = vec![];
        let condition = if let Some(names) = using {
            let mut condition = None;
            for name in names {
                let left_key = crate::analysis::scope::ResolutionScope::new(left_schema, [])
                    .with_case_sensitive(self.context.config.case_sensitive)
                    .resolve(std::slice::from_ref(&name), None)?;
                let right_key = crate::analysis::scope::ResolutionScope::new(right_schema, [])
                    .with_case_sensitive(self.context.config.case_sensitive)
                    .resolve(std::slice::from_ref(&name), None)?;
                let a = self.reference(left_schema.attribute(left_key.id)?)?;
                let b = self.reference(right_schema.attribute(right_key.id)?)?;
                let comparison = typing::resolve_binary(
                    self.context.ids.expression(),
                    a,
                    datafusion_expr::Operator::Eq,
                    b,
                    &mut self.context.ids,
                )?;
                let common = if let ExpressionNode::Binary { left, .. } =
                    comparison.node.resolved("join key comparison")?
                {
                    left.result_type
                        .resolved("join key type")?
                        .data_type
                        .clone()
                } else {
                    return Err(PlanError::internal("join key comparison must be binary"));
                };
                condition = Some(match condition {
                    None => comparison,
                    Some(previous) => typing::resolve_binary(
                        self.context.ids.expression(),
                        previous,
                        datafusion_expr::Operator::And,
                        comparison,
                        &mut self.context.ids,
                    )?,
                });
                keys.push((name, left_key.id, right_key.id, common));
            }
            condition.map(Box::new)
        } else {
            match join.join_criteria {
                None => None,
                Some(spec::JoinCriteria::On(condition)) => {
                    let condition = self.expression(condition, &merged).await?.expression;
                    self.validate_join_condition(&condition, kind)?;
                    Some(Box::new(typing::coerce(
                        condition,
                        &DataType::Boolean,
                        &mut self.context.ids,
                    )?))
                }
                _ => return Err(PlanError::internal("join criteria dispatched twice")),
            }
        };
        let mut attributes = vec![];
        if !matches!(kind, J::RightSemi | J::RightAnti) {
            for attribute in &left_schema.attributes {
                attributes.push(Self::join_attribute(
                    attribute,
                    matches!(kind, J::Right | J::Full),
                )?);
            }
        }
        if !matches!(kind, J::LeftSemi | J::LeftAnti) {
            for attribute in &right_schema.attributes {
                attributes.push(Self::join_attribute(
                    attribute,
                    matches!(kind, J::Left | J::Full),
                )?);
            }
        }
        let joined = LogicalPlan {
            node: Deferred::Resolved(PlanNode::Join {
                lateral,
                left: Box::new(left),
                right: Box::new(right),
                kind,
                condition,
            }),
            schema: Deferred::Resolved(PlanSchema { attributes }),
        };
        if keys.is_empty() {
            return Ok(joined);
        }
        self.project_using_join(joined, keys, kind)
    }

    fn project_using_join(
        &mut self,
        input: LogicalPlan,
        keys: Vec<(String, AttributeId, AttributeId, DataType)>,
        kind: datafusion_expr::JoinType,
    ) -> PlanResult<LogicalPlan> {
        use datafusion_expr::JoinType as J;
        let schema = input.schema.resolved("USING join input")?;
        let mut expressions = vec![];
        let key_ids = keys
            .iter()
            .flat_map(|(_, a, b, _)| [*a, *b])
            .collect::<std::collections::HashSet<_>>();
        for (name, left, right, _) in keys {
            let expression = if kind == J::Full {
                let left = self.reference(schema.attribute(left)?)?;
                let right = self.reference(schema.attribute(right)?)?;
                let arguments = self.temporal_branches(vec![left, right])?;
                typing::resolve_scalar_call(
                    self.context.ids.expression(),
                    self.scalar_kernel("coalesce")?,
                    arguments,
                    &mut self.context.ids,
                )?
            } else {
                let source = if matches!(kind, J::Right | J::RightSemi | J::RightAnti) {
                    right
                } else {
                    left
                };
                self.reference(schema.attribute(source)?)?
            };
            let attribute = self.attribute(
                name,
                expression
                    .result_type
                    .resolved("USING output type")?
                    .clone(),
            );
            expressions.push(Projection {
                expression,
                attribute,
            });
        }
        for source in &schema.attributes {
            if key_ids.contains(&source.id) {
                continue;
            }
            let expression = self.reference(source)?;
            let mut projection = self.projection(
                NamedExpression {
                    name: source.name.clone(),
                    expression,
                },
                schema,
            )?;
            projection.attribute.hidden = source.hidden;
            expressions.push(projection);
        }
        // Qualified references retain each side's original key. The merged key
        // is the only key exposed to unqualified lookup and wildcard expansion.
        for source in &schema.attributes {
            if !key_ids.contains(&source.id) {
                continue;
            }
            let expression = self.reference(source)?;
            let mut projection = self.projection(
                NamedExpression {
                    name: source.name.clone(),
                    expression,
                },
                schema,
            )?;
            projection.attribute.hidden = true;
            expressions.push(projection);
        }
        let schema = PlanSchema {
            attributes: expressions
                .iter()
                .map(|projection| projection.attribute.clone())
                .collect(),
        };
        Ok(LogicalPlan {
            node: Deferred::Resolved(PlanNode::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        })
    }

    fn join_attribute(attribute: &Attribute, nullable: bool) -> PlanResult<Attribute> {
        let mut attribute = attribute.clone();
        if nullable {
            let mut field = attribute.result_type.into_resolved("join output type")?;
            field.nullable = true;
            attribute.result_type = Deferred::Resolved(field);
        }
        Ok(attribute)
    }
}

const IMPLICIT_CARTESIAN_PRODUCT_MSG: &str = "Detected implicit cartesian product for INNER join between logical plans. \
    Join condition is missing or trivial. \
    Either: use the CROSS JOIN syntax to allow cartesian products between \
    these relations, or: enable implicit cartesian products by setting the \
    configuration variable spark.sql.crossJoin.enabled=true;";

impl Resolver<'_, '_> {
    fn validate_join_condition(
        &self,
        expression: &LogicalExpression,
        kind: datafusion_expr::JoinType,
    ) -> PlanResult<()> {
        fn conjuncts(
            expression: &LogicalExpression,
            python: &mut bool,
            other: &mut bool,
        ) -> PlanResult<()> {
            match expression.node.resolved("join conjunct")? {
                ExpressionNode::WithMetadata(inner) => conjuncts(inner, python, other)?,
                ExpressionNode::Binary {
                    left,
                    operator: datafusion_expr::Operator::And,
                    right,
                } => {
                    conjuncts(left, python, other)?;
                    conjuncts(right, python, other)?;
                }
                ExpressionNode::ScalarFunction { function, .. }
                    if function
                        .inner()
                        .is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>() =>
                {
                    *python = true
                }
                _ => *other = true,
            }
            Ok(())
        }
        let (mut python, mut other) = (false, false);
        conjuncts(expression, &mut python, &mut other)?;
        if python {
            use datafusion_expr::JoinType as J;
            if kind == J::Inner {
                if !other && !self.context.config.cross_join_enabled {
                    return Err(PlanError::analysis(IMPLICIT_CARTESIAN_PRODUCT_MSG));
                }
            } else {
                let name = match kind {
                    J::Left => "LEFT OUTER",
                    J::Right => "RIGHT OUTER",
                    J::Full => "FULL OUTER",
                    J::LeftSemi => "LEFT SEMI",
                    J::RightSemi => "RIGHT SEMI",
                    J::LeftAnti => "LEFT ANTI",
                    J::RightAnti => "RIGHT ANTI",
                    J::LeftMark => "LEFT MARK",
                    J::RightMark => "RIGHT MARK",
                    J::Inner => "INNER",
                };
                return Err(PlanError::analysis(format!(
                    "Python UDF in the ON clause of a {name} JOIN."
                )));
            }
        }
        Ok(())
    }
}
