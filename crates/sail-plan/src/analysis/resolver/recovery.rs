use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    /// Resolve against the nearest scope first, then carry missing attributes
    /// through operators that preserve their meaning. A projection can change
    /// names without changing which input expression its output represents.
    #[async_recursion::async_recursion]
    async fn recover_aggregate_sort(
        &mut self,
        mut input: LogicalPlan,
        request: spec::Expr,
        sorting: bool,
    ) -> PlanResult<(LogicalPlan, NamedExpression)> {
        let result = self
            .expression(request.clone(), input.schema.resolved("expression input")?)
            .await;
        match &result {
            Ok(value)
                if !value
                    .expression
                    .any(&|node| matches!(node, ExpressionNode::AggregateFunction { .. }))? =>
            {
                return Ok((input, result?));
            }
            Err(PlanError::AnalysisError(message))
                if message.starts_with("unresolved attribute:") => {}
            Err(_) => {
                return Err(result
                    .err()
                    .ok_or_else(|| PlanError::internal("missing resolution error"))?);
            }
            _ => {}
        }
        let original_error = result.err().unwrap_or_else(|| {
            PlanError::analysis("aggregate expression is not available in this scope")
        });
        let mut schema = input.schema.into_resolved("recovery schema")?;
        let (node, value) = match input.node.into_resolved("recovery input")? {
            PlanNode::Project {
                input: child,
                mut expressions,
            } => {
                let (child, mut value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                value.expression = self.rebase_projection(value.expression, &expressions)?;
                let mut required = vec![];
                value
                    .expression
                    .clone()
                    .transform_up::<PlanError>(&mut |expression| {
                        if let Deferred::Resolved(ExpressionNode::Attribute(id)) = &expression.node
                            && !schema.attributes.iter().any(|a| a.id == *id)
                            && !required.contains(id)
                        {
                            required.push(*id);
                        }
                        Ok(expression)
                    })?;
                for id in required {
                    let attribute = child
                        .schema
                        .resolved("recovered projection input")?
                        .attribute(id)?
                        .clone();
                    expressions.push(Projection {
                        expression: self.reference(&attribute)?,
                        attribute: attribute.clone(),
                    });
                    schema.attributes.push(attribute);
                }
                (
                    PlanNode::Project {
                        input: Box::new(child),
                        expressions,
                    },
                    value,
                )
            }
            PlanNode::Aggregate {
                input: child,
                grouping,
                grouping_sets,
                grouping_id,
                mut aggregates,
            } => {
                let mut value = self
                    .expression(request, child.schema.resolved("aggregate recovery input")?)
                    .await?;
                value.expression = self.aggregate_expression(
                    value.expression,
                    &grouping,
                    &mut aggregates,
                    grouping_id.as_ref(),
                )?;
                schema.attributes = grouping
                    .iter()
                    .map(|p| p.attribute.clone())
                    .chain(grouping_id.iter().cloned())
                    .chain(aggregates.iter().map(|p| p.attribute.clone()))
                    .collect();
                (
                    PlanNode::Aggregate {
                        input: child,
                        grouping,
                        grouping_sets,
                        grouping_id,
                        aggregates,
                    },
                    value,
                )
            }
            PlanNode::Filter {
                input: child,
                condition,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    PlanNode::Filter {
                        input: Box::new(child),
                        condition,
                    },
                    value,
                )
            }
            PlanNode::Sort {
                input: child,
                order,
                global,
                required,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    PlanNode::Sort {
                        input: Box::new(child),
                        order,
                        global,
                        required,
                    },
                    value,
                )
            }
            PlanNode::Window {
                input: child,
                expressions,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                // Window outputs follow all input attributes at the lowering boundary.
                schema = child.schema.resolved("recovered window input")?.clone();
                schema
                    .attributes
                    .extend(expressions.iter().map(|p| p.attribute.clone()));
                (
                    PlanNode::Window {
                        input: Box::new(child),
                        expressions,
                    },
                    value,
                )
            }
            PlanNode::Repartition {
                input: child,
                partitioning,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    PlanNode::Repartition {
                        input: Box::new(child),
                        partitioning,
                    },
                    value,
                )
            }
            PlanNode::Limit {
                input: child,
                skip,
                fetch,
            } if !sorting => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    PlanNode::Limit {
                        input: Box::new(child),
                        skip,
                        fetch,
                    },
                    value,
                )
            }
            _ => return Err(original_error),
        };
        input.node = Deferred::Resolved(node);
        input.schema = Deferred::Resolved(schema);
        Ok((input, value))
    }

    fn rebase_projection(
        &mut self,
        expression: LogicalExpression,
        projections: &[Projection],
    ) -> PlanResult<LogicalExpression> {
        for projection in projections {
            if expression.equivalent(&projection.expression)? {
                return self.reference(&projection.attribute);
            }
        }
        expression.map_children(&mut |child| self.rebase_projection(child, projections))
    }

    pub(super) fn restore_output(
        &mut self,
        input: LogicalPlan,
        schema: PlanSchema,
    ) -> PlanResult<LogicalPlan> {
        if input
            .schema
            .resolved("recovered output")?
            .attributes
            .iter()
            .map(|a| a.id)
            .eq(schema.attributes.iter().map(|a| a.id))
        {
            return Ok(input);
        }
        let expressions = schema
            .attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: self.reference(attribute)?,
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(LogicalPlan {
            node: Deferred::Resolved(PlanNode::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        })
    }
}

fn extend_schema(schema: &mut PlanSchema, child: &LogicalPlan) -> PlanResult<()> {
    for attribute in &child.schema.resolved("recovered input")?.attributes {
        if !schema.attributes.iter().any(|a| a.id == attribute.id) {
            schema.attributes.push(attribute.clone());
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct RecoveryScope {
    combined: Vec<AttributeId>,
    schemas: Vec<PlanSchema>,
    local: PlanSchema,
    sorting: bool,
}

fn recovery_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match &plan.node {
        Deferred::Resolved(
            PlanNode::Project { input, .. }
            | PlanNode::Filter { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Window { input, .. }
            | PlanNode::Repartition { input, .. }
            | PlanNode::RowIdentity { input, .. }
            | PlanNode::Unnest { input, .. },
        ) => Some(input),
        _ => None,
    }
}

impl Resolver<'_, '_> {
    pub(super) fn discard_extraction_scope(
        &mut self,
        expression: &LogicalExpression,
    ) -> PlanResult<()> {
        let mut attributes = vec![];
        expression.clone().transform_up::<PlanError>(&mut |value| {
            match &value.node {
                Deferred::Resolved(ExpressionNode::Attribute(id)) => attributes.push((0, *id)),
                Deferred::Resolved(ExpressionNode::OuterAttribute { depth, attribute }) => {
                    attributes.push((*depth, *attribute))
                }
                _ => {}
            }
            Ok(value)
        })?;
        for (depth, id) in attributes {
            let scope = if depth == 0 {
                self.recovery.as_mut()
            } else {
                self.outer
                    .len()
                    .checked_sub(depth)
                    .and_then(|index| self.outer_recovery.get_mut(&index))
            };
            if let Some(scope) = scope
                && let Some(index) = scope
                    .schemas
                    .iter()
                    .position(|schema| schema.attributes.iter().any(|a| a.id == id))
                && (index > 0 || scope.sorting)
            {
                scope.schemas.remove(index);
            }
        }
        Ok(())
    }

    pub(super) fn recovery_local_schema<'s>(&'s self, schema: &'s PlanSchema) -> &'s PlanSchema {
        self.recovery
            .as_ref()
            .filter(|r| r.applies(schema))
            .map_or(schema, |r| &r.local)
    }

    pub(super) fn recovery_attribute(
        &mut self,
        id: ExpressionId,
        parts: &[String],
        plan_id: Option<i64>,
        schema: &PlanSchema,
    ) -> Option<PlanResult<NamedExpression>> {
        let scope = self.recovery.as_ref().filter(|r| r.applies(schema))?;
        let schemas = scope.schemas.clone();
        for (index, candidate) in schemas.iter().enumerate() {
            let recovery = self.recovery.take();
            let outer = std::mem::take(&mut self.outer);
            let result = self.resolve_attribute(id, parts, plan_id, candidate);
            self.outer = outer;
            self.recovery = recovery;
            match result {
                Ok(value) => return Some(Ok(value)),
                Err(PlanError::AnalysisError(message))
                    if message.starts_with("unresolved attribute:") =>
                {
                    if index == 0
                        && plan_id.is_none()
                        && matches!(parts, [name] if matches!(name.to_ascii_lowercase().as_str(), "current_date" | "current_timestamp" | "current_time" | "current_user" | "user" | "session_user"))
                    {
                        return Some(Err(PlanError::analysis(message)));
                    }
                }
                Err(error) => {
                    if let Some(scope) = &mut self.recovery
                        && (index > 0 || scope.sorting)
                    {
                        scope.schemas.remove(index);
                    }
                    return Some(Err(error));
                }
            }
        }
        let recovery = self.recovery.take();
        let result = self.resolve_attribute(id, parts, plan_id, &PlanSchema::default());
        self.recovery = recovery;
        Some(result)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn recover_expression(
        &mut self,
        input: LogicalPlan,
        request: spec::Expr,
        sorting: bool,
    ) -> PlanResult<(LogicalPlan, NamedExpression)> {
        let mut schemas = vec![input.schema.resolved("recovery input")?.clone()];
        let mut child = &input;
        while let Some(next) = recovery_child(child) {
            schemas.push(next.schema.resolved("recovery child")?.clone());
            child = next;
        }
        if sorting && matches!(&child.node, Deferred::Resolved(PlanNode::Aggregate { .. })) {
            // Aggregate arguments need extraction as well as name binding.
            return self.recover_aggregate_sort(input, request, sorting).await;
        }
        if schemas.len() == 1 {
            return self
                .expression(request, &schemas[0])
                .await
                .map(|value| (input, value));
        }
        let mut combined = PlanSchema::default();
        let mut seen = std::collections::HashSet::new();
        for schema in &schemas {
            for attribute in &schema.attributes {
                if seen.insert(attribute.id) {
                    combined.attributes.push(attribute.clone());
                }
            }
        }
        let previous = self.recovery.replace(RecoveryScope {
            combined: combined.attributes.iter().map(|a| a.id).collect(),
            local: schemas[0].clone(),
            schemas,
            sorting,
        });
        let result = loop {
            let count = self.recovery.as_ref().map_or(0, |r| r.schemas.len());
            match self.expression(request.clone(), &combined).await {
                Ok(value) => break Ok(value),
                Err(error) => {
                    let remaining = self.recovery.as_ref().map_or(0, |r| r.schemas.len());
                    if remaining >= count {
                        break Err(error);
                    }
                }
            }
        };
        self.recovery = previous;
        let value = result?;
        let mut required = Required {
            reachable: seen,
            attributes: Default::default(),
        };
        use sail_query::rewrite::QueryRewriter;
        required.rewrite_expression(value.expression.clone())?;
        let input = self.carry_attributes(input, &required.attributes)?;
        Ok((input, value))
    }

    fn carry_attributes(
        &mut self,
        mut plan: LogicalPlan,
        required: &std::collections::HashSet<AttributeId>,
    ) -> PlanResult<LogicalPlan> {
        let output = plan.schema.resolved("recovery output")?;
        let missing = required
            .iter()
            .filter(|id| !output.attributes.iter().any(|a| a.id == **id))
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if missing.is_empty() {
            return Ok(plan);
        }
        let mut schema = output.clone();
        let node = plan.node.into_resolved("recovery node")?;
        let node = match node {
            PlanNode::Project {
                input,
                mut expressions,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                for attribute in &input.schema.resolved("projection recovery")?.attributes {
                    if missing.contains(&attribute.id) {
                        let mut attribute = attribute.clone();
                        attribute.hidden = true;
                        expressions.push(Projection {
                            expression: self.reference(&attribute)?,
                            attribute: attribute.clone(),
                        });
                        schema.attributes.push(attribute);
                    }
                }
                PlanNode::Project {
                    input: Box::new(input),
                    expressions,
                }
            }
            PlanNode::Window { input, expressions } => {
                let input = self.carry_attributes(*input, &missing)?;
                schema = input.schema.resolved("window recovery")?.clone();
                schema
                    .attributes
                    .extend(expressions.iter().map(|p| p.attribute.clone()));
                PlanNode::Window {
                    input: Box::new(input),
                    expressions,
                }
            }
            PlanNode::Filter { input, condition } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                PlanNode::Filter {
                    input: Box::new(input),
                    condition,
                }
            }
            PlanNode::Sort {
                input,
                order,
                global,
                required,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                PlanNode::Sort {
                    input: Box::new(input),
                    order,
                    global,
                    required,
                }
            }
            PlanNode::Limit { input, skip, fetch } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                PlanNode::Limit {
                    input: Box::new(input),
                    skip,
                    fetch,
                }
            }
            PlanNode::Repartition {
                input,
                partitioning,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                PlanNode::Repartition {
                    input: Box::new(input),
                    partitioning,
                }
            }
            PlanNode::RowIdentity {
                input,
                attribute,
                kind,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                schema = input.schema.resolved("identity recovery")?.clone();
                schema.attributes.push(attribute.clone());
                PlanNode::RowIdentity {
                    input: Box::new(input),
                    attribute,
                    kind,
                }
            }
            PlanNode::Unnest {
                input,
                column,
                outer,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                for attribute in &input.schema.resolved("unnest recovery")?.attributes {
                    if missing.contains(&attribute.id) {
                        schema.attributes.push(attribute.clone());
                    }
                }
                PlanNode::Unnest {
                    input: Box::new(input),
                    column,
                    outer,
                }
            }
            _ => {
                return Err(PlanError::analysis(
                    "cannot resolve attribute across a relation boundary",
                ));
            }
        };
        plan.node = Deferred::Resolved(node);
        plan.schema = Deferred::Resolved(schema);
        Ok(plan)
    }
}

impl RecoveryScope {
    fn applies(&self, schema: &PlanSchema) -> bool {
        self.combined
            .iter()
            .copied()
            .eq(schema.attributes.iter().map(|a| a.id))
    }
}

struct Required {
    reachable: std::collections::HashSet<AttributeId>,
    attributes: std::collections::HashSet<AttributeId>,
}
impl sail_query::rewrite::QueryRewriter for Required {
    type Error = PlanError;
    fn rewrite_expression(
        &mut self,
        expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        if let Deferred::Resolved(
            ExpressionNode::Attribute(id) | ExpressionNode::OuterAttribute { attribute: id, .. },
        ) = &expression.node
            && self.reachable.contains(id)
        {
            self.attributes.insert(*id);
        }
        sail_query::rewrite::walk_expression(self, expression)
    }
}
