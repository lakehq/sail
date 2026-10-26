use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    /// Resolve against the nearest scope first, then carry missing attributes
    /// through operators that preserve their meaning. A projection can change
    /// names without changing which input expression its output represents.
    #[async_recursion::async_recursion]
    async fn recover_aggregate_sort(
        &mut self,
        mut input: Query,
        request: spec::Expr,
        sorting: bool,
    ) -> PlanResult<(Query, NamedExpr)> {
        let result = self
            .resolve_named_expression(request.clone(), input.schema.resolved("expression input")?)
            .await;
        match &result {
            Ok(value)
                if !value
                    .expression
                    .any(&|node| matches!(node, ExprKind::AggregateFunction { .. }))? =>
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
        let (node, value) = match input.kind.into_resolved("recovery input")? {
            QueryKind::Project {
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
                        if let Deferred::Resolved(ExprKind::Attribute(id)) = &expression.kind
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
                    QueryKind::Project {
                        input: Box::new(child),
                        expressions,
                    },
                    value,
                )
            }
            QueryKind::Aggregate {
                input: child,
                grouping,
                grouping_sets,
                grouping_id,
                mut aggregates,
            } => {
                let mut value = self
                    .resolve_named_expression(
                        request,
                        child.schema.resolved("aggregate recovery input")?,
                    )
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
                    QueryKind::Aggregate {
                        input: child,
                        grouping,
                        grouping_sets,
                        grouping_id,
                        aggregates,
                    },
                    value,
                )
            }
            QueryKind::Filter {
                input: child,
                condition,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    QueryKind::Filter {
                        input: Box::new(child),
                        condition,
                    },
                    value,
                )
            }
            QueryKind::Sort {
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
                    QueryKind::Sort {
                        input: Box::new(child),
                        order,
                        global,
                        required,
                    },
                    value,
                )
            }
            QueryKind::Window {
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
                    QueryKind::Window {
                        input: Box::new(child),
                        expressions,
                    },
                    value,
                )
            }
            QueryKind::Repartition {
                input: child,
                partitioning,
            } => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    QueryKind::Repartition {
                        input: Box::new(child),
                        partitioning,
                    },
                    value,
                )
            }
            QueryKind::Limit {
                input: child,
                skip,
                fetch,
            } if !sorting => {
                let (child, value) = self
                    .recover_aggregate_sort(*child, request, sorting)
                    .await?;
                extend_schema(&mut schema, &child)?;
                (
                    QueryKind::Limit {
                        input: Box::new(child),
                        skip,
                        fetch,
                    },
                    value,
                )
            }
            _ => return Err(original_error),
        };
        input.kind = Deferred::Resolved(node);
        input.schema = Deferred::Resolved(schema);
        Ok((input, value))
    }

    fn rebase_projection(
        &mut self,
        expression: Expr,
        projections: &[Projection],
    ) -> PlanResult<Expr> {
        for projection in projections {
            if expression.equivalent(&projection.expression)? {
                return self.reference(&projection.attribute);
            }
        }
        expression.map_children(&mut |child| self.rebase_projection(child, projections))
    }

    pub(in crate::resolver) fn restore_output(
        &mut self,
        input: Query,
        schema: QuerySchema,
    ) -> PlanResult<Query> {
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
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        })
    }
}

fn extend_schema(schema: &mut QuerySchema, child: &Query) -> PlanResult<()> {
    for attribute in &child.schema.resolved("recovered input")?.attributes {
        if !schema.attributes.iter().any(|a| a.id == attribute.id) {
            schema.attributes.push(attribute.clone());
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(in crate::resolver) struct RecoveryScope {
    combined: Vec<AttributeId>,
    schemas: Vec<QuerySchema>,
    local: QuerySchema,
    sorting: bool,
}

fn recovery_child(plan: &Query) -> Option<&Query> {
    match &plan.kind {
        Deferred::Resolved(
            QueryKind::Project { input, .. }
            | QueryKind::Filter { input, .. }
            | QueryKind::Sort { input, .. }
            | QueryKind::Limit { input, .. }
            | QueryKind::Window { input, .. }
            | QueryKind::Repartition { input, .. }
            | QueryKind::RowIdentity { input, .. }
            | QueryKind::Unnest { input, .. },
        ) => Some(input),
        _ => None,
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn discard_extraction_scope(
        &mut self,
        expression: &Expr,
    ) -> PlanResult<()> {
        let mut attributes = vec![];
        expression.clone().transform_up::<PlanError>(&mut |value| {
            match &value.kind {
                Deferred::Resolved(ExprKind::Attribute(id)) => attributes.push((0, *id)),
                Deferred::Resolved(ExprKind::OuterAttribute { depth, attribute }) => {
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

    pub(in crate::resolver) fn recovery_local_schema<'s>(
        &'s self,
        schema: &'s QuerySchema,
    ) -> &'s QuerySchema {
        self.recovery
            .as_ref()
            .filter(|r| r.applies(schema))
            .map_or(schema, |r| &r.local)
    }

    pub(in crate::resolver) fn recovery_attribute(
        &mut self,
        id: ExprId,
        parts: &[String],
        plan_id: Option<i64>,
        schema: &QuerySchema,
    ) -> Option<PlanResult<NamedExpr>> {
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
        let result = self.resolve_attribute(id, parts, plan_id, &QuerySchema::default());
        self.recovery = recovery;
        Some(result)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn recover_expression(
        &mut self,
        input: Query,
        request: spec::Expr,
        sorting: bool,
    ) -> PlanResult<(Query, NamedExpr)> {
        let mut schemas = vec![input.schema.resolved("recovery input")?.clone()];
        let mut child = &input;
        while let Some(next) = recovery_child(child) {
            schemas.push(next.schema.resolved("recovery child")?.clone());
            child = next;
        }
        if sorting && matches!(&child.kind, Deferred::Resolved(QueryKind::Aggregate { .. })) {
            // Aggregate arguments need extraction as well as name binding.
            return self.recover_aggregate_sort(input, request, sorting).await;
        }
        if schemas.len() == 1 {
            return self
                .resolve_named_expression(request, &schemas[0])
                .await
                .map(|value| (input, value));
        }
        let mut combined = QuerySchema::default();
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
            match self
                .resolve_named_expression(request.clone(), &combined)
                .await
            {
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
        use sail_common_plan::rewrite::QueryRewriter;
        required.rewrite_expression(value.expression.clone())?;
        let input = self.carry_attributes(input, &required.attributes)?;
        Ok((input, value))
    }

    fn carry_attributes(
        &mut self,
        mut plan: Query,
        required: &std::collections::HashSet<AttributeId>,
    ) -> PlanResult<Query> {
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
        let node = plan.kind.into_resolved("recovery node")?;
        let node = match node {
            QueryKind::Project {
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
                QueryKind::Project {
                    input: Box::new(input),
                    expressions,
                }
            }
            QueryKind::Window { input, expressions } => {
                let input = self.carry_attributes(*input, &missing)?;
                schema = input.schema.resolved("window recovery")?.clone();
                schema
                    .attributes
                    .extend(expressions.iter().map(|p| p.attribute.clone()));
                QueryKind::Window {
                    input: Box::new(input),
                    expressions,
                }
            }
            QueryKind::Filter { input, condition } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                QueryKind::Filter {
                    input: Box::new(input),
                    condition,
                }
            }
            QueryKind::Sort {
                input,
                order,
                global,
                required,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                QueryKind::Sort {
                    input: Box::new(input),
                    order,
                    global,
                    required,
                }
            }
            QueryKind::Limit { input, skip, fetch } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                QueryKind::Limit {
                    input: Box::new(input),
                    skip,
                    fetch,
                }
            }
            QueryKind::Repartition {
                input,
                partitioning,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                extend_schema(&mut schema, &input)?;
                QueryKind::Repartition {
                    input: Box::new(input),
                    partitioning,
                }
            }
            QueryKind::RowIdentity {
                input,
                attribute,
                kind,
            } => {
                let input = self.carry_attributes(*input, &missing)?;
                schema = input.schema.resolved("identity recovery")?.clone();
                schema.attributes.push(attribute.clone());
                QueryKind::RowIdentity {
                    input: Box::new(input),
                    attribute,
                    kind,
                }
            }
            QueryKind::Unnest {
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
                QueryKind::Unnest {
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
        plan.kind = Deferred::Resolved(node);
        plan.schema = Deferred::Resolved(schema);
        Ok(plan)
    }
}

impl RecoveryScope {
    fn applies(&self, schema: &QuerySchema) -> bool {
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
impl sail_common_plan::rewrite::QueryRewriter for Required {
    type Error = PlanError;
    fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
        if let Deferred::Resolved(
            ExprKind::Attribute(id) | ExprKind::OuterAttribute { attribute: id, .. },
        ) = &expression.kind
            && self.reachable.contains(id)
        {
            self.attributes.insert(*id);
        }
        sail_common_plan::rewrite::walk_expression(self, expression)
    }
}
