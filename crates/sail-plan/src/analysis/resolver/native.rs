//! Complete deferred fields in an already assembled native tree.
use std::collections::VecDeque;

use arrow_schema::DataType;
use datafusion_expr::{JoinType, ValueOrLambda};
use sail_query::rewrite::{QueryRewriter, walk_plan};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

// The synchronous tree walker defines traversal order once. Async resolution
// consumes the immediate children in that order without entering nested scopes.
#[derive(Default)]
struct Children {
    plans: Vec<LogicalPlan>,
    expressions: Vec<LogicalExpression>,
}
impl QueryRewriter for Children {
    type Error = PlanError;
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        self.plans.push(plan.clone());
        Ok(plan)
    }
    fn rewrite_expression(
        &mut self,
        expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        self.expressions.push(expression.clone());
        Ok(expression)
    }
}
struct Replace {
    plans: VecDeque<LogicalPlan>,
    expressions: VecDeque<LogicalExpression>,
}
impl QueryRewriter for Replace {
    type Error = PlanError;
    fn rewrite_plan(&mut self, _: LogicalPlan) -> PlanResult<LogicalPlan> {
        self.plans
            .pop_front()
            .ok_or_else(|| PlanError::internal("missing resolved child plan"))
    }
    fn rewrite_expression(&mut self, _: LogicalExpression) -> PlanResult<LogicalExpression> {
        self.expressions
            .pop_front()
            .ok_or_else(|| PlanError::internal("missing resolved expression"))
    }
}

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn native_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        if let Deferred::Unresolved(request) = plan.node {
            return self.query(*request).await;
        }
        let lateral = matches!(
            &plan.node,
            Deferred::Resolved(PlanNode::Join { lateral: true, .. })
        );
        let mut children = Children::default();
        let plan = walk_plan(&mut children, plan)?;
        let mut plans: Vec<LogicalPlan> = vec![];
        for (index, child) in children.plans.into_iter().enumerate() {
            if lateral && index == 1 {
                self.outer
                    .push(plans[0].schema.resolved("lateral input")?.clone());
            }
            let result = self.native_plan(child).await;
            if lateral && index == 1 {
                self.outer.pop();
            }
            plans.push(result?);
        }
        let mut scope = PlanSchema::default();
        for input in &plans {
            scope
                .attributes
                .extend(input.schema.resolved("native input")?.attributes.clone());
        }
        let mut expressions = VecDeque::new();
        for expression in children.expressions {
            expressions.push_back(self.native_expression(expression, &scope).await?);
        }
        let mut plan = walk_plan(
            &mut Replace {
                plans: plans.into(),
                expressions,
            },
            plan,
        )?;
        self.native_schema(&mut plan)?;
        Ok(plan)
    }

    #[async_recursion::async_recursion]
    async fn native_expression(
        &mut self,
        mut expression: LogicalExpression,
        schema: &PlanSchema,
    ) -> PlanResult<LogicalExpression> {
        let id = expression.id;
        if let Deferred::Unresolved(request) = expression.node {
            let mut result = self.expression(*request, schema).await?.expression;
            result.id = id;
            return Ok(result);
        }
        let mut children = vec![];
        expression = expression.map_children::<PlanError>(&mut |child| {
            children.push(child.clone());
            Ok(child)
        })?;
        let mut resolved = VecDeque::new();
        for child in children {
            resolved.push_back(self.native_expression(child, schema).await?);
        }
        expression = expression.map_children::<PlanError>(&mut |_| {
            resolved
                .pop_front()
                .ok_or_else(|| PlanError::internal("missing expression child"))
        })?;
        let mut node = expression.node.into_resolved("native expression")?;
        match &mut node {
            ExpressionNode::ScalarSubquery(query)
            | ExpressionNode::Exists { query, .. }
            | ExpressionNode::InSubquery { query, .. } => {
                self.outer.push(schema.clone());
                let result = self.native_plan(*query.clone()).await;
                self.outer.pop();
                **query = result?;
            }
            ExpressionNode::HigherOrderFunction {
                function,
                arguments,
            } => {
                let mut complete = false;
                for step in 0..100 {
                    let mut fields = arguments
                        .iter()
                        .map(|arg| {
                            Ok(match arg {
                                FunctionArgument::Value(v) => ValueOrLambda::Value(
                                    v.result_type.resolved("higher-order input")?.to_field(""),
                                ),
                                FunctionArgument::Lambda { body, .. } => {
                                    ValueOrLambda::Lambda(match &body.result_type {
                                        Deferred::Resolved(t) => Some(t.to_field("")),
                                        Deferred::Unresolved(_) => None,
                                    })
                                }
                            })
                        })
                        .collect::<PlanResult<Vec<_>>>()?;
                    let targets =
                        crate::analysis::typing::higher_order::value_fields(function, &fields)?;
                    fields = targets.clone();
                    for (argument, target) in arguments.iter_mut().zip(targets) {
                        if let (FunctionArgument::Value(value), ValueOrLambda::Value(target)) =
                            (argument, target)
                        {
                            **value = self.cast(*value.clone(), target.data_type().clone())?;
                        }
                    }
                    let (done, inferred) = match function.lambda_parameters(step, &fields)? {
                        datafusion_expr::LambdaParametersProgress::Complete(fields) => {
                            (true, fields.into_iter().map(Some).collect::<Vec<_>>())
                        }
                        datafusion_expr::LambdaParametersProgress::Partial(fields) => {
                            (false, fields)
                        }
                    };
                    let mut inferred = inferred.into_iter();
                    for argument in arguments.iter_mut() {
                        if let FunctionArgument::Lambda {
                            parameters, body, ..
                        } = argument
                        {
                            let Some(fields) = inferred.next().flatten() else {
                                continue;
                            };
                            if parameters.attributes.len() > fields.len() {
                                return Err(PlanError::analysis("lambda has too many parameters"));
                            }
                            for (parameter, field) in parameters.attributes.iter_mut().zip(fields) {
                                parameter.result_type =
                                    Deferred::Resolved(ExpressionType::from_field(&field));
                            }
                            self.lambdas.push(parameters.clone());
                            let result = self.native_expression(*body.clone(), schema).await;
                            self.lambdas.pop();
                            **body = result?;
                        }
                    }
                    if done {
                        complete = true;
                        break;
                    }
                }
                if !complete {
                    return Err(PlanError::analysis(
                        "native lambda inference did not converge",
                    ));
                }
            }
            _ => {}
        }
        let result = match &mut node {
            ExpressionNode::Literal(value) => {
                let mut result = ExpressionType::new(value.data_type(), value.is_null());
                if let Deferred::Resolved(previous) = &expression.result_type
                    && previous.data_type == result.data_type
                {
                    result.metadata.clone_from(&previous.metadata);
                }
                result
            }
            ExpressionNode::Attribute(attribute) => schema
                .attribute(*attribute)?
                .result_type
                .resolved("attribute type")?
                .clone(),
            ExpressionNode::OuterAttribute { depth, attribute } => self
                .outer
                .get(
                    self.outer
                        .len()
                        .checked_sub(*depth)
                        .ok_or_else(|| PlanError::analysis("invalid outer scope depth"))?,
                )
                .ok_or_else(|| PlanError::analysis("missing outer scope"))?
                .attribute(*attribute)?
                .result_type
                .resolved("outer attribute")?
                .clone(),
            ExpressionNode::LambdaVariable(attribute) => self
                .lambdas
                .iter()
                .rev()
                .find_map(|scope| scope.attributes.iter().find(|a| a.id == *attribute))
                .ok_or_else(|| PlanError::analysis("lambda variable is out of scope"))?
                .result_type
                .resolved("lambda variable")?
                .clone(),
            ExpressionNode::ScalarFunction {
                function,
                arguments,
            } => {
                return typing::resolve_scalar_call(
                    id,
                    function.clone(),
                    std::mem::take(arguments),
                    &mut self.context.ids,
                );
            }
            ExpressionNode::Binary {
                left,
                operator,
                right,
            } => {
                let mut result = self.binary(*left.clone(), *operator, *right.clone())?;
                result.id = id;
                return Ok(result);
            }
            ExpressionNode::Case {
                operand,
                branches,
                otherwise,
            } => {
                return typing::resolve_case(
                    id,
                    operand.take().map(|v| *v),
                    std::mem::take(branches),
                    otherwise.take().map(|v| *v),
                    &mut self.context.ids,
                );
            }
            ExpressionNode::InList {
                expression,
                values,
                negated,
            } => {
                let mut result =
                    self.in_list(*expression.clone(), std::mem::take(values), *negated)?;
                result.id = id;
                return Ok(result);
            }
            ExpressionNode::AggregateFunction {
                function,
                arguments,
                ..
            } => {
                let result = crate::analysis::typing::aggregate::resolve(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &result.arguments,
                    &mut self.context.ids,
                )?;
                ExpressionType::from_field(&result.result)
            }
            ExpressionNode::WindowFunction {
                function,
                arguments,
                order_by,
                frame,
                ..
            } => {
                let result = crate::analysis::typing::aggregate::resolve_window(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &result.arguments,
                    &mut self.context.ids,
                )?;
                let types = order_by
                    .iter()
                    .map(|s| {
                        Ok(s.expression
                            .result_type
                            .resolved("window ordering")?
                            .data_type
                            .clone())
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                *frame = crate::analysis::typing::window::resolve_frame(frame.clone(), &types)?;
                ExpressionType::from_field(&result.result)
            }
            ExpressionNode::HigherOrderFunction {
                function,
                arguments,
            } => {
                let fields = arguments
                    .iter()
                    .map(|a| {
                        Ok(match a {
                            FunctionArgument::Value(v) => ValueOrLambda::Value(
                                v.result_type.resolved("higher-order value")?.to_field(""),
                            ),
                            FunctionArgument::Lambda { body, .. } => ValueOrLambda::Lambda(
                                body.result_type.resolved("lambda result")?.to_field(""),
                            ),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let literals = arguments
                    .iter()
                    .map(|a| match a {
                        FunctionArgument::Value(v) => match &v.node {
                            Deferred::Resolved(ExpressionNode::Literal(v)) => Some(v),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let result =
                    crate::analysis::typing::higher_order::resolve(function, &fields, &literals)?;
                for (argument, target) in arguments.iter_mut().zip(result.arguments) {
                    if let (FunctionArgument::Value(v), ValueOrLambda::Value(target)) =
                        (argument, target)
                    {
                        **v = self.cast(*v.clone(), target.data_type().clone())?;
                    }
                }
                ExpressionType::from_field(&result.result)
            }
            ExpressionNode::Cast {
                expression,
                target,
                safe,
            } => {
                target.nullable = *safe || expression.result_type.resolved("cast input")?.nullable;
                target.clone()
            }
            ExpressionNode::WithMetadata(child) => {
                let mut result = child.result_type.resolved("metadata value")?.clone();
                if let Deferred::Resolved(existing) = &expression.result_type {
                    result.metadata.clone_from(&existing.metadata);
                }
                result
            }
            ExpressionNode::Negative(child) => {
                child.result_type.resolved("negation input")?.clone()
            }
            ExpressionNode::Not(child) => {
                **child = self.cast(*child.clone(), DataType::Boolean)?;
                child.result_type.resolved("NOT input")?.clone()
            }
            ExpressionNode::IsNull(_)
            | ExpressionNode::IsNotNull(_)
            | ExpressionNode::TruthTest { .. }
            | ExpressionNode::Exists { .. } => ExpressionType::new(DataType::Boolean, false),
            ExpressionNode::Between {
                expression,
                low,
                high,
                ..
            } => {
                let target = crate::analysis::typing::coercion::comparison_type(&[
                    expression
                        .result_type
                        .resolved("BETWEEN value")?
                        .data_type
                        .clone(),
                    low.result_type.resolved("BETWEEN low")?.data_type.clone(),
                    high.result_type.resolved("BETWEEN high")?.data_type.clone(),
                ])?;
                **expression = self.cast(*expression.clone(), target.clone())?;
                **low = self.cast(*low.clone(), target.clone())?;
                **high = self.cast(*high.clone(), target)?;
                ExpressionType::new(
                    DataType::Boolean,
                    expression.result_type.resolved("BETWEEN value")?.nullable
                        || low.result_type.resolved("BETWEEN low")?.nullable
                        || high.result_type.resolved("BETWEEN high")?.nullable,
                )
            }
            ExpressionNode::Like {
                expression,
                pattern,
                ..
            }
            | ExpressionNode::SimilarTo {
                expression,
                pattern,
                ..
            } => {
                **expression = self.cast(*expression.clone(), DataType::Utf8)?;
                **pattern = self.cast(*pattern.clone(), DataType::Utf8)?;
                ExpressionType::new(
                    DataType::Boolean,
                    expression.result_type.resolved("pattern value")?.nullable
                        || pattern.result_type.resolved("pattern")?.nullable,
                )
            }
            ExpressionNode::ScalarSubquery(query) => {
                let [attribute] = query
                    .schema
                    .resolved("scalar subquery")?
                    .attributes
                    .as_slice()
                else {
                    return Err(PlanError::analysis(
                        "scalar subquery must return one column",
                    ));
                };
                let mut field = attribute.result_type.resolved("subquery type")?.clone();
                field.nullable = true;
                field
            }
            ExpressionNode::InSubquery {
                expression, query, ..
            } => {
                let [attribute] = query.schema.resolved("IN subquery")?.attributes.as_slice()
                else {
                    return Err(PlanError::analysis("IN subquery must return one column"));
                };
                let right = self.reference(attribute)?;
                let mut values = self
                    .in_arguments(vec![*expression.clone(), right])?
                    .into_iter();
                let left = values
                    .next()
                    .ok_or_else(|| PlanError::internal("missing IN left"))?;
                let right = values
                    .next()
                    .ok_or_else(|| PlanError::internal("missing IN right"))?;
                let target = crate::analysis::typing::coercion::comparison_type(&[
                    left.result_type.resolved("IN left")?.data_type.clone(),
                    right.result_type.resolved("IN right")?.data_type.clone(),
                ])?;
                **expression = self.cast(left, target.clone())?;
                let right = self.cast(right, target)?;
                let nullable = expression.result_type.resolved("IN left")?.nullable
                    || right.result_type.resolved("IN right")?.nullable;
                let name = attribute.name.clone();
                **query = self.project(
                    *query.clone(),
                    vec![super::NamedExpression {
                        name,
                        expression: right,
                    }],
                )?;
                ExpressionType::new(DataType::Boolean, nullable)
            }
        };
        Ok(LogicalExpression {
            id,
            node: Deferred::Resolved(node),
            result_type: Deferred::Resolved(result),
        })
    }

    fn native_schema(&mut self, plan: &mut LogicalPlan) -> PlanResult<()> {
        let node = match &mut plan.node {
            Deferred::Resolved(node) => node,
            _ => return Err(PlanError::internal("unresolved native node")),
        };
        let projection = |p: &mut Projection| -> PlanResult<Attribute> {
            p.attribute.result_type = p.expression.result_type.clone();
            Ok(p.attribute.clone())
        };
        let mut schema = match node {
            PlanNode::Project { expressions, .. } => PlanSchema {
                attributes: expressions
                    .iter_mut()
                    .map(projection)
                    .collect::<PlanResult<_>>()?,
            },
            PlanNode::Window { input, expressions } => {
                let mut schema = input.schema.resolved("window input")?.clone();
                schema.attributes.extend(
                    expressions
                        .iter_mut()
                        .map(projection)
                        .collect::<PlanResult<Vec<_>>>()?,
                );
                schema
            }
            PlanNode::Aggregate {
                grouping,
                aggregates,
                grouping_sets,
                grouping_id,
                ..
            } => {
                let mut attributes = grouping
                    .iter_mut()
                    .map(projection)
                    .collect::<PlanResult<Vec<_>>>()?;
                if grouping_sets.is_some() {
                    for attribute in &mut attributes {
                        let mut t = attribute.result_type.resolved("grouping type")?.clone();
                        t.nullable = true;
                        attribute.result_type = Deferred::Resolved(t);
                    }
                }
                if let Some(id) = grouping_id {
                    if matches!(id.result_type, Deferred::Unresolved(_)) {
                        return Err(PlanError::analysis(
                            "grouping identity requires a declared type",
                        ));
                    }
                    attributes.push(id.clone());
                }
                attributes.extend(
                    aggregates
                        .iter_mut()
                        .map(projection)
                        .collect::<PlanResult<Vec<_>>>()?,
                );
                PlanSchema { attributes }
            }
            PlanNode::Scope { input }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Distinct { input }
            | PlanNode::DistinctOn { input, .. }
            | PlanNode::Repartition { input, .. } => input.schema.resolved("native input")?.clone(),
            PlanNode::Filter { input, condition } => {
                **condition = self.cast(*condition.clone(), DataType::Boolean)?;
                input.schema.resolved("filter input")?.clone()
            }
            PlanNode::RowIdentity {
                input, attribute, ..
            } => {
                let mut schema = input.schema.resolved("row identity input")?.clone();
                attribute.result_type =
                    Deferred::Resolved(ExpressionType::new(DataType::Int64, false));
                schema.attributes.push(attribute.clone());
                schema
            }
            PlanNode::Join {
                left,
                right,
                kind,
                condition,
                ..
            } => {
                if let Some(value) = condition {
                    **value = self.cast(*value.clone(), DataType::Boolean)?;
                }
                let mut a = left.schema.resolved("join left")?.attributes.clone();
                let mut b = right.schema.resolved("join right")?.attributes.clone();
                for (columns, nullable) in [
                    (&mut a, matches!(kind, JoinType::Right | JoinType::Full)),
                    (&mut b, matches!(kind, JoinType::Left | JoinType::Full)),
                ] {
                    if nullable {
                        for attribute in columns {
                            let mut field = attribute.result_type.resolved("join field")?.clone();
                            field.nullable = true;
                            attribute.result_type = Deferred::Resolved(field);
                        }
                    }
                }
                let attributes = match kind {
                    JoinType::LeftSemi | JoinType::LeftAnti => a,
                    JoinType::RightSemi | JoinType::RightAnti => b,
                    JoinType::LeftMark | JoinType::RightMark => {
                        let mut attributes = if *kind == JoinType::LeftMark { a } else { b };
                        let mut mark = match &plan.schema {
                            Deferred::Resolved(schema)
                                if schema.attributes.len() == attributes.len() + 1 =>
                            {
                                schema.attributes[attributes.len()].clone()
                            }
                            _ => self.attribute(
                                "exists".into(),
                                ExpressionType::new(DataType::Boolean, false),
                            ),
                        };
                        mark.result_type =
                            Deferred::Resolved(ExpressionType::new(DataType::Boolean, false));
                        attributes.push(mark);
                        attributes
                    }
                    _ => {
                        a.extend(b);
                        a
                    }
                };
                PlanSchema { attributes }
            }
            PlanNode::Empty { .. } => PlanSchema::default(),
            PlanNode::Range { .. } => PlanSchema {
                attributes: vec![
                    self.attribute("id".into(), ExpressionType::new(DataType::Int64, false)),
                ],
            },
            PlanNode::Scan {
                source, projection, ..
            } => {
                let schema = source.schema();
                let indices = projection
                    .clone()
                    .unwrap_or_else(|| (0..schema.fields().len()).collect());
                PlanSchema {
                    attributes: indices
                        .into_iter()
                        .map(|index| {
                            let field = schema.fields().get(index).ok_or_else(|| {
                                PlanError::analysis("scan projection is out of bounds")
                            })?;
                            Ok(self
                                .attribute(field.name().clone(), ExpressionType::from_field(field)))
                        })
                        .collect::<PlanResult<_>>()?,
                }
            }
            PlanNode::MapPartitions { function, .. } => PlanSchema {
                attributes: function
                    .output_schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        self.attribute(field.name().clone(), ExpressionType::from_field(field))
                    })
                    .collect(),
            },
            PlanNode::Values { rows } => {
                let width = rows.first().map_or(0, Vec::len);
                if rows.iter().any(|row| row.len() != width) {
                    return Err(PlanError::analysis("VALUES rows have different widths"));
                }
                let mut attributes = vec![];
                for index in 0..width {
                    let fields = rows
                        .iter()
                        .map(|row| row[index].result_type.resolved("VALUES field"))
                        .collect::<Result<Vec<_>, _>>()?;
                    let target = crate::analysis::typing::coercion::common_type(
                        &fields
                            .iter()
                            .map(|f| f.data_type.clone())
                            .collect::<Vec<_>>(),
                    )?;
                    let nullable = fields.iter().any(|f| f.nullable);
                    for row in rows.iter_mut() {
                        row[index] = self.cast(row[index].clone(), target.clone())?;
                    }
                    attributes.push(self.attribute(
                        format!("col{}", index + 1),
                        ExpressionType::new(target, nullable),
                    ));
                }
                PlanSchema { attributes }
            }
            PlanNode::Union { inputs } => {
                let resolved = self.union(std::mem::take(inputs))?;
                *inputs = match resolved.node.into_resolved("union")? {
                    PlanNode::Union { inputs } => inputs,
                    _ => unreachable!(),
                };
                resolved.schema.into_resolved("union output")?
            }
            PlanNode::Unnest { input, column, .. } => {
                let mut schema = input.schema.resolved("unnest input")?.clone();
                let attribute = schema
                    .attributes
                    .iter_mut()
                    .find(|a| a.id == *column)
                    .ok_or_else(|| PlanError::analysis("missing UNNEST input"))?;
                let field = match &attribute.result_type.resolved("unnest field")?.data_type {
                    DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => f,
                    _ => return Err(PlanError::analysis("UNNEST requires an array")),
                };
                let mut result = ExpressionType::from_field(field);
                result.nullable = true;
                attribute.result_type = Deferred::Resolved(result);
                schema
            }
            PlanNode::RemoteCheckpoint { .. } => {
                plan.schema.resolved("checkpoint declared schema")?.clone()
            }
        };
        if let Deferred::Resolved(existing) = &plan.schema {
            if existing.attributes.len() != schema.attributes.len() {
                return Err(PlanError::analysis(
                    "native output schema has an incorrect width",
                ));
            }
            for (attribute, existing) in schema.attributes.iter_mut().zip(&existing.attributes) {
                let result_type = attribute.result_type.clone();
                *attribute = existing.clone();
                attribute.result_type = result_type;
            }
        }
        plan.schema = Deferred::Resolved(schema);
        Ok(())
    }
}
