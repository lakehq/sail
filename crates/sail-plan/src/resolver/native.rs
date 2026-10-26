//! Complete deferred fields in an already assembled native tree.
use std::collections::VecDeque;

use arrow_schema::DataType;
use datafusion_expr::{JoinType, ValueOrLambda};
use sail_common_plan::rewrite::{QueryRewriter, walk_query};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

// The synchronous tree walker defines traversal order once. Async resolution
// consumes the immediate children in that order without entering nested scopes.
#[derive(Default)]
struct Children {
    plans: Vec<Query>,
    expressions: Vec<Expr>,
}
impl QueryRewriter for Children {
    type Error = PlanError;
    fn rewrite_query(&mut self, plan: Query) -> PlanResult<Query> {
        self.plans.push(plan.clone());
        Ok(plan)
    }
    fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
        self.expressions.push(expression.clone());
        Ok(expression)
    }
}
struct Replace {
    plans: VecDeque<Query>,
    expressions: VecDeque<Expr>,
}
impl QueryRewriter for Replace {
    type Error = PlanError;
    fn rewrite_query(&mut self, _: Query) -> PlanResult<Query> {
        self.plans
            .pop_front()
            .ok_or_else(|| PlanError::internal("missing resolved child plan"))
    }
    fn rewrite_expression(&mut self, _: Expr) -> PlanResult<Expr> {
        self.expressions
            .pop_front()
            .ok_or_else(|| PlanError::internal("missing resolved expression"))
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query(&mut self, plan: Query) -> PlanResult<Query> {
        if let Deferred::Unresolved(request) = plan.kind {
            return self.resolve_query_plan(*request).await;
        }
        let lateral = matches!(
            &plan.kind,
            Deferred::Resolved(QueryKind::Join { lateral: true, .. })
        );
        let mut children = Children::default();
        let plan = walk_query(&mut children, plan)?;
        let mut plans: Vec<Query> = vec![];
        for (index, child) in children.plans.into_iter().enumerate() {
            if lateral && index == 1 {
                self.outer
                    .push(plans[0].schema.resolved("lateral input")?.clone());
            }
            let result = self.resolve_query(child).await;
            if lateral && index == 1 {
                self.outer.pop();
            }
            plans.push(result?);
        }
        let mut scope = QuerySchema::default();
        for input in &plans {
            scope
                .attributes
                .extend(input.schema.resolved("native input")?.attributes.clone());
        }
        let mut expressions = VecDeque::new();
        for expression in children.expressions {
            expressions.push_back(self.resolve_expression(expression, &scope).await?);
        }
        let mut plan = walk_query(
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
    pub(in crate::resolver) async fn resolve_expression(
        &mut self,
        mut expression: Expr,
        schema: &QuerySchema,
    ) -> PlanResult<Expr> {
        let id = expression.id;
        if let Deferred::Unresolved(request) = expression.kind {
            let mut result = self
                .resolve_named_expression(*request, schema)
                .await?
                .expression;
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
            resolved.push_back(self.resolve_expression(child, schema).await?);
        }
        expression = expression.map_children::<PlanError>(&mut |_| {
            resolved
                .pop_front()
                .ok_or_else(|| PlanError::internal("missing expression child"))
        })?;
        let mut node = expression.kind.into_resolved("native expression")?;
        match &mut node {
            ExprKind::ScalarSubquery(query)
            | ExprKind::Exists { query, .. }
            | ExprKind::InSubquery { query, .. } => {
                self.outer.push(schema.clone());
                let result = self.resolve_query(*query.clone()).await;
                self.outer.pop();
                **query = result?;
            }
            ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => {
                let mut complete = false;
                for step in 0..100 {
                    let mut fields = arguments
                        .iter()
                        .map(|arg| {
                            Ok(match arg {
                                FunctionArg::Value(v) => ValueOrLambda::Value(
                                    v.result_type.resolved("higher-order input")?.to_field(""),
                                ),
                                FunctionArg::Lambda { body, .. } => {
                                    ValueOrLambda::Lambda(match &body.result_type {
                                        Deferred::Resolved(t) => Some(t.to_field("")),
                                        Deferred::Unresolved(_) => None,
                                    })
                                }
                            })
                        })
                        .collect::<PlanResult<Vec<_>>>()?;
                    let targets =
                        crate::analyzer::typing::higher_order::value_fields(function, &fields)?;
                    fields = targets.clone();
                    for (argument, target) in arguments.iter_mut().zip(targets) {
                        if let (FunctionArg::Value(value), ValueOrLambda::Value(target)) =
                            (argument, target)
                        {
                            **value = self
                                .context
                                .function_context()
                                .cast(*value.clone(), target.data_type().clone())?;
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
                        if let FunctionArg::Lambda {
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
                                    Deferred::Resolved(ExprType::from_field(&field));
                            }
                            self.lambdas.push(parameters.clone());
                            let result = self.resolve_expression(*body.clone(), schema).await;
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
            ExprKind::Literal(value) => {
                let mut result = ExprType::new(value.data_type(), value.is_null());
                if let Deferred::Resolved(previous) = &expression.result_type
                    && previous.data_type == result.data_type
                {
                    result.metadata.clone_from(&previous.metadata);
                }
                result
            }
            ExprKind::Attribute(attribute) => schema
                .attribute(*attribute)?
                .result_type
                .resolved("attribute type")?
                .clone(),
            ExprKind::OuterAttribute { depth, attribute } => self
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
            ExprKind::LambdaVariable(attribute) => self
                .lambdas
                .iter()
                .rev()
                .find_map(|scope| scope.attributes.iter().find(|a| a.id == *attribute))
                .ok_or_else(|| PlanError::analysis("lambda variable is out of scope"))?
                .result_type
                .resolved("lambda variable")?
                .clone(),
            ExprKind::ScalarFunction {
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
            ExprKind::Binary {
                left,
                operator,
                right,
            } => {
                let mut result = self.context.function_context().binary(
                    *left.clone(),
                    *operator,
                    *right.clone(),
                )?;
                result.id = id;
                return Ok(result);
            }
            ExprKind::Case {
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
            ExprKind::InList {
                expression,
                values,
                negated,
            } => {
                let mut result = crate::function::scalar::predicate::in_list(
                    &mut self.context.function_context(),
                    *expression.clone(),
                    std::mem::take(values),
                    *negated,
                )?;
                result.id = id;
                return Ok(result);
            }
            ExprKind::AggregateFunction {
                function,
                arguments,
                ..
            } => {
                let result = crate::analyzer::typing::aggregate::resolve(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &result.arguments,
                    &mut self.context.ids,
                )?;
                ExprType::from_field(&result.result)
            }
            ExprKind::WindowFunction {
                function,
                arguments,
                order_by,
                frame,
                ..
            } => {
                let result = crate::analyzer::typing::aggregate::resolve_window(
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
                *frame = crate::analyzer::typing::window::resolve_frame(frame.clone(), &types)?;
                ExprType::from_field(&result.result)
            }
            ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => {
                let fields = arguments
                    .iter()
                    .map(|a| {
                        Ok(match a {
                            FunctionArg::Value(v) => ValueOrLambda::Value(
                                v.result_type.resolved("higher-order value")?.to_field(""),
                            ),
                            FunctionArg::Lambda { body, .. } => ValueOrLambda::Lambda(
                                body.result_type.resolved("lambda result")?.to_field(""),
                            ),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let literals = arguments
                    .iter()
                    .map(|a| match a {
                        FunctionArg::Value(v) => match &v.kind {
                            Deferred::Resolved(ExprKind::Literal(v)) => Some(v),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let result =
                    crate::analyzer::typing::higher_order::resolve(function, &fields, &literals)?;
                for (argument, target) in arguments.iter_mut().zip(result.arguments) {
                    if let (FunctionArg::Value(v), ValueOrLambda::Value(target)) =
                        (argument, target)
                    {
                        **v = self
                            .context
                            .function_context()
                            .cast(*v.clone(), target.data_type().clone())?;
                    }
                }
                ExprType::from_field(&result.result)
            }
            ExprKind::Cast {
                expression,
                target,
                safe,
            } => {
                target.nullable = *safe || expression.result_type.resolved("cast input")?.nullable;
                target.clone()
            }
            ExprKind::WithMetadata(child) => {
                let mut result = child.result_type.resolved("metadata value")?.clone();
                if let Deferred::Resolved(existing) = &expression.result_type {
                    result.metadata.clone_from(&existing.metadata);
                }
                result
            }
            ExprKind::Negative(child) => child.result_type.resolved("negation input")?.clone(),
            ExprKind::Not(child) => {
                **child = self
                    .context
                    .function_context()
                    .cast(*child.clone(), DataType::Boolean)?;
                child.result_type.resolved("NOT input")?.clone()
            }
            ExprKind::IsNull(_)
            | ExprKind::IsNotNull(_)
            | ExprKind::TruthTest { .. }
            | ExprKind::Exists { .. } => ExprType::new(DataType::Boolean, false),
            ExprKind::Between {
                expression,
                low,
                high,
                ..
            } => {
                let target = crate::analyzer::typing::coercion::comparison_type(&[
                    expression
                        .result_type
                        .resolved("BETWEEN value")?
                        .data_type
                        .clone(),
                    low.result_type.resolved("BETWEEN low")?.data_type.clone(),
                    high.result_type.resolved("BETWEEN high")?.data_type.clone(),
                ])?;
                **expression = self
                    .context
                    .function_context()
                    .cast(*expression.clone(), target.clone())?;
                **low = self
                    .context
                    .function_context()
                    .cast(*low.clone(), target.clone())?;
                **high = self
                    .context
                    .function_context()
                    .cast(*high.clone(), target)?;
                ExprType::new(
                    DataType::Boolean,
                    expression.result_type.resolved("BETWEEN value")?.nullable
                        || low.result_type.resolved("BETWEEN low")?.nullable
                        || high.result_type.resolved("BETWEEN high")?.nullable,
                )
            }
            ExprKind::Like {
                expression,
                pattern,
                ..
            }
            | ExprKind::SimilarTo {
                expression,
                pattern,
                ..
            } => {
                **expression = self
                    .context
                    .function_context()
                    .cast(*expression.clone(), DataType::Utf8)?;
                **pattern = self
                    .context
                    .function_context()
                    .cast(*pattern.clone(), DataType::Utf8)?;
                ExprType::new(
                    DataType::Boolean,
                    expression.result_type.resolved("pattern value")?.nullable
                        || pattern.result_type.resolved("pattern")?.nullable,
                )
            }
            ExprKind::ScalarSubquery(query) => {
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
            ExprKind::InSubquery {
                expression, query, ..
            } => {
                let [attribute] = query.schema.resolved("IN subquery")?.attributes.as_slice()
                else {
                    return Err(PlanError::analysis("IN subquery must return one column"));
                };
                let right = self.reference(attribute)?;
                let mut values = crate::function::scalar::predicate::in_arguments(
                    &mut self.context.function_context(),
                    vec![*expression.clone(), right],
                )?
                .into_iter();
                let left = values
                    .next()
                    .ok_or_else(|| PlanError::internal("missing IN left"))?;
                let right = values
                    .next()
                    .ok_or_else(|| PlanError::internal("missing IN right"))?;
                let target = crate::analyzer::typing::coercion::comparison_type(&[
                    left.result_type.resolved("IN left")?.data_type.clone(),
                    right.result_type.resolved("IN right")?.data_type.clone(),
                ])?;
                **expression = self.context.function_context().cast(left, target.clone())?;
                let right = self.context.function_context().cast(right, target)?;
                let nullable = expression.result_type.resolved("IN left")?.nullable
                    || right.result_type.resolved("IN right")?.nullable;
                let name = attribute.name.clone();
                **query = self.project(
                    *query.clone(),
                    vec![crate::resolver::NamedExpression {
                        name,
                        expression: right,
                    }],
                )?;
                ExprType::new(DataType::Boolean, nullable)
            }
        };
        Ok(Expr {
            id,
            kind: Deferred::Resolved(node),
            result_type: Deferred::Resolved(result),
        })
    }

    fn native_schema(&mut self, plan: &mut Query) -> PlanResult<()> {
        let node = match &mut plan.kind {
            Deferred::Resolved(node) => node,
            _ => return Err(PlanError::internal("unresolved native node")),
        };
        let projection = |p: &mut Projection| -> PlanResult<Attribute> {
            p.attribute.result_type = p.expression.result_type.clone();
            Ok(p.attribute.clone())
        };
        let mut schema = match node {
            QueryKind::Project { expressions, .. } => QuerySchema {
                attributes: expressions
                    .iter_mut()
                    .map(projection)
                    .collect::<PlanResult<_>>()?,
            },
            QueryKind::Window { input, expressions } => {
                let mut schema = input.schema.resolved("window input")?.clone();
                schema.attributes.extend(
                    expressions
                        .iter_mut()
                        .map(projection)
                        .collect::<PlanResult<Vec<_>>>()?,
                );
                schema
            }
            QueryKind::Aggregate {
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
                QuerySchema { attributes }
            }
            QueryKind::Scope { input }
            | QueryKind::Sort { input, .. }
            | QueryKind::Limit { input, .. }
            | QueryKind::Distinct { input }
            | QueryKind::DistinctOn { input, .. }
            | QueryKind::Repartition { input, .. } => {
                input.schema.resolved("native input")?.clone()
            }
            QueryKind::Filter { input, condition } => {
                **condition = self
                    .context
                    .function_context()
                    .cast(*condition.clone(), DataType::Boolean)?;
                input.schema.resolved("filter input")?.clone()
            }
            QueryKind::RowIdentity {
                input, attribute, ..
            } => {
                let mut schema = input.schema.resolved("row identity input")?.clone();
                attribute.result_type = Deferred::Resolved(ExprType::new(DataType::Int64, false));
                schema.attributes.push(attribute.clone());
                schema
            }
            QueryKind::Join {
                left,
                right,
                kind,
                condition,
                ..
            } => {
                if let Some(value) = condition {
                    **value = self
                        .context
                        .function_context()
                        .cast(*value.clone(), DataType::Boolean)?;
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
                            _ => self.context.function_context().attribute(
                                "exists".into(),
                                ExprType::new(DataType::Boolean, false),
                            ),
                        };
                        mark.result_type =
                            Deferred::Resolved(ExprType::new(DataType::Boolean, false));
                        attributes.push(mark);
                        attributes
                    }
                    _ => {
                        a.extend(b);
                        a
                    }
                };
                QuerySchema { attributes }
            }
            QueryKind::Empty { .. } => QuerySchema::default(),
            QueryKind::Range { .. } => QuerySchema {
                attributes: vec![
                    self.context
                        .function_context()
                        .attribute("id".into(), ExprType::new(DataType::Int64, false)),
                ],
            },
            QueryKind::Scan {
                source, projection, ..
            } => {
                let schema = source.schema();
                let indices = projection
                    .clone()
                    .unwrap_or_else(|| (0..schema.fields().len()).collect());
                QuerySchema {
                    attributes: indices
                        .into_iter()
                        .map(|index| {
                            let field = schema.fields().get(index).ok_or_else(|| {
                                PlanError::analysis("scan projection is out of bounds")
                            })?;
                            Ok(self
                                .context
                                .function_context()
                                .attribute(field.name().clone(), ExprType::from_field(field)))
                        })
                        .collect::<PlanResult<_>>()?,
                }
            }
            QueryKind::MapPartitions { function, .. } => QuerySchema {
                attributes: function
                    .output_schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        self.context
                            .function_context()
                            .attribute(field.name().clone(), ExprType::from_field(field))
                    })
                    .collect(),
            },
            QueryKind::Values { rows } => {
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
                    let target = crate::analyzer::typing::coercion::common_type(
                        &fields
                            .iter()
                            .map(|f| f.data_type.clone())
                            .collect::<Vec<_>>(),
                    )?;
                    let nullable = fields.iter().any(|f| f.nullable);
                    for row in rows.iter_mut() {
                        row[index] = self
                            .context
                            .function_context()
                            .cast(row[index].clone(), target.clone())?;
                    }
                    attributes.push(
                        self.context.function_context().attribute(
                            format!("col{}", index + 1),
                            ExprType::new(target, nullable),
                        ),
                    );
                }
                QuerySchema { attributes }
            }
            QueryKind::Union { inputs } => {
                let resolved = self.union(std::mem::take(inputs))?;
                *inputs = match resolved.kind.into_resolved("union")? {
                    QueryKind::Union { inputs } => inputs,
                    _ => unreachable!(),
                };
                resolved.schema.into_resolved("union output")?
            }
            QueryKind::Unnest { input, column, .. } => {
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
                let mut result = ExprType::from_field(field);
                result.nullable = true;
                attribute.result_type = Deferred::Resolved(result);
                schema
            }
            QueryKind::RemoteCheckpoint { .. } => {
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
