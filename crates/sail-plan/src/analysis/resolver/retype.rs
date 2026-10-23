use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::PlanResult;

impl Resolver<'_, '_> {
    /// Recompute parent fields after a transformation changes its children's
    /// fields (for example, grouping sets make grouping outputs nullable).
    pub(super) fn retype(
        &mut self,
        mut expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        let mut result = expression
            .result_type
            .into_resolved("rewritten expression type")?;
        let nullable = |value: &LogicalExpression| -> PlanResult<bool> {
            Ok(value.result_type.resolved("rewritten child")?.nullable)
        };
        let node = match expression.node.into_resolved("rewritten expression")? {
            ExpressionNode::ScalarFunction {
                function,
                arguments,
            } => {
                return typing::resolve_scalar_call(
                    expression.id,
                    function,
                    arguments,
                    &mut self.context.ids,
                );
            }
            ExpressionNode::Binary {
                left,
                operator,
                right,
            } => {
                return typing::resolve_binary(
                    expression.id,
                    *left,
                    operator,
                    *right,
                    &mut self.context.ids,
                );
            }
            ExpressionNode::Case {
                operand,
                branches,
                otherwise,
            } => {
                return typing::resolve_case(
                    expression.id,
                    operand.map(|e| *e),
                    branches,
                    otherwise.map(|e| *e),
                    &mut self.context.ids,
                );
            }
            ExpressionNode::InList {
                expression: value,
                values,
                negated,
            } => {
                return typing::resolve_in_list(
                    expression.id,
                    *value,
                    values,
                    negated,
                    &mut self.context.ids,
                );
            }
            ExpressionNode::Cast {
                expression: child,
                mut target,
                safe,
            } => {
                let source = child.result_type.resolved("cast child")?.to_field("");
                let field = sail_type_system::coercion::cast_field(&source, &target.data_type);
                target.nullable = safe || field.is_nullable();
                result = target.clone();
                ExpressionNode::Cast {
                    expression: child,
                    target,
                    safe,
                }
            }
            ExpressionNode::WithMetadata(child) => {
                let source = child.result_type.resolved("metadata child")?;
                result.data_type = source.data_type.clone();
                result.nullable = source.nullable;
                ExpressionNode::WithMetadata(child)
            }
            node @ (ExpressionNode::Not(_) | ExpressionNode::Negative(_)) => {
                let (ExpressionNode::Not(child) | ExpressionNode::Negative(child)) = &node else {
                    unreachable!()
                };
                result.nullable = nullable(child)?;
                node
            }
            node @ (ExpressionNode::Like { .. } | ExpressionNode::SimilarTo { .. }) => {
                let (ExpressionNode::Like {
                    expression: child,
                    pattern,
                    ..
                }
                | ExpressionNode::SimilarTo {
                    expression: child,
                    pattern,
                    ..
                }) = &node
                else {
                    unreachable!()
                };
                result.nullable = nullable(child)? || nullable(pattern)?;
                node
            }
            node @ ExpressionNode::Between { .. } => {
                let ExpressionNode::Between {
                    expression: child,
                    low,
                    high,
                    ..
                } = &node
                else {
                    unreachable!()
                };
                result.nullable = nullable(child)? || nullable(low)? || nullable(high)?;
                node
            }
            mut node @ ExpressionNode::AggregateFunction { .. } => {
                let ExpressionNode::AggregateFunction {
                    function,
                    arguments,
                    ..
                } = &mut node
                else {
                    unreachable!()
                };
                let resolved = sail_type_system::aggregate::resolve(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &resolved.arguments,
                    &mut self.context.ids,
                )?;
                result = ExpressionType::from_field(&resolved.result);
                node
            }
            mut node @ ExpressionNode::WindowFunction { .. } => {
                let ExpressionNode::WindowFunction {
                    function,
                    arguments,
                    ..
                } = &mut node
                else {
                    unreachable!()
                };
                let resolved = sail_type_system::aggregate::resolve_window(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &resolved.arguments,
                    &mut self.context.ids,
                )?;
                result = ExpressionType::from_field(&resolved.result);
                node
            }
            mut node @ ExpressionNode::HigherOrderFunction { .. } => {
                use datafusion_expr::ValueOrLambda;
                let ExpressionNode::HigherOrderFunction {
                    function,
                    arguments,
                } = &mut node
                else {
                    unreachable!()
                };
                let fields = arguments
                    .iter()
                    .map(|argument| {
                        Ok(match argument {
                            FunctionArgument::Value(value) => ValueOrLambda::Value(
                                value
                                    .result_type
                                    .resolved("higher-order input")?
                                    .to_field(""),
                            ),
                            FunctionArgument::Lambda { body, .. } => ValueOrLambda::Lambda(
                                body.result_type.resolved("lambda body")?.to_field(""),
                            ),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let literals = arguments
                    .iter()
                    .map(|argument| match argument {
                        FunctionArgument::Value(value) => match &value.node {
                            Deferred::Resolved(ExpressionNode::Literal(value)) => Some(value),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let resolved =
                    sail_type_system::higher_order::resolve(function, &fields, &literals)?;
                for (argument, target) in arguments.iter_mut().zip(&resolved.arguments) {
                    if let (FunctionArgument::Value(value), ValueOrLambda::Value(field)) =
                        (argument, target)
                    {
                        **value = typing::coerce(
                            *value.clone(),
                            field.data_type(),
                            &mut self.context.ids,
                        )?;
                    }
                }
                result = ExpressionType::from_field(&resolved.result);
                node
            }
            node @ ExpressionNode::ScalarSubquery(_) => {
                let ExpressionNode::ScalarSubquery(query) = &node else {
                    unreachable!()
                };
                let [attribute] = query
                    .schema
                    .resolved("scalar subquery")?
                    .attributes
                    .as_slice()
                else {
                    return Err(crate::error::PlanError::analysis(
                        "scalar subquery must return one column",
                    ));
                };
                result = attribute
                    .result_type
                    .resolved("scalar subquery output")?
                    .clone();
                result.nullable = true;
                node
            }
            node @ (ExpressionNode::Literal(_)
            | ExpressionNode::Attribute(_)
            | ExpressionNode::OuterAttribute { .. }
            | ExpressionNode::LambdaVariable(_)
            | ExpressionNode::IsNull(_)
            | ExpressionNode::IsNotNull(_)
            | ExpressionNode::TruthTest { .. }
            | ExpressionNode::Exists { .. }
            | ExpressionNode::InSubquery { .. }) => node,
        };
        expression.node = Deferred::Resolved(node);
        expression.result_type = Deferred::Resolved(result);
        Ok(expression)
    }
}
