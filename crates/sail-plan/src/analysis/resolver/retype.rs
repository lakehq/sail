use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::analysis::typing;
use crate::error::PlanResult;

impl Resolver<'_, '_> {
    /// Recompute parent fields after a transformation changes its children's
    /// fields (for example, grouping sets make grouping outputs nullable).
    pub(super) fn retype(&mut self, mut expression: ExprNode) -> PlanResult<ExprNode> {
        let mut result = expression
            .result_type
            .into_resolved("rewritten expression type")?;
        let nullable = |value: &ExprNode| -> PlanResult<bool> {
            Ok(value.result_type.resolved("rewritten child")?.nullable)
        };
        let node = match expression.kind.into_resolved("rewritten expression")? {
            ExprKind::ScalarFunction {
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
            ExprKind::Binary {
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
            ExprKind::Case {
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
            ExprKind::InList {
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
            ExprKind::Cast {
                expression: child,
                mut target,
                safe,
            } => {
                let source = child.result_type.resolved("cast child")?.to_field("");
                let field =
                    crate::analysis::typing::coercion::cast_field(&source, &target.data_type);
                target.nullable = safe || field.is_nullable();
                result = target.clone();
                ExprKind::Cast {
                    expression: child,
                    target,
                    safe,
                }
            }
            ExprKind::WithMetadata(child) => {
                let source = child.result_type.resolved("metadata child")?;
                result.data_type = source.data_type.clone();
                result.nullable = source.nullable;
                ExprKind::WithMetadata(child)
            }
            node @ (ExprKind::Not(_) | ExprKind::Negative(_)) => {
                let (ExprKind::Not(child) | ExprKind::Negative(child)) = &node else {
                    unreachable!()
                };
                result.nullable = nullable(child)?;
                node
            }
            node @ (ExprKind::Like { .. } | ExprKind::SimilarTo { .. }) => {
                let (ExprKind::Like {
                    expression: child,
                    pattern,
                    ..
                }
                | ExprKind::SimilarTo {
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
            node @ ExprKind::Between { .. } => {
                let ExprKind::Between {
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
            mut node @ ExprKind::AggregateFunction { .. } => {
                let ExprKind::AggregateFunction {
                    function,
                    arguments,
                    ..
                } = &mut node
                else {
                    unreachable!()
                };
                let resolved = crate::analysis::typing::aggregate::resolve(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &resolved.arguments,
                    &mut self.context.ids,
                )?;
                result = ExprType::from_field(&resolved.result);
                node
            }
            mut node @ ExprKind::WindowFunction { .. } => {
                let ExprKind::WindowFunction {
                    function,
                    arguments,
                    ..
                } = &mut node
                else {
                    unreachable!()
                };
                let resolved = crate::analysis::typing::aggregate::resolve_window(
                    function,
                    &typing::argument_fields(arguments)?,
                )?;
                *arguments = typing::coerce_arguments(
                    std::mem::take(arguments),
                    &resolved.arguments,
                    &mut self.context.ids,
                )?;
                result = ExprType::from_field(&resolved.result);
                node
            }
            mut node @ ExprKind::HigherOrderFunction { .. } => {
                use datafusion_expr::ValueOrLambda;
                let ExprKind::HigherOrderFunction {
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
                            FunctionArg::Value(value) => ValueOrLambda::Value(
                                value
                                    .result_type
                                    .resolved("higher-order input")?
                                    .to_field(""),
                            ),
                            FunctionArg::Lambda { body, .. } => ValueOrLambda::Lambda(
                                body.result_type.resolved("lambda body")?.to_field(""),
                            ),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let literals = arguments
                    .iter()
                    .map(|argument| match argument {
                        FunctionArg::Value(value) => match &value.kind {
                            Deferred::Resolved(ExprKind::Literal(value)) => Some(value),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let resolved =
                    crate::analysis::typing::higher_order::resolve(function, &fields, &literals)?;
                for (argument, target) in arguments.iter_mut().zip(&resolved.arguments) {
                    if let (FunctionArg::Value(value), ValueOrLambda::Value(field)) =
                        (argument, target)
                    {
                        **value = typing::coerce(
                            *value.clone(),
                            field.data_type(),
                            &mut self.context.ids,
                        )?;
                    }
                }
                result = ExprType::from_field(&resolved.result);
                node
            }
            node @ ExprKind::ScalarSubquery(_) => {
                let ExprKind::ScalarSubquery(query) = &node else {
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
            node @ (ExprKind::Literal(_)
            | ExprKind::Attribute(_)
            | ExprKind::OuterAttribute { .. }
            | ExprKind::LambdaVariable(_)
            | ExprKind::IsNull(_)
            | ExprKind::IsNotNull(_)
            | ExprKind::TruthTest { .. }
            | ExprKind::Exists { .. }
            | ExprKind::InSubquery { .. }) => node,
        };
        expression.kind = Deferred::Resolved(node);
        expression.result_type = Deferred::Resolved(result);
        Ok(expression)
    }
}
