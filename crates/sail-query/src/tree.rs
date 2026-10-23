//! Native expression traversal. Query boundaries are explicit: ordinary
//! expression rewrites do not descend into subqueries or lambda bodies.

use datafusion_common::{DataFusionError, Result as PlanResult};

use super::deferred::Deferred;
use super::ir::*;

impl LogicalExpression {
    pub fn transform_up<E: From<DataFusionError>>(
        self,
        transform: &mut impl FnMut(Self) -> Result<Self, E>,
    ) -> Result<Self, E> {
        let expression = self.map_children(&mut |child| child.transform_up(transform))?;
        transform(expression)
    }

    pub fn map_children<E: From<DataFusionError>>(
        mut self,
        map: &mut impl FnMut(Self) -> Result<Self, E>,
    ) -> Result<Self, E> {
        let Deferred::Resolved(node) = self.node else {
            return Ok(self);
        };
        fn boxed<E: From<DataFusionError>>(
            value: Box<LogicalExpression>,
            map: &mut impl FnMut(LogicalExpression) -> Result<LogicalExpression, E>,
        ) -> Result<Box<LogicalExpression>, E> {
            map(*value).map(Box::new)
        }
        fn sorts<E: From<DataFusionError>>(
            values: Vec<SortExpression>,
            map: &mut impl FnMut(LogicalExpression) -> Result<LogicalExpression, E>,
        ) -> Result<Vec<SortExpression>, E> {
            values
                .into_iter()
                .map(|sort| {
                    Ok(SortExpression {
                        expression: map(sort.expression)?,
                        ..sort
                    })
                })
                .collect()
        }
        self.node = Deferred::Resolved(match node {
            ExpressionNode::Literal(_)
            | ExpressionNode::Attribute(_)
            | ExpressionNode::OuterAttribute { .. }
            | ExpressionNode::LambdaVariable(_)
            | ExpressionNode::ScalarSubquery(_)
            | ExpressionNode::Exists { .. } => node,
            ExpressionNode::WithMetadata(expression) => {
                ExpressionNode::WithMetadata(boxed(expression, map)?)
            }
            ExpressionNode::TruthTest {
                expression,
                value,
                negated,
            } => ExpressionNode::TruthTest {
                expression: boxed(expression, map)?,
                value,
                negated,
            },
            ExpressionNode::ScalarFunction {
                function,
                arguments,
            } => ExpressionNode::ScalarFunction {
                function,
                arguments: arguments.into_iter().map(map).collect::<Result<_, E>>()?,
            },
            ExpressionNode::AggregateFunction {
                function,
                arguments,
                distinct,
                filter,
                order_by,
                null_treatment,
            } => ExpressionNode::AggregateFunction {
                function,
                arguments: arguments
                    .into_iter()
                    .map(&mut *map)
                    .collect::<Result<_, E>>()?,
                distinct,
                filter: filter.map(|value| boxed(value, map)).transpose()?,
                order_by: sorts(order_by, map)?,
                null_treatment,
            },
            ExpressionNode::WindowFunction {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
                filter,
                distinct,
                null_treatment,
            } => ExpressionNode::WindowFunction {
                function,
                arguments: arguments
                    .into_iter()
                    .map(&mut *map)
                    .collect::<Result<_, E>>()?,
                partition_by: partition_by
                    .into_iter()
                    .map(&mut *map)
                    .collect::<Result<_, E>>()?,
                order_by: sorts(order_by, map)?,
                frame,
                filter: filter.map(|value| boxed(value, map)).transpose()?,
                distinct,
                null_treatment,
            },
            ExpressionNode::HigherOrderFunction {
                function,
                arguments,
            } => ExpressionNode::HigherOrderFunction {
                function,
                arguments: arguments
                    .into_iter()
                    .map(|argument| {
                        Ok(match argument {
                            FunctionArgument::Value(value) => {
                                FunctionArgument::Value(boxed(value, map)?)
                            }
                            lambda @ FunctionArgument::Lambda { .. } => lambda,
                        })
                    })
                    .collect::<Result<_, E>>()?,
            },
            ExpressionNode::Binary {
                left,
                operator,
                right,
            } => ExpressionNode::Binary {
                left: boxed(left, map)?,
                operator,
                right: boxed(right, map)?,
            },
            ExpressionNode::Cast {
                expression,
                target,
                safe,
            } => ExpressionNode::Cast {
                expression: boxed(expression, map)?,
                target,
                safe,
            },
            ExpressionNode::Not(expression) => ExpressionNode::Not(boxed(expression, map)?),
            ExpressionNode::IsNull(expression) => ExpressionNode::IsNull(boxed(expression, map)?),
            ExpressionNode::IsNotNull(expression) => {
                ExpressionNode::IsNotNull(boxed(expression, map)?)
            }
            ExpressionNode::Negative(expression) => {
                ExpressionNode::Negative(boxed(expression, map)?)
            }
            ExpressionNode::Case {
                operand,
                branches,
                otherwise,
            } => ExpressionNode::Case {
                operand: operand.map(|value| boxed(value, map)).transpose()?,
                branches: branches
                    .into_iter()
                    .map(|(condition, value)| Ok((map(condition)?, map(value)?)))
                    .collect::<Result<_, E>>()?,
                otherwise: otherwise.map(|value| boxed(value, map)).transpose()?,
            },
            ExpressionNode::Between {
                expression,
                low,
                high,
                negated,
            } => ExpressionNode::Between {
                expression: boxed(expression, map)?,
                low: boxed(low, map)?,
                high: boxed(high, map)?,
                negated,
            },
            ExpressionNode::Like {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => ExpressionNode::Like {
                expression: boxed(expression, map)?,
                pattern: boxed(pattern, map)?,
                negated,
                escape_char,
                case_insensitive,
            },
            ExpressionNode::SimilarTo {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => ExpressionNode::SimilarTo {
                expression: boxed(expression, map)?,
                pattern: boxed(pattern, map)?,
                negated,
                escape_char,
                case_insensitive,
            },
            ExpressionNode::InList {
                expression,
                values,
                negated,
            } => ExpressionNode::InList {
                expression: boxed(expression, map)?,
                values: values.into_iter().map(map).collect::<Result<_, E>>()?,
                negated,
            },
            ExpressionNode::InSubquery {
                expression,
                query,
                negated,
            } => ExpressionNode::InSubquery {
                expression: boxed(expression, map)?,
                query,
                negated,
            },
        });
        Ok(self)
    }

    pub fn any(&self, predicate: &impl Fn(&ExpressionNode) -> bool) -> PlanResult<bool> {
        let node = self.node.resolved("expression traversal")?;
        if predicate(node) {
            return Ok(true);
        }
        let mut found = false;
        self.clone().map_children(&mut |child| {
            found |= child.any(predicate)?;
            Ok::<_, DataFusionError>(child)
        })?;
        Ok(found)
    }

    /// Equality for grouping keys. Occurrence IDs and display names are not
    /// semantic inputs. Subqueries and lambdas are intentionally not unified.
    pub fn equivalent(&self, other: &Self) -> PlanResult<bool> {
        if self.result_type.resolved("expression type")?
            != other.result_type.resolved("expression type")?
        {
            return Ok(false);
        }
        fn list(left: &[LogicalExpression], right: &[LogicalExpression]) -> PlanResult<bool> {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !left.equivalent(right)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Ok(
            match (
                self.node.resolved("expression")?,
                other.node.resolved("expression")?,
            ) {
                (ExpressionNode::Literal(a), ExpressionNode::Literal(b)) => a == b,
                (ExpressionNode::Attribute(a), ExpressionNode::Attribute(b)) => a == b,
                (ExpressionNode::ScalarSubquery(a), ExpressionNode::ScalarSubquery(b)) => {
                    // Copies of a bound subquery retain its output slot identities.
                    // Independently resolved queries always allocate distinct slots.
                    let a = &a.schema.resolved("subquery output")?.attributes;
                    let b = &b.schema.resolved("subquery output")?.attributes;
                    !a.is_empty() && a.iter().map(|a| a.id).eq(b.iter().map(|b| b.id))
                }
                (
                    ExpressionNode::OuterAttribute {
                        depth: a,
                        attribute: x,
                    },
                    ExpressionNode::OuterAttribute {
                        depth: b,
                        attribute: y,
                    },
                ) => a == b && x == y,
                (
                    ExpressionNode::ScalarFunction {
                        function: a,
                        arguments: x,
                    },
                    ExpressionNode::ScalarFunction {
                        function: b,
                        arguments: y,
                    },
                ) => a == b && list(x, y)?,
                (
                    ExpressionNode::Binary {
                        left: a,
                        operator: x,
                        right: b,
                    },
                    ExpressionNode::Binary {
                        left: c,
                        operator: y,
                        right: d,
                    },
                ) => x == y && a.equivalent(c)? && b.equivalent(d)?,
                (
                    ExpressionNode::Cast {
                        expression: a,
                        target: x,
                        safe: s,
                    },
                    ExpressionNode::Cast {
                        expression: b,
                        target: y,
                        safe: t,
                    },
                ) => x == y && s == t && a.equivalent(b)?,
                (ExpressionNode::Not(a), ExpressionNode::Not(b))
                | (ExpressionNode::IsNull(a), ExpressionNode::IsNull(b))
                | (ExpressionNode::IsNotNull(a), ExpressionNode::IsNotNull(b))
                | (ExpressionNode::Negative(a), ExpressionNode::Negative(b))
                | (ExpressionNode::WithMetadata(a), ExpressionNode::WithMetadata(b)) => {
                    a.equivalent(b)?
                }
                (
                    ExpressionNode::TruthTest {
                        expression: a,
                        value: x,
                        negated: n,
                    },
                    ExpressionNode::TruthTest {
                        expression: b,
                        value: y,
                        negated: m,
                    },
                ) => x == y && n == m && a.equivalent(b)?,
                (
                    ExpressionNode::Between {
                        expression: a,
                        low: l,
                        high: h,
                        negated: n,
                    },
                    ExpressionNode::Between {
                        expression: b,
                        low: k,
                        high: j,
                        negated: m,
                    },
                ) => n == m && a.equivalent(b)? && l.equivalent(k)? && h.equivalent(j)?,
                (
                    ExpressionNode::Like {
                        expression: a,
                        pattern: x,
                        negated: n,
                        escape_char: e,
                        case_insensitive: i,
                    },
                    ExpressionNode::Like {
                        expression: b,
                        pattern: y,
                        negated: m,
                        escape_char: f,
                        case_insensitive: j,
                    },
                )
                | (
                    ExpressionNode::SimilarTo {
                        expression: a,
                        pattern: x,
                        negated: n,
                        escape_char: e,
                        case_insensitive: i,
                    },
                    ExpressionNode::SimilarTo {
                        expression: b,
                        pattern: y,
                        negated: m,
                        escape_char: f,
                        case_insensitive: j,
                    },
                ) => n == m && e == f && i == j && a.equivalent(b)? && x.equivalent(y)?,
                (
                    ExpressionNode::InList {
                        expression: a,
                        values: x,
                        negated: n,
                    },
                    ExpressionNode::InList {
                        expression: b,
                        values: y,
                        negated: m,
                    },
                ) => n == m && a.equivalent(b)? && list(x, y)?,
                (
                    ExpressionNode::Case {
                        operand: a,
                        branches: x,
                        otherwise: p,
                    },
                    ExpressionNode::Case {
                        operand: b,
                        branches: y,
                        otherwise: q,
                    },
                ) => {
                    let operand = match (a, b) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a.equivalent(b)?,
                        _ => false,
                    };
                    let otherwise = match (p, q) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a.equivalent(b)?,
                        _ => false,
                    };
                    let mut branches = x.len() == y.len();
                    for ((a, b), (c, d)) in x.iter().zip(y) {
                        branches &= a.equivalent(c)? && b.equivalent(d)?;
                    }
                    operand && otherwise && branches
                }
                (
                    ExpressionNode::AggregateFunction {
                        function: a,
                        arguments: x,
                        distinct: d,
                        filter: f,
                        order_by: o,
                        null_treatment: n,
                    },
                    ExpressionNode::AggregateFunction {
                        function: b,
                        arguments: y,
                        distinct: e,
                        filter: g,
                        order_by: p,
                        null_treatment: m,
                    },
                ) => {
                    let filter = match (f, g) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a.equivalent(b)?,
                        _ => false,
                    };
                    let mut sorts = o.len() == p.len();
                    for (a, b) in o.iter().zip(p) {
                        sorts &= a.ascending == b.ascending
                            && a.nulls_first == b.nulls_first
                            && a.expression.equivalent(&b.expression)?;
                    }
                    a == b && d == e && n == m && filter && sorts && list(x, y)?
                }
                (
                    ExpressionNode::WindowFunction {
                        function: a,
                        arguments: x,
                        partition_by: p,
                        order_by: o,
                        frame: f,
                        filter: c,
                        distinct: d,
                        null_treatment: n,
                    },
                    ExpressionNode::WindowFunction {
                        function: b,
                        arguments: y,
                        partition_by: q,
                        order_by: r,
                        frame: g,
                        filter: e,
                        distinct: h,
                        null_treatment: m,
                    },
                ) => {
                    let filter = match (c, e) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a.equivalent(b)?,
                        _ => false,
                    };
                    let mut sorts = o.len() == r.len();
                    for (a, b) in o.iter().zip(r) {
                        sorts &= a.ascending == b.ascending
                            && a.nulls_first == b.nulls_first
                            && a.expression.equivalent(&b.expression)?;
                    }
                    a == b
                        && f == g
                        && d == h
                        && n == m
                        && filter
                        && sorts
                        && list(x, y)?
                        && list(p, q)?
                }
                _ => false,
            },
        )
    }
}
