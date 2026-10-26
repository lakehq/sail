//! Native expression traversal. Query boundaries are explicit: ordinary
//! expression rewrites do not descend into subqueries or lambda bodies.

use datafusion_common::{DataFusionError, Result as PlanResult};

use super::deferred::Deferred;
use super::model::*;

impl Expr {
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
        let Deferred::Resolved(node) = self.kind else {
            return Ok(self);
        };
        fn boxed<E: From<DataFusionError>>(
            value: Box<Expr>,
            map: &mut impl FnMut(Expr) -> Result<Expr, E>,
        ) -> Result<Box<Expr>, E> {
            map(*value).map(Box::new)
        }
        fn sorts<E: From<DataFusionError>>(
            values: Vec<SortKey>,
            map: &mut impl FnMut(Expr) -> Result<Expr, E>,
        ) -> Result<Vec<SortKey>, E> {
            values
                .into_iter()
                .map(|sort| {
                    Ok(SortKey {
                        expression: map(sort.expression)?,
                        ..sort
                    })
                })
                .collect()
        }
        self.kind = Deferred::Resolved(match node {
            ExprKind::Literal(_)
            | ExprKind::Attribute(_)
            | ExprKind::OuterAttribute { .. }
            | ExprKind::LambdaVariable(_)
            | ExprKind::ScalarSubquery(_)
            | ExprKind::Exists { .. } => node,
            ExprKind::WithMetadata(expression) => ExprKind::WithMetadata(boxed(expression, map)?),
            ExprKind::TruthTest {
                expression,
                value,
                negated,
            } => ExprKind::TruthTest {
                expression: boxed(expression, map)?,
                value,
                negated,
            },
            ExprKind::ScalarFunction {
                function,
                arguments,
            } => ExprKind::ScalarFunction {
                function,
                arguments: arguments.into_iter().map(map).collect::<Result<_, E>>()?,
            },
            ExprKind::AggregateFunction {
                function,
                arguments,
                distinct,
                filter,
                order_by,
                null_treatment,
            } => ExprKind::AggregateFunction {
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
            ExprKind::WindowFunction {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
                filter,
                distinct,
                null_treatment,
            } => ExprKind::WindowFunction {
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
            ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => ExprKind::HigherOrderFunction {
                function,
                arguments: arguments
                    .into_iter()
                    .map(|argument| {
                        Ok(match argument {
                            FunctionArg::Value(value) => FunctionArg::Value(boxed(value, map)?),
                            lambda @ FunctionArg::Lambda { .. } => lambda,
                        })
                    })
                    .collect::<Result<_, E>>()?,
            },
            ExprKind::Binary {
                left,
                operator,
                right,
            } => ExprKind::Binary {
                left: boxed(left, map)?,
                operator,
                right: boxed(right, map)?,
            },
            ExprKind::Cast {
                expression,
                target,
                safe,
            } => ExprKind::Cast {
                expression: boxed(expression, map)?,
                target,
                safe,
            },
            ExprKind::Not(expression) => ExprKind::Not(boxed(expression, map)?),
            ExprKind::IsNull(expression) => ExprKind::IsNull(boxed(expression, map)?),
            ExprKind::IsNotNull(expression) => ExprKind::IsNotNull(boxed(expression, map)?),
            ExprKind::Negative(expression) => ExprKind::Negative(boxed(expression, map)?),
            ExprKind::Case {
                operand,
                branches,
                otherwise,
            } => ExprKind::Case {
                operand: operand.map(|value| boxed(value, map)).transpose()?,
                branches: branches
                    .into_iter()
                    .map(|(condition, value)| Ok((map(condition)?, map(value)?)))
                    .collect::<Result<_, E>>()?,
                otherwise: otherwise.map(|value| boxed(value, map)).transpose()?,
            },
            ExprKind::Between {
                expression,
                low,
                high,
                negated,
            } => ExprKind::Between {
                expression: boxed(expression, map)?,
                low: boxed(low, map)?,
                high: boxed(high, map)?,
                negated,
            },
            ExprKind::Like {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => ExprKind::Like {
                expression: boxed(expression, map)?,
                pattern: boxed(pattern, map)?,
                negated,
                escape_char,
                case_insensitive,
            },
            ExprKind::SimilarTo {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => ExprKind::SimilarTo {
                expression: boxed(expression, map)?,
                pattern: boxed(pattern, map)?,
                negated,
                escape_char,
                case_insensitive,
            },
            ExprKind::InList {
                expression,
                values,
                negated,
            } => ExprKind::InList {
                expression: boxed(expression, map)?,
                values: values.into_iter().map(map).collect::<Result<_, E>>()?,
                negated,
            },
            ExprKind::InSubquery {
                expression,
                query,
                negated,
            } => ExprKind::InSubquery {
                expression: boxed(expression, map)?,
                query,
                negated,
            },
        });
        Ok(self)
    }

    pub fn any(&self, predicate: &impl Fn(&ExprKind) -> bool) -> PlanResult<bool> {
        let node = self.kind.resolved("expression traversal")?;
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
        fn list(left: &[Expr], right: &[Expr]) -> PlanResult<bool> {
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
                self.kind.resolved("expression")?,
                other.kind.resolved("expression")?,
            ) {
                (ExprKind::Literal(a), ExprKind::Literal(b)) => a == b,
                (ExprKind::Attribute(a), ExprKind::Attribute(b)) => a == b,
                (ExprKind::ScalarSubquery(a), ExprKind::ScalarSubquery(b)) => {
                    // Copies of a bound subquery retain its output slot identities.
                    // Independently resolved queries always allocate distinct slots.
                    let a = &a.schema.resolved("subquery output")?.attributes;
                    let b = &b.schema.resolved("subquery output")?.attributes;
                    !a.is_empty() && a.iter().map(|a| a.id).eq(b.iter().map(|b| b.id))
                }
                (
                    ExprKind::OuterAttribute {
                        depth: a,
                        attribute: x,
                    },
                    ExprKind::OuterAttribute {
                        depth: b,
                        attribute: y,
                    },
                ) => a == b && x == y,
                (
                    ExprKind::ScalarFunction {
                        function: a,
                        arguments: x,
                    },
                    ExprKind::ScalarFunction {
                        function: b,
                        arguments: y,
                    },
                ) => a == b && list(x, y)?,
                (
                    ExprKind::Binary {
                        left: a,
                        operator: x,
                        right: b,
                    },
                    ExprKind::Binary {
                        left: c,
                        operator: y,
                        right: d,
                    },
                ) => x == y && a.equivalent(c)? && b.equivalent(d)?,
                (
                    ExprKind::Cast {
                        expression: a,
                        target: x,
                        safe: s,
                    },
                    ExprKind::Cast {
                        expression: b,
                        target: y,
                        safe: t,
                    },
                ) => x == y && s == t && a.equivalent(b)?,
                (ExprKind::Not(a), ExprKind::Not(b))
                | (ExprKind::IsNull(a), ExprKind::IsNull(b))
                | (ExprKind::IsNotNull(a), ExprKind::IsNotNull(b))
                | (ExprKind::Negative(a), ExprKind::Negative(b))
                | (ExprKind::WithMetadata(a), ExprKind::WithMetadata(b)) => a.equivalent(b)?,
                (
                    ExprKind::TruthTest {
                        expression: a,
                        value: x,
                        negated: n,
                    },
                    ExprKind::TruthTest {
                        expression: b,
                        value: y,
                        negated: m,
                    },
                ) => x == y && n == m && a.equivalent(b)?,
                (
                    ExprKind::Between {
                        expression: a,
                        low: l,
                        high: h,
                        negated: n,
                    },
                    ExprKind::Between {
                        expression: b,
                        low: k,
                        high: j,
                        negated: m,
                    },
                ) => n == m && a.equivalent(b)? && l.equivalent(k)? && h.equivalent(j)?,
                (
                    ExprKind::Like {
                        expression: a,
                        pattern: x,
                        negated: n,
                        escape_char: e,
                        case_insensitive: i,
                    },
                    ExprKind::Like {
                        expression: b,
                        pattern: y,
                        negated: m,
                        escape_char: f,
                        case_insensitive: j,
                    },
                )
                | (
                    ExprKind::SimilarTo {
                        expression: a,
                        pattern: x,
                        negated: n,
                        escape_char: e,
                        case_insensitive: i,
                    },
                    ExprKind::SimilarTo {
                        expression: b,
                        pattern: y,
                        negated: m,
                        escape_char: f,
                        case_insensitive: j,
                    },
                ) => n == m && e == f && i == j && a.equivalent(b)? && x.equivalent(y)?,
                (
                    ExprKind::InList {
                        expression: a,
                        values: x,
                        negated: n,
                    },
                    ExprKind::InList {
                        expression: b,
                        values: y,
                        negated: m,
                    },
                ) => n == m && a.equivalent(b)? && list(x, y)?,
                (
                    ExprKind::Case {
                        operand: a,
                        branches: x,
                        otherwise: p,
                    },
                    ExprKind::Case {
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
                    ExprKind::AggregateFunction {
                        function: a,
                        arguments: x,
                        distinct: d,
                        filter: f,
                        order_by: o,
                        null_treatment: n,
                    },
                    ExprKind::AggregateFunction {
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
                    ExprKind::WindowFunction {
                        function: a,
                        arguments: x,
                        partition_by: p,
                        order_by: o,
                        frame: f,
                        filter: c,
                        distinct: d,
                        null_treatment: n,
                    },
                    ExprKind::WindowFunction {
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
