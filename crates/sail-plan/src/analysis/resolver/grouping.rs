use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

const MAX_GROUPING_SETS: usize = 65536;

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn grouping_requests(
        &mut self,
        requests: Vec<spec::Expr>,
        projections: &[Option<NamedExpression>],
        schema: &PlanSchema,
    ) -> PlanResult<(Vec<NamedExpression>, Option<Vec<Vec<usize>>>)> {
        let has_sets = requests.iter().any(|r| {
            matches!(
                r,
                spec::Expr::Rollup(_) | spec::Expr::Cube(_) | spec::Expr::GroupingSets(_)
            )
        });
        let mut keys = vec![];
        let mut sets = vec![vec![]];
        for request in requests {
            let partial = self
                .grouping_request(request, projections, schema, &mut keys)
                .await?;
            if sets.len().saturating_mul(partial.len()) > MAX_GROUPING_SETS {
                return Err(PlanError::invalid("too many grouping sets"));
            }
            let mut combined = vec![];
            for left in &sets {
                for right in &partial {
                    let mut set = left.clone();
                    for index in right {
                        if !set.contains(index) {
                            set.push(*index);
                        }
                    }
                    combined.push(set);
                }
            }
            sets = combined;
        }
        if has_sets && keys.len() > 64 {
            return Err(PlanError::invalid(
                "grouping set size cannot be greater than 64",
            ));
        }
        Ok((keys, has_sets.then_some(sets)))
    }

    #[async_recursion::async_recursion]
    async fn grouping_request(
        &mut self,
        request: spec::Expr,
        projections: &[Option<NamedExpression>],
        schema: &PlanSchema,
        keys: &mut Vec<NamedExpression>,
    ) -> PlanResult<Vec<Vec<usize>>> {
        match request {
            spec::Expr::GroupingSets(sets) => {
                if sets.len() > MAX_GROUPING_SETS {
                    return Err(PlanError::invalid("too many grouping sets"));
                }
                let mut result = vec![];
                for requests in sets {
                    let mut set = vec![];
                    for request in requests {
                        let index = self
                            .grouping_key(request, projections, schema, keys)
                            .await?;
                        if !set.contains(&index) {
                            set.push(index);
                        }
                    }
                    result.push(set);
                }
                Ok(result)
            }
            request @ (spec::Expr::Rollup(_) | spec::Expr::Cube(_)) => {
                let cube = matches!(request, spec::Expr::Cube(_));
                let (spec::Expr::Rollup(requests) | spec::Expr::Cube(requests)) = request else {
                    unreachable!()
                };
                let mut indices = vec![];
                for request in requests {
                    indices.push(
                        self.grouping_key(request, projections, schema, keys)
                            .await?,
                    );
                }
                if cube {
                    let size = 1usize
                        .checked_shl(indices.len() as u32)
                        .filter(|n| *n <= MAX_GROUPING_SETS)
                        .ok_or_else(|| PlanError::invalid("too many grouping sets"))?;
                    Ok((0..size)
                        .map(|mask| {
                            indices
                                .iter()
                                .enumerate()
                                .filter_map(|(i, value)| (mask & (1 << i) != 0).then_some(*value))
                                .collect()
                        })
                        .collect())
                } else {
                    Ok((0..=indices.len())
                        .map(|length| indices[..length].to_vec())
                        .collect())
                }
            }
            request => Ok(vec![vec![
                self.grouping_key(request, projections, schema, keys)
                    .await?,
            ]]),
        }
    }

    #[async_recursion::async_recursion]
    async fn grouping_key(
        &mut self,
        request: spec::Expr,
        projections: &[Option<NamedExpression>],
        schema: &PlanSchema,
        keys: &mut Vec<NamedExpression>,
    ) -> PlanResult<usize> {
        let value = if let Some(position) = super::aggregate_request::ordinal(&request) {
            if position < 1 || position as usize > projections.len() {
                return Err(PlanError::invalid(format!(
                    "Cannot resolve column position {position}. Valid positions are 1 to {}.",
                    projections.len()
                )));
            }
            let projection = projections[position as usize - 1].as_ref().ok_or_else(|| {
                PlanError::analysis("grouping position references an unresolved projection")
            })?;
            NamedExpression {
                name: projection.name.clone(),
                expression: self.copy_expression(&projection.expression)?,
            }
        } else {
            self.expression(request.clone(), schema).await?
        };
        if value.expression.any(&|node| {
            matches!(
                node,
                ExpressionNode::AggregateFunction { .. } | ExpressionNode::WindowFunction { .. }
            )
        })? {
            return Err(PlanError::analysis(
                "aggregate or window function is not allowed in GROUP BY",
            ));
        }
        for (index, key) in keys.iter().enumerate() {
            if key.expression.equivalent(&value.expression)? {
                if super::aggregate_request::ordinal(&request).is_none() {
                    self.expression_replacements.push((
                        request,
                        NamedExpression {
                            name: key.name.clone(),
                            expression: key.expression.clone(),
                        },
                    ));
                }
                return Ok(index);
            }
        }
        let index = keys.len();
        if super::aggregate_request::ordinal(&request).is_none() {
            // Repeated grouping requests share the analyzed kernel identity,
            // including stateful functions that allocate an instance per call.
            self.expression_replacements.push((
                request,
                NamedExpression {
                    name: value.name.clone(),
                    expression: value.expression.clone(),
                },
            ));
        }
        keys.push(value);
        Ok(index)
    }

    pub(super) fn grouping_mask(
        &mut self,
        name: &str,
        arguments: &[LogicalExpression],
        grouping: &[Projection],
        id: Option<&Attribute>,
    ) -> PlanResult<LogicalExpression> {
        let id = id.ok_or_else(|| PlanError::analysis("[UNSUPPORTED_GROUPING_EXPRESSION] grouping functions can only be used with grouping sets, cube, or rollup"))?;
        let value = self.reference(id)?;
        let kind = id
            .result_type
            .resolved("grouping ID type")?
            .data_type
            .clone();
        let literal = |number| ScalarValue::UInt64(Some(number)).cast_to(&kind);
        if name == "grouping_id" {
            if !arguments.is_empty() {
                let mut matches = arguments.len() == grouping.len();
                for (argument, key) in arguments.iter().zip(grouping) {
                    matches &= argument.equivalent(&key.expression)?;
                }
                if !matches {
                    return Err(PlanError::analysis(
                        "[GROUPING_ID_COLUMN_MISMATCH] grouping_id columns do not match grouping columns",
                    ));
                }
            }
            // Duplicate set ordinals are backend bookkeeping, not grouping bits.
            let bits = if grouping.len() == 64 {
                u64::MAX
            } else {
                (1u64 << grouping.len()) - 1
            };
            let mask = self.literal(literal(bits)?);
            let value = self.binary(value, Operator::BitwiseAnd, mask)?;
            return self.cast(value, DataType::Int64);
        }
        let [argument] = arguments else {
            return Err(PlanError::invalid("grouping requires exactly one argument"));
        };
        let mut position = None;
        for (i, key) in grouping.iter().enumerate() {
            if argument.equivalent(&key.expression)? {
                position = Some(i);
                break;
            }
        }
        let position = position.ok_or_else(|| {
            PlanError::analysis(
                "[GROUPING_COLUMN_MISMATCH] grouping column is not in grouping columns",
            )
        })?;
        let shift = grouping.len() - 1 - position;
        let mask = self.literal(literal(1u64 << shift)?);
        let value = self.binary(value, Operator::BitwiseAnd, mask)?;
        let shift = self.literal(literal(shift as u64)?);
        let value = self.binary(value, Operator::BitwiseShiftRight, shift)?;
        self.cast(value, DataType::Int8)
    }
}
