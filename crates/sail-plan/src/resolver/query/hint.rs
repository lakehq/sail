use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{Expr, Extension, LogicalPlan};
use log::warn;
use sail_common::spec;
use sail_logical_plan::join_hint::{JoinHintNode, JoinStrategy};
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_hint(
        &self,
        input: spec::QueryPlan,
        name: String,
        parameters: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;

        if self.config.disable_hints {
            return Ok(input);
        }
        if let Some(strategy) = JoinStrategy::from_hint(&name) {
            return self.resolve_join_hint(input, strategy, parameters, state);
        }
        let range = name.eq_ignore_ascii_case("REPARTITION_BY_RANGE");
        let rebalance = name.eq_ignore_ascii_case("REBALANCE");
        if rebalance && !self.config.adaptive_enabled {
            return Ok(input);
        }
        if name.eq_ignore_ascii_case("REPARTITION") || range || rebalance {
            let mut parameters = parameters.into_iter().peekable();
            let num_partitions = if parameters.peek().is_some_and(is_partition_count_literal) {
                let parameter = parameters
                    .next()
                    .ok_or_else(|| PlanError::internal("missing hint parameter"))?;
                Some(
                    self.resolve_hint_partition_count(&name, &[parameter], input.schema(), state)
                        .await?,
                )
            } else {
                None
            };
            let columns = parameters
                .map(|parameter| match parameter {
                    spec::Expr::UnresolvedAttribute { .. } => Ok(parameter),
                    spec::Expr::Literal(spec::Literal::Utf8 { value: Some(name) }) => {
                        let name = sail_sql_analyzer::expression::from_ast_object_name(
                            sail_sql_analyzer::parser::parse_object_name(&name)
                                .map_err(|e| PlanError::analysis(e.to_string()))?,
                        )?;
                        Ok(spec::Expr::UnresolvedAttribute {
                            name,
                            plan_id: None,
                            is_metadata_column: false,
                        })
                    }
                    _ => Err(PlanError::analysis(format!(
                        "{name} hint parameters must be column names after the optional partition count",
                    ))),
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let num_partitions = Some(num_partitions.unwrap_or(self.config.shuffle_partitions));
            if !columns.is_empty() {
                let columns = if range {
                    columns
                        .into_iter()
                        .map(|child| {
                            spec::Expr::SortOrder(spec::SortOrder {
                                child: Box::new(child),
                                direction: spec::SortDirection::Ascending,
                                null_ordering: spec::NullOrdering::NullsFirst,
                            })
                        })
                        .collect()
                } else {
                    columns
                };
                return self
                    .resolve_repartition_by_expression(input, columns, num_partitions, state)
                    .await;
            }
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ExplicitRepartitionNode::new(
                    Arc::new(input),
                    num_partitions,
                    ExplicitRepartitionKind::RoundRobin,
                    vec![],
                )),
            }));
        }

        let repartition_kind = if name.eq_ignore_ascii_case("COALESCE") {
            Some(ExplicitRepartitionKind::Coalesce)
        } else {
            None
        };

        if let Some(repartition_kind) = repartition_kind {
            let num_partitions = self
                .resolve_hint_partition_count(&name, &parameters, input.schema(), state)
                .await?;
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ExplicitRepartitionNode::new(
                    Arc::new(input),
                    Some(num_partitions),
                    repartition_kind,
                    vec![],
                )),
            }));
        }

        warn!("Unrecognized hint: {name}");
        Ok(input)
    }

    fn resolve_join_hint(
        &self,
        input: LogicalPlan,
        strategy: JoinStrategy,
        parameters: Vec<spec::Expr>,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        fn wrap(input: LogicalPlan, strategy: JoinStrategy) -> LogicalPlan {
            // The outer (leftmost SQL) hint overrides a previous hint on the same relation.
            let input = if let LogicalPlan::Extension(extension) = &input
                && let Some(hint) = extension.node.as_any().downcast_ref::<JoinHintNode>()
            {
                warn!(
                    "Join hint {:?} is overridden by {strategy:?}",
                    hint.strategy
                );
                Arc::clone(&hint.input)
            } else {
                Arc::new(input)
            };
            LogicalPlan::Extension(Extension {
                node: Arc::new(JoinHintNode { input, strategy }),
            })
        }
        if parameters.is_empty() {
            return Ok(wrap(input, strategy));
        }
        let names = parameters
            .into_iter()
            .map(|parameter| match parameter {
                spec::Expr::UnresolvedAttribute { name, .. } => Ok(name),
                spec::Expr::Literal(spec::Literal::Utf8 { value: Some(name) }) => {
                    Ok(sail_sql_analyzer::expression::from_ast_object_name(
                        sail_sql_analyzer::parser::parse_object_name(&name)
                            .map_err(|e| PlanError::analysis(e.to_string()))?,
                    )?)
                }
                _ => Err(PlanError::analysis(
                    "join hint parameters must be relation names",
                )),
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut matched = vec![false; names.len()];
        let input =
            input
                .transform_down(|plan| {
                    let target = if let LogicalPlan::Extension(extension) = &plan
                        && let Some(hint) = extension.node.as_any().downcast_ref::<JoinHintNode>()
                    {
                        hint.input.as_ref()
                    } else {
                        &plan
                    };
                    let reference = match target {
                        LogicalPlan::SubqueryAlias(alias) => Some(&alias.alias),
                        LogicalPlan::TableScan(scan) => Some(&scan.table_name),
                        _ => state.hint_relation(target),
                    };
                    if let Some(reference) = reference {
                        let parts = state.relation_name(reference);
                        let mut found = false;
                        for (name, matched) in names.iter().zip(&mut matched) {
                            if name.parts().len() <= parts.len()
                                && name.parts().iter().rev().zip(parts.iter().rev()).all(
                                    |(a, b)| {
                                        if self.config.case_sensitive {
                                            a.as_ref() == *b
                                        } else {
                                            a.as_ref().eq_ignore_ascii_case(b)
                                        }
                                    },
                                )
                            {
                                *matched = true;
                                found = true;
                            }
                        }
                        return Ok(Transformed::new(
                            if found { wrap(plan, strategy) } else { plan },
                            found,
                            TreeNodeRecursion::Jump,
                        ));
                    }
                    if let LogicalPlan::Extension(extension) = &plan
                        && extension.node.as_any().is::<JoinHintNode>()
                    {
                        return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
                    }
                    Ok(Transformed::no(plan))
                })?
                .data;
        for (name, matched) in names.iter().zip(matched) {
            if !matched {
                warn!("Relation {name:?} was not found for join hint {strategy:?}");
            }
        }
        Ok(input)
    }

    async fn resolve_hint_partition_count(
        &self,
        hint_name: &str,
        parameters: &[spec::Expr],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<usize> {
        let hint_name = hint_name.to_uppercase();
        let [parameter] = parameters else {
            return Err(PlanError::analysis(format!(
                "{hint_name} hint requires exactly one partition count"
            )));
        };

        let expr = self
            .resolve_expression(parameter.clone(), schema, state)
            .await?;
        let value = literal_partition_count(&hint_name, &expr)?;
        if value < 1 {
            return Err(PlanError::invalid(format!(
                "{hint_name} hint requires at least one partition"
            )));
        }
        Ok(value)
    }
}

fn is_partition_count_literal(expr: &spec::Expr) -> bool {
    // SQL signs are represented as unary functions before expression resolution.
    let expr = if let spec::Expr::UnresolvedFunction(function) = expr
        && matches!(function.function_name.parts(), [name] if matches!(name.as_ref(), "+" | "-"))
        && let [argument] = function.arguments.as_slice()
    {
        argument
    } else {
        expr
    };
    // Sail also recognizes Int64 counts; Spark only recognizes Int8/Int16/Int32.
    matches!(
        expr,
        spec::Expr::Literal(
            spec::Literal::Int8 { .. }
                | spec::Literal::Int16 { .. }
                | spec::Literal::Int32 { .. }
                | spec::Literal::Int64 { .. }
        )
    )
}

// Int64 hint counts are a Sail extension: plans use usize and worker messages use u64.
// Accepting the type does not guarantee that an arbitrarily large count is executable.
fn literal_partition_count(hint_name: &str, expr: &Expr) -> PlanResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int16(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int32(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int64(Some(value)), _metadata) => usize::try_from(*value)
            .map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint partition count is out of range"))
            }),
        _ => Err(PlanError::analysis(format!(
            "{hint_name} hint partition count must be an integer"
        ))),
    }
}
