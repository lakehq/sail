use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, Spans, internal_err};
use datafusion_expr::expr::Alias;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Projection, Subquery, build_join_schema, lit,
};
use sail_common::spec;
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_function::scalar::misc::scalar_subquery_result::ScalarSubqueryResult;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_lateral_join(
        &self,
        left: spec::QueryPlan,
        right: spec::QueryPlan,
        join_condition: Option<spec::Expr>,
        join_type: spec::JoinType,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let left = self.resolve_query_plan(left, state).await?;

        // Resolve the right side in a new query scope so that column references
        // to the left side are treated as outer references (OuterReferenceColumn).
        let right = {
            let schema = if left.schema().fields().is_empty() {
                state
                    .get_outer_query_schema()
                    .cloned()
                    .unwrap_or_else(|| left.schema().clone())
            } else {
                left.schema().clone()
            };
            let mut scope = state.enter_query_scope(schema);
            self.resolve_query_plan(right, scope.state()).await?
        };

        if has_volatile_expressions(&right)?
            && !lateral_max_rows(&left)?.is_some_and(|rows| rows <= 1)
        {
            return Err(PlanError::analysis(
                "[UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.NON_DETERMINISTIC_LATERAL_SUBQUERIES] \
                 Non-deterministic lateral subqueries are not supported when joining with \
                 outer relations that produce more than one row",
            ));
        }

        // FROM LATERAL (...) without a preceding relation has a one-row,
        // zero-column left input. Preserve the enclosing correlation scope.
        if matches!(&left, LogicalPlan::EmptyRelation(empty) if empty.produce_one_row && empty.schema.fields().is_empty())
            && matches!(join_type, spec::JoinType::Inner | spec::JoinType::Cross)
            && join_condition.is_none()
        {
            return Ok(right);
        }
        let outer_ref_columns = right.all_out_ref_exprs();

        // If the right plan contains outer references deeper than the top-level
        // Projection (e.g. in a Filter or Aggregate), wrap it as a Subquery node
        // so DataFusion's DecorrelateLateralJoin optimizer can process it.
        //
        // Before wrapping, collapse redundant Projection layers introduced by the
        // opaque field ID rename mechanism.
        let right = if !outer_ref_columns.is_empty() {
            let simplified = collapse_projections(right)?;
            LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(simplified),
                outer_ref_columns,
                spans: Spans::new(),
            })
        } else {
            right
        };

        let df_join_type = match join_type {
            spec::JoinType::Inner | spec::JoinType::Cross => datafusion_common::JoinType::Inner,
            spec::JoinType::LeftOuter => datafusion_common::JoinType::Left,
            spec::JoinType::RightOuter => datafusion_common::JoinType::Right,
            spec::JoinType::FullOuter => datafusion_common::JoinType::Full,
            spec::JoinType::LeftSemi => datafusion_common::JoinType::LeftSemi,
            spec::JoinType::LeftAnti => datafusion_common::JoinType::LeftAnti,
            spec::JoinType::RightSemi => datafusion_common::JoinType::RightSemi,
            spec::JoinType::RightAnti => datafusion_common::JoinType::RightAnti,
        };

        let join_schema = Arc::new(build_join_schema(
            left.schema(),
            right.schema(),
            &df_join_type,
        )?);

        let condition = if let Some(cond) = join_condition {
            Some(
                self.resolve_expression(cond, &join_schema, state)
                    .await?
                    .unalias_nested()
                    .data,
            )
        } else {
            None
        };

        if condition
            .as_ref()
            .map(is_nondeterministic)
            .transpose()?
            .unwrap_or(false)
        {
            return Err(PlanError::analysis(
                "[UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.LATERAL_JOIN_CONDITION_NON_DETERMINISTIC] \
                 Lateral join condition must be deterministic",
            ));
        }

        // Track whether the right side was wrapped as a Subquery so we can add
        // an output Projection below to flatten the schema.
        let wrapped_as_subquery = matches!(&right, LogicalPlan::Subquery(_));

        // For inner joins without a condition, use cross join. Otherwise use join_on.
        let plan =
            if condition.is_none() && matches!(df_join_type, datafusion_common::JoinType::Inner) {
                LogicalPlanBuilder::from(left).cross_join(right)?.build()?
            } else {
                LogicalPlanBuilder::from(left)
                    .join_on(
                        right,
                        df_join_type,
                        Some(condition.unwrap_or_else(|| datafusion_expr::lit(true))),
                    )?
                    .build()?
            };

        // When the right side is a Subquery, add an explicit Projection to expose
        // all join output columns. This ensures the Subquery's internal schema
        // is properly flattened into the join result.
        let plan = if wrapped_as_subquery {
            let original_columns: Vec<Expr> = plan
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            LogicalPlanBuilder::from(plan)
                .project(original_columns)?
                .build()?
        } else {
            plan
        };

        Ok(plan)
    }
}

// Resolution precedes DataFusion's limit coercion, so SQL integer literals may
// still be Int32. Normalize constant bounds only on the cardinality-check copy.
fn lateral_max_rows(plan: &LogicalPlan) -> datafusion_common::Result<Option<usize>> {
    let normalized = plan
        .clone()
        .transform_up(|plan| {
            let LogicalPlan::Limit(mut limit) = plan else {
                return Ok(Transformed::no(plan));
            };
            if let Some(fetch) = limit.fetch.as_deref_mut()
                && !fetch.is_volatile()
                && let Ok(value) = LiteralEvaluator::new().evaluate(fetch)
                && let Ok(value) = LiteralValue(&value).try_to_i64()
            {
                *fetch = lit(value);
            }
            Ok(Transformed::yes(LogicalPlan::Limit(limit)))
        })?
        .data;
    Ok(normalized.max_rows())
}

fn is_nondeterministic(expr: &Expr) -> datafusion_common::Result<bool> {
    expr.exists(|expr| {
        // An evaluation-sensitive scalar-cardinality assertion is not randomness.
        Ok(expr.is_volatile_node()
            && !matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<ScalarSubqueryResult>()))
    })
}

fn has_volatile_expressions(plan: &LogicalPlan) -> datafusion_common::Result<bool> {
    let mut volatile = false;
    plan.apply_with_subqueries(|node| {
        for expr in node.expressions() {
            volatile |= is_nondeterministic(&expr)?;
        }
        Ok(if volatile {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(volatile)
}

#[cfg(test)]
mod tests {
    use datafusion_common::{Column, Result};
    use datafusion_expr::{Expr, LogicalPlanBuilder, col, lit};

    use super::{collapse_projections, lateral_max_rows};

    #[test]
    fn lateral_cardinality_accepts_uncoerced_limit() -> Result<()> {
        let input = LogicalPlanBuilder::values(vec![vec![lit(1)], vec![lit(2)]])?.build()?;
        let limited = datafusion_expr::LogicalPlan::Limit(datafusion_expr::Limit {
            skip: None,
            fetch: Some(Box::new(lit(1_i32))),
            input: std::sync::Arc::new(input.clone()),
        });
        assert_eq!(lateral_max_rows(&limited)?, Some(1));
        assert_eq!(lateral_max_rows(&input)?, Some(2));
        Ok(())
    }

    #[test]
    fn rename_projections_preserve_alias_boundary() -> Result<()> {
        let source = LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])?
            .project(vec![col("column1").alias("id")])?
            .alias("?table?")?
            .project(vec![
                Expr::Column(Column::new(Some("?table?"), "id")).alias("id"),
            ])?
            .alias("i")?
            .project(vec![col("i.id").alias("field")])?
            .build()?;
        assert_eq!(collapse_projections(source.clone())?, source);
        Ok(())
    }

    #[test]
    fn adjacent_rename_projections_still_collapse() -> Result<()> {
        let source = LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])?.build()?;
        let plan = LogicalPlanBuilder::from(source.clone())
            .project(vec![col("column1").alias("id")])?
            .project(vec![col("id").alias("field")])?
            .build()?;
        let expected = LogicalPlanBuilder::from(source)
            .project(vec![col("column1").alias("field")])?
            .build()?;
        assert_eq!(collapse_projections(plan)?, expected);
        Ok(())
    }
}

fn is_rename_projection(proj: &Projection) -> bool {
    proj.expr.iter().all(|e| {
        matches!(e, Expr::Alias(Alias { expr, .. }) if matches!(expr.as_ref(), Expr::Column(_)))
    })
}

/// Collapse redundant Projection layers and duplicate SubqueryAlias nodes
/// in a plan tree. Traverses bottom-up so inner layers merge first.
///
/// Rule 1: Adjacent Projections where both are pure rename (Col AS name)
///         are merged into one. If the result is identity, it is removed.
/// Rule 2: Duplicate SubqueryAlias(t) over SubqueryAlias(t) is collapsed
///         into one.
fn collapse_projections(plan: LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.transform_up(|node| {
        match node {
            // Rule 1: Merge adjacent rename Projections
            LogicalPlan::Projection(outer) if is_rename_projection(&outer) => {
                match outer.input.as_ref() {
                    LogicalPlan::Projection(inner) if is_rename_projection(inner) => {
                        let LogicalPlan::Projection(inner) = Arc::unwrap_or_clone(outer.input)
                        else {
                            return internal_err!("expected Projection input");
                        };

                        let inner_map = build_output_name_map(&inner.expr);
                        let merged = inline_projection(&outer.expr, &inner_map);

                        if is_identity_projection(&merged, inner.input.schema()) {
                            Ok(Transformed::yes(Arc::unwrap_or_clone(inner.input)))
                        } else {
                            Ok(Transformed::yes(LogicalPlan::Projection(
                                Projection::try_new(merged, inner.input)?,
                            )))
                        }
                    }
                    // Do not merge through SubqueryAlias: its input columns have
                    // different qualifiers from the columns exposed to the outer projection.
                    _ => Ok(Transformed::no(LogicalPlan::Projection(outer))),
                }
            }

            // Rule 2: Remove duplicate SubqueryAlias
            LogicalPlan::SubqueryAlias(ref outer)
                if matches!(outer.input.as_ref(), LogicalPlan::SubqueryAlias(inner)
                    if inner.alias == outer.alias) =>
            {
                Ok(Transformed::yes(Arc::unwrap_or_clone(match node {
                    LogicalPlan::SubqueryAlias(sa) => sa.input,
                    _ => return internal_err!("expected SubqueryAlias"),
                })))
            }

            other => Ok(Transformed::no(other)),
        }
    })
    .map(|t| t.data)
}

/// Build a map from output name to source expression for a Projection's exprs.
fn build_output_name_map(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|e| match e {
            Expr::Alias(Alias { expr, name, .. }) => Some((name.clone(), *expr.clone())),
            Expr::Column(col) => Some((col.name.clone(), e.clone())),
            _ => None,
        })
        .collect()
}

/// For each expr in outer, replace Column references using the inner map,
/// keeping outer's alias.
fn inline_projection(outer_exprs: &[Expr], inner_map: &HashMap<String, Expr>) -> Vec<Expr> {
    outer_exprs
        .iter()
        .map(|expr| match expr {
            Expr::Alias(Alias {
                expr: inner_expr,
                relation,
                name,
                metadata,
            }) => {
                let resolved = match inner_expr.as_ref() {
                    Expr::Column(col) => inner_map
                        .get(&col.name)
                        .cloned()
                        .unwrap_or(*inner_expr.clone()),
                    other => other.clone(),
                };
                Expr::Alias(Alias {
                    expr: Box::new(resolved),
                    relation: relation.clone(),
                    name: name.clone(),
                    metadata: metadata.clone(),
                })
            }
            Expr::Column(col) => inner_map.get(&col.name).cloned().unwrap_or(expr.clone()),
            other => other.clone(),
        })
        .collect()
}

/// Check if every expr is Col(x) AS x (identity) matching the input schema.
fn is_identity_projection(exprs: &[Expr], input_schema: &DFSchemaRef) -> bool {
    let input_columns: Vec<Column> = input_schema.columns();

    if exprs.len() != input_columns.len() {
        return false;
    }

    exprs
        .iter()
        .zip(input_columns.iter())
        .all(|(expr, input_col)| match expr {
            Expr::Alias(Alias {
                expr: inner,
                name,
                relation,
                ..
            }) => {
                if let Expr::Column(col) = inner.as_ref() {
                    col == input_col && name == &col.name && *relation == col.relation
                } else {
                    false
                }
            }
            Expr::Column(col) => col == input_col,
            _ => false,
        })
}
