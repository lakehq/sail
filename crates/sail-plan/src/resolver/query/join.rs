use std::sync::Arc;

use datafusion_common::tree_node::TreeNode;
use datafusion_common::{Column, JoinType, NullEquality};
use datafusion_expr::{build_join_schema, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_functions::expr_fn::coalesce;
use sail_common::spec;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

/// Returns `true` if the expression contains a Python scalar UDF anywhere in its tree.
fn expr_contains_python_udf(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarFunction(sf) => Ok(sf
            .func
            .inner()
            .as_any()
            .downcast_ref::<PySparkUDF>()
            .is_some()),
        _ => Ok(false),
    })
    .unwrap_or(false)
}

/// Returns a string representation of the join type suitable for error messages.
fn join_type_name(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Left => "LEFT OUTER",
        JoinType::Right => "RIGHT OUTER",
        JoinType::Full => "FULL OUTER",
        JoinType::LeftSemi => "LEFT SEMI",
        JoinType::LeftAnti => "LEFT ANTI",
        JoinType::RightSemi => "RIGHT SEMI",
        JoinType::RightAnti => "RIGHT ANTI",
        JoinType::Inner => "INNER",
        JoinType::LeftMark => "LEFT MARK",
        JoinType::RightMark => "RIGHT MARK",
    }
}

/// Splits an expression into its top-level AND conjuncts.
fn split_conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
            left,
            op: datafusion_expr::Operator::And,
            right,
        }) => {
            let mut parts = split_conjuncts(*left);
            parts.extend(split_conjuncts(*right));
            parts
        }
        other => vec![other],
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_join(
        &self,
        join: spec::Join,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Join {
            left,
            right,
            join_type,
            join_criteria,
            join_data_type,
        } = join;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        let join_type = match join_type {
            spec::JoinType::Inner => Some(JoinType::Inner),
            spec::JoinType::LeftOuter => Some(JoinType::Left),
            spec::JoinType::RightOuter => Some(JoinType::Right),
            spec::JoinType::FullOuter => Some(JoinType::Full),
            spec::JoinType::LeftSemi => Some(JoinType::LeftSemi),
            spec::JoinType::RightSemi => Some(JoinType::RightSemi),
            spec::JoinType::LeftAnti => Some(JoinType::LeftAnti),
            spec::JoinType::RightAnti => Some(JoinType::RightAnti),
            spec::JoinType::Cross => None,
        };

        match (join_type, join_criteria) {
            (None, Some(_)) => Err(PlanError::invalid("cross join with join criteria")),
            // When the join criteria are not specified, any join type has the semantics of a cross join.
            (Some(_), None) | (None, None) => {
                if join_data_type.is_some() {
                    return Err(PlanError::invalid("cross join with join data type"));
                }
                Ok(LogicalPlanBuilder::from(left).cross_join(right)?.build()?)
            }
            (Some(join_type), Some(spec::JoinCriteria::On(condition))) => {
                // Use Inner to build the schema for resolving the ON condition,
                // because the condition may reference columns from both sides.
                // Semi/anti/mark joins restrict the output schema, but the ON
                // condition still needs access to both sides.
                let join_schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &JoinType::Inner,
                )?);
                let condition = self
                    .resolve_expression(condition, &join_schema, state)
                    .await?
                    .unalias_nested()
                    .data;

                // Validate Python UDF usage in the join condition, matching
                // Spark's ExtractPythonUDFFromJoinCondition analysis rule.
                let conjuncts = split_conjuncts(condition.clone());
                let (udf_conjuncts, other_conjuncts): (Vec<_>, Vec<_>) =
                    conjuncts.into_iter().partition(expr_contains_python_udf);
                if !udf_conjuncts.is_empty() {
                    match join_type {
                        JoinType::Inner => {
                            // For inner joins, Python UDF conditions become a Filter on top of
                            // a cross join when there are no remaining equi-join conditions.
                            if other_conjuncts.is_empty() && !self.config.cross_join_enabled {
                                return Err(PlanError::AnalysisError(
                                    "Detected implicit cartesian product for INNER join between \
                                     logical plans. Please use the CROSS JOIN syntax to allow \
                                     cartesian products between these plans."
                                        .to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(PlanError::AnalysisError(format!(
                                "Python UDF in the ON clause of a {} JOIN.",
                                join_type_name(join_type)
                            )));
                        }
                    }
                }

                let plan = LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(condition))?
                    .build()?;
                Ok(plan)
            }
            (Some(join_type), Some(spec::JoinCriteria::Natural)) => {
                let left_names = Self::get_field_names(left.schema(), state)?;
                let right_names = Self::get_field_names(right.schema(), state)?;
                let using = left_names
                    .iter()
                    .filter(|name| right_names.contains(name))
                    .map(|x| x.clone().into())
                    .collect::<Vec<_>>();
                // We let the column resolver return errors when either plan contains
                // duplicated columns for the natural join key.
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
            (Some(join_type), Some(spec::JoinCriteria::Using(using))) => {
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
        }
    }

    fn resolve_query_join_using_columns(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        using: Vec<spec::Identifier>,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<(String, (Column, Column))>> {
        using
            .into_iter()
            .map(|name| {
                let left_column = self.resolve_one_column(left.schema(), name.as_ref(), state)?;
                let right_column = self.resolve_one_column(right.schema(), name.as_ref(), state)?;
                Ok((name.into(), (left_column, right_column)))
            })
            .collect()
    }

    fn resolve_query_join_using(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        join_columns: Vec<(String, (Column, Column))>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let (left_columns, right_columns) = join_columns
            .iter()
            .map(|(_, (left, right))| (left.clone(), right.clone()))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let builder = LogicalPlanBuilder::from(left).join_detailed(
            right,
            join_type,
            (left_columns.clone(), right_columns.clone()),
            None,
            NullEquality::NullEqualsNothing,
        )?;
        // Re-register join key columns as hidden fields so that subsequent
        // attribute resolution (by plan_id) can still find them.
        let hidden_columns = builder
            .schema()
            .columns()
            .into_iter()
            .map(|col| {
                if left_columns.iter().any(|x| x.name == col.name)
                    || right_columns.iter().any(|x| x.name == col.name)
                {
                    let info = state.get_field_info(col.name())?.clone();
                    let field_id = state.register_hidden_field_name(info.name());
                    for plan_id in info.plan_ids() {
                        state.register_plan_id_for_field(&field_id, plan_id)?;
                    }
                    Ok(Expr::Column(col).alias(field_id))
                } else {
                    Ok(Expr::Column(col))
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let builder = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        coalesce(vec![Expr::Column(left), Expr::Column(right)])
                            .alias(state.register_field_name(name))
                    })
                    .chain(hidden_columns);
                builder.project(projections)?
            }
            JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftMark
            | JoinType::RightMark => {
                let uses_right = matches!(
                    join_type,
                    JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
                );
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        let col = if uses_right { right } else { left };
                        Expr::Column(col).alias(state.register_field_name(name))
                    })
                    .chain(hidden_columns);
                builder.project(projections)?
            }
        };
        Ok(builder.build()?)
    }
}
