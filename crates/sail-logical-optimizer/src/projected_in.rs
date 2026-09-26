use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::Arc;

use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::{Optimizer, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, DFSchemaRef, Result, ScalarValue, plan_err};
use datafusion_expr::expr::InSubquery;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{
    Distinct, Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Projection,
    SubqueryAlias, Union, expr_fn, lit,
};

/// Spark folds expressions before replacing projected IN with an existence join.
#[derive(Debug)]
pub struct RewriteProjectedIn {
    normalization: Optimizer,
}

impl RewriteProjectedIn {
    pub fn new(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self {
            normalization: Optimizer::with_rules(rules),
        }
    }
}

impl OptimizerRule for RewriteProjectedIn {
    fn name(&self) -> &str {
        "rewrite_projected_in"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut found = false;
        let mut normalize = false;
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::Projection(projection) = plan {
                for expr in &projection.expr {
                    expr.apply(|expr| {
                        if let Expr::InSubquery(subquery) = expr
                            && subquery.subquery.outer_ref_columns.is_empty()
                        {
                            found = true;
                            normalize |= match subquery.expr.as_ref() {
                                Expr::Literal(_, _) => false,
                                // Scalar subquery fields can be nonnullable even
                                // though an empty scalar result is NULL.
                                expr if !expr.nullable(projection.input.schema())?
                                    && !expr.exists(|expr| {
                                        Ok(matches!(expr, Expr::ScalarSubquery(_)))
                                    })? =>
                                {
                                    false
                                }
                                Expr::Column(_) => !has_only_column_producers(&projection.input)?,
                                _ => true,
                            };
                        }
                        Ok(if normalize {
                            TreeNodeRecursion::Stop
                        } else {
                            TreeNodeRecursion::Continue
                        })
                    })?;
                }
            }
            Ok(if normalize {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if !found {
            return Ok(Transformed::no(plan));
        }

        // Literals need no folding, a nonnullable operand cannot fold to NULL,
        // and ordinary data columns have no producer constants to expose.
        // Avoid an extra optimizer pass for these queries. Computed operands
        // use the real query context.
        let plan = if normalize {
            self.normalization.optimize(plan, config, |_, _| {})?
        } else {
            plan
        };
        let plan = plan
            .transform_up_with_subqueries(|plan| rewrite_projection(plan, config))?
            .data;
        Ok(Transformed::yes(plan))
    }
}

/// Prove that a nullable column comes only from data or column renames. Check
/// every UNION arm, and leave joins and computed producers to normalization.
fn has_only_column_producers(plan: &LogicalPlan) -> Result<bool> {
    let mut columns_only = true;
    plan.apply(|plan| {
        columns_only &= match plan {
            LogicalPlan::Projection(projection) => projection.expr.iter().all(|expr| {
                let mut expr = expr;
                while let Expr::Alias(alias) = expr {
                    expr = &alias.expr;
                }
                matches!(expr, Expr::Column(_))
            }),
            LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(Distinct::All(_)) => true,
            _ => plan.inputs().is_empty(),
        };
        Ok(if columns_only {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(columns_only)
}

fn has_uncorrelated_in(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::InSubquery(subquery)
            if subquery.subquery.outer_ref_columns.is_empty()))
    })
}

fn simplifier(schema: DFSchemaRef, config: &dyn OptimizerConfig) -> ExprSimplifier {
    ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(schema)
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build(),
    )
}

fn replace_constants(expr: Expr, constants: &HashMap<Column, Expr>) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(value) = constants.get(column)
        {
            return Ok(Transformed::yes(value.clone()));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Only producer expressions are constants: a nullable VALUES/table column is
/// still a column, even when every row happens to contain NULL. As in Spark's
/// FoldablePropagation, UNION and null-producing outer-join sides are boundaries.
fn constant_columns(
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
    needed: &HashSet<Column>,
) -> Result<HashMap<Column, Expr>> {
    let (expressions, input) = match plan {
        LogicalPlan::Projection(projection) => (
            projection.expr.iter().collect::<Vec<_>>(),
            &projection.input,
        ),
        LogicalPlan::Aggregate(aggregate) => (
            aggregate
                .group_expr
                .iter()
                .chain(&aggregate.aggr_expr)
                .collect(),
            &aggregate.input,
        ),
        LogicalPlan::SubqueryAlias(alias) => {
            let mapping = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns());
            let input_needed = mapping
                .filter_map(|(input, output)| needed.contains(&output).then_some(input))
                .collect();
            let mut constants = constant_columns(&alias.input, config, &input_needed)?;
            return Ok(alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
                .filter_map(|(input, output)| constants.remove(&input).map(|value| (output, value)))
                .collect());
        }
        LogicalPlan::Filter(filter) => return constant_columns(&filter.input, config, needed),
        LogicalPlan::Sort(sort) => return constant_columns(&sort.input, config, needed),
        LogicalPlan::Limit(limit) => return constant_columns(&limit.input, config, needed),
        LogicalPlan::Repartition(repartition) => {
            return constant_columns(&repartition.input, config, needed);
        }
        LogicalPlan::Window(window) => return constant_columns(&window.input, config, needed),
        LogicalPlan::Distinct(Distinct::All(input)) => {
            return constant_columns(input, config, needed);
        }
        LogicalPlan::Join(join) => {
            let mut constants = HashMap::new();
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::LeftMark
            ) {
                constants.extend(constant_columns(&join.left, config, needed)?);
            }
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::RightMark
            ) {
                constants.extend(constant_columns(&join.right, config, needed)?);
            }
            return Ok(constants);
        }
        _ => return Ok(HashMap::new()),
    };
    if expressions.len() != plan.schema().fields().len() {
        return Ok(HashMap::new());
    }
    let selected = expressions
        .into_iter()
        .zip(plan.schema().columns())
        .filter(|(_, column)| needed.contains(column))
        .collect::<Vec<_>>();
    let input_needed = selected
        .iter()
        .flat_map(|(expr, _)| expr.column_refs().into_iter().cloned())
        .collect();
    let constants = constant_columns(input, config, &input_needed)?;
    let simplifier = simplifier(Arc::clone(input.schema()), config);
    let mut output = HashMap::new();
    for (expr, column) in selected {
        let expr = simplifier.simplify(replace_constants(expr.clone().unalias(), &constants)?)?;
        if matches!(expr, Expr::Literal(_, _)) {
            output.insert(column, expr);
        }
    }
    Ok(output)
}

fn has_volatile_expression(expr: &Expr) -> Result<bool> {
    if expr.is_volatile() {
        return Ok(true);
    }
    expr.exists(|expr| {
        let subquery = match expr {
            Expr::InSubquery(subquery) => &subquery.subquery,
            Expr::ScalarSubquery(subquery) => subquery,
            Expr::Exists(exists) => &exists.subquery,
            _ => return Ok(false),
        };
        let mut volatile = false;
        subquery.subquery.apply_with_subqueries(|plan| {
            volatile |= plan.expressions().iter().any(Expr::is_volatile);
            Ok(if volatile {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(volatile)
    })
}

fn union_branches(plan: &LogicalPlan) -> Result<Option<Vec<LogicalPlan>>> {
    match plan {
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                if has_volatile_expression(expr)? {
                    return Ok(None);
                }
            }
            union_branches(&projection.input)?
                .map(|inputs| {
                    inputs
                        .into_iter()
                        .map(|input| {
                            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                                projection.expr.clone(),
                                Arc::new(input),
                                Arc::clone(&projection.schema),
                            )?))
                        })
                        .collect()
                })
                .transpose()
        }
        LogicalPlan::Union(union) => {
            let inputs = union
                .inputs
                .iter()
                .map(|input| {
                    let expr = input
                        .schema()
                        .columns()
                        .into_iter()
                        .zip(union.schema.columns())
                        .map(|(input, output)| {
                            Expr::Column(input).alias_qualified(output.relation, output.name)
                        })
                        .collect();
                    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                        expr,
                        Arc::clone(input),
                        Arc::clone(&union.schema),
                    )?))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(inputs))
        }
        LogicalPlan::SubqueryAlias(alias) => union_branches(&alias.input)?
            .map(|inputs| {
                inputs
                    .into_iter()
                    .map(|input| {
                        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                            Arc::new(input),
                            alias.alias.clone(),
                        )?))
                    })
                    .collect()
            })
            .transpose(),
        _ => Ok(None),
    }
}

fn rewrite_projection(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(Transformed::no(plan));
    };
    let mut found = false;
    for expr in &projection.expr {
        found |= has_uncorrelated_in(expr)?;
    }
    if !found {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    }
    let needed = projection
        .expr
        .iter()
        .filter(|expr| has_uncorrelated_in(expr).unwrap_or(false))
        .flat_map(|expr| expr.column_refs().into_iter().cloned())
        .collect();
    // Spark pushes deterministic projections through UNION before folding their
    // operands. Only split the joins when branch constants affect these IN expressions.
    let mut deterministic = true;
    for expr in &projection.expr {
        deterministic &= !has_volatile_expression(expr)?;
    }
    if deterministic && let Some(inputs) = union_branches(&projection.input)? {
        let mut needs_null_propagation = false;
        for input in &inputs {
            let constants = constant_columns(input, config, &needed)?;
            if constants.is_empty() {
                continue;
            }
            let simplifier = simplifier(Arc::clone(input.schema()), config);
            for expr in &projection.expr {
                needs_null_propagation |= expr.exists(|expr| {
                    let Expr::InSubquery(subquery) = expr else {
                        return Ok(false);
                    };
                    if !subquery.subquery.outer_ref_columns.is_empty()
                        || matches!(subquery.expr.as_ref(), Expr::Literal(value, _) if value.is_null())
                    {
                        return Ok(false);
                    }
                    let operand = simplifier.simplify(replace_constants(*subquery.expr.clone(), &constants)?)?;
                    Ok(matches!(operand, Expr::Literal(value, _) if value.is_null()))
                })?;
            }
        }
        if needs_null_propagation {
            let inputs = inputs
                .into_iter()
                .map(|input| {
                    let branch = LogicalPlan::Projection(Projection::try_new_with_schema(
                        projection.expr.clone(),
                        Arc::new(input),
                        Arc::clone(&projection.schema),
                    )?);
                    Ok(Arc::new(rewrite_projection(branch, config)?.data))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Transformed::yes(LogicalPlan::Union(Union {
                inputs,
                schema: projection.schema,
            })));
        }
    }
    let constants = constant_columns(&projection.input, config, &needed)?;
    let simplifier = simplifier(Arc::clone(projection.input.schema()), config);
    let mut rewriter = InRewriter {
        plan: Arc::unwrap_or_clone(projection.input),
        config,
    };
    let expr = projection
        .expr
        .into_iter()
        .map(|expr| {
            if !has_uncorrelated_in(&expr)? {
                return Ok(expr);
            }
            let name = NamePreserver::new_for_projection().save(&expr);
            let expr = simplifier.simplify(replace_constants(expr, &constants)?)?;
            Ok(name.restore(expr.rewrite(&mut rewriter)?.data))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new_with_schema(expr, Arc::new(rewriter.plan), projection.schema)?,
    )))
}

struct InRewriter<'a> {
    plan: LogicalPlan,
    config: &'a dyn OptimizerConfig,
}

impl TreeNodeRewriter for InRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::Not(child) = &expr
            && let Expr::InSubquery(subquery) = child.as_ref()
            && subquery.subquery.outer_ref_columns.is_empty()
        {
            let mut subquery = subquery.clone();
            subquery.negated = !subquery.negated;
            return Ok(Transformed::yes(Expr::InSubquery(subquery)));
        }
        Ok(Transformed::no(expr))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let Expr::InSubquery(subquery) = expr else {
            return Ok(Transformed::no(expr));
        };
        if !subquery.subquery.outer_ref_columns.is_empty() {
            return Ok(Transformed::no(Expr::InSubquery(subquery)));
        }
        let InSubquery {
            expr,
            subquery,
            negated,
        } = subquery;
        let columns = subquery.subquery.schema().columns();
        let [column] = columns.as_slice() else {
            return plan_err!("IN subquery must return exactly one column");
        };
        let nullable = expr.nullable(self.plan.schema().as_ref())?
            || subquery.subquery.schema().field(0).is_nullable();
        let null_literal = matches!(expr.as_ref(), Expr::Literal(value, _) if value.is_null());
        let alias = self.config.alias_generator().next("__sail_in");
        let predicate = (*expr).eq(Expr::Column(Column::new(Some(alias.clone()), &column.name)));
        let predicate = if negated || null_literal {
            predicate.clone().or(predicate.is_null())
        } else {
            predicate
        };
        let query = LogicalPlanBuilder::from(subquery.subquery)
            .alias(alias.clone())?
            .build()?;
        self.plan = LogicalPlanBuilder::from(mem::take(&mut self.plan))
            .join_on(query, JoinType::LeftMark, Some(predicate))?
            .build()?;
        let mark = Expr::Column(Column::new(Some(alias), "mark"));
        let result = if null_literal {
            expr_fn::when(mark, lit(ScalarValue::Boolean(None))).otherwise(lit(negated))?
        } else {
            let mark = if nullable {
                expr_fn::when(lit(true), mark).end()?
            } else {
                mark
            };
            if negated { !mark } else { mark }
        };
        Ok(Transformed::yes(result))
    }
}
