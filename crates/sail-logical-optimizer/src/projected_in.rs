use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::Arc;

use datafusion::catalog::MemTable;
use datafusion::datasource::DefaultTableSource;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
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
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

/// Spark folds expressions before replacing projected IN with an existence join.
/// Fold only the affected expressions and their constant producers here; running
/// another optimizer over the query would change unrelated rule ordering.
#[derive(Debug)]
pub struct RewriteProjectedIn;

impl OptimizerRule for RewriteProjectedIn {
    fn name(&self) -> &str {
        "rewrite_projected_in"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        rewrite_projection(plan, config)
    }
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
    if needed.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(producer_constants(plan, config, needed)?.columns)
}

#[derive(Default)]
struct ProducerConstants {
    columns: HashMap<Column, Expr>,
    local_relation: bool,
}

/// Spark's ConvertToLocalRelation evaluates whole projections before constant
/// propagation. Its Unevaluable check includes subqueries and Python UDFs, but
/// permits ordinary nondeterministic functions such as rand().
fn locally_evaluable(expr: &Expr) -> Result<bool> {
    Ok(!expr.exists(|expr| {
        Ok(match expr {
            Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::ScalarSubquery(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::Unnest(_) => true,
            Expr::ScalarFunction(function) => {
                let function = function.func.inner();
                function.is::<PySparkUDF>()
                    || function.is::<PySparkUnresolvedUDF>()
                    || function.is::<PySparkCoGroupMapUDF>()
            }
            _ => false,
        })
    })?)
}

fn producer_constants(
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
    needed: &HashSet<Column>,
) -> Result<ProducerConstants> {
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
            let mut constants = producer_constants(&alias.input, config, &input_needed)?;
            constants.columns = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
                .filter_map(|(input, output)| {
                    constants
                        .columns
                        .remove(&input)
                        .map(|value| (output, value))
                })
                .collect();
            return Ok(constants);
        }
        LogicalPlan::Filter(filter) => {
            // Spark simplifies null-rejecting predicates before propagating constants.
            // Inspect the simplified predicate and join type only on a copy.
            let mut input = filter.input.as_ref();
            while let LogicalPlan::Projection(projection) = input {
                input = &projection.input;
            }
            if !needed.is_empty()
                && matches!(input, LogicalPlan::Join(join) if join.join_type.is_outer())
            {
                let mut normalized = filter.clone();
                normalized.predicate = simplifier(Arc::clone(filter.input.schema()), config)
                    .simplify(normalized.predicate)?;
                let rewritten =
                    EliminateOuterJoin.rewrite(LogicalPlan::Filter(normalized), config)?;
                if rewritten.transformed {
                    return producer_constants(&rewritten.data, config, needed);
                }
            }
            let mut constants = producer_constants(&filter.input, config, needed)?;
            if constants.local_relation {
                constants.local_relation = locally_evaluable(&filter.predicate)?;
            }
            return Ok(constants);
        }
        LogicalPlan::Limit(limit) => {
            let mut constants = producer_constants(&limit.input, config, needed)?;
            constants.local_relation &=
                limit.skip.is_none() && matches!(limit.fetch.as_deref(), Some(Expr::Literal(_, _)));
            return Ok(constants);
        }
        LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            let mut constants = producer_constants(plan.inputs()[0], config, needed)?;
            constants.local_relation = false;
            return Ok(constants);
        }
        LogicalPlan::Join(join) => {
            let mut constants = ProducerConstants::default();
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::LeftMark
            ) {
                constants
                    .columns
                    .extend(constant_columns(&join.left, config, needed)?);
            }
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::RightMark
            ) {
                constants
                    .columns
                    .extend(constant_columns(&join.right, config, needed)?);
            }
            return Ok(constants);
        }
        LogicalPlan::Values(_) => {
            return Ok(ProducerConstants {
                local_relation: true,
                ..Default::default()
            });
        }
        LogicalPlan::TableScan(scan) => {
            return Ok(ProducerConstants {
                local_relation: scan
                    .source
                    .downcast_ref::<DefaultTableSource>()
                    .is_some_and(|source| {
                        let mut provider = &source.table_provider;
                        while let Some(renamed) = provider.downcast_ref::<RenameTableProvider>() {
                            provider = renamed.inner();
                        }
                        provider.is::<MemTable>()
                    }),
                ..Default::default()
            });
        }
        _ => return Ok(ProducerConstants::default()),
    };
    if expressions.len() != plan.schema().fields().len() {
        return Ok(ProducerConstants::default());
    }
    let selected = expressions
        .iter()
        .copied()
        .zip(plan.schema().columns())
        .filter(|(_, column)| needed.contains(column))
        .collect::<Vec<_>>();
    let input_needed = selected
        .iter()
        .flat_map(|(expr, _)| expr.column_refs().into_iter().cloned())
        .collect();
    let constants = producer_constants(input, config, &input_needed)?;
    if constants.local_relation && matches!(plan, LogicalPlan::Projection(_)) {
        let mut local_relation = true;
        for expr in expressions {
            local_relation &= locally_evaluable(expr)?;
        }
        if local_relation {
            return Ok(ProducerConstants {
                local_relation: true,
                ..Default::default()
            });
        }
    }
    let simplifier = simplifier(Arc::clone(input.schema()), config);
    let mut output = ProducerConstants::default();
    for (expr, column) in selected {
        let expr = simplifier.simplify(replace_constants(
            expr.clone().unalias(),
            &constants.columns,
        )?)?;
        if matches!(expr, Expr::Literal(_, _)) {
            output.columns.insert(column, expr);
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

#[cfg(test)]
mod tests {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::{col, in_subquery};

    use super::*;

    #[test]
    fn projected_in_does_not_optimize_its_input() -> Result<()> {
        let input = LogicalPlanBuilder::empty(true)
            .project(vec![(lit(1_i32) + lit(2_i32)).alias("value")])?
            .filter(lit(true))?
            .build()?;
        let config = OptimizerContext::new();
        let unrelated = LogicalPlanBuilder::from(input.clone())
            .project(vec![col("value")])?
            .build()?;
        let unchanged = RewriteProjectedIn.rewrite(unrelated.clone(), &config)?;
        assert!(!unchanged.transformed);
        assert_eq!(unchanged.data, unrelated);

        let subquery = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i32).alias("candidate")])?
            .build()?;
        // A computed nullable operand used to run a nested optimizer, which
        // also folded the unrelated input projection and removed its filter.
        let operand = lit(ScalarValue::Int32(None)) + lit(1_i32);
        let plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![
                in_subquery(operand, Arc::new(subquery)).alias("present"),
            ])?
            .build()?;
        let rewritten = RewriteProjectedIn.rewrite(plan, &config)?;
        assert!(rewritten.transformed);
        let LogicalPlan::Projection(projection) = rewritten.data else {
            return plan_err!("expected a projection");
        };
        let LogicalPlan::Join(join) = projection.input.as_ref() else {
            return plan_err!("expected an existence join");
        };
        assert_eq!(join.join_type, JoinType::LeftMark);
        assert_eq!(join.left.as_ref(), &input);
        Ok(())
    }
}
