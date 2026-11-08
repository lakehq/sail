use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, Extension, LogicalPlan, ident};
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_function::scalar::explode::Explode;
use sail_function::scalar::math::partition_random::{PartitionSeed, seed_argument_position};
use sail_function::scalar::multi_expr::MultiExpr;
use sail_logical_plan::seeded_random::{SeededRandomColumn, SeededRandomNode};
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

/// How the calls of random functions with a seed in an expression are evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeededRandomScope {
    /// The whole expression is evaluated by a [`SeededRandomNode`], so that each call
    /// advances its generator only for the rows that evaluate it. Spark evaluates
    /// nondeterministic expressions in projections and filters this way.
    Expression,
    /// Each call is evaluated by a [`SeededRandomNode`] for every row. Spark pulls the calls
    /// out of the other operators (`PullOutNondeterministic`), such as sorts, grouping
    /// expressions, and repartitioning.
    Call,
}

/// Replaces the expressions with calls of random functions with a seed by columns of a
/// [`SeededRandomNode`], which seeds each call once per partition as Spark does.
///
/// In the [`SeededRandomScope::Expression`] scope, the largest subexpressions that the node
/// can evaluate are replaced. An expression with a window function is evaluated around the
/// window, so only the arguments and the window specification are replaced, and the rest is
/// replaced when the rewriter is applied again after the window functions are planned. An
/// expression with an aggregate function, a subquery, or a function that must be planned
/// separately cannot be evaluated by the node, so its calls are replaced individually.
pub(crate) struct SeededRandomRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    scope: SeededRandomScope,
    input: Option<LogicalPlan>,
    columns: Vec<SeededRandomColumn>,
}

impl<'s> PlanRewriter<'s> for SeededRandomRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self::new(plan, state, SeededRandomScope::Expression)
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for SeededRandomRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let Transformed {
            data, transformed, ..
        } = match self.scope {
            SeededRandomScope::Expression => self.rewrite_expression(node)?,
            SeededRandomScope::Call => self.rewrite_calls(node)?,
        };
        Ok(Transformed::new(data, transformed, TreeNodeRecursion::Jump))
    }
}

impl<'s> SeededRandomRewriter<'s> {
    pub(crate) fn new(
        plan: LogicalPlan,
        state: &'s mut PlanResolverState,
        scope: SeededRandomScope,
    ) -> Self {
        Self {
            plan,
            state,
            scope,
            input: None,
            columns: vec![],
        }
    }

    fn rewrite_expression(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if !contains_seeded_random_call(&expr) {
            return Ok(Transformed::no(expr));
        }
        if can_evaluate_in_node(&expr) {
            return self.materialize(expr);
        }
        match expr {
            Expr::WindowFunction(_) => expr.map_children(|child| self.rewrite_expression(child)),
            expr if contains_window_function(&expr) => expr.map_children(|child| {
                if contains_window_function(&child) {
                    self.rewrite_expression(child)
                } else {
                    Ok(Transformed::no(child))
                }
            }),
            expr => expr.map_children(|child| self.rewrite_expression(child)),
        }
    }

    fn rewrite_calls(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform_up(|expr| {
            if is_seeded_random_call(&expr) {
                self.materialize(expr)
            } else {
                Ok(Transformed::no(expr))
            }
        })
    }

    /// Replaces an expression by a column of the node.
    fn materialize(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        // Equal expressions over the same rows produce the same values.
        let name = match self.columns.iter().find(|column| column.expr == node) {
            Some(column) => column.name.clone(),
            None => {
                // Create an internal-only field ID without a referenceable name.
                let name = self.state.next_field_id();
                self.columns.push(SeededRandomColumn {
                    name: name.clone(),
                    expr: node,
                    seed: PartitionSeed::Offset,
                });
                let input = match self.input.take() {
                    Some(input) => input,
                    None => {
                        let plan = mem::replace(&mut self.plan, empty_logical_plan());
                        // The random values depend on the user's input order even when the
                        // consumer does not.
                        let (plan, _, _) = PlanResolver::require_input_sort_inner(plan)
                            .map_err(|error| DataFusionError::External(Box::new(error)))?;
                        plan
                    }
                };
                self.plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(SeededRandomNode::try_new(
                        Arc::new(input.clone()),
                        self.columns.clone(),
                    )?),
                });
                self.input = Some(input);
                name
            }
        };
        Ok(Transformed::yes(ident(&name).alias(&name)))
    }
}

/// Returns whether the expression is a call of a random function with a seed whose arguments
/// are constant, as Spark requires. Other calls are evaluated as scalar functions.
fn is_seeded_random_call(expr: &Expr) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return false;
    };
    seed_argument_position(func, args.len()).is_some()
        && args.iter().all(|arg| {
            arg.column_refs().is_empty()
                && !arg.is_volatile()
                && !arg
                    .exists(|e| Ok(matches!(e, Expr::LambdaVariable(_))))
                    .unwrap_or(true)
        })
}

pub(crate) fn contains_seeded_random_call(expr: &Expr) -> bool {
    expr.exists(|e| Ok(is_seeded_random_call(e)))
        .unwrap_or(false)
}

fn contains_window_function(expr: &Expr) -> bool {
    expr.exists(|e| Ok(matches!(e, Expr::WindowFunction(_))))
        .unwrap_or(false)
}

/// Returns whether the node can evaluate the expression, which must not contain operators
/// or functions that are planned separately.
fn can_evaluate_in_node(expr: &Expr) -> bool {
    !expr
        .exists(|e| {
            Ok(match e {
                Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::Placeholder(_)
                | Expr::ScalarVariable(_, _)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_) => true,
                Expr::ScalarFunction(ScalarFunction { func, .. }) => {
                    func.as_async().is_some()
                        || is_input_file_metadata_function(func)
                        || func.inner().is::<Explode>()
                        || func.inner().is::<MultiExpr>()
                        || func
                            .inner()
                            .downcast_ref::<PySparkUDF>()
                            .is_some_and(|udf| udf.kind() == PySparkUdfKind::ScalarPandasIter)
                }
                _ => false,
            })
        })
        .unwrap_or(true)
}

/// Returns the calls of random functions with a seed in the expression, which Spark
/// evaluates for every row of operators other than projections and filters.
pub(crate) fn seeded_random_calls(expr: &Expr) -> Vec<&Expr> {
    let mut calls = vec![];
    let _ = expr.apply(|e| {
        if is_seeded_random_call(e) {
            calls.push(e);
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    calls
}
