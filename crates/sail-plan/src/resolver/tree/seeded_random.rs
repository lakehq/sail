use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::expr::{BinaryExpr, Case, ScalarFunction};
use datafusion_expr::{Expr, Extension, LogicalPlan, Operator, ident};
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
/// expression with an aggregate function, a correlated or predicate subquery, or a function
/// that must be planned separately cannot be evaluated by the node, so its calls are replaced
/// individually, except for the calls in children that are not evaluated for every row, which
/// stay scalar functions.
pub(crate) struct SeededRandomRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    scope: SeededRandomScope,
    /// Whether the plan has outer references, which the node would separate from the
    /// operators above it, so that the subquery could not be decorrelated. It is found when
    /// an expression first has a call to replace.
    correlated: Option<bool>,
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
        if contains_seeded_random_call(&node) && self.is_correlated() {
            return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
        }
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
            correlated: None,
            input: None,
            columns: vec![],
        }
    }

    fn is_correlated(&mut self) -> bool {
        // The plan is not replaced by the node before the first call is replaced.
        *self.correlated.get_or_insert_with(|| {
            self.plan
                .exists(|plan| Ok(plan.contains_outer_reference()))
                .unwrap_or(true)
        })
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
            expr => {
                // The calls in the children that are not evaluated for every row stay
                // scalar functions, so that they advance their generators only for the rows
                // that evaluate them.
                // TODO: Such calls are not seeded per partition, as Spark does.
                let lazy = lazy_children(&expr);
                let mut index = 0;
                let result = expr.map_children(|child| {
                    let is_lazy = lazy.get(index).copied().unwrap_or(false);
                    index += 1;
                    if is_lazy {
                        Ok(Transformed::no(child))
                    } else {
                        self.rewrite_expression(child)
                    }
                });
                debug_assert!(result.is_err() || index == lazy.len());
                result
            }
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
        // Equal expressions over the same rows produce the same values, unless they contain
        // other volatile calls, which are evaluated independently.
        let reusable = !node
            .exists(|e| Ok(e.is_volatile_node() && !is_seeded_random_call(e)))
            .unwrap_or(true);
        let existing = if reusable {
            self.columns.iter().find(|column| column.expr == node)
        } else {
            None
        };
        let name = match existing {
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

/// Returns, for each child of the expression in order, whether the child may not be
/// evaluated for every row, as in the common subexpression elimination of DataFusion.
fn lazy_children(expr: &Expr) -> Vec<bool> {
    let lazy: Vec<&Expr> = match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) => func
            .conditional_arguments(args)
            .map(|(_, lazy)| lazy)
            .unwrap_or_default(),
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::And | Operator::Or,
            right,
            ..
        }) => vec![right.as_ref()],
        Expr::Case(Case {
            when_then_expr,
            else_expr,
            ..
        }) => when_then_expr
            .iter()
            .take(1)
            .map(|(_, then)| then.as_ref())
            .chain(
                when_then_expr
                    .iter()
                    .skip(1)
                    .flat_map(|(when, then)| [when.as_ref(), then.as_ref()]),
            )
            .chain(else_expr.iter().map(|e| e.as_ref()))
            .collect(),
        _ => vec![],
    };
    let mut children = vec![];
    let _ = expr.apply_children(|child| {
        children.push(lazy.iter().any(|e| std::ptr::eq(*e, child)));
        Ok(TreeNodeRecursion::Continue)
    });
    children
}

/// Returns whether the expression contains a generator, which is planned separately.
pub(crate) fn contains_generator(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(
            matches!(e, Expr::ScalarFunction(ScalarFunction { func, .. })
            if func.inner().is::<Explode>()),
        )
    })
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
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::Placeholder(_)
                | Expr::ScalarVariable(_, _)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_) => true,
                // An uncorrelated scalar subquery is evaluated before the node as a constant.
                Expr::ScalarSubquery(subquery) => !subquery.outer_ref_columns.is_empty(),
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
