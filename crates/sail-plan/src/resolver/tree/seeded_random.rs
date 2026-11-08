use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::expr::{BinaryExpr, Case, HigherOrderFunction, ScalarFunction};
use datafusion_expr::{Expr, Extension, LogicalPlan, Operator, ident};
use sail_function::scalar::math::partition_random::{PartitionSeed, seed_argument_position};
use sail_logical_plan::seeded_random::{SeededRandomColumn, SeededRandomNode};

use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

/// Replaces the calls of random functions with a seed by columns of a [`SeededRandomNode`],
/// which seeds each call once per partition as Spark does.
///
/// The rewriter is applied to projections (including `withColumn`, lateral column aliases,
/// pivots, aggregate output, and the window and aggregate functions within them), filters,
/// `HAVING`, grouping expressions, sort expressions, and repartitioning expressions. Only the
/// calls that are evaluated for every row are replaced, since Spark advances the generator only
/// for the rows that evaluate the call. The calls in conditionally evaluated expressions (e.g.
/// `CASE` branches, the right side of `AND` and `OR`, lazy function arguments, and lambda
/// functions), and the calls elsewhere (e.g. in join conditions) are evaluated as scalar
/// functions, which restart from the seed for every batch.
pub(crate) struct SeededRandomRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    input: Option<LogicalPlan>,
    columns: Vec<SeededRandomColumn>,
}

impl<'s> PlanRewriter<'s> for SeededRandomRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self {
            plan,
            state,
            input: None,
            columns: vec![],
        }
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
        } = self.rewrite(node)?;
        Ok(Transformed::new(data, transformed, TreeNodeRecursion::Jump))
    }
}

impl SeededRandomRewriter<'_> {
    /// Replaces the calls that are evaluated whenever the expression is evaluated.
    fn rewrite(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Lambda(_) => Ok(Transformed::no(expr)),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (Operator::And | Operator::Or),
                right,
            }) => Ok(self.rewrite(*left)?.update_data(|left| {
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right,
                })
            })),
            // The base expression and the first condition are evaluated for every row.
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let mut transformed = false;
                let expr = match expr {
                    Some(expr) => {
                        let expr = self.rewrite(*expr)?;
                        transformed |= expr.transformed;
                        Some(Box::new(expr.data))
                    }
                    None => None,
                };
                let mut when_then_expr = when_then_expr.into_iter();
                let first = match when_then_expr.next() {
                    Some((when, then)) => {
                        let when = self.rewrite(*when)?;
                        transformed |= when.transformed;
                        Some((Box::new(when.data), then))
                    }
                    None => None,
                };
                let expr = Expr::Case(Case {
                    expr,
                    when_then_expr: first.into_iter().chain(when_then_expr).collect(),
                    else_expr,
                });
                Ok(Transformed::new_transformed(expr, transformed))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let eager = eager_arguments(func.conditional_arguments(&args), &args);
                let args = self.rewrite_arguments(args, &eager)?;
                args.transform_data(|args| {
                    self.materialize(Expr::ScalarFunction(ScalarFunction { func, args }))
                })
            }
            Expr::HigherOrderFunction(HigherOrderFunction { func, args }) => {
                let eager = eager_arguments(func.conditional_arguments(&args), &args);
                Ok(self.rewrite_arguments(args, &eager)?.update_data(|args| {
                    Expr::HigherOrderFunction(HigherOrderFunction { func, args })
                }))
            }
            expr => expr.map_children(|child| self.rewrite(child)),
        }
    }

    fn rewrite_arguments(
        &mut self,
        args: Vec<Expr>,
        eager: &[bool],
    ) -> Result<Transformed<Vec<Expr>>> {
        let mut transformed = false;
        let args = args
            .into_iter()
            .zip(eager)
            .map(|(arg, &eager)| {
                if !eager {
                    return Ok(arg);
                }
                let arg = self.rewrite(arg)?;
                transformed |= arg.transformed;
                Ok(arg.data)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::new_transformed(args, transformed))
    }

    /// Replaces a call of a random function with a seed by a column of the node.
    fn materialize(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        if !is_seeded_random_call(&node) {
            return Ok(Transformed::no(node));
        }
        // Equal calls over the same rows produce the same values.
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

/// Returns whether each argument is evaluated whenever the function is evaluated.
fn eager_arguments(conditional: Option<(Vec<&Expr>, Vec<&Expr>)>, args: &[Expr]) -> Vec<bool> {
    match conditional {
        None => vec![true; args.len()],
        Some((eager, _)) => args
            .iter()
            .map(|arg| eager.iter().any(|x| std::ptr::eq(*x, arg)))
            .collect(),
    }
}

/// Returns whether the expression is a call of a random function with a seed whose arguments
/// are constant, as Spark requires. Other calls are evaluated as scalar functions.
fn is_seeded_random_call(expr: &Expr) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return false;
    };
    seed_argument_position(func, args.len()).is_some()
        && args
            .iter()
            .all(|arg| arg.column_refs().is_empty() && !arg.is_volatile())
}

/// Returns the calls of random functions with a seed that are evaluated whenever the
/// expression is evaluated, which are the calls that [`SeededRandomRewriter`] replaces.
pub(crate) fn unconditional_seeded_random_calls(expr: &Expr) -> Vec<&Expr> {
    fn visit<'a>(expr: &'a Expr, calls: &mut Vec<&'a Expr>) {
        match expr {
            Expr::Lambda(_) => {}
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And | Operator::Or,
                ..
            }) => visit(left, calls),
            Expr::Case(Case {
                expr,
                when_then_expr,
                ..
            }) => {
                if let Some(expr) = expr {
                    visit(expr, calls);
                }
                if let Some((when, _)) = when_then_expr.first() {
                    visit(when, calls);
                }
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let eager = eager_arguments(func.conditional_arguments(args), args);
                for (arg, eager) in args.iter().zip(eager) {
                    if eager {
                        visit(arg, calls);
                    }
                }
                if is_seeded_random_call(expr) {
                    calls.push(expr);
                }
            }
            Expr::HigherOrderFunction(HigherOrderFunction { func, args }) => {
                let eager = eager_arguments(func.conditional_arguments(args), args);
                for (arg, eager) in args.iter().zip(eager) {
                    if eager {
                        visit(arg, calls);
                    }
                }
            }
            expr => {
                let _ = expr.apply_children(|child| {
                    visit(child, calls);
                    Ok(TreeNodeRecursion::Continue)
                });
            }
        }
    }
    let mut calls = vec![];
    visit(expr, &mut calls);
    calls
}
