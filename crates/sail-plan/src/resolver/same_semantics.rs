use std::sync::Arc;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::{Alias, InList};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan};
use datafusion_expr_common::operator::Operator;
use sail_common::spec;
use sail_function::scalar::hash::spark_compatible_murmur3_hash;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {

    pub async fn resolve_same_semantics(&self, target_plan: spec::QueryPlan, other_plan: spec::QueryPlan) -> PlanResult<bool> {
        let target_plan_str = self.resolve_canonical_plan_string(target_plan).await?;
        let other_plan_str = self.resolve_canonical_plan_string(other_plan).await?;
        Ok(target_plan_str == other_plan_str)
    }

    async fn resolve_canonical_plan_string(&self, plan: spec::QueryPlan) -> PlanResult<LogicalPlan> {
        let mut state = PlanResolverState::new();
        let initial_logical = self.resolve_query_plan(plan, &mut state).await?;

        let session_state = self.ctx.state();
        let config_options = session_state.config_options();
        let analyzed_logical = session_state.analyzer().execute_and_check(
            initial_logical,
            config_options.as_ref(),
            |_, _| {}
        )?;

        let canonical = Self::canonicalize_plan(analyzed_logical)?;
        Ok(canonical)
    }

    fn canonicalize_plan(plan: LogicalPlan)-> Result<LogicalPlan, DataFusionError> {
        plan.transform_up(|node| {
            let node_input_schema: Option<DFSchemaRef> = match node.inputs().as_slice() {
                [] => None,
                [single] => Some(Arc::clone(single.schema())),
                [first, rest @ ..] => {
                    rest
                        .iter()
                        .try_fold((**first.schema()).clone(), |merged, input|
                            merged.join(input.schema())
                        )
                        .ok()
                        .map(|merged| Arc::new(merged))
                }
            };
            node.map_expressions(|expr| canonicalize_expr(expr, node_input_schema.as_ref()))
        })
        .data()
    }
}


fn canonicalize_expr(expr: Expr, input_schema: Option<&DFSchemaRef>) -> Result<Transformed<Expr>, DataFusionError> {
       expr.transform_up(|e| match e {
            // (2) Alias name erasure
            Expr::Alias(alias) => Ok(Transformed::yes(
                Expr::Alias( Alias { name: String::new(), ..alias })
            )),
            // (15) Not(comparison) => complement operator
            Expr::Not(inner) => {
                if let Expr::BinaryExpr( BinaryExpr {left, op, right}) = *inner {
                    if let Some(negated_op) = negate_comparison_op(op) {
                        return Ok(Transformed::yes( Expr::BinaryExpr( BinaryExpr{
                            left,
                            op: negated_op,
                            right
                        })));
                    }
                    Ok(Transformed::no(Expr::Not( Box::new(Expr::BinaryExpr(
                        BinaryExpr {left, op, right}
                    )))))
                } else {
                    Ok(Transformed::no(Expr::Not(inner)))
                }
            },
            Expr::BinaryExpr( BinaryExpr {left, op, right}) => match op {
                // (16) Comparison flip: canonical form has lower-hash
                // operand on the left side
                Operator::Eq
                | Operator::Gt
                | Operator::Lt
                | Operator::GtEq
                | Operator::LtEq => {
                    if expr_hash(&left) > expr_hash(&right) {
                        if let Some(swapped_op) = swap_comparison_op(op) {
                            return Ok(Transformed::yes(Expr::BinaryExpr( BinaryExpr {
                                left: right,
                                op: swapped_op,
                                right: left
                            })));
                        }
                    }
                    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {left, op, right})))
                },
                // (17) And: flatten all adjacent Ands, cort by hash, rebuild | (18) Or: flattens all adhjacent Ors, sort by hash, rebuild
                Operator::And | Operator::Or => {
                    let mut operands = gather_binary_op(*left, op);
                    operands.extend(gather_binary_op(*right, op));
                    operands.sort_by_key(|e| expr_hash(e));
                    Ok(Transformed::yes( rebuild_binary_op(operands, op)))
                },
                // (20) Add: commutative ADD reordering with a guard | (21) MULTIPLY: commutative MULTIPLY reordering with a guard
                Operator::Plus | Operator::Multiply => {
                    let original = Expr::BinaryExpr(BinaryExpr {left: left.clone(), op, right: right.clone()});

                    let Some(input_schema) = input_schema else {
                        return Ok(Transformed::no(original))
                    };

                    let mut operands = gather_binary_op(*left, op); // flattening
                    operands.extend(gather_binary_op(*right, op)); // flattening
                    operands.sort_by_key(|e| expr_hash(e)); // reorderiing based on hash, ascending order
                    let rebuilt = rebuild_binary_op(operands, op); //rebuilt based on new order

                    match (original.get_type(input_schema), rebuilt.get_type(input_schema)) {
                        (Ok(t1), Ok(t2)) if t1 == t2 => Ok(Transformed::yes(rebuilt)),
                        _ => Ok(Transformed::no(original))
                    }
                },
                _ => Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {left, op, right})))
            },
            // (19) InList: sort list elements by hash
            Expr::InList( InList {expr, list, negated}) => {
                let mut sorted = list;
                sorted.sort_by_key(|e| expr_hash(e));
                Ok(Transformed::yes(Expr::InList( InList {
                    expr,
                    list: sorted,
                    negated
                })))
            }
            other =>  Ok(Transformed::no(other))
       })
}

fn expr_hash(expr: &Expr) -> u32 {
    spark_compatible_murmur3_hash(format!("{expr:?}"), 42)
}

fn negate_comparison_op(op: Operator) -> Option<Operator> {
    match op {
        Operator::Gt => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::GtEq),
        Operator::GtEq => Some(Operator::Lt),
        Operator::LtEq => Some(Operator::Gt),
        _ => None
    }
}

fn swap_comparison_op(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::Lt => Some(Operator::Gt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None
    }
}

fn gather_binary_op(expr: Expr, op: Operator) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr( BinaryExpr {
            left,
            op: node_op,
            right
        }) if node_op == op => {
            let mut operands = gather_binary_op(*left, op);
            operands.extend(gather_binary_op(*right, op));
            operands
        },
        other => vec![other]
    }
}

fn rebuild_binary_op(exprs: Vec<Expr>, op: Operator) -> Expr {
    exprs
        .into_iter()
        .reduce(|acc, e| {
            Expr::BinaryExpr(BinaryExpr { left: Box::new(acc), op, right: Box::new(e) })
        })
        .expect("exprs should not be empty")
}
