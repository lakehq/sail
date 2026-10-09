use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::dml::WriteOp;
use datafusion_expr::expr::Case;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_function::scalar::conditional::SparkConditionalCast;

/// Finish lowering Spark conditionals after their schemas have been resolved.
#[derive(Debug, Default)]
pub struct SimplifyConditionals;

impl OptimizerRule for SimplifyConditionals {
    fn name(&self) -> &str {
        "simplify_conditionals"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // This rule runs on every node in every optimizer pass. Most nodes have no NVL2,
        // so skip schema construction and name preservation for them.
        if !has_expression(
            &plan,
            |expr| matches!(expr, Expr::Case(case) if is_nvl2_case(case)),
        )? {
            return Ok(Transformed::no(plan));
        }
        let schema = expression_schema(&plan)?;
        map_node_expressions(plan, |expr| {
            expr.transform_up(|expr| {
                let Expr::Case(mut case) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if !is_nvl2_case(&case) {
                    return Ok(Transformed::no(Expr::Case(case)));
                }
                // NVL2 uses simple CASE during resolution to keep Spark's
                // branch-based nullability. After binding, searched CASE
                // has a faster vectorized evaluator. Keep the nullable
                // branch in ELSE so this also preserves the final schema.
                let Some(condition) = case.expr.take() else {
                    unreachable!()
                };
                let (_, if_null) = case.when_then_expr.remove(0);
                let Some(if_non_null) = case.else_expr.take() else {
                    unreachable!()
                };
                let tested = match *condition {
                    Expr::IsNull(tested) => tested,
                    // Constant folding resolved the tested value, but DataFusion
                    // simplifies only searched CASE. Select the branch, as Spark
                    // folds NVL2's `If(IsNotNull(...))` replacement.
                    Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(is_null)), _) => {
                        return Ok(Transformed::yes(if is_null {
                            *if_null
                        } else {
                            *if_non_null
                        }));
                    }
                    _ => unreachable!(),
                };
                let (condition, then_expr, else_expr) = if if_null.nullable(&schema)? {
                    (tested.is_not_null(), if_non_null, if_null)
                } else {
                    (tested.is_null(), if_null, if_non_null)
                };
                case.when_then_expr = vec![(Box::new(condition), then_expr)];
                case.else_expr = Some(else_expr);
                Ok(Transformed::yes(Expr::Case(case)))
            })
        })
    }
}

/// DataFusion probes IN-list values on an empty batch to build constant sets.
/// CASE can return a scalar on that batch even when it depends on input rows.
/// Guard the consumer, rather than adding a wrapper to every nested NVL2.
/// Columns also need protection: physical projection pushdown may inline CASE
/// into them before an IN expression is reconstructed on a cluster worker.
///
/// This runs once after logical optimization. A guarded value is a candidate for
/// common subexpression elimination, so guarding it in every optimizer pass would
/// extract and guard it again until the pass limit is reached.
#[derive(Debug, Default)]
pub struct GuardInListValues;

#[async_trait]
impl LogicalRewriter for GuardInListValues {
    fn name(&self) -> &str {
        "guard_in_list_values"
    }

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            if !has_expression(&plan, |expr| {
                matches!(expr, Expr::InList(in_list) if in_list.list.iter().any(needs_guard))
            })? {
                return Ok(Transformed::no(plan));
            }
            let schema = expression_schema(&plan)?;
            map_node_expressions(plan, |expr| {
                expr.transform_up(|expr| {
                    let Expr::InList(mut in_list) = expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let mut changed = false;
                    for value in &mut in_list.list {
                        if !needs_guard(value) {
                            continue;
                        }
                        let data_type = value.get_type(&schema)?;
                        *value = ScalarUDF::from(SparkConditionalCast::new(data_type))
                            .call(vec![value.clone()]);
                        changed = true;
                    }
                    Ok(Transformed::new_transformed(Expr::InList(in_list), changed))
                })
            })
        })
    }
}

/// The simple CASE that NVL2 lowers to during resolution, including after its
/// tested `IS NULL` has been folded into a constant.
fn is_nvl2_case(case: &Case) -> bool {
    matches!(
        case.expr.as_deref(),
        Some(Expr::IsNull(_) | Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(_)), _))
    ) && case.when_then_expr.len() == 1
        && matches!(
            case.when_then_expr[0].0.as_ref(),
            Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(true)), _)
        )
        && case.else_expr.is_some()
}

/// An IN-list value that is neither a literal nor already guarded.
fn needs_guard(value: &Expr) -> bool {
    !matches!(value, Expr::Literal(..))
        && !matches!(value, Expr::ScalarFunction(function)
            if function.func.inner().is::<SparkConditionalCast>())
}

/// Whether any expression of the node contains a matching subexpression.
fn has_expression(plan: &LogicalPlan, matches: impl Fn(&Expr) -> bool) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|expr| Ok(matches(expr)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// The schema that the node's expressions are resolved against.
fn expression_schema(plan: &LogicalPlan) -> Result<DFSchema> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = plan {
        schema.merge(&DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?);
    }
    if let LogicalPlan::Dml(dml) = plan
        && matches!(dml.op, WriteOp::MergeInto(_))
    {
        schema.merge(&DFSchema::try_from_qualified_schema(
            dml.table_name.clone(),
            &dml.target.schema(),
        )?);
    }
    Ok(schema)
}

/// Rewrite the node's expressions, preserving their names so that its schema is unchanged.
fn map_node_expressions(
    plan: LogicalPlan,
    mut f: impl FnMut(Expr) -> Result<Transformed<Expr>>,
) -> Result<Transformed<LogicalPlan>> {
    let names = NamePreserver::new(&plan);
    let mut rewrite = |expr: Expr| {
        let name = names.save(&expr);
        f(expr)?.map_data(|expr| Ok(name.restore(expr)))
    };
    plan.map_expressions(|expr| {
        // Preserve the aliasing of grouping sets, as DataFusion's expression
        // simplifier does. An aliased grouping set is no longer recognized
        // by `Aggregate`, which changes its output schema.
        if let Expr::GroupingSet(_) = &expr {
            expr.map_children(&mut rewrite)
        } else {
            rewrite(expr)
        }
    })
}
