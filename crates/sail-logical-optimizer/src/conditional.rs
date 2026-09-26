use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::dml::WriteOp;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF};
use sail_function::scalar::conditional::SparkConditionalCast;

/// Finish lowering Spark conditionals after their schemas have been resolved.
///
/// DataFusion probes IN-list values on an empty batch to build constant sets.
/// CASE can return a scalar on that batch even when it depends on input rows.
/// Guard the consumer, rather than adding a wrapper to every nested NVL2.
/// Columns also need protection: physical projection pushdown may inline CASE
/// into them before an IN expression is reconstructed on a cluster worker.
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
        let mut schema = merge_schema(&plan.inputs());
        if let LogicalPlan::TableScan(scan) = &plan {
            schema.merge(&DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &scan.source.schema(),
            )?);
        }
        if let LogicalPlan::Dml(dml) = &plan
            && matches!(dml.op, WriteOp::MergeInto(_))
        {
            schema.merge(&DFSchema::try_from_qualified_schema(
                dml.table_name.clone(),
                &dml.target.schema(),
            )?);
        }
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            expr.transform_up(|expr| {
                if let Expr::Case(mut case) = expr {
                    if matches!(case.expr.as_deref(), Some(Expr::IsNull(_)))
                        && case.when_then_expr.len() == 1
                        && matches!(
                            case.when_then_expr[0].0.as_ref(),
                            Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(true)), _)
                        )
                        && case.else_expr.is_some()
                    {
                        // NVL2 uses simple CASE during resolution to keep Spark's
                        // branch-based nullability. After binding, searched CASE
                        // has a faster vectorized evaluator. Keep the nullable
                        // branch in ELSE so this also preserves the final schema.
                        let Some(condition) = case.expr.take() else {
                            unreachable!()
                        };
                        let Expr::IsNull(tested) = *condition else {
                            unreachable!()
                        };
                        let (_, if_null) = case.when_then_expr.remove(0);
                        let Some(if_non_null) = case.else_expr.take() else {
                            unreachable!()
                        };
                        let (condition, then_expr, else_expr) = if if_null.nullable(&schema)? {
                            (tested.is_not_null(), if_non_null, if_null)
                        } else {
                            (tested.is_null(), if_null, if_non_null)
                        };
                        case.when_then_expr = vec![(Box::new(condition), then_expr)];
                        case.else_expr = Some(else_expr);
                        return Ok(Transformed::yes(Expr::Case(case)));
                    }
                    return Ok(Transformed::no(Expr::Case(case)));
                }
                let Expr::InList(mut in_list) = expr else {
                    return Ok(Transformed::no(expr));
                };
                let mut changed = false;
                for value in &mut in_list.list {
                    if matches!(value, Expr::Literal(..))
                        || matches!(value, Expr::ScalarFunction(function)
                            if function.func.inner().is::<SparkConditionalCast>())
                    {
                        continue;
                    }
                    let data_type = value.get_type(&schema)?;
                    *value = ScalarUDF::from(SparkConditionalCast::new(data_type))
                        .call(vec![value.clone()]);
                    changed = true;
                }
                Ok(Transformed::new_transformed(Expr::InList(in_list), changed))
            })?
            .map_data(|expr| Ok(name.restore(expr)))
        })
    }
}
