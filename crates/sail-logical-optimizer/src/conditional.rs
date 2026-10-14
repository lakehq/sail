use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::dml::WriteOp;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_function::scalar::conversion::spark_conditional_cast::SparkConditionalCast;

/// DataFusion probes IN-list values on an empty batch to build constant sets.
/// CASE can return a scalar on that batch even when it depends on input rows.
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
