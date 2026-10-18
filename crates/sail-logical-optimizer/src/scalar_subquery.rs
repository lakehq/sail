use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::Cast;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::{Expr, LogicalPlan, cast, lit, scalar_subquery, when};

#[derive(Debug)]
pub struct DecorrelateScalarProjection;

impl AnalyzerRule for DecorrelateScalarProjection {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.rewrite_with_subqueries(&mut ScalarProjectionRewriter::default())
            .data()
    }

    fn name(&self) -> &str {
        "decorrelate_scalar_projection"
    }
}

#[derive(Default)]
struct ScalarProjectionRewriter {
    changed_children: Vec<bool>,
}

impl TreeNodeRewriter for ScalarProjectionRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        self.changed_children.push(false);
        Ok(Transformed::no(plan))
    }

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let children_changed = self.changed_children.pop().unwrap_or(false);
        // Only rewritten subtrees can make their ancestors' schemas stale.
        let plan = if children_changed {
            match plan {
                LogicalPlan::Union(union) => TypeCoercionRewriter::coerce_union(union)?,
                plan => plan.recompute_schema()?,
            }
        } else {
            plan
        };
        let rewritten = decorrelate_scalar_projection(plan)?;
        if let Some(parent_changed) = self.changed_children.last_mut() {
            *parent_changed |= children_changed || rewritten.transformed;
        }
        Ok(rewritten)
    }
}

fn decorrelate_scalar_projection(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let input_schema = match &plan {
        LogicalPlan::Projection(projection) => Arc::clone(projection.input.schema()),
        LogicalPlan::Filter(filter) => Arc::clone(filter.input.schema()),
        _ => return Ok(Transformed::no(plan)),
    };
    let names = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expression| {
        let name = names.save(&expression);
        expression
            .transform_up(|expression| {
                let Expr::ScalarSubquery(subquery) = &expression else {
                    return Ok(Transformed::no(expression));
                };
                let (projection, limit) = match subquery.subquery.as_ref() {
                    LogicalPlan::Projection(projection) => (projection, None),
                    LogicalPlan::Limit(limit) => {
                        let LogicalPlan::Projection(projection) = limit.input.as_ref() else {
                            return Ok(Transformed::no(expression));
                        };
                        if subquery
                            .subquery
                            .expressions()
                            .iter()
                            .any(Expr::contains_outer)
                        {
                            return Ok(Transformed::no(expression));
                        }
                        (projection, Some(limit))
                    }
                    _ => return Ok(Transformed::no(expression)),
                };
                let [value] = projection.expr.as_slice() else {
                    return Ok(Transformed::no(expression));
                };
                // Move only projection correlation. The producer and nested scopes
                // retain their ordinary analysis and execution.
                if !value.contains_outer()
                    || !projection.input.all_out_ref_exprs().is_empty()
                    || value.exists(|expression| {
                        Ok(match expression {
                            Expr::OuterReferenceColumn(_, column) => {
                                !input_schema.has_column(column)
                            }
                            Expr::ScalarSubquery(query) => !query.outer_ref_columns.is_empty(),
                            Expr::Exists(query) => !query.subquery.outer_ref_columns.is_empty(),
                            Expr::InSubquery(query) => !query.subquery.outer_ref_columns.is_empty(),
                            Expr::SetComparison(query) => {
                                !query.subquery.outer_ref_columns.is_empty()
                            }
                            _ => false,
                        })
                    })?
                {
                    return Ok(Transformed::no(expression));
                }

                let local_schema = projection.input.schema();
                let indices = value
                    .column_refs()
                    .into_iter()
                    .map(|column| local_schema.index_of_column(column))
                    .collect::<Result<BTreeSet<_>>>()?;
                let columns = local_schema.columns();
                let mut args = vec![lit("present"), lit(true)];
                let mut fields = vec![Arc::new(Field::new("present", DataType::Boolean, true))];
                for index in indices {
                    let name = index.to_string();
                    args.extend([lit(name.clone()), Expr::Column(columns[index].clone())]);
                    fields.push(Arc::new(
                        local_schema
                            .field(index)
                            .as_ref()
                            .clone()
                            .with_name(name)
                            .with_nullable(true),
                    ));
                }
                // Keep only referenced fields, preserving their metadata. Native scalar
                // execution supplies NULL for no row and rejects more than one row.
                let payload = cast(named_struct(args), DataType::Struct(fields.into()));
                let mut payload = LogicalPlan::Projection(Projection::try_new(
                    vec![payload],
                    Arc::clone(&projection.input),
                )?);
                if let Some(limit) = limit {
                    let mut limit = limit.clone();
                    limit.input = Arc::new(payload);
                    payload = LogicalPlan::Limit(limit);
                }
                let payload = scalar_subquery(Arc::new(payload));
                let value = value
                    .clone()
                    .unalias()
                    .transform_up(|expression| match expression {
                        Expr::OuterReferenceColumn(_, column) => {
                            Ok(Transformed::yes(Expr::Column(column)))
                        }
                        Expr::Column(column) => {
                            let index = local_schema.index_of_column(&column)?;
                            Ok(Transformed::yes(get_field(
                                payload.clone(),
                                index.to_string(),
                            )))
                        }
                        _ => Ok(Transformed::no(expression)),
                    })?
                    .data;
                let field = projection.schema.field(0);
                // A missing scalar row is NULL even if its projection (e.g. COALESCE)
                // would produce a value or error. An actual all-NULL row still executes it.
                let value = when(
                    get_field(payload, "present").is_null(),
                    lit(ScalarValue::try_from(field.data_type())?),
                )
                .otherwise(value)?;
                let value = if field.metadata().is_empty() {
                    value
                } else {
                    Expr::Cast(Cast::new_from_field(
                        Box::new(value),
                        Arc::new(field.as_ref().clone().with_nullable(true)),
                    ))
                };
                Ok(Transformed::yes(value))
            })
            .map(|rewritten| rewritten.update_data(|expression| name.restore(expression)))
    })?;
    if rewritten.transformed {
        rewritten.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(rewritten)
    }
}
