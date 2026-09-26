use std::sync::Arc;

use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, ExprSchemable, Limit, LogicalPlan, Projection, SubqueryAlias, Union};

/// Keep UNION's fallible numeric casts inside their selecting conditional before
/// constant folding, common-expression extraction, and physical lambda binding.
#[derive(Debug, Default)]
pub struct PushUnionConditional;

impl OptimizerRule for PushUnionConditional {
    fn name(&self) -> &str {
        "push_union_conditional"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let mut conditional = false;
        for expression in &projection.expr {
            conditional |= expression.exists(|expr| Ok(expr.short_circuits()))?;
        }
        if !conditional {
            return Ok(Transformed::no(plan));
        }

        let mut wrappers = Vec::new();
        let mut input = &plan;
        loop {
            match input {
                LogicalPlan::Projection(projection) => {
                    if projection.expr.iter().any(Expr::is_volatile)
                        || has_correlated_subquery(&projection.expr)?
                    {
                        return Ok(Transformed::no(plan));
                    }
                    wrappers.push(input);
                    input = projection.input.as_ref();
                }
                LogicalPlan::SubqueryAlias(alias) => {
                    wrappers.push(input);
                    input = alias.input.as_ref();
                }
                _ => break,
            }
        }
        if !has_strict_union(input)? {
            return Ok(Transformed::no(plan));
        }
        if let LogicalPlan::Limit(limit) = input {
            // Spark also pushes deterministic projections through LIMIT/OFFSET.
            // TODO: Match Spark's eager errors from constant UNION casts below LIMIT.
            // DataFusion defers failed UDF folding, so this can suppress errors that
            // Spark raises before pushing through UNION. Preserve Spark's empty-input
            // pruning and conditional error context when implementing shared folding.
            // Keep the limit above the projected UNION so row selection is unchanged.
            return Ok(Transformed::yes(LogicalPlan::Limit(Limit {
                skip: limit.skip.clone(),
                fetch: limit.fetch.clone(),
                input: project_input(&wrappers, &limit.input, config)?,
            })));
        }
        let LogicalPlan::Union(union) = input else {
            return Ok(Transformed::no(plan));
        };

        // Spark's PushProjectionThroughUnion and CollapseProject run before CSE.
        // Retain repeated composite producers: expanding even constant arithmetic
        // before folding can grow exponentially. Literal casts may move into the
        // selecting branch without duplicating a computation tree.
        let inputs = union
            .inputs
            .iter()
            .map(|input| project_input(&wrappers, input, config))
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::yes(LogicalPlan::Union(Union {
            inputs,
            schema: Arc::clone(plan.schema()),
        })))
    }
}

fn has_strict_union(mut input: &LogicalPlan) -> Result<bool> {
    loop {
        input = match input {
            LogicalPlan::Projection(projection) => projection.input.as_ref(),
            LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
            LogicalPlan::Limit(limit) => limit.input.as_ref(),
            _ => break,
        };
    }
    if !matches!(input, LogicalPlan::Union(_)) {
        return Ok(false);
    }
    let mut found = false;
    input.apply(|input| {
        if !matches!(
            input,
            LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_)
        ) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if let LogicalPlan::Projection(projection) = input {
            for expr in &projection.expr {
                if expr.exists(|expr| {
                    let Expr::ScalarFunction(function) = expr else {
                        return Ok(false);
                    };
                    let [argument] = function.args.as_slice() else {
                        return Ok(false);
                    };
                    Ok(function.func.name() == "spark_conditional_cast"
                        && argument.get_type(projection.input.schema())?
                            != expr.get_type(projection.input.schema())?)
                })? {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn project_input(
    wrappers: &[&LogicalPlan],
    input: &Arc<LogicalPlan>,
    config: &dyn OptimizerConfig,
) -> Result<Arc<LogicalPlan>> {
    let mut input = inline_input(input, config)?;
    for wrapper in wrappers.iter().rev() {
        input = Arc::new(match wrapper {
            LogicalPlan::Projection(projection) => {
                let expressions =
                    remap_columns(&projection.expr, projection.input.schema(), input.schema())?;
                LogicalPlan::Projection(inline_projection(
                    Projection::try_new_with_schema(
                        expressions,
                        input,
                        Arc::clone(&projection.schema),
                    )?,
                    config,
                )?)
            }
            LogicalPlan::SubqueryAlias(alias) => {
                LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, alias.alias.clone())?)
            }
            _ => unreachable!(),
        });
    }
    Ok(input)
}

fn inline_input(
    input: &Arc<LogicalPlan>,
    config: &dyn OptimizerConfig,
) -> Result<Arc<LogicalPlan>> {
    match input.as_ref() {
        LogicalPlan::Projection(projection) => {
            Ok(Arc::new(LogicalPlan::Projection(inline_projection(
                Projection::try_new_with_schema(
                    projection.expr.clone(),
                    inline_input(&projection.input, config)?,
                    Arc::clone(&projection.schema),
                )?,
                config,
            )?)))
        }
        LogicalPlan::SubqueryAlias(alias) => Ok(Arc::new(LogicalPlan::SubqueryAlias(
            SubqueryAlias::try_new(inline_input(&alias.input, config)?, alias.alias.clone())?,
        ))),
        _ => Ok(Arc::clone(input)),
    }
}

fn remap_columns(expressions: &[Expr], source: &DFSchema, target: &DFSchema) -> Result<Vec<Expr>> {
    let name_preserver = NamePreserver::new_for_projection();
    expressions
        .iter()
        .cloned()
        .map(|expr| {
            let name = name_preserver.save(&expr);
            expr.transform_up(|expr| match expr {
                Expr::Column(column) => {
                    let index = source.index_of_column(&column)?;
                    Ok(Transformed::yes(Expr::Column(Column::from(
                        target.qualified_field(index),
                    ))))
                }
                _ => Ok(Transformed::no(expr)),
            })
            .map(|result| name.restore(result.data))
        })
        .collect()
}

fn inline_projection(
    mut projection: Projection,
    config: &dyn OptimizerConfig,
) -> Result<Projection> {
    // Correlated subquery plans retain outer bindings that expression-only column remapping
    // cannot update. Let the existing decorrelation rules lower them to joins
    // before moving or inlining their projections.
    if has_correlated_subquery(&projection.expr)? {
        return Ok(projection);
    }
    loop {
        match projection.input.as_ref() {
            LogicalPlan::SubqueryAlias(alias) => {
                projection.expr =
                    remap_columns(&projection.expr, &alias.schema, alias.input.schema())?;
                projection.input = Arc::clone(&alias.input);
            }
            LogicalPlan::Projection(inner) => {
                if has_correlated_subquery(&inner.expr)? {
                    break;
                }
                let mut references = Default::default();
                for expression in &projection.expr {
                    expression.add_column_ref_counts(&mut references);
                }
                let name_preserver = NamePreserver::new_for_projection();
                let simplifier = ExprSimplifier::new(
                    SimplifyContext::builder()
                        .with_schema(Arc::clone(inner.input.schema()))
                        .with_config_options(config.options())
                        .with_query_execution_start_time(config.query_execution_start_time())
                        .build(),
                );
                let mut producers = inner.expr.clone();
                let mut can_inline = true;
                for (column, count) in references {
                    let producer = &mut producers[inner.schema.index_of_column(column)?];
                    if producer.is_volatile() {
                        can_inline = false;
                        break;
                    }
                    if count > 1
                        && !producer.placement().should_push_to_leaves()
                        && !is_literal_cast(producer)
                    {
                        // Fold constant constructors once before considering duplication.
                        // Failed literal casts must retain their runtime error and remain
                        // unevaluated when an enclosing CASE does not select them.
                        let name = name_preserver.save(producer);
                        if let Ok(simplified) = simplifier.simplify(producer.clone()) {
                            *producer = name.restore(simplified);
                        }
                        if !producer.placement().should_push_to_leaves()
                            && !is_literal_cast(producer)
                        {
                            can_inline = false;
                            break;
                        }
                    }
                }
                if !can_inline {
                    break;
                }
                let name_preserver = NamePreserver::new_for_projection();
                projection.expr = projection
                    .expr
                    .into_iter()
                    .map(|expr| {
                        let name = name_preserver.save(&expr);
                        expr.transform_up(|expr| match expr {
                            Expr::Column(column) => {
                                let index = inner.schema.index_of_column(&column)?;
                                Ok(Transformed::yes(
                                    producers[index].clone().unalias_nested().data,
                                ))
                            }
                            _ => Ok(Transformed::no(expr)),
                        })
                        .map(|result| name.restore(result.data))
                    })
                    .collect::<Result<Vec<_>>>()?;
                projection.input = Arc::clone(&inner.input);
            }
            _ => break,
        }
    }
    Ok(projection)
}

fn has_correlated_subquery(expressions: &[Expr]) -> Result<bool> {
    for expression in expressions {
        if expression.exists(|expr| {
            let subquery = match expr {
                Expr::ScalarSubquery(subquery) => subquery,
                Expr::Exists(exists) => &exists.subquery,
                Expr::InSubquery(in_subquery) => &in_subquery.subquery,
                Expr::SetComparison(comparison) => &comparison.subquery,
                _ => return Ok(false),
            };
            Ok(!subquery.outer_ref_columns.is_empty())
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

// DataFusion also retains repeated literals. These linear cast chains are cheap
// and must stay inside CASE so invalid unselected STRING values are not evaluated.
fn is_literal_cast(expression: &Expr) -> bool {
    match expression {
        Expr::Literal(_, _) => true,
        Expr::Alias(alias) => is_literal_cast(&alias.expr),
        Expr::Cast(cast) => is_literal_cast(&cast.expr),
        Expr::TryCast(cast) => is_literal_cast(&cast.expr),
        Expr::ScalarFunction(function) if function.func.name() == "spark_conditional_cast" => {
            matches!(function.args.as_slice(), [argument] if is_literal_cast(argument))
        }
        _ => false,
    }
}
