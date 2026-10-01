use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{
    Aggregate, Expr, ExprSchemable, Limit, LogicalPlan, Projection, SubqueryAlias, Union, Window,
};

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
        let projection = match &plan {
            LogicalPlan::Projection(projection) => {
                if matches!(projection.input.as_ref(), LogicalPlan::Window(_)) {
                    return extract_window_conditionals(plan, config);
                }
                // Most projections are not above a UNION. Check that before scanning
                // expressions, which is otherwise repeated for every projection in a chain.
                if !has_strict_union(&plan)? {
                    return Ok(Transformed::no(plan));
                }
                projection
            }
            LogicalPlan::Aggregate(_) => return pull_out_grouping_conditionals(plan, config),
            _ => return Ok(Transformed::no(plan)),
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
        // Moving a conditional only changes evaluation when it selects a strictly cast
        // UNION column. Other columns are evaluated in the inputs either way.
        if !selects_strict_column(&wrappers, input)? {
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

/// Like Spark's `ExtractWindowExpressions`, evaluates conditional select-list items and
/// window inputs below the windows, where the projection can then move through the UNION.
fn extract_window_conditionals(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut windows = Vec::new();
    let mut input = &projection.input;
    while let LogicalPlan::Window(window) = input.as_ref() {
        windows.push(window);
        input = &window.input;
    }
    if !has_strict_union(input)? {
        return Ok(Transformed::no(plan));
    }
    let Some(mut extractor) = ConditionalExtractor::try_new(input, config)? else {
        return Ok(Transformed::no(plan));
    };
    // TODO: Match Spark's eager cast errors when a composite UNION producer is
    // shared by window arguments and select-list items. Shared projection inlining
    // can still make these casts lazy when Spark's CollapseProject keeps them eager.
    let window_exprs = windows
        .iter()
        .map(|window| {
            window
                .window_expr
                .iter()
                .map(|expr| {
                    with_name(expr, |expr| {
                        expr.transform_down(|expr| {
                            let Expr::WindowFunction(mut function) = expr else {
                                return Ok(Transformed::no(expr));
                            };
                            let params = &mut function.params;
                            for exprs in [&mut params.args, &mut params.partition_by] {
                                *exprs = std::mem::take(exprs)
                                    .into_iter()
                                    .map(|expr| extractor.extract(expr))
                                    .collect::<Result<_>>()?;
                            }
                            for sort in &mut params.order_by {
                                sort.expr = extractor.extract(sort.expr.clone())?;
                            }
                            params.filter = params
                                .filter
                                .take()
                                .map(|filter| extractor.extract(*filter).map(Box::new))
                                .transpose()?;
                            Ok(Transformed::yes(Expr::WindowFunction(function)))
                        })
                        .and_then(|result| extractor.unqualify(result.data))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let exprs = projection
        .expr
        .iter()
        .map(|expr| with_name(expr, |expr| extractor.rewrite(expr)))
        .collect::<Result<Vec<_>>>()?;
    let Some(mut input) = extractor.project(input)? else {
        return Ok(Transformed::no(plan));
    };
    for window_exprs in window_exprs.into_iter().rev() {
        input = Arc::new(LogicalPlan::Window(Window::try_new(window_exprs, input)?));
    }
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new_with_schema(exprs, input, Arc::clone(&projection.schema))?,
    )))
}

/// Like Spark's `PullOutGroupingExpressions`, evaluates conditional grouping expressions
/// below the aggregate, where the projection can then move through the UNION.
fn pull_out_grouping_conditionals(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Aggregate(aggregate) = &plan else {
        return Ok(Transformed::no(plan));
    };
    if !has_strict_union(&aggregate.input)? {
        return Ok(Transformed::no(plan));
    }
    let Some(mut extractor) = ConditionalExtractor::try_new(&aggregate.input, config)? else {
        return Ok(Transformed::no(plan));
    };
    let mut grouping_keys = HashMap::<Expr, Expr>::new();
    let group_expr = aggregate
        .group_expr
        .iter()
        .map(|expr| {
            if matches!(expr, Expr::GroupingSet(_)) {
                // Grouping sets must remain grouping sets: alias their rewritten keys,
                // not the container, so Aggregate retains its grouping ID and nullability.
                expr.clone()
                    .map_children(|expr| {
                        // Reuse the same extracted column when distinct sets share a key.
                        // Otherwise Aggregate would treat it as several output fields.
                        if let Some(rewritten) = grouping_keys.get(&expr) {
                            return Ok(Transformed::yes(rewritten.clone()));
                        }
                        let rewritten = with_name(&expr, |expr| extractor.rewrite(expr))?;
                        grouping_keys.insert(expr, rewritten.clone());
                        Ok(Transformed::yes(rewritten))
                    })
                    .map(|result| result.data)
            } else {
                with_name(expr, |expr| extractor.rewrite(expr))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let aggr_expr = aggregate
        .aggr_expr
        .iter()
        .map(|expr| with_name(expr, |expr| extractor.unqualify(expr)))
        .collect::<Result<Vec<_>>>()?;
    let Some(input) = extractor.project(&aggregate.input)? else {
        return Ok(Transformed::no(plan));
    };
    Ok(Transformed::yes(LogicalPlan::Aggregate(
        Aggregate::try_new(input, group_expr, aggr_expr)?,
    )))
}

/// Rewrites an expression and keeps its output name.
fn with_name(expr: &Expr, rewrite: impl FnOnce(Expr) -> Result<Expr>) -> Result<Expr> {
    let name = NamePreserver::new_for_projection().save(expr);
    Ok(name.restore(rewrite(expr.clone().unalias_nested().data)?))
}

/// Collects conditional expressions to evaluate in a projection above an input.
/// The projection exposes the input columns unqualified: DataFusion derives
/// unqualified UNION fields once the projection moves through the UNION.
struct ConditionalExtractor<'a> {
    schema: &'a DFSchema,
    config: &'a dyn OptimizerConfig,
    strict: Vec<bool>,
    extracted: Vec<Expr>,
}

impl<'a> ConditionalExtractor<'a> {
    fn try_new(input: &'a LogicalPlan, config: &'a dyn OptimizerConfig) -> Result<Option<Self>> {
        let schema = input.schema();
        let strict = strict_columns(input)?;
        let mut names = HashSet::new();
        Ok((strict.contains(&true)
            && schema
                .fields()
                .iter()
                .all(|field| names.insert(field.name())))
        .then_some(Self {
            schema,
            config,
            strict,
            extracted: vec![],
        }))
    }

    fn rewrite(&mut self, expr: Expr) -> Result<Expr> {
        let expr = self.extract(expr)?;
        self.unqualify(expr)
    }

    /// Replaces a conditional expression over the input columns with a column reference.
    fn extract(&mut self, expr: Expr) -> Result<Expr> {
        if matches!(expr, Expr::Column(_))
            || !expr.exists(|expr| Ok(expr.short_circuits()))?
            || expr.is_volatile()
            || has_correlated_subquery(std::slice::from_ref(&expr))?
            || !expr
                .column_refs()
                .iter()
                .all(|column| self.schema.has_column(column))
            // Like `PushUnionConditional`, move only conditionals that select a strictly
            // cast UNION column; other columns are evaluated in the inputs either way.
            || !expr.column_refs().iter().any(|column| {
                self.schema
                    .index_of_column(column)
                    .is_ok_and(|index| self.strict[index])
            })
        {
            return Ok(expr);
        }
        let name = self.config.alias_generator().next("__union_conditional");
        self.extracted.push(expr.alias(&name));
        Ok(Expr::Column(Column::new_unqualified(name)))
    }

    /// Refers to input columns by their projected names.
    fn unqualify(&self, expr: Expr) -> Result<Expr> {
        expr.transform_up(|expr| match expr {
            Expr::Column(column)
                if column.relation.is_some() && self.schema.has_column(&column) =>
            {
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    column.name,
                ))))
            }
            _ => Ok(Transformed::no(expr)),
        })
        .map(|result| result.data)
    }

    fn project(self, input: &Arc<LogicalPlan>) -> Result<Option<Arc<LogicalPlan>>> {
        if self.extracted.is_empty() {
            return Ok(None);
        }
        let expressions = self
            .schema
            .iter()
            .map(|(qualifier, field)| {
                let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
                if qualifier.is_some() {
                    column.alias(field.name())
                } else {
                    column
                }
            })
            .chain(self.extracted)
            .collect();
        Ok(Some(Arc::new(LogicalPlan::Projection(
            Projection::try_new(expressions, Arc::clone(input))?,
        ))))
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
                if has_strict_cast(expr, projection.input.schema())? {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// Whether the expression contains a type-changing `spark_conditional_cast`.
fn has_strict_cast(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    expr.exists(|expr| {
        let Expr::ScalarFunction(function) = expr else {
            return Ok(false);
        };
        let [argument] = function.args.as_slice() else {
            return Ok(false);
        };
        Ok(function.func.name() == "spark_conditional_cast"
            && argument.get_type(schema)? != expr.get_type(schema)?)
    })
}

/// For each output column, whether the nodes that `has_strict_union` inspects
/// produce it with a type-changing `spark_conditional_cast`.
fn strict_columns(plan: &LogicalPlan) -> Result<Vec<bool>> {
    match plan {
        LogicalPlan::Union(union) => {
            let mut strict = vec![false; plan.schema().fields().len()];
            for input in &union.inputs {
                for (column, input) in strict.iter_mut().zip(strict_columns(input)?) {
                    *column |= input;
                }
            }
            Ok(strict)
        }
        LogicalPlan::SubqueryAlias(alias) => strict_columns(&alias.input),
        LogicalPlan::Limit(limit) => strict_columns(&limit.input),
        LogicalPlan::Projection(projection) => {
            let schema = projection.input.schema();
            let input = strict_columns(&projection.input)?;
            projection
                .expr
                .iter()
                .map(|expr| {
                    if has_strict_cast(expr, schema)? {
                        return Ok(true);
                    }
                    for column in expr.column_refs() {
                        if input[schema.index_of_column(column)?] {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
                .collect()
        }
        _ => Ok(vec![false; plan.schema().fields().len()]),
    }
}

/// Whether a short-circuiting expression of the top projection depends on a
/// strictly cast column of the UNION below the wrappers.
fn selects_strict_column(wrappers: &[&LogicalPlan], input: &LogicalPlan) -> Result<bool> {
    let Some((LogicalPlan::Projection(top), wrappers)) = wrappers.split_first() else {
        return Ok(true);
    };
    let schema = top.input.schema();
    let mut columns = HashSet::new();
    for expr in &top.expr {
        expr.apply(|expr| {
            if !expr.short_circuits() {
                return Ok(TreeNodeRecursion::Continue);
            }
            for column in expr.column_refs() {
                columns.insert(schema.index_of_column(column)?);
            }
            Ok(TreeNodeRecursion::Jump)
        })?;
    }
    for wrapper in wrappers {
        if let LogicalPlan::Projection(projection) = wrapper {
            let schema = projection.input.schema();
            let mut inputs = HashSet::new();
            for index in columns {
                for column in projection.expr[index].column_refs() {
                    inputs.insert(schema.index_of_column(column)?);
                }
            }
            columns = inputs;
        }
    }
    let strict = strict_columns(input)?;
    Ok(columns.into_iter().any(|index| strict[index]))
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
