use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::MemTable;
use datafusion::datasource::DefaultTableSource;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::simplify_expressions::{
    ExprSimplifier, SimplifyContext, SimplifyExpressions,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, DFSchemaRef, Result, ScalarValue, plan_err};
use datafusion_expr::expr::InSubquery;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{
    Distinct, Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    SubqueryAlias, Union, expr_fn, lit,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_python_udf::udf::expr_contains_python_udf;

mod conditional;
mod local;

/// Spark folds expressions before replacing projected IN with an existence join.
/// Fold affected expressions and literal NULL operands first, then push their
/// alias filters before materializing the remaining existence joins. Restrict
/// pushdown to these predicates so unrelated optimizer ordering is preserved.
#[derive(Debug)]
pub struct RewriteProjectedIn;

impl OptimizerRule for RewriteProjectedIn {
    fn name(&self) -> &str {
        "rewrite_projected_in"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut prepared_marks = HashSet::new();
        let prepared = plan.transform_up_with_subqueries(|plan| {
            rewrite_projection(plan, config, true, &mut prepared_marks)
        })?;
        if !prepared.transformed {
            return Ok(prepared);
        }
        let pushed = prepared.data.transform_down_with_subqueries(|plan| {
            let LogicalPlan::Filter(filter) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let needed = filter
                .predicate
                .column_refs()
                .into_iter()
                .cloned()
                .collect();
            if references_projected_in(&filter.input, &needed)? {
                PushDownFilter::new().rewrite(plan, config)
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        // Prune unused IN expressions before they become joins, including those
        // referenced only by a filter that was just pushed down. NULL producers
        // were classified before projection merging can inline their operands.
        let mut remaining = false;
        pushed.data.apply_with_subqueries(|plan| {
            if let LogicalPlan::Projection(projection) = plan {
                for expr in &projection.expr {
                    remaining |= has_uncorrelated_in(expr)?;
                }
            }
            Ok(if remaining {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        let original = (!remaining).then(|| pushed.data.clone());
        let pruned = OptimizeProjections::new()
            .rewrite(pushed.data, config)?
            .data;
        // Literal NULL operands already became existence joins during preparation.
        // Remove only joins created here whose mark disappeared during pruning.
        let mut referenced = HashSet::new();
        referenced.extend(pruned.schema().columns());
        pruned.apply_with_subqueries(|plan| {
            for expr in plan.expressions() {
                referenced.extend(expr.column_refs().into_iter().cloned());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let pruned = pruned.transform_up_with_subqueries(|plan| {
            if let LogicalPlan::Join(join) = &plan
                && join.join_type == JoinType::LeftMark
                && let Some(mark) = join.schema.columns().last()
                && prepared_marks.contains(mark)
                && !referenced.contains(mark)
            {
                return Ok(Transformed::yes(Arc::unwrap_or_clone(Arc::clone(
                    &join.left,
                ))));
            }
            Ok(Transformed::no(plan))
        })?;
        // Keep unrelated inputs intact when no IN expression or join was pruned.
        let plan = match original {
            Some(original) if !pruned.transformed => original,
            _ => pruned.data,
        };
        let rewritten = plan.transform_up_with_subqueries(|plan| {
            rewrite_projection(plan, config, false, &mut prepared_marks)
        })?;
        Ok(Transformed::yes(rewritten.data))
    }
}

fn has_uncorrelated_in(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::InSubquery(subquery)
            if subquery.subquery.outer_ref_columns.is_empty()))
    })
}

/// Keep non-projection conditional subqueries outside this projection rule.
/// Their normalization still belongs to the ordinary predicate optimizer.
fn needs_conditional_normalization(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        let coalesce_only = match expr {
            Expr::Not(_) => false,
            Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Eq | Operator::NotEq) => true,
            _ => return Ok(false),
        };
        expr.exists(|child| {
            let boundary = matches!(child, Expr::ScalarFunction(function)
                if matches!(function.func.name(), "coalesce" | "nvl" | "nvl2"))
                || (!coalesce_only && matches!(child, Expr::Case(_)));
            Ok(boundary && has_uncorrelated_in(child)?)
        })
    })
}

/// Only push filters whose referenced producers contain projected IN. Spark does
/// this before existence joins are introduced, and does not cross LIMIT/OFFSET or
/// a projection with nondeterministic expressions.
fn references_projected_in(plan: &LogicalPlan, needed: &HashSet<Column>) -> Result<bool> {
    if needed.is_empty() || plan.fetch()?.is_some() || plan.skip()?.is_some() {
        return Ok(false);
    }
    match plan {
        LogicalPlan::Projection(projection) => {
            let mut input_needed = HashSet::new();
            for expr in &projection.expr {
                if has_volatile_expression(expr)? {
                    return Ok(false);
                }
            }
            for (expr, column) in projection.expr.iter().zip(projection.schema.columns()) {
                if needed.contains(&column) {
                    if has_uncorrelated_in(expr)? {
                        return Ok(true);
                    }
                    input_needed.extend(expr.column_refs().into_iter().cloned());
                }
            }
            references_projected_in(&projection.input, &input_needed)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input_needed = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
                .filter_map(|(input, output)| needed.contains(&output).then_some(input))
                .collect();
            references_projected_in(&alias.input, &input_needed)
        }
        LogicalPlan::Filter(filter) => references_projected_in(&filter.input, needed),
        LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            references_projected_in(plan.inputs()[0], needed)
        }
        LogicalPlan::Join(join) => Ok(references_projected_in(&join.left, needed)?
            || references_projected_in(&join.right, needed)?),
        _ => Ok(false),
    }
}

fn simplifier(schema: DFSchemaRef, config: &dyn OptimizerConfig) -> ExprSimplifier {
    ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(schema)
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build(),
    )
}

fn replace_constants(expr: Expr, constants: &HashMap<Column, Expr>) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(value) = constants.get(column)
        {
            return Ok(Transformed::yes(value.clone()));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Only producer expressions are constants: a nullable VALUES/table column is
/// still a column, even when every row happens to contain NULL. As in Spark's
/// FoldablePropagation, UNION and null-producing outer-join sides are boundaries.
fn constant_columns(
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
    needed: &HashSet<Column>,
) -> Result<HashMap<Column, Expr>> {
    if needed.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(producer_constants(plan, config, needed)?.columns)
}

#[derive(Default)]
struct ProducerConstants {
    columns: HashMap<Column, Expr>,
    local_relation: bool,
}

/// Spark's ConvertToLocalRelation evaluates whole projections before constant
/// propagation. Its Unevaluable check includes subqueries and Python UDFs, but
/// permits ordinary nondeterministic functions such as rand().
fn locally_evaluable(expr: &Expr) -> Result<bool> {
    Ok(!expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::ScalarSubquery(_)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::GroupingSet(_)
                | Expr::Placeholder(_)
                | Expr::Unnest(_)
        ))
    })? && !expr_contains_python_udf(expr)?)
}

fn producer_constants(
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
    needed: &HashSet<Column>,
) -> Result<ProducerConstants> {
    let (expressions, input) = match plan {
        LogicalPlan::Projection(projection) => (
            projection.expr.iter().collect::<Vec<_>>(),
            &projection.input,
        ),
        LogicalPlan::Aggregate(aggregate) => (
            aggregate
                .group_expr
                .iter()
                .chain(&aggregate.aggr_expr)
                .collect(),
            &aggregate.input,
        ),
        LogicalPlan::SubqueryAlias(alias) => {
            let mapping = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns());
            let input_needed = mapping
                .filter_map(|(input, output)| needed.contains(&output).then_some(input))
                .collect();
            let mut constants = producer_constants(&alias.input, config, &input_needed)?;
            constants.columns = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
                .filter_map(|(input, output)| {
                    constants
                        .columns
                        .remove(&input)
                        .map(|value| (output, value))
                })
                .collect();
            return Ok(constants);
        }
        LogicalPlan::Filter(filter) => {
            // Spark simplifies null-rejecting predicates before propagating constants.
            // Inspect the simplified predicate and join type only on a copy.
            // TODO: Normalize aliases above the join before checking whether a
            // null-rejecting predicate exposes its nullable-side constants.
            let mut input = filter.input.as_ref();
            while let LogicalPlan::Projection(projection) = input {
                input = &projection.input;
            }
            if !needed.is_empty()
                && matches!(input, LogicalPlan::Join(join) if join.join_type.is_outer())
            {
                let mut normalized = filter.clone();
                normalized.predicate = simplifier(Arc::clone(filter.input.schema()), config)
                    .simplify(normalized.predicate)?;
                let rewritten =
                    EliminateOuterJoin.rewrite(LogicalPlan::Filter(normalized), config)?;
                if rewritten.transformed {
                    return producer_constants(&rewritten.data, config, needed);
                }
            }
            let mut constants = producer_constants(&filter.input, config, needed)?;
            if constants.local_relation {
                constants.local_relation = locally_evaluable(&filter.predicate)?;
            }
            return Ok(constants);
        }
        LogicalPlan::Limit(limit) => {
            let mut constants = producer_constants(&limit.input, config, needed)?;
            // The analyzer widens SQL's integer literal to Int64. Keep computed
            // limits and explicit OFFSET as Spark's local-materialization boundaries.
            let literal_fetch = match limit.fetch.as_deref() {
                Some(Expr::Literal(_, _)) => true,
                Some(Expr::Cast(cast)) => {
                    cast.field.data_type() == &DataType::Int64
                        && matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Int32(_), _))
                }
                _ => false,
            };
            constants.local_relation &= limit.skip.is_none() && literal_fetch;
            return Ok(constants);
        }
        LogicalPlan::Unnest(unnest) => {
            let needed = needed
                .iter()
                .filter(|column| !unnest.exec_columns.contains(column))
                .cloned()
                .collect();
            let mut constants = producer_constants(&unnest.input, config, &needed)?;
            constants.local_relation = false;
            return Ok(constants);
        }
        LogicalPlan::Extension(extension)
            if extension.node.as_any().is::<MonotonicIdNode>()
                || extension.node.as_any().is::<SparkPartitionIdNode>() =>
        {
            // Spark evaluates these scalar functions inside local projections.
            // Their Sail plan nodes do not introduce a local-evaluation boundary.
            return producer_constants(plan.inputs()[0], config, needed);
        }
        LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            let mut constants = producer_constants(plan.inputs()[0], config, needed)?;
            constants.local_relation = false;
            return Ok(constants);
        }
        LogicalPlan::Join(join) => {
            let mut constants = ProducerConstants::default();
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::LeftMark
            ) {
                constants
                    .columns
                    .extend(constant_columns(&join.left, config, needed)?);
            }
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::RightMark
            ) {
                constants
                    .columns
                    .extend(constant_columns(&join.right, config, needed)?);
            }
            return Ok(constants);
        }
        LogicalPlan::Values(_) => {
            return Ok(ProducerConstants {
                local_relation: true,
                ..Default::default()
            });
        }
        LogicalPlan::TableScan(scan) => {
            return Ok(ProducerConstants {
                local_relation: scan
                    .source
                    .downcast_ref::<DefaultTableSource>()
                    .is_some_and(|source| {
                        let mut provider = &source.table_provider;
                        while let Some(renamed) = provider.downcast_ref::<RenameTableProvider>() {
                            provider = renamed.inner();
                        }
                        provider.is::<MemTable>()
                    }),
                ..Default::default()
            });
        }
        _ => return Ok(ProducerConstants::default()),
    };
    if expressions.len() != plan.schema().fields().len() {
        return Ok(ProducerConstants::default());
    }
    let selected = expressions
        .iter()
        .copied()
        .zip(plan.schema().columns())
        .filter(|(_, column)| needed.contains(column))
        .collect::<Vec<_>>();
    let input_needed = selected
        .iter()
        .flat_map(|(expr, _)| expr.column_refs().into_iter().cloned())
        .collect();
    let constants = producer_constants(input, config, &input_needed)?;
    if constants.local_relation && matches!(plan, LogicalPlan::Projection(_)) {
        let mut local_relation = true;
        for expr in &expressions {
            local_relation &= locally_evaluable(expr)?;
        }
        if local_relation {
            return Ok(ProducerConstants {
                local_relation: true,
                ..Default::default()
            });
        }
    }
    // Spark extracts regular SELECT expressions below its Window operators.
    // Sail keeps them in this projection, but their local evaluation still
    // determines whether a NULL operand is a stored value or a foldable literal.
    let mut local_window_outputs = HashSet::new();
    if matches!(plan, LogicalPlan::Projection(_)) {
        let mut before_window = input.as_ref();
        let mut window_columns = HashSet::new();
        let mut evaluable = true;
        while let LogicalPlan::Window(window) = before_window {
            window_columns.extend(
                window
                    .schema
                    .columns()
                    .into_iter()
                    .skip(window.input.schema().fields().len()),
            );
            for expr in &window.window_expr {
                expr.apply_children(|child| {
                    evaluable &= locally_evaluable(child)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            before_window = &window.input;
        }
        if !window_columns.is_empty()
            && producer_constants(before_window, config, &HashSet::new())?.local_relation
        {
            for (expr, column) in expressions.iter().zip(plan.schema().columns()) {
                if expr
                    .column_refs()
                    .into_iter()
                    .all(|column| !window_columns.contains(column))
                {
                    evaluable &= locally_evaluable(expr)?;
                    local_window_outputs.insert(column);
                }
            }
            if !evaluable {
                local_window_outputs.clear();
            }
        }
    }
    let mut output = ProducerConstants::default();
    for (expr, column) in selected {
        if local_window_outputs.contains(&column) {
            continue;
        }
        let expr = conditional::simplify(
            replace_constants(expr.clone().unalias(), &constants.columns)?,
            Arc::clone(input.schema()),
            config,
        )?;
        if matches!(expr, Expr::Literal(_, _)) {
            output.columns.insert(column, expr);
        }
    }
    Ok(output)
}

fn has_volatile_expression(expr: &Expr) -> Result<bool> {
    if expr.is_volatile() {
        return Ok(true);
    }
    expr.exists(|expr| {
        let subquery = match expr {
            Expr::InSubquery(subquery) => &subquery.subquery,
            Expr::ScalarSubquery(subquery) => subquery,
            Expr::Exists(exists) => &exists.subquery,
            _ => return Ok(false),
        };
        let mut volatile = false;
        subquery.subquery.apply_with_subqueries(|plan| {
            volatile |= plan.expressions().iter().any(Expr::is_volatile);
            Ok(if volatile {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(volatile)
    })
}

fn union_branches(
    plan: &LogicalPlan,
    needed: &HashSet<Column>,
) -> Result<Option<Vec<LogicalPlan>>> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let mut input_needed = HashSet::new();
            for (expr, column) in projection.expr.iter().zip(projection.schema.columns()) {
                if !needed.contains(&column) {
                    continue;
                }
                if has_volatile_expression(expr)? {
                    return Ok(None);
                }
                input_needed.extend(expr.column_refs().into_iter().cloned());
            }
            union_branches(&projection.input, &input_needed)?
                .map(|inputs| {
                    inputs
                        .into_iter()
                        .map(|input| {
                            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                                projection.expr.clone(),
                                Arc::new(input),
                                Arc::clone(&projection.schema),
                            )?))
                        })
                        .collect()
                })
                .transpose()
        }
        LogicalPlan::Union(union) => {
            let inputs = union
                .inputs
                .iter()
                .map(|input| {
                    let expr = input
                        .schema()
                        .columns()
                        .into_iter()
                        .zip(union.schema.columns())
                        .map(|(input, output)| {
                            Expr::Column(input).alias_qualified(output.relation, output.name)
                        })
                        .collect();
                    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                        expr,
                        Arc::clone(input),
                        Arc::clone(&union.schema),
                    )?))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(inputs))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input_needed = alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
                .filter_map(|(input, output)| needed.contains(&output).then_some(input))
                .collect();
            union_branches(&alias.input, &input_needed)?
                .map(|inputs| {
                    inputs
                        .into_iter()
                        .map(|input| {
                            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                                Arc::new(input),
                                alias.alias.clone(),
                            )?))
                        })
                        .collect()
                })
                .transpose()
        }
        LogicalPlan::Filter(filter) => {
            // TODO: Apply Spark's random-bound simplification before deciding
            // whether a filter blocks UNION branch folding.
            if has_volatile_expression(&filter.predicate)? {
                return Ok(None);
            }
            let mut input_needed = needed.clone();
            input_needed.extend(filter.predicate.column_refs().into_iter().cloned());
            Ok(union_branches(&filter.input, &input_needed)?.map(|inputs| {
                inputs
                    .into_iter()
                    .map(|input| {
                        let mut branch = filter.clone();
                        branch.input = Arc::new(input);
                        LogicalPlan::Filter(branch)
                    })
                    .collect()
            }))
        }
        // TODO: Push deterministic projected IN through LIMIT/OFFSET before
        // UNION branch folding while retaining the global cardinality boundary.
        _ => Ok(None),
    }
}

fn rewrite_projection(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
    prepare: bool,
    prepared_marks: &mut HashSet<Column>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(mut projection) = plan else {
        return Ok(Transformed::no(plan));
    };
    let mut found = false;
    for expr in &projection.expr {
        found |= has_uncorrelated_in(expr)?;
    }
    if !found {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    }
    if prepare {
        // Early local evaluation also fixes the LHS producer boundary, including
        // local inputs exposed by empty UNION branches and outer joins. Reuse the
        // evaluated input so volatile expressions are not run just for analysis.
        projection.input = Arc::new(local::materialize(
            Arc::unwrap_or_clone(projection.input),
            config,
        )?);
    }
    let needed = projection
        .expr
        .iter()
        .filter(|expr| has_uncorrelated_in(expr).unwrap_or(false))
        .flat_map(|expr| expr.column_refs().into_iter().cloned())
        .collect();
    // Spark pushes deterministic projections through UNION before folding their
    // operands. Only split the joins when branch constants affect these IN expressions.
    let mut deterministic = true;
    for expr in &projection.expr {
        deterministic &= !has_volatile_expression(expr)?;
    }
    let projection_needed = projection
        .expr
        .iter()
        .flat_map(|expr| expr.column_refs().into_iter().cloned())
        .collect();
    if prepare
        && deterministic
        && let Some(inputs) = union_branches(&projection.input, &projection_needed)?
    {
        let mut needs_null_propagation = false;
        for input in &inputs {
            let constants = constant_columns(input, config, &needed)?;
            if constants.is_empty() {
                continue;
            }
            for expr in &projection.expr {
                needs_null_propagation |= expr.exists(|expr| {
                    let Expr::InSubquery(subquery) = expr else {
                        return Ok(false);
                    };
                    if !subquery.subquery.outer_ref_columns.is_empty()
                        || matches!(subquery.expr.as_ref(), Expr::Literal(value, _) if value.is_null())
                    {
                        return Ok(false);
                    }
                    let operand = conditional::simplify(
                        replace_constants(*subquery.expr.clone(), &constants)?,
                        Arc::clone(input.schema()),
                        config,
                    )?;
                    Ok(matches!(operand, Expr::Literal(value, _) if value.is_null()))
                })?;
            }
        }
        if needs_null_propagation {
            let inputs = inputs
                .into_iter()
                .map(|input| {
                    let branch = LogicalPlan::Projection(Projection::try_new_with_schema(
                        projection.expr.clone(),
                        Arc::new(input),
                        Arc::clone(&projection.schema),
                    )?);
                    Ok(Arc::new(
                        rewrite_projection(branch, config, true, prepared_marks)?.data,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Transformed::yes(LogicalPlan::Union(Union {
                inputs,
                schema: projection.schema,
            })));
        }
    }
    let constants = if prepare {
        constant_columns(&projection.input, config, &needed)?
    } else {
        HashMap::new()
    };
    let schema = Arc::clone(projection.input.schema());
    let mut rewriter = InRewriter {
        plan: Arc::unwrap_or_clone(projection.input),
        config,
        prepare,
        prepared_marks,
    };
    let expr = projection
        .expr
        .into_iter()
        .map(|expr| {
            if !has_uncorrelated_in(&expr)? {
                return Ok(expr);
            }
            let name = NamePreserver::new_for_projection().save(&expr);
            // Spark optimizes subqueries before folding the containing expression,
            // including branches whose IN expression will disappear entirely.
            let expr = if prepare {
                prepare_in_subqueries(expr, config)?
            } else {
                expr
            };
            // Spark removes identity casts before deciding whether NOT applies
            // directly to IN (and therefore needs a null-aware existence join).
            let expr = replace_constants(expr, &constants)?
                .transform_up(|expr| match expr {
                    Expr::Cast(cast)
                        if cast.expr.get_type(schema.as_ref())? == *cast.field.data_type() =>
                    {
                        Ok(Transformed::yes(*cast.expr))
                    }
                    Expr::TryCast(cast)
                        if cast.expr.get_type(schema.as_ref())? == *cast.field.data_type() =>
                    {
                        Ok(Transformed::yes(*cast.expr))
                    }
                    expr => Ok(Transformed::no(expr)),
                })
                .data()?;
            let expr = conditional::simplify(expr, Arc::clone(&schema), config)?;
            Ok(name.restore(expr.rewrite(&mut rewriter)?.data))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new_with_schema(expr, Arc::new(rewriter.plan), projection.schema)?,
    )))
}

fn prepare_in_subqueries(expr: Expr, config: &dyn OptimizerConfig) -> Result<Expr> {
    expr.transform_up(|expr| {
        let Expr::InSubquery(mut subquery) = expr else {
            return Ok(Transformed::no(expr));
        };
        if !subquery.subquery.outer_ref_columns.is_empty() {
            return Ok(Transformed::no(Expr::InSubquery(subquery)));
        }
        let query = local::materialize(Arc::unwrap_or_clone(subquery.subquery.subquery), config)?;
        subquery.subquery.subquery = Arc::new(
            OptimizeProjections::new()
                .rewrite(query, config)?
                .data
                .transform_up_with_subqueries(|plan| {
                    if let LogicalPlan::Projection(projection) = &plan {
                        let schema = Arc::clone(projection.input.schema());
                        let names = NamePreserver::new_for_projection();
                        return plan.map_expressions(|expr| {
                            let name = names.save(&expr);
                            let simplified =
                                conditional::simplify(expr.clone(), Arc::clone(&schema), config)?;
                            let changed = simplified != expr;
                            Ok(Transformed::new_transformed(
                                name.restore(simplified),
                                changed,
                            ))
                        });
                    }
                    // Nested projected IN was already prepared. Do not erase
                    // its preserved CASE/COALESCE boundary while folding RHS.
                    for expr in plan.expressions() {
                        if needs_conditional_normalization(&expr)? {
                            return Ok(Transformed::no(plan));
                        }
                    }
                    SimplifyExpressions::default().rewrite(plan, config)
                })?
                .data,
        );
        Ok(Transformed::yes(Expr::InSubquery(subquery)))
    })
    .data()
}

// TODO: Allow existence-only RHS pruning across local and aggregate subplans
// after matching Spark's eager subquery evaluation order.
fn can_prune_null_in_rhs(plan: &LogicalPlan, config: &dyn OptimizerConfig) -> Result<bool> {
    let needed = HashSet::new();
    let mut can_prune = true;
    plan.apply_with_subqueries(|plan| {
        if matches!(plan, LogicalPlan::Aggregate(_))
            || (matches!(plan, LogicalPlan::Values(_) | LogicalPlan::TableScan(_))
                && producer_constants(plan, config, &needed)?.local_relation)
        {
            can_prune = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(can_prune)
}

struct InRewriter<'a> {
    plan: LogicalPlan,
    config: &'a dyn OptimizerConfig,
    prepare: bool,
    prepared_marks: &'a mut HashSet<Column>,
}

impl TreeNodeRewriter for InRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::Not(child) = &expr
            && let Expr::InSubquery(subquery) = child.as_ref()
            && subquery.subquery.outer_ref_columns.is_empty()
        {
            let mut subquery = subquery.clone();
            subquery.negated = !subquery.negated;
            return Ok(Transformed::yes(Expr::InSubquery(subquery)));
        }
        Ok(Transformed::no(expr))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let Expr::InSubquery(mut subquery) = expr else {
            return Ok(Transformed::no(expr));
        };
        if !subquery.subquery.outer_ref_columns.is_empty() {
            return Ok(Transformed::no(Expr::InSubquery(subquery)));
        }
        if self.prepare {
            subquery.expr = Box::new(conditional::simplify(
                *subquery.expr,
                Arc::clone(self.plan.schema()),
                self.config,
            )?);
        }
        let null_literal = self.prepare
            && matches!(subquery.expr.as_ref(), Expr::Literal(value, _) if value.is_null());
        if self.prepare && !null_literal {
            return Ok(Transformed::no(Expr::InSubquery(subquery)));
        }
        let InSubquery {
            expr,
            subquery,
            negated,
        } = subquery;
        let columns = subquery.subquery.schema().columns();
        let [column] = columns.as_slice() else {
            return plan_err!("IN subquery must return exactly one column");
        };
        let nullable = expr.nullable(self.plan.schema().as_ref())?
            || subquery.subquery.schema().field(0).is_nullable();
        let existence_only =
            null_literal && can_prune_null_in_rhs(&subquery.subquery, self.config)?;
        let alias = self.config.alias_generator().next("__sail_in");
        let predicate = (*expr).eq(Expr::Column(Column::new(Some(alias.clone()), &column.name)));
        let predicate = if existence_only {
            lit(true)
        } else if negated || null_literal {
            predicate.clone().or(predicate.is_null())
        } else {
            predicate
        };
        let query = LogicalPlanBuilder::from(subquery.subquery);
        let query = if existence_only {
            // Spark rewrites a literal NULL operand to EXISTS: only row existence
            // matters, and the subquery stops after its first qualifying row.
            // TODO: Preserve this error boundary when physical repartitioning
            // speculatively evaluates later RHS batches despite the limit.
            query
                .project(vec![lit(true).alias(&column.name)])?
                .limit(0, Some(1))?
        } else {
            query
        };
        let query = query.alias(alias.clone())?.build()?;
        self.plan = LogicalPlanBuilder::from(mem::take(&mut self.plan))
            .join_on(query, JoinType::LeftMark, Some(predicate))?
            .build()?;
        let mark = Column::new(Some(alias), "mark");
        if self.prepare {
            self.prepared_marks.insert(mark.clone());
        }
        let mark = Expr::Column(mark);
        let result = if null_literal {
            expr_fn::when(mark, lit(ScalarValue::Boolean(None))).otherwise(lit(negated))?
        } else {
            let mark = if nullable {
                expr_fn::when(lit(true), mark).end()?
            } else {
                mark
            };
            if negated { !mark } else { mark }
        };
        Ok(Transformed::yes(result))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::{col, in_subquery};

    use super::*;

    #[test]
    fn projected_in_does_not_optimize_its_input() -> Result<()> {
        let input = LogicalPlanBuilder::empty(true)
            .project(vec![(lit(1_i32) + lit(2_i32)).alias("value")])?
            .filter(lit(true))?
            .build()?;
        let config = OptimizerContext::new();
        let unrelated = LogicalPlanBuilder::from(input.clone())
            .project(vec![col("value")])?
            .build()?;
        let unchanged = RewriteProjectedIn.rewrite(unrelated.clone(), &config)?;
        assert!(!unchanged.transformed);
        assert_eq!(unchanged.data, unrelated);

        let subquery = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i32).alias("candidate")])?
            .build()?;
        // A computed nullable operand used to run a nested optimizer, which
        // also folded the unrelated input projection and removed its filter.
        let operand = lit(ScalarValue::Int32(None)) + lit(1_i32);
        let plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![
                in_subquery(operand, Arc::new(subquery)).alias("present"),
            ])?
            .build()?;
        let rewritten = RewriteProjectedIn.rewrite(plan, &config)?;
        assert!(rewritten.transformed);
        let LogicalPlan::Projection(projection) = rewritten.data else {
            return plan_err!("expected a projection");
        };
        let LogicalPlan::Join(join) = projection.input.as_ref() else {
            return plan_err!("expected an existence join");
        };
        assert_eq!(join.join_type, JoinType::LeftMark);
        assert_eq!(join.left.as_ref(), &input);
        Ok(())
    }
}
