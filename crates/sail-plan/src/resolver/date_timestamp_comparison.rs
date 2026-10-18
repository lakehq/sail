use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Offset, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::catalog::MemTable;
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, JoinType, Result, ScalarValue};
use datafusion_expr::expr::{Alias, BinaryExpr, Case};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{
    Expr, ExprSchemable, Filter, LogicalPlan, Operator, Projection, Union, cast, lit,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::logical_expr::is_comparison_operator;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date_trunc::SparkStartOfDay;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::math::random::Random;
use sail_python_udf::udf::expr_contains_python_udf;

use crate::config::PlanConfig;
use crate::function::common::cast_temporal;

#[derive(Default)]
struct PlanEvaluationFacts {
    foldable_columns: HashMap<Column, Expr>,
    converts_to_local_relation: bool,
}

// Spark folds constants and local relations before unwrapping DATE/TIMESTAMP comparisons.
// In a zone with a skipped day these orders are observably different. Keep that distinction
// here, where defining projections and null-extending joins are available, rather than in
// expression resolution (which sees only a column's type).
pub(super) fn rewrite_date_timestamp_comparisons(
    plan: LogicalPlan,
    config: &PlanConfig,
) -> Result<LogicalPlan> {
    let mut candidate = false;
    plan.apply_with_subqueries(|plan| {
        let mut schema = DFSchema::empty();
        for input in plan.inputs() {
            schema.merge(input.schema());
        }
        let timestamp = |expr: &Expr| {
            expr.get_type(&schema)
                .is_ok_and(|t| matches!(t, DataType::Timestamp(_, _)))
        };
        for expr in plan.expressions() {
            if expr.exists(|e| {
                Ok(match e {
                    Expr::BinaryExpr(b) => {
                        is_comparison_operator(b.op)
                            && (is_start_of_day_expression(&b.left)
                                || is_start_of_day_expression(&b.right)
                                || timestamp(&b.left)
                                || timestamp(&b.right))
                    }
                    Expr::InList(list) => {
                        (is_start_of_day_expression(&list.expr) || timestamp(&list.expr))
                            && !list.list.is_empty()
                    }
                    _ => false,
                })
            })? {
                candidate = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !candidate {
        return Ok(plan);
    }
    Ok(rewrite_date_comparisons_with_plan_facts(plan, config, true)?.0)
}

fn rewrite_date_comparisons_with_plan_facts(
    plan: LogicalPlan,
    config: &PlanConfig,
    rewrite_scalar: bool,
) -> Result<(LogicalPlan, PlanEvaluationFacts)> {
    // DATE's UNION recovery revisits processed trees; a CASE's first timestamp inverse
    // must run only on the original traversal, including its children and subqueries.
    let plan = if rewrite_scalar {
        expose_timestamp_comparison_producers(plan, config)?.data
    } else {
        plan
    };
    let plan = plan
        .map_subqueries(|p| {
            Ok(Transformed::yes(
                rewrite_date_comparisons_with_plan_facts(p, config, rewrite_scalar)?.0,
            ))
        })?
        .data;
    let mut children = Vec::new();
    let plan = plan
        .map_children(|p| {
            let (p, facts) = rewrite_date_comparisons_with_plan_facts(p, config, rewrite_scalar)?;
            children.push(facts);
            Ok(Transformed::yes(p))
        })?
        .data;
    let mut schema = DFSchema::empty();
    let mut foldable_columns = HashMap::new();
    for (input, facts) in plan.inputs().into_iter().zip(&children) {
        schema.merge(input.schema());
        foldable_columns.extend(facts.foldable_columns.clone());
    }
    if plan.inputs().is_empty() {
        schema.merge(plan.schema());
    }
    restore_null_rejected_join_foldables(&plan, &mut foldable_columns, config)?;
    let converts_to_local_relation = match &plan {
        LogicalPlan::Values(_) => true,
        LogicalPlan::TableScan(scan) => source_as_provider(&scan.source)
            .is_ok_and(|provider| is_in_memory_table_provider(provider.as_ref())),
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::SubqueryAlias(_) => children
            .first()
            .is_some_and(|f| f.converts_to_local_relation),
        _ => false,
    };
    // Spark's ConvertToLocalRelation rejects Unevaluable expressions, including subqueries.
    let converts_to_local_relation = converts_to_local_relation
        && !plan
            .expressions()
            .iter()
            .map(contains_unevaluable_expression)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|value| value);
    let schema = Arc::new(schema);
    let plan = if rewrite_scalar && !converts_to_local_relation {
        let visible = timestamp_comparison_producers(&plan, config)?;
        let empty = HashMap::new();
        let (visible_columns, visible_schema) = visible
            .as_ref()
            .map(|(columns, input, _)| (columns, input.schema().as_ref()))
            .unwrap_or((&empty, schema.as_ref()));
        let names = NamePreserver::new(&plan);
        let rewrite = |expr: Expr| {
            let name = names.save(&expr);
            let rewritten = expr.transform_up(|expr| {
                rewrite_timestamp_comparison(
                    expr,
                    &foldable_columns,
                    visible_columns,
                    visible_schema,
                    &schema,
                    config,
                )
            })?;
            Ok(Transformed::new_transformed(
                name.restore(rewritten.data),
                rewritten.transformed,
            ))
        };
        plan.map_expressions(|expr| {
            if matches!(&expr, Expr::GroupingSet(_)) {
                expr.map_children(&rewrite)
            } else {
                rewrite(expr)
            }
        })?
        .data
    } else {
        plan
    };
    let name_preserver = NamePreserver::new(&plan);
    let rewrite_expr = |expr: Expr| {
        let name = name_preserver.save(&expr);
        let expr = expr.transform_up(|expr| {
            rewrite_date_timestamp_comparison(expr, &foldable_columns, &schema, config)
        })?;
        Ok(Transformed::new_transformed(
            name.restore(expr.data),
            expr.transformed,
        ))
    };
    let rewritten = if converts_to_local_relation {
        Transformed::no(plan.clone())
    } else {
        plan.clone().map_expressions(|expr| {
            if let Expr::GroupingSet(_) = &expr {
                expr.map_children(&rewrite_expr)
            } else {
                rewrite_expr(expr)
            }
        })?
    };
    // Only an eligible skipped-day comparison can change a plan's shape. Spark exposes
    // each UNION branch before unwrapping it; its constants may instead fold as timestamps.
    if rewritten.transformed {
        let (pushed, residual) = match &plan {
            LogicalPlan::Projection(p) if p.expr.iter().all(|e| !e.is_volatile()) => {
                (Some(plan.clone()), None)
            }
            LogicalPlan::Filter(f) => {
                let (pushed, residual): (Vec<_>, Vec<_>) =
                    split_conjunction_owned(f.predicate.clone())
                        .into_iter()
                        .partition(|e| !e.is_volatile());
                let eligible = pushed
                    .iter()
                    .map(|e| rewrite_expr(e.clone()))
                    .collect::<Result<Vec<_>>>()?
                    .iter()
                    .any(|e| e.transformed);
                let pushed = if eligible {
                    conjunction(pushed)
                        .map(|predicate| {
                            Filter::try_new(predicate, Arc::clone(&f.input))
                                .map(LogicalPlan::Filter)
                        })
                        .transpose()?
                } else {
                    None
                };
                (pushed, conjunction(residual))
            }
            _ => (None, None),
        };
        if let Some(pushed) = pushed
            && let Some(branches) = expose_union_branches(pushed.inputs()[0])?
        {
            let inputs = branches
                .into_iter()
                .map(|branch| {
                    let branch = pushed.with_new_exprs(pushed.expressions(), vec![branch])?;
                    Ok(Arc::new(
                        rewrite_date_comparisons_with_plan_facts(branch, config, false)?.0,
                    ))
                })
                .collect::<Result<_>>()?;
            let union = LogicalPlan::Union(Union::try_new(inputs)?);
            // UNION output is unqualified. A distributed Filter may previously have
            // exposed a subquery qualifier that its parent still references.
            let union = if union.schema().columns() == plan.schema().columns() {
                union
            } else {
                let expr = union
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(plan.schema().columns())
                    .map(|(old, new)| {
                        Expr::Alias(Alias::new(Expr::Column(old), new.relation, new.name))
                    })
                    .collect();
                LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr,
                    Arc::new(union),
                    Arc::clone(plan.schema()),
                )?)
            };
            return if let Some(predicate) = residual {
                rewrite_date_comparisons_with_plan_facts(
                    LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(union))?),
                    config,
                    false,
                )
            } else {
                Ok((union, PlanEvaluationFacts::default()))
            };
        }
    }
    let plan = rewritten.data;
    let mut output = HashMap::new();
    match &plan {
        LogicalPlan::Projection(p) => {
            for (column, expr) in p.schema.columns().into_iter().zip(&p.expr) {
                let expr = substitute_column_expressions(
                    expr.clone().unalias_nested().data,
                    &foldable_columns,
                )?;
                if is_foldable_expression(&expr)? {
                    output.insert(column, expr);
                }
            }
        }
        LogicalPlan::Aggregate(a) => {
            // Spark's Expand gives grouping-set keys new attributes, even when a key
            // appears in every set. Only ordinary grouping keys retain foldable provenance.
            let groups: &[Expr] = if matches!(a.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
                &[]
            } else {
                &a.group_expr
            };
            let groups = a.schema.columns().into_iter().zip(groups);
            let aggregates = a
                .schema
                .columns()
                .into_iter()
                .skip(a.group_expr_len()?)
                .zip(&a.aggr_expr);
            for (column, expr) in groups.chain(aggregates) {
                let expr = substitute_column_expressions(
                    expr.clone().unalias_nested().data,
                    &foldable_columns,
                )?;
                if is_foldable_expression(&expr)? {
                    output.insert(column, expr);
                }
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            for (old, new) in alias
                .input
                .schema()
                .columns()
                .into_iter()
                .zip(alias.schema.columns())
            {
                if let Some(expr) = foldable_columns.get(&old) {
                    output.insert(new, expr.clone());
                }
            }
        }
        LogicalPlan::Join(join) => {
            // Spark FoldablePropagation excludes the null-extended side of an outer join.
            for (i, child) in children.into_iter().enumerate() {
                let nullable = matches!(
                    (join.join_type, i),
                    (JoinType::Full, _) | (JoinType::Left, 1) | (JoinType::Right, 0)
                );
                if !nullable {
                    output.extend(child.foldable_columns);
                }
            }
            output.retain(|column, _| plan.schema().has_column(column));
        }
        LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Unnest(_) => output = foldable_columns,
        _ => {}
    }
    Ok((
        plan,
        PlanEvaluationFacts {
            foldable_columns: output,
            converts_to_local_relation,
        },
    ))
}

fn is_in_memory_table_provider(provider: &dyn TableProvider) -> bool {
    provider.is::<MemTable>()
        || provider
            .downcast_ref::<RenameTableProvider>()
            .is_some_and(|renamed| is_in_memory_table_provider(renamed.inner().as_ref()))
}

// Spark eliminates null-extended join rows before propagating constants. Recover only
// the corresponding provenance; the normal optimizer still owns the actual join rewrite.
fn restore_null_rejected_join_foldables(
    plan: &LogicalPlan,
    foldable_columns: &mut HashMap<Column, Expr>,
    config: &PlanConfig,
) -> Result<()> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(());
    };
    let mut condition = filter.predicate.clone();
    let mut input = filter.input.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = input {
        let names = alias
            .schema
            .columns()
            .into_iter()
            .zip(alias.input.schema().columns())
            .map(|(old, new)| (old, Expr::Column(new)))
            .collect();
        condition = substitute_column_expressions(condition, &names)?;
        input = &alias.input;
    }
    let LogicalPlan::Join(join) = input else {
        return Ok(());
    };
    let mut conditions = split_conjunction_owned(condition);
    // EliminateOuterJoin also sees filter.constraints. Spark derives IS NOT NULL for
    // attributes below null-intolerant expressions, even across the two join inputs.
    let mut required = Vec::new();
    for condition in &conditions {
        if condition.exists(|expr| {
            let subquery = match expr {
                Expr::ScalarSubquery(subquery) => subquery,
                Expr::Exists(expr) => &expr.subquery,
                Expr::InSubquery(expr) => &expr.subquery,
                Expr::SetComparison(expr) => &expr.subquery,
                _ => return Ok(false),
            };
            Ok(!subquery.outer_ref_columns.is_empty())
        })? {
            continue;
        }
        let condition = match condition {
            Expr::IsNotNull(expr) => expr,
            expr => expr,
        };
        collect_null_intolerant_columns(condition, &mut required);
    }
    let inferred: Vec<_> = required
        .into_iter()
        .map(|column| Expr::Column(column.clone()).is_not_null())
        .collect();
    conditions.extend(inferred);
    for (index, side) in [&join.left, &join.right].into_iter().enumerate() {
        if !matches!(
            (join.join_type, index),
            (JoinType::Full, _) | (JoinType::Left, 1) | (JoinType::Right, 0)
        ) {
            continue;
        }
        let simplifier = ExprSimplifier::new(
            SimplifyContextBuilder::default()
                .with_schema(Arc::clone(side.schema()))
                .build(),
        );
        for conjunct in &conditions {
            if conjunct.is_volatile()
                || expr_contains_python_udf(conjunct)?
                || !conjunct
                    .column_refs()
                    .iter()
                    .all(|c| side.schema().has_column(c))
            {
                continue;
            }
            let nulls = conjunct
                .clone()
                .transform_up(|expr| {
                    if matches!(expr, Expr::Column(_)) {
                        Ok(Transformed::yes(lit(ScalarValue::try_from(
                            &expr.get_type(side.schema())?,
                        )?)))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?
                .data;
            if !is_foldable_expression(&nulls)? {
                continue;
            }
            let result = simplifier
                .coerce(nulls, side.schema())
                .and_then(|e| simplifier.simplify(e));
            if matches!(
                result,
                Ok(Expr::Literal(ScalarValue::Boolean(None | Some(false)), _))
            ) {
                for (column, value) in
                    rewrite_date_comparisons_with_plan_facts(side.as_ref().clone(), config, false)?
                        .1
                        .foldable_columns
                {
                    if let Some(index) = join
                        .schema
                        .columns()
                        .iter()
                        .position(|output| output == &column)
                        && let Some(output) = filter.input.schema().columns().get(index)
                    {
                        foldable_columns.insert(output.clone(), value);
                    }
                }
                break;
            }
        }
    }
    Ok(())
}

fn collect_null_intolerant_columns<'a>(expr: &'a Expr, columns: &mut Vec<&'a Column>) {
    match expr {
        Expr::Column(column) => columns.push(column),
        Expr::Alias(alias) => collect_null_intolerant_columns(&alias.expr, columns),
        Expr::Cast(cast) => collect_null_intolerant_columns(&cast.expr, columns),
        Expr::TryCast(cast) => collect_null_intolerant_columns(&cast.expr, columns),
        Expr::Not(expr) | Expr::Negative(expr) => collect_null_intolerant_columns(expr, columns),
        Expr::BinaryExpr(binary) if binary.op.returns_null_on_null() => {
            collect_null_intolerant_columns(&binary.left, columns);
            collect_null_intolerant_columns(&binary.right, columns);
        }
        Expr::ScalarFunction(function) if function.func.is_strict() => {
            for arg in &function.args {
                collect_null_intolerant_columns(arg, columns);
            }
        }
        _ => {}
    }
}

// Preserve the input/output names while distributing aliases and deterministic local
// projections over a union. The comparison itself is then resolved independently per branch.
fn expose_union_branches(plan: &LogicalPlan) -> Result<Option<Vec<LogicalPlan>>> {
    if let LogicalPlan::Union(union) = plan {
        return union
            .inputs
            .iter()
            .map(|input| {
                let expr = input
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(union.schema.columns())
                    .map(|(old, new)| {
                        Expr::Alias(Alias::new(Expr::Column(old), new.relation, new.name))
                    })
                    .collect();
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr,
                    Arc::clone(input),
                    Arc::clone(&union.schema),
                )?))
            })
            .collect::<Result<Vec<_>>>()
            .map(Some);
    }
    if matches!(
        plan,
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
    ) && plan.expressions().iter().all(|e| !e.is_volatile())
        && let Some(branches) = expose_union_branches(plan.inputs()[0])?
    {
        return branches
            .into_iter()
            .map(|branch| plan.with_new_exprs(plan.expressions(), vec![branch]))
            .collect::<Result<Vec<_>>>()
            .map(Some);
    }
    Ok(None)
}

fn substitute_column_expressions(
    expr: Expr,
    foldable_columns: &HashMap<Column, Expr>,
) -> Result<Expr> {
    Ok(expr
        .transform_up(|expr| {
            if let Expr::Column(column) = &expr
                && let Some(value) = foldable_columns.get(column).or_else(|| {
                    // Resolution can retain a subquery qualifier on an unqualified field.
                    // Opaque field names remain unique in that input schema.
                    let mut matches = foldable_columns.iter().filter(|(key, _)| {
                        key.name == column.name
                            && (key.relation.is_none() || column.relation.is_none())
                    });
                    let (_, value) = matches.next()?;
                    matches.next().is_none().then_some(value)
                })
            {
                Ok(Transformed::yes(value.clone()))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

fn is_foldable_expression(expr: &Expr) -> Result<bool> {
    Ok(!expr.is_volatile()
        && !contains_unevaluable_expression(expr)?
        && !expr.exists(|e| {
            Ok(matches!(
                e,
                Expr::Column(_) | Expr::OuterReferenceColumn(..) | Expr::LambdaVariable(_)
            ))
        })?)
}

fn contains_unevaluable_expression(expr: &Expr) -> Result<bool> {
    Ok(expr_contains_python_udf(expr)?
        || expr.exists(|e| {
            Ok(matches!(
                e,
                Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_)
                    | Expr::AggregateFunction(_)
                    | Expr::WindowFunction(_)
                    | Expr::GroupingSet(_)
                    | Expr::Placeholder(_)
            ))
        })?)
}

fn is_foldable_timestamp_expression(expr: &Expr) -> Result<bool> {
    Ok(is_foldable_expression(expr)?
        && !expr.exists(|e| Ok(matches!(e, Expr::Lambda(_) | Expr::HigherOrderFunction(_))))?)
}

type TimestampComparisonProducers = (HashMap<Column, Expr>, Arc<LogicalPlan>, bool);

fn timestamp_comparison_producers(
    plan: &LogicalPlan,
    config: &PlanConfig,
) -> Result<Option<TimestampComparisonProducers>> {
    let mut input = match plan {
        LogicalPlan::Projection(p) => Arc::clone(&p.input),
        LogicalPlan::Filter(f) => Arc::clone(&f.input),
        _ => return Ok(None),
    };
    let mut consumers = plan.expressions();
    let mut referenced = Vec::new();
    for expr in &consumers {
        expr.apply(|expr| {
            let source = match expr {
                Expr::BinaryExpr(b) if is_comparison_operator(b.op) => {
                    if is_foldable_timestamp_expression(&b.right)? {
                        Some(b.left.as_ref())
                    } else if is_foldable_timestamp_expression(&b.left)? {
                        Some(b.right.as_ref())
                    } else {
                        None
                    }
                }
                Expr::InList(list)
                    if !list.list.is_empty()
                        && list
                            .list
                            .iter()
                            .map(is_foldable_timestamp_expression)
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .all(|v| v) =>
                {
                    Some(list.expr.as_ref())
                }
                _ => None,
            };
            if let Some(source) = source
                && matches!(source.get_type(input.schema())?, DataType::Timestamp(_, _))
            {
                referenced.extend(source.column_refs().into_iter().cloned());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    if referenced.is_empty() {
        return Ok(None);
    }
    let mut visible: HashMap<_, _> = input
        .schema()
        .columns()
        .into_iter()
        .map(|column| (column.clone(), Expr::Column(column)))
        .collect();
    if visible.len() != input.schema().fields().len() {
        return Ok(None);
    }
    loop {
        let (replacements, next): (HashMap<_, _>, _) = match input.as_ref() {
            LogicalPlan::SubqueryAlias(alias) => (
                alias
                    .schema
                    .columns()
                    .into_iter()
                    .zip(alias.input.schema().columns().into_iter().map(Expr::Column))
                    .collect(),
                Arc::clone(&alias.input),
            ),
            LogicalPlan::Projection(p) => {
                // Even an unused unevaluable producer prevents Spark's early local evaluation.
                if p.expr
                    .iter()
                    .map(contains_unevaluable_expression)
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .any(|v| v)
                {
                    return Ok(None);
                }
                let columns = p.schema.columns();
                let mut counts = datafusion_common::HashMap::new();
                for consumer in &consumers {
                    consumer.add_column_ref_counts(&mut counts);
                }
                let mut uses = vec![0; p.expr.len()];
                for (column, count) in counts {
                    let exact = columns.iter().position(|c| c == column);
                    let index = exact.or_else(|| {
                        let mut matches = columns.iter().enumerate().filter(|(_, c)| {
                            c.name == column.name
                                && (c.relation.is_none() || column.relation.is_none())
                        });
                        let (index, _) = matches.next()?;
                        matches.next().is_none().then_some(index)
                    });
                    let Some(index) = index else { return Ok(None) };
                    uses[index] += count;
                }
                for (producer, count) in p.expr.iter().zip(uses) {
                    let mut producer = producer;
                    while let Expr::Alias(alias) = producer {
                        producer = &alias.expr;
                    }
                    if (count != 0 || matches!(plan, LogicalPlan::Filter(_)))
                        && producer.is_volatile()
                    {
                        return Ok(None);
                    }
                    if matches!(plan, LogicalPlan::Projection(_))
                        && count > 1
                        && !matches!(producer, Expr::Column(_))
                        && !is_foldable_timestamp_expression(producer)?
                    {
                        return Ok(None);
                    }
                }
                (
                    columns
                        .into_iter()
                        .zip(p.expr.iter().map(|e| e.clone().unalias_nested().data))
                        .collect(),
                    Arc::clone(&p.input),
                )
            }
            _ => return Ok(None),
        };
        if replacements.len() != input.schema().fields().len() {
            return Ok(None);
        }
        consumers = consumers
            .into_iter()
            .map(|expr| substitute_column_expressions(expr, &replacements))
            .collect::<Result<_>>()?;
        visible = visible
            .into_iter()
            .map(|(column, expr)| Ok((column, substitute_column_expressions(expr, &replacements)?)))
            .collect::<Result<_>>()?;
        input = next;
        let mut candidate = false;
        let mut expose = false;
        for column in &referenced {
            let value = substitute_column_expressions(Expr::Column(column.clone()), &visible)?;
            if let Some(case) = timestamp_case(&value, input.schema(), config)? {
                candidate = true;
                let foldable = case_results(&case, input.schema())?
                    .iter()
                    .map(|value| has_foldable_result(value, input.schema()))
                    .collect::<Result<Vec<_>>>()?;
                expose |= foldable.iter().filter(|v| !**v).count() == 1;
            } else if timestamp_cast_input(&value, input.schema(), config)?.is_some() {
                candidate = true;
                expose = true;
            }
        }
        if candidate {
            return Ok(Some((visible, input, expose)));
        }
    }
}

fn expose_timestamp_comparison_producers(
    plan: LogicalPlan,
    config: &PlanConfig,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let Some((visible, input, expose)) = timestamp_comparison_producers(&plan, config)? else {
        return Ok(Transformed::no(plan));
    };
    let mut expressions = Vec::new();
    for (index, expr) in projection.expr.iter().enumerate() {
        let expr = substitute_column_expressions(expr.clone(), &visible)?
            .unalias_nested()
            .data;
        let (qualifier, field) = projection.schema.qualified_field(index);
        if expr.get_type(input.schema()).ok().as_ref() != Some(field.data_type()) {
            return Ok(Transformed::no(plan));
        }
        expressions.push(expr.alias_qualified_with_metadata(
            qualifier.cloned(),
            field.name(),
            Some(FieldMetadata::from(field.metadata().clone())),
        ));
    }
    // Each candidate consumes a lower projection; expose deeper producers before
    // processing children or applying the non-idempotent comparison rewrite.
    let deeper = expose_timestamp_comparison_producers(
        LogicalPlan::Projection(Projection::try_new_with_schema(
            expressions,
            input,
            Arc::clone(&projection.schema),
        )?),
        config,
    )?;
    Ok(if expose || deeper.transformed {
        Transformed::yes(deeper.data)
    } else {
        Transformed::no(plan)
    })
}

fn timestamp_identity_input<'a>(mut expr: &'a Expr, schema: &DFSchema) -> Result<&'a Expr> {
    while let Expr::Cast(cast) = expr {
        if cast.expr.get_type(schema)? != *cast.field.data_type() {
            break;
        }
        expr = &cast.expr;
    }
    Ok(expr)
}

// Reconstruct the complete generated conversion, including its integer barrier and zones.
// A public convert_tz call or an intervening DATE cast is not a timestamp cast.
fn timestamp_cast_input(
    expr: &Expr,
    schema: &DFSchema,
    config: &PlanConfig,
) -> Result<Option<Expr>> {
    if let Expr::Alias(alias) = expr {
        return Ok(
            timestamp_cast_input(&alias.expr, schema, config)?.map(|input| {
                let mut alias = alias.clone();
                alias.expr = Box::new(input);
                Expr::Alias(alias)
            }),
        );
    }
    let Ok(target) = expr.get_type(schema) else {
        return Ok(None);
    };
    let DataType::Timestamp(TimeUnit::Microsecond, target_zone) = &target else {
        return Ok(None);
    };
    let outer = timestamp_identity_input(expr, schema)?;
    if !matches!(outer, Expr::Cast(c) if c.expr.get_type(schema)? == DataType::Int64) {
        return Ok(None);
    }
    let mut input = None;
    expr.apply(|candidate| {
        let Ok(source) = candidate.get_type(schema) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let DataType::Timestamp(unit, zone) = &source
            && zone.is_some() != target_zone.is_some()
        {
            let rebuilt = cast_temporal(
                candidate.clone(),
                &source,
                target.clone(),
                &config.session_timezone,
                false,
            )?;
            if timestamp_identity_input(&rebuilt, schema)? == outer {
                // cast_temporal normalizes units before shifting; retain that truncation.
                input = Some(if *unit == TimeUnit::Microsecond {
                    candidate.clone()
                } else {
                    cast(
                        candidate.clone(),
                        DataType::Timestamp(TimeUnit::Microsecond, zone.clone()),
                    )
                });
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(input)
}

fn timestamp_case(expr: &Expr, schema: &DFSchema, config: &PlanConfig) -> Result<Option<Case>> {
    if let Expr::Case(case) = expr {
        return Ok(Some(case.clone()));
    }
    if let Some(input) = timestamp_cast_input(expr, schema, config)? {
        return timestamp_case(&input, schema, config);
    }
    match expr {
        Expr::Alias(alias) => timestamp_case(&alias.expr, schema, config),
        Expr::Cast(cast)
            if matches!((cast.field.data_type(), cast.expr.get_type(schema)?),
                (DataType::Timestamp(_, a), DataType::Timestamp(_, b))
                    if a.is_some() == b.is_some()) =>
        {
            timestamp_case(&cast.expr, schema, config)
        }
        Expr::ScalarFunction(f) if f.func.inner().is::<SparkTimestamp>() && f.args.len() == 1 => {
            timestamp_case(&f.args[0], schema, config)
        }
        _ => Ok(None),
    }
}

fn case_results(case: &Case, schema: &DFSchema) -> Result<Vec<Expr>> {
    let mut results = Vec::new();
    for (condition, value) in &case.when_then_expr {
        results.push(value.as_ref().clone());
        // Sail represents a searched CASE's ELSE as a final WHEN true.
        if case.expr.is_none()
            && matches!(
                condition.as_ref(),
                Expr::Literal(ScalarValue::Boolean(Some(true)), _)
            )
        {
            return Ok(results);
        }
    }
    results.push(match &case.else_expr {
        Some(value) => value.as_ref().clone(),
        None => lit(ScalarValue::try_from(
            &Expr::Case(case.clone()).get_type(schema)?,
        )?),
    });
    Ok(results)
}

// A read-only proof of Spark's identical-result CASE simplification. Conditions stay
// in the emitted expression; this fact only preserves a later folding opportunity.
fn has_foldable_result(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    if is_foldable_timestamp_expression(expr)? {
        return Ok(true);
    }
    if let Expr::Alias(alias) = expr {
        return has_foldable_result(&alias.expr, schema);
    }
    let Expr::Case(case) = expr else {
        return Ok(false);
    };
    for condition in case
        .expr
        .iter()
        .chain(case.when_then_expr.iter().map(|(condition, _)| condition))
    {
        if condition.is_volatile() || contains_unevaluable_expression(condition)? {
            return Ok(false);
        }
    }
    let results = case_results(case, schema)?;
    let Some(first) = results.first() else {
        return Ok(false);
    };
    Ok(results.iter().all(|value| value == first) && has_foldable_result(first, schema)?)
}

fn static_timestamp_bound(
    expr: Expr,
    foldable_columns: &HashMap<Column, Expr>,
    schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let expr = substitute_column_expressions(expr, foldable_columns)?;
    if !is_foldable_timestamp_expression(&expr)?
        || expr.exists(|e| {
            Ok(matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<TimestampNow>()))
        })?
    {
        return Ok(None);
    }
    let simplifier = ExprSimplifier::new(
        SimplifyContextBuilder::default()
            .with_schema(Arc::clone(schema))
            .build(),
    );
    let Ok(expr) = simplifier.coerce(expr, schema) else {
        return Ok(None);
    };
    let Ok(value) = LiteralEvaluator::new().evaluate(&expr) else {
        return Ok(None);
    };
    Ok(matches!(value.data_type(), DataType::Timestamp(_, _))
        .then(|| Expr::Literal(value, expr.metadata(schema.as_ref()).ok())))
}

fn rewrite_timestamp_comparison(
    expr: Expr,
    foldable_columns: &HashMap<Column, Expr>,
    visible_columns: &HashMap<Column, Expr>,
    visible_schema: &DFSchema,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> Result<Transformed<Expr>> {
    if let Expr::InList(list) = &expr {
        if timestamp_cast_input(&list.expr, schema, config)?.is_none() {
            return Ok(Transformed::no(expr));
        }
        let bounds = list
            .list
            .iter()
            .map(|bound| static_timestamp_bound(bound.clone(), foldable_columns, schema))
            .collect::<Result<Option<Vec<_>>>>()?;
        let Some(bounds) = bounds else {
            return Ok(Transformed::no(expr));
        };
        let Some(bound) = bounds.first() else {
            return Ok(Transformed::no(expr));
        };
        // NULL is a distinct IN-set member, not a removable duplicate of a bound.
        if !matches!(bound, Expr::Literal(value, _) if !value.is_null())
            || bounds.iter().any(|value| value != bound)
        {
            return Ok(Transformed::no(expr));
        }
        if list
            .list
            .iter()
            .any(|value| !matches!(value, Expr::Literal(_, _)))
        {
            let visible =
                substitute_column_expressions(list.expr.as_ref().clone(), visible_columns)?;
            if let Some(case) = timestamp_case(&visible, visible_schema, config)? {
                let results = case_results(&case, visible_schema)?;
                if results
                    .iter()
                    .map(|value| has_foldable_result(value, visible_schema))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .all(|value| value)
                {
                    return Ok(Transformed::no(expr));
                }
            }
            if let Some(rewritten) = rewrite_timestamp_case(
                &list.expr,
                Operator::Eq,
                bound,
                foldable_columns,
                schema,
                config,
            )? {
                return Ok(Transformed::yes(if list.negated {
                    !rewritten
                } else {
                    rewritten
                }));
            }
        }
        let comparison = list.expr.as_ref().clone().eq(bound.clone());
        let rewritten = rewrite_timestamp_comparison(
            comparison,
            foldable_columns,
            visible_columns,
            visible_schema,
            schema,
            config,
        )?;
        return Ok(if rewritten.transformed {
            Transformed::yes(if list.negated {
                !rewritten.data
            } else {
                rewritten.data
            })
        } else {
            Transformed::no(expr)
        });
    }
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !is_comparison_operator(binary.op) {
        return Ok(Transformed::no(expr));
    }
    let mut selected = None;
    for (source, bound, op) in [
        (binary.left.as_ref(), binary.right.as_ref(), binary.op),
        (
            binary.right.as_ref(),
            binary.left.as_ref(),
            binary.op.swap().unwrap_or(binary.op),
        ),
    ] {
        let Some(input) = timestamp_cast_input(source, schema, config)? else {
            continue;
        };
        // Test original foldability before the shadow CASE proof: the first inverse has
        // already occurred when Spark later folds conditional result conversions.
        if is_foldable_timestamp_expression(&substitute_column_expressions(
            input.clone(),
            foldable_columns,
        )?)? {
            continue;
        }
        if let Some(bound) = static_timestamp_bound(bound.clone(), foldable_columns, schema)? {
            selected = Some((input, bound, op));
            break;
        }
    }
    let Some((input, bound, op)) = selected else {
        return Ok(Transformed::no(expr));
    };
    let Expr::Literal(value, metadata) = &bound else {
        return Ok(Transformed::no(expr));
    };
    if value.is_null() {
        return Ok(Transformed::no(expr));
    }
    let target = input.get_type(schema)?;
    let inverse = cast_temporal(
        bound.clone(),
        &value.data_type(),
        target.clone(),
        &config.session_timezone,
        false,
    )?;
    let evaluator = LiteralEvaluator::new();
    let Ok(inverse) = evaluator.evaluate(&inverse) else {
        return Ok(Transformed::no(expr));
    };
    let round_trip = cast_temporal(
        lit(inverse.clone()),
        &target,
        value.data_type(),
        &config.session_timezone,
        false,
    )?;
    if evaluator.evaluate(&round_trip).ok().as_ref() != Some(value) {
        return Ok(Transformed::no(expr));
    }
    let bound = Expr::Literal(inverse, metadata.clone());
    let comparison = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(input.clone()),
        op,
        Box::new(bound.clone()),
    ));
    if let Some(case) =
        rewrite_timestamp_case(&input, op, &bound, foldable_columns, schema, config)?
    {
        return Ok(Transformed::yes(case));
    }
    // Only a proved opaque value may lose another cast. An unproved CASE is a
    // folding boundary, not evidence of a materialized value.
    let source = substitute_column_expressions(input, foldable_columns)?;
    if !contains_unevaluable_expression(&source)?
        && !source.exists(|e| {
            Ok(matches!(
                e,
                Expr::Case(_) | Expr::Lambda(_) | Expr::HigherOrderFunction(_)
            ))
        })?
        && (source.is_volatile()
            || source.exists(|e| Ok(matches!(e, Expr::Column(_) | Expr::LambdaVariable(_))))?)
    {
        return Ok(Transformed::yes(
            rewrite_timestamp_comparison(
                comparison,
                foldable_columns,
                visible_columns,
                visible_schema,
                schema,
                config,
            )?
            .data,
        ));
    }
    Ok(Transformed::yes(comparison))
}

fn rewrite_timestamp_case(
    source: &Expr,
    op: Operator,
    bound: &Expr,
    foldable_columns: &HashMap<Column, Expr>,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> Result<Option<Expr>> {
    let Some(mut case) = timestamp_case(source, schema, config)? else {
        return Ok(None);
    };
    let results = case_results(&case, schema)?;
    let foldable = results
        .iter()
        .map(|value| has_foldable_result(value, schema))
        .collect::<Result<Vec<_>>>()?;
    if foldable.iter().filter(|value| !**value).count() != 1 {
        return Ok(None);
    }
    let original = Expr::Case(case.clone());
    let rewrite = |value: Expr| -> Result<Expr> {
        let folded = has_foldable_result(&value, schema)?;
        let source = source
            .clone()
            .transform_up(|expr| {
                Ok(if expr == original {
                    Transformed::yes(value.clone())
                } else {
                    Transformed::no(expr)
                })
            })?
            .data;
        let comparison = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(source),
            op,
            Box::new(bound.clone()),
        ));
        if folded {
            // Keep even an invalid/unreachable cast conditional; do not evaluate it here.
            Ok(comparison)
        } else {
            Ok(rewrite_timestamp_comparison(
                comparison,
                foldable_columns,
                &HashMap::new(),
                schema,
                schema,
                config,
            )?
            .data)
        }
    };
    for (_, value) in &mut case.when_then_expr {
        **value = rewrite(value.as_ref().clone())?;
    }
    case.else_expr = Some(Box::new(rewrite(match case.else_expr {
        Some(value) => *value,
        None => lit(ScalarValue::try_from(&original.get_type(schema)?)?),
    })?));
    Ok(Some(Expr::Case(case)))
}

fn is_start_of_day_expression(expr: &Expr) -> bool {
    let mut expr = expr;
    while let Expr::Cast(c) = expr {
        expr = &c.expr;
    }
    matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkStartOfDay>())
}

// Recognize only the DATE conversion generated by cast_temporal. Stop at its DATE input.
fn date_input_of_timestamp_cast<'a>(expr: &'a Expr, schema: &DFSchema) -> Result<Option<&'a Expr>> {
    if !matches!(expr.get_type(schema)?, DataType::Timestamp(_, Some(_))) {
        return Ok(None);
    }
    let mut expr = expr;
    while let Expr::Cast(c) = expr {
        expr = &c.expr;
    }
    let Expr::ScalarFunction(f) = expr else {
        return Ok(None);
    };
    if !f.func.inner().is::<SparkStartOfDay>() {
        return Ok(None);
    }
    expr = &f.args[0];
    loop {
        if expr.get_type(schema)? == DataType::Date32 {
            return Ok(Some(expr));
        }
        expr = match expr {
            Expr::Cast(c) => &c.expr,
            Expr::TryCast(c) => &c.expr,
            Expr::ScalarFunction(f) if f.func.inner().is::<ConvertTz>() => &f.args[2],
            _ => return Ok(None),
        };
    }
}

fn rewrite_date_timestamp_comparison(
    expr: Expr,
    foldable_columns: &HashMap<Column, Expr>,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> Result<Transformed<Expr>> {
    if let Expr::InList(list) = &expr
        && is_start_of_day_expression(&list.expr)
    {
        // ConstantFolding/FoldablePropagation expose literal bounds to OptimizeIn on
        // its next iteration. A single distinct literal then becomes equality.
        let bounds = list
            .list
            .iter()
            .map(|bound| simplify_timestamp_bound(bound.clone(), foldable_columns, schema))
            .collect::<Result<Option<Vec<_>>>>()?;
        let Some(bounds) = bounds else {
            return Ok(Transformed::no(expr));
        };
        let Some(bound) = bounds.first() else {
            return Ok(Transformed::no(expr));
        };
        if !matches!(
            bound,
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), Some(_)), _)
        ) || bounds.iter().any(|value| value != bound)
        {
            return Ok(Transformed::no(expr));
        }
        let comparison = list.expr.as_ref().clone().eq(bound.clone());
        let rewritten =
            rewrite_date_timestamp_comparison(comparison, foldable_columns, schema, config)?;
        if rewritten.transformed {
            return Ok(Transformed::yes(if list.negated {
                !rewritten.data
            } else {
                rewritten.data
            }));
        }
        return Ok(Transformed::no(expr));
    }
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !is_start_of_day_expression(&binary.left) && !is_start_of_day_expression(&binary.right) {
        return Ok(Transformed::no(expr));
    }
    if !is_comparison_operator(binary.op) {
        return Ok(Transformed::no(expr));
    }
    let (date, bound, op) = if let Some(date) = date_input_of_timestamp_cast(&binary.left, schema)?
    {
        (date, binary.right.as_ref(), binary.op)
    } else if let Some(date) = date_input_of_timestamp_cast(&binary.right, schema)? {
        let op = binary.op.swap().unwrap_or(binary.op);
        (date, binary.left.as_ref(), op)
    } else {
        return Ok(Transformed::no(expr));
    };
    if matches!(op, Operator::Gt | Operator::LtEq) {
        return Ok(Transformed::no(expr));
    }
    // ConstantFolding runs before UnwrapCastInBinaryComparison. Test foldability before
    // simplifying CASE expressions, since SimplifyConditionals runs between these rules.
    let folded_date = substitute_column_expressions(date.clone(), foldable_columns)?;
    let bound = substitute_column_expressions(bound.clone(), foldable_columns)?;
    if is_foldable_expression(&folded_date)? {
        return Ok(Transformed::no(expr));
    }
    let Some(bound) = simplify_timestamp_bound(bound, foldable_columns, schema)? else {
        return Ok(Transformed::no(expr));
    };
    let simplifier = ExprSimplifier::new(
        SimplifyContextBuilder::default()
            .with_schema(Arc::clone(schema))
            .build(),
    );
    let Expr::Literal(ScalarValue::TimestampMicrosecond(Some(micros), Some(_)), _) = bound else {
        return Ok(Transformed::no(expr));
    };
    let Some(instant) = DateTime::<Utc>::from_timestamp_micros(micros) else {
        return Ok(Transformed::no(expr));
    };
    let timezone = config.session_timezone.parse::<Tz>()?;
    let Some(local) = instant
        .naive_utc()
        .checked_add_offset(instant.with_timezone(&timezone).offset().fix())
    else {
        return Ok(Transformed::no(expr));
    };
    let date_bound = local.date().num_days_from_ce() - 719_163;
    // Direct timestamp comparison differs only when the preceding calendar day was
    // skipped and maps to this very same start of day. Leave ordinary casts alone.
    let previous_day = cast_temporal(
        lit(ScalarValue::Date32(Some(date_bound - 1))),
        &DataType::Date32,
        DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(&config.session_timezone)),
        ),
        &config.session_timezone,
        false,
    )?;
    let Ok(previous_day) = simplifier.simplify(previous_day) else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(previous_day,
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(previous), _), _) if previous == micros)
    {
        return Ok(Transformed::no(expr));
    }
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(date.clone()),
        op,
        Box::new(lit(ScalarValue::Date32(Some(date_bound)))),
    ))))
}

fn simplify_timestamp_bound(
    bound: Expr,
    foldable_columns: &HashMap<Column, Expr>,
    schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let bound = substitute_column_expressions(bound, foldable_columns)?;
    if contains_unevaluable_expression(&bound)?
        || bound.exists(|e| {
            Ok(matches!(e,
                Expr::ScalarFunction(f) if f.func.inner().is::<TimestampNow>()
            ))
        })?
    {
        return Ok(None);
    }
    let simplifier = ExprSimplifier::new(
        SimplifyContextBuilder::default()
            .with_schema(Arc::clone(schema))
            .build(),
    );
    let Ok(bound) = simplifier.coerce(bound, schema) else {
        return Ok(None);
    };
    // Spark OptimizeRand removes comparisons fixed by rand's [0, 1) range before
    // folding a timestamp bound. Apply only that rule here; never sample a bound.
    let bound = bound
        .transform_up(|expr| {
            let Expr::BinaryExpr(binary) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let random = |expr: &Expr| -> Result<bool> {
                let Expr::ScalarFunction(function) = expr else {
                    return Ok(false);
                };
                if !function.func.inner().is::<Random>() {
                    return Ok(false);
                }
                let [seed] = function.args.as_slice() else {
                    return Ok(function.args.is_empty());
                };
                if !is_foldable_expression(seed)? {
                    return Ok(false);
                }
                // Spark validates the scalar seed before OptimizeRand. Preserve Sail's
                // runtime errors for invalid/non-scalar seeds instead of erasing them.
                Ok(matches!(simplifier.simplify(seed.clone()),
                    Ok(Expr::Literal(value, _)) if value.data_type().is_integer() || value.is_null()))
            };
            let (op, number) = if random(&binary.left)? {
                (binary.op, &binary.right)
            } else if random(&binary.right)?
                && let Some(op) = binary.op.swap()
            {
                (op, &binary.left)
            } else {
                return Ok(Transformed::no(expr));
            };
            if !is_foldable_expression(number)? {
                return Ok(Transformed::no(expr));
            }
            let Ok(Expr::Literal(ScalarValue::Float64(Some(value)), _)) =
                simplifier.simplify(number.as_ref().clone())
            else {
                return Ok(Transformed::no(expr));
            };
            let result = match op {
                Operator::Gt if value < 0.0 => Some(true),
                Operator::GtEq if value <= 0.0 => Some(true),
                Operator::Gt | Operator::GtEq if value >= 1.0 => Some(false),
                Operator::Lt | Operator::LtEq if value >= 1.0 => Some(true),
                Operator::Lt if value <= 0.0 => Some(false),
                Operator::LtEq if value < 0.0 => Some(false),
                _ => None,
            };
            Ok(match result {
                Some(result) => Transformed::yes(lit(result)),
                None => Transformed::no(expr),
            })
        })?
        .data;
    let Ok(bound) = simplifier.simplify(bound) else {
        return Ok(None);
    };
    Ok((!bound.is_volatile()).then_some(bound))
}
