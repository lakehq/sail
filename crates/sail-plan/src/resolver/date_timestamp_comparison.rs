use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Offset, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::catalog::MemTable;
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, JoinType, Result, ScalarValue};
use datafusion_expr::expr::{Alias, BinaryExpr};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{Expr, ExprSchemable, Filter, LogicalPlan, Operator, Projection, Union, lit};
use sail_common_datafusion::logical_expr::is_comparison_operator;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkStartOfDay;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_year::SparkYear;
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
        for expr in plan.expressions() {
            if expr.exists(|e| {
                Ok(match e {
                    Expr::BinaryExpr(b) => {
                        is_comparison_operator(b.op)
                            && (is_start_of_day_expression(&b.left)
                                || is_start_of_day_expression(&b.right))
                    }
                    Expr::InList(list) => {
                        is_start_of_day_expression(&list.expr) && !list.list.is_empty()
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
    Ok(rewrite_date_comparisons_with_plan_facts(plan, config)?.0)
}

fn rewrite_date_comparisons_with_plan_facts(
    plan: LogicalPlan,
    config: &PlanConfig,
) -> Result<(LogicalPlan, PlanEvaluationFacts)> {
    let plan = plan
        .map_subqueries(|p| {
            Ok(Transformed::yes(rewrite_date_timestamp_comparisons(
                p, config,
            )?))
        })?
        .data;
    let mut children = Vec::new();
    let plan = plan
        .map_children(|p| {
            let (p, facts) = rewrite_date_comparisons_with_plan_facts(p, config)?;
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
    let rewrite_expr = |expr: Expr| {
        expr.transform_up(|expr| {
            rewrite_date_timestamp_comparison(expr, &foldable_columns, &schema, config)
        })
    };
    let rewritten = if converts_to_local_relation {
        Transformed::no(plan.clone())
    } else {
        plan.clone().map_expressions(rewrite_expr)?
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
                        rewrite_date_comparisons_with_plan_facts(branch, config)?.0,
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
                    rewrite_date_comparisons_with_plan_facts(side.as_ref().clone(), config)?
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
        // Spark GetDateField/LastDay are null-intolerant; these Sail implementations
        // do not expose DataFusion's optional strictness metadata.
        Expr::ScalarFunction(function)
            if function.func.is_strict()
                || function.func.inner().is::<SparkYear>()
                || function.func.inner().is::<SparkDatePart>()
                || function.func.inner().is::<SparkLastDay>() =>
        {
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
