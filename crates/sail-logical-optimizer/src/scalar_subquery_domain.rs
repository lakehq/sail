use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::{Aggregate, Union, Unnest};
use datafusion_expr::utils::{conjunction, grouping_set_to_exprlist, powerset, split_conjunction};
use datafusion_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Operator};
use sail_common_datafusion::decorrelate::PullUpCorrelatedExpr;

/// Evaluate non-equality scalar aggregates over the distinct outer values.
/// Native decorrelation can then pull up equality on those values without
/// incorrectly grouping the inner input by a non-equality predicate's columns.
#[derive(Debug)]
pub struct NormalizeScalarSubqueryDomain;

impl OptimizerRule for NormalizeScalarSubqueryDomain {
    fn name(&self) -> &str {
        "normalize_scalar_subquery_domain"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let input = match &plan {
            LogicalPlan::Projection(projection) => Arc::clone(&projection.input),
            LogicalPlan::Filter(filter) => Arc::clone(&filter.input),
            _ => return Ok(Transformed::no(plan)),
        };
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            expr.transform_up(|expr| {
                let Expr::ScalarSubquery(mut query) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if let Some(rewritten) = grouped_domain(&query.subquery, &input, config)? {
                    query.subquery = Arc::new(rewritten);
                    return Ok(Transformed::yes(Expr::ScalarSubquery(query)));
                }
                let mut unsupported = false;
                let mut needs_domain = false;
                let mut aggregates = 0;
                let mut filters = 0;
                query.subquery.apply(|node| {
                    match node {
                        LogicalPlan::Aggregate(aggregate) => {
                            aggregates += 1;
                            unsupported |= !aggregate.group_expr.is_empty();
                        }
                        LogicalPlan::Filter(filter) if filter.predicate.contains_outer() => {
                            // This traversal is top-down. A filter encountered
                            // before the aggregate is HAVING, not its input.
                            unsupported |= aggregates == 0;
                            filters += 1;
                            needs_domain |= split_conjunction(&filter.predicate)
                                .iter()
                                .any(|expr| expr.contains_outer() && !native_equality(expr));
                        }
                        LogicalPlan::Join(_)
                        | LogicalPlan::Union(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Limit(_)
                        | LogicalPlan::Subquery(_) => unsupported = true,
                        _ => unsupported |= node.contains_outer_reference(),
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                if unsupported || !needs_domain || aggregates != 1 || filters != 1 {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                // The domain reads the outer input again; do not duplicate volatile
                // evaluations whose values must be shared with the original row.
                if input.exists(|node| Ok(node.expressions().iter().any(Expr::is_volatile)))? {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                if query
                    .subquery
                    .exists(|node| Ok(node.expressions().iter().any(Expr::is_volatile)))?
                {
                    return datafusion_common::not_impl_err!(
                        "correlated scalar aggregate with volatile inner expressions"
                    );
                }
                let outer = query.subquery.all_out_ref_exprs();
                let mut bindings = Vec::new();
                for reference in outer {
                    let Expr::OuterReferenceColumn(_, column) = &reference else {
                        continue;
                    };
                    if !input.schema().has_column(column) {
                        return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                    }
                    let key =
                        Column::new_unqualified(config.alias_generator().next("__sail_domain"));
                    bindings.push((reference.clone(), column.clone(), key));
                }
                if bindings.is_empty() {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                let domain = LogicalPlanBuilder::from(Arc::clone(&input))
                    .project(
                        bindings
                            .iter()
                            .map(|(_, outer, key)| Expr::Column(outer.clone()).alias(&key.name)),
                    )?
                    .distinct()?
                    .build()?;
                let output = query.subquery.schema().columns();
                let default = empty_scalar_value(&query.subquery)?;
                let correlation = conjunction(bindings.iter().map(|(reference, _, key)| {
                    datafusion_expr::expr_fn::binary_expr(
                        Expr::Column(key.clone()),
                        Operator::IsNotDistinctFrom,
                        reference.clone(),
                    )
                }))
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!("empty scalar correlation domain")
                })?;
                let mut attached = false;
                let mut aggregate_marker = None;
                let rewritten = Arc::unwrap_or_clone(query.subquery)
                    .transform_up(|node| {
                        let was_attached = attached;
                        let node = if let LogicalPlan::Filter(mut filter) = node {
                            if filter.predicate.contains_outer() {
                                filter.input = Arc::new(
                                    LogicalPlanBuilder::from(filter.input)
                                        .cross_join(domain.clone())?
                                        .build()?,
                                );
                                attached = true;
                            }
                            LogicalPlan::Filter(filter)
                        } else {
                            node
                        };
                        let names = NamePreserver::new(&node);
                        let node = node
                            .map_expressions(|expr| {
                                let name = names.save(&expr);
                                expr.transform_up(|expr| {
                                    if let Some((_, _, key)) =
                                        bindings.iter().find(|(reference, _, _)| reference == &expr)
                                    {
                                        Ok(Transformed::yes(Expr::Column(key.clone())))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .map(|result| result.update_data(|expr| name.restore(expr)))
                            })?
                            .data;
                        if !was_attached {
                            return Ok(Transformed::yes(node));
                        }
                        let keys = || bindings.iter().map(|(_, _, key)| Expr::Column(key.clone()));
                        let result = match node {
                            LogicalPlan::Filter(mut filter) => {
                                // Keep HAVING and the lifted correlation in one filter
                                // so scalar-subquery validation still sees the aggregate.
                                while let LogicalPlan::Filter(inner) = filter.input.as_ref() {
                                    filter.predicate =
                                        filter.predicate.and(inner.predicate.clone());
                                    filter.input = Arc::clone(&inner.input);
                                }
                                LogicalPlan::Filter(filter)
                            }
                            LogicalPlan::Projection(mut projection) => {
                                projection.expr.extend(keys());
                                LogicalPlanBuilder::from(projection.input)
                                    .project(projection.expr)?
                                    .build()?
                            }
                            LogicalPlan::Aggregate(mut aggregate) => {
                                aggregate.group_expr.extend(keys());
                                let aggregate = LogicalPlanBuilder::from(aggregate.input)
                                    .aggregate(aggregate.group_expr, aggregate.aggr_expr)?
                                    .filter(correlation.clone())?
                                    .build()?;
                                // Test for an input group before HAVING can remove it.
                                // A rejected nonempty group must not receive the empty
                                // aggregate's default value.
                                aggregate_marker = Some(Arc::new(
                                    LogicalPlanBuilder::from(aggregate.clone())
                                        .project(vec![datafusion_expr::lit(true)])?
                                        .build()?,
                                ));
                                aggregate
                            }
                            // Ordering a scalar aggregate cannot change its value.
                            // Remove this sort so native decorrelation can reach
                            // the correlation we attached above the aggregate.
                            LogicalPlan::Sort(sort)
                                if aggregate_marker.is_some() && sort.fetch.is_none() =>
                            {
                                Arc::unwrap_or_clone(sort.input)
                            }
                            other => other,
                        };
                        Ok(Transformed::yes(result))
                    })?
                    .data;
                query.subquery = Arc::new(
                    LogicalPlanBuilder::from(rewritten)
                        .project(output.into_iter().map(Expr::Column))?
                        .build()?,
                );
                let result = if matches!(default, Expr::Literal(ref value, _) if value.is_null()) {
                    Expr::ScalarSubquery(query)
                } else {
                    let mut marker = query.clone();
                    marker.subquery = aggregate_marker.ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!(
                            "missing scalar aggregate marker"
                        )
                    })?;
                    datafusion_expr::when(
                        Expr::ScalarSubquery(marker).is_not_null(),
                        Expr::ScalarSubquery(query),
                    )
                    .otherwise(default)?
                };
                Ok(Transformed::yes(result))
            })
            .map(|result| result.update_data(|expr| name.restore(expr)))
        })
    }
}

/// A bounded unary pipeline. Classify the entire plan before changing any node.
/// The terminal SUM/MAX and its column/cast suffix are NULL on missing input;
/// intermediate empty grouping sets, in contrast, must be completed in place.
fn grouped_pipeline(plan: &LogicalPlan) -> Result<bool> {
    fn null_preserving(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) => true,
            Expr::Alias(alias) => null_preserving(&alias.expr),
            Expr::Cast(cast) => null_preserving(&cast.expr),
            _ => false,
        }
    }
    let mut terminal = false;
    let mut grouped = false;
    let mut correlated = false;
    let mut eligible = true;
    plan.apply(|node| {
        for expr in node.expressions() {
            eligible &= !expr.is_volatile()
                && !expr.exists(|expr| {
                    Ok(matches!(
                        expr,
                        Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
                    ))
                })?;
            if expr.contains_outer() {
                correlated = true;
                eligible &= terminal && matches!(node, LogicalPlan::Filter(_));
            }
        }
        match node {
            LogicalPlan::Projection(projection) => {
                eligible &= terminal || projection.expr.iter().all(null_preserving);
            }
            LogicalPlan::SubqueryAlias(_) => {}
            LogicalPlan::Filter(_) => eligible &= terminal,
            LogicalPlan::Aggregate(aggregate) => {
                if !terminal {
                    eligible &= aggregate.group_expr.is_empty()
                        && !aggregate.aggr_expr.is_empty()
                        && aggregate
                            .aggr_expr
                            .iter()
                            .all(|expr| supported_aggregate(expr, true));
                    terminal = true;
                } else {
                    eligible &= !aggregate.group_expr.is_empty()
                        && aggregate
                            .aggr_expr
                            .iter()
                            .all(|expr| supported_aggregate(expr, false))
                        && grouping_branches(aggregate)?.is_some();
                    grouped = true;
                }
            }
            LogicalPlan::Unnest(unnest) => {
                eligible &= terminal
                    && unnest.exec_columns.len() == 1
                    && unnest.list_type_columns.len() == 1
                    && unnest.struct_type_columns.is_empty();
            }
            LogicalPlan::TableScan(scan) => eligible &= terminal && scan.fetch.is_none(),
            LogicalPlan::Values(_) | LogicalPlan::EmptyRelation(_) => eligible &= terminal,
            _ => eligible = false,
        }
        Ok(if eligible {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(eligible && terminal && grouped && correlated)
}

fn supported_aggregate(expr: &Expr, terminal: bool) -> bool {
    let Expr::AggregateFunction(aggregate) = expr.clone().unalias() else {
        return false;
    };
    let function = aggregate.func.inner();
    (function.is::<datafusion::functions_aggregate::sum::Sum>()
        || function.is::<datafusion::functions_aggregate::min_max::Max>()
        || (!terminal && function.is::<datafusion::functions_aggregate::count::Count>()))
        && aggregate.params.order_by.is_empty()
}

struct GroupingBranch {
    active: Vec<usize>,
    id: Option<ScalarValue>,
}

/// Preserve the original bit positions and duplicate ordinal, never including
/// private domain columns in the grouping ID. The type is the resolved type,
/// including any width required by repeated sets.
fn grouping_id(active: &[bool], ordinal: u64, data_type: &DataType) -> Option<ScalarValue> {
    let width = match data_type {
        DataType::UInt8 => 8,
        DataType::UInt16 => 16,
        DataType::UInt32 => 32,
        DataType::UInt64 => 64,
        _ => return None,
    };
    let ordinal_bits = (u64::BITS - ordinal.leading_zeros()) as usize;
    if active.len() + ordinal_bits > width {
        return None;
    }
    let mut value = if ordinal == 0 {
        0
    } else {
        ordinal << active.len()
    };
    for (index, included) in active.iter().enumerate() {
        if !included {
            value |= 1 << (active.len() - index - 1);
        }
    }
    Some(match data_type {
        DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(value)),
        _ => return None,
    })
}

/// Check expansion cardinality without constructing prefixes or a powerset.
/// ROLLUP's width already has to fit the 64-bit grouping ID.
fn grouping_set_count(grouping: &GroupingSet) -> Option<usize> {
    match grouping {
        GroupingSet::GroupingSets(sets) if !sets.is_empty() && sets.len() <= 4096 => {
            Some(sets.len())
        }
        GroupingSet::Rollup(keys) if keys.len() <= 64 => Some(keys.len() + 1),
        GroupingSet::Cube(keys) if keys.len() <= 12 => Some(1 << keys.len()),
        _ => None,
    }
}

#[cfg(test)]
thread_local! {
    static GROUPING_SET_MATERIALIZATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn grouping_branches(aggregate: &Aggregate) -> Result<Option<Vec<GroupingBranch>>> {
    let keys = grouping_set_to_exprlist(&aggregate.group_expr)?;
    let [Expr::GroupingSet(grouping)] = aggregate.group_expr.as_slice() else {
        return Ok(Some(vec![GroupingBranch {
            active: (0..keys.len()).collect(),
            id: None,
        }]));
    };
    let Some(count) = grouping_set_count(grouping).filter(|_| keys.len() <= 64) else {
        return Ok(None);
    };
    // Eligibility also calls this helper, before the whole-pipeline budget.
    // Reject unrepresentable widths before allocating any expanded sets.
    #[cfg(test)]
    GROUPING_SET_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
    let sets: Vec<Vec<&Expr>> = match grouping {
        GroupingSet::GroupingSets(sets) => sets.iter().map(|set| set.iter().collect()).collect(),
        GroupingSet::Rollup(keys) => (0..=keys.len())
            .map(|end| keys[..end].iter().collect())
            .collect(),
        GroupingSet::Cube(keys) => powerset(keys)?,
    };
    let mut ordinals = HashMap::<Vec<bool>, u64>::new();
    let mut branches = Vec::with_capacity(count);
    for set in sets {
        let active: Vec<_> = keys.iter().map(|key| set.contains(key)).collect();
        let ordinal = ordinals.entry(active.clone()).or_default();
        let Some(id) = grouping_id(
            &active,
            *ordinal,
            aggregate.schema.field(keys.len()).data_type(),
        ) else {
            return Ok(None);
        };
        *ordinal += 1;
        branches.push(GroupingBranch {
            active: active
                .iter()
                .enumerate()
                .filter_map(|(i, active)| active.then_some(i))
                .collect(),
            id: Some(id),
        });
    }
    Ok(Some(branches))
}

struct DomainBinding {
    reference: Expr,
    outer: Column,
    key: Column,
}

// Count occurrences, not distinct Arc pointers: neither optimization nor physical
// planning memoizes shared relational inputs. Include expression/schema work so
// wide plans and repeated outer domains consume the same budget as inner scans.
const MAX_GROUPED_DOMAIN_WORK: usize = 16_384;

fn node_work(plan: &LogicalPlan) -> Result<Option<usize>> {
    let mut work = 1usize.checked_add(plan.schema().fields().len());
    plan.apply_expressions(|expr| {
        expr.apply(|_| {
            work = work.and_then(|work| work.checked_add(1));
            Ok(
                if work.is_some_and(|work| work <= MAX_GROUPED_DOMAIN_WORK) {
                    TreeNodeRecursion::Continue
                } else {
                    TreeNodeRecursion::Stop
                },
            )
        })
    })?;
    Ok(work.filter(|&work| work <= MAX_GROUPED_DOMAIN_WORK))
}

fn plan_work(plan: &LogicalPlan) -> Result<Option<usize>> {
    let mut work = Some(0usize);
    plan.apply_with_subqueries(|node| {
        work = work
            .zip(node_work(node)?)
            .and_then(|(total, node)| total.checked_add(node));
        Ok(
            if work.is_some_and(|work| work <= MAX_GROUPED_DOMAIN_WORK) {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            },
        )
    })?;
    Ok(work.filter(|&work| work <= MAX_GROUPED_DOMAIN_WORK))
}

/// An upper bound for lowering one aggregate. Each ordinary branch repeats its
/// input; each empty-set completion repeats both that input and the domain. The
/// allowance covers generated aggregates, projections, anti-join predicates,
/// DISTINCT, UNION and output restoration, including their expression/field work.
fn aggregate_work(
    input: usize,
    domain: usize,
    own: usize,
    width: usize,
    bindings: usize,
    branches: usize,
    empty: usize,
) -> Option<usize> {
    let generated = width
        .checked_add(bindings)?
        .checked_mul(16)?
        .checked_add(16)?;
    input
        .checked_add(own)?
        .checked_add(generated)?
        .checked_mul(branches.checked_add(empty)?)?
        .checked_add(domain.checked_mul(empty)?)?
        .checked_add(generated)
}

/// Preflight the *whole* pipeline before allocating aliases or rewriting nodes.
/// Checked arithmetic and a single cumulative cap bound nested branch products,
/// including missing-domain completion, not just the sets of any one aggregate.
fn grouped_work(plan: &LogicalPlan, outer: &LogicalPlan, bindings: usize) -> Result<Option<usize>> {
    fn estimate(plan: &LogicalPlan, domain: usize, bindings: usize) -> Result<Option<usize>> {
        let Some(own) = node_work(plan)? else {
            return Ok(None);
        };
        let work = match plan {
            LogicalPlan::TableScan(_) | LogicalPlan::Values(_) | LogicalPlan::EmptyRelation(_) => {
                // Original leaf, domain, and the cross join (including its fields).
                own.checked_add(domain)
                    .and_then(|work| work.checked_add(own))
                    .and_then(|work| work.checked_add(bindings))
            }
            _ => {
                let inputs = plan.inputs();
                let [input] = inputs.as_slice() else {
                    return Ok(None);
                };
                let Some(input) = estimate(input, domain, bindings)? else {
                    return Ok(None);
                };
                if let LogicalPlan::Aggregate(aggregate) = plan {
                    let Some(branches) = grouping_branches(aggregate)? else {
                        return Ok(None);
                    };
                    let empty = branches
                        .iter()
                        .filter(|branch| branch.active.is_empty() && branch.id.is_some())
                        .count();
                    aggregate_work(
                        input,
                        domain,
                        own,
                        plan.schema().fields().len(),
                        bindings,
                        branches.len(),
                        empty,
                    )
                } else {
                    // Carry private columns; SubqueryAlias additionally restores
                    // every public qualifier via a projection at this stage.
                    bindings
                        .checked_mul(4)
                        .and_then(|extra| extra.checked_add(own.checked_mul(4)?))
                        .and_then(|extra| input.checked_add(extra))
                }
            }
        };
        Ok(work.filter(|&work| work <= MAX_GROUPED_DOMAIN_WORK))
    }
    let Some(outer) = plan_work(outer)? else {
        return Ok(None);
    };
    let Some(domain) = bindings
        .checked_mul(4)
        .and_then(|keys| outer.checked_add(keys)?.checked_add(2))
    else {
        return Ok(None);
    };
    Ok(estimate(plan, domain, bindings)?.and_then(|work| {
        // Final public projection plus the outer input and its scalar join.
        work.checked_add(outer)?
            .checked_add(plan.schema().fields().len().checked_mul(4)?)?
            .checked_add(4)
            .filter(|&work| work <= MAX_GROUPED_DOMAIN_WORK)
    }))
}

fn grouped_domain(
    plan: &LogicalPlan,
    outer: &Arc<LogicalPlan>,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    if !grouped_pipeline(plan)?
        || outer.exists(|node| {
            Ok(node
                .expressions()
                .iter()
                .any(|expr| expr.is_volatile() || expr.contains_outer())
                || matches!(
                    node,
                    LogicalPlan::Extension(_)
                        | LogicalPlan::Subquery(_)
                        | LogicalPlan::RecursiveQuery(_)
                ))
        })?
    {
        return Ok(None);
    }
    // Use the same guarded pull-up as ScalarSubqueryToJoin, on a disposable
    // clone. Native-safe pipelines stay native even when expansion would exceed
    // our budget; in particular duplicate nonempty grouping chains stay linear.
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    if plan.clone().rewrite(&mut pull_up).is_ok() && pull_up.can_pull_up {
        return Ok(None);
    }
    let references = plan.all_out_ref_exprs();
    if grouped_work(plan, outer, references.len())?.is_none() {
        return Ok(None);
    }
    let mut reserved = HashSet::new();
    for root in [plan, outer.as_ref()] {
        root.apply(|node| {
            reserved.extend(
                node.schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone()),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let mut bindings = Vec::new();
    for reference in references {
        let Expr::OuterReferenceColumn(field, column) = &reference else {
            return Ok(None);
        };
        if !outer.schema().has_column(column)
            || outer
                .schema()
                .field(outer.schema().index_of_column(column)?)
                .data_type()
                != field.data_type()
            || !(field.data_type().is_integer()
                || matches!(
                    field.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ))
        {
            return Ok(None);
        }
        let key = loop {
            let name = config.alias_generator().next("__sail_group_domain");
            if reserved.insert(name.clone()) {
                break Column::new_unqualified(name);
            }
        };
        bindings.push(DomainBinding {
            outer: column.clone(),
            reference,
            key,
        });
    }
    if bindings.is_empty() {
        return Ok(None);
    }
    let domain = LogicalPlanBuilder::from(Arc::clone(outer))
        .project(
            bindings
                .iter()
                .map(|binding| Expr::Column(binding.outer.clone()).alias(&binding.key.name)),
        )?
        .distinct()?
        .build()?;
    let rewritten = bind_grouped_plan(plan, &domain, &bindings)?;
    Ok(Some(
        LogicalPlanBuilder::from(rewritten)
            .project(plan.schema().columns().into_iter().map(Expr::Column))?
            .build()?,
    ))
}

fn binding_exprs(bindings: &[DomainBinding]) -> Vec<Expr> {
    bindings
        .iter()
        .map(|binding| Expr::Column(binding.key.clone()))
        .collect()
}

/// Restore public column identity by position and private binding identity by
/// lineage. In particular aliases must not capture the private key qualifier.
fn restore_bound_output(
    plan: LogicalPlan,
    original: &DFSchema,
    bindings: &[DomainBinding],
) -> Result<LogicalPlan> {
    let columns = plan.schema().columns();
    let mut exprs: Vec<_> = original
        .iter()
        .enumerate()
        .map(|(index, (qualifier, field))| {
            Expr::Column(columns[index].clone()).alias_qualified(qualifier.cloned(), field.name())
        })
        .collect();
    exprs.extend(
        columns[original.fields().len()..]
            .iter()
            .zip(bindings)
            .map(|(column, binding)| Expr::Column(column.clone()).alias(&binding.key.name)),
    );
    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}

fn bind_grouped_plan(
    plan: &LogicalPlan,
    domain: &LogicalPlan,
    bindings: &[DomainBinding],
) -> Result<LogicalPlan> {
    let keys = || binding_exprs(bindings);
    let result = match plan {
        LogicalPlan::Projection(projection) => {
            let input = bind_grouped_plan(&projection.input, domain, bindings)?;
            let mut exprs = projection.expr.clone();
            exprs.extend(keys());
            LogicalPlanBuilder::from(input).project(exprs)?.build()?
        }
        LogicalPlan::Filter(filter) => {
            let input = bind_grouped_plan(&filter.input, domain, bindings)?;
            let predicate = filter
                .predicate
                .clone()
                .transform_up(|expr| {
                    if let Some(binding) = bindings.iter().find(|binding| binding.reference == expr)
                    {
                        Ok(Transformed::yes(Expr::Column(binding.key.clone())))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?
                .data;
            LogicalPlanBuilder::from(input).filter(predicate)?.build()?
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input = bind_grouped_plan(&alias.input, domain, bindings)?;
            let aliased = LogicalPlanBuilder::from(input)
                .alias(alias.alias.clone())?
                .build()?;
            restore_bound_output(aliased, plan.schema(), bindings)?
        }
        LogicalPlan::Unnest(unnest) => {
            let input = bind_grouped_plan(&unnest.input, domain, bindings)?;
            LogicalPlan::Unnest(Unnest::try_new(
                Arc::new(input),
                unnest.exec_columns.clone(),
                unnest.options.clone(),
            )?)
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = bind_grouped_plan(&aggregate.input, domain, bindings)?;
            lower_bound_aggregate(aggregate, input, domain, bindings)?
        }
        LogicalPlan::TableScan(_) | LogicalPlan::Values(_) | LogicalPlan::EmptyRelation(_) => {
            LogicalPlanBuilder::from(plan.clone())
                .cross_join(domain.clone())?
                .build()?
        }
        _ => return datafusion_common::internal_err!("ineligible grouping domain node"),
    };
    Ok(result)
}

fn lower_bound_aggregate(
    aggregate: &Aggregate,
    input: LogicalPlan,
    domain: &LogicalPlan,
    bindings: &[DomainBinding],
) -> Result<LogicalPlan> {
    let keys = grouping_set_to_exprlist(&aggregate.group_expr)?;
    let branches = grouping_branches(aggregate)?.ok_or_else(|| {
        datafusion_common::internal_datafusion_err!("ineligible grouping branches")
    })?;
    let mut outputs = Vec::new();
    for branch in branches {
        let mut group_expr = binding_exprs(bindings);
        group_expr.extend(branch.active.iter().map(|&index| keys[index].clone()));
        let actual = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_expr, aggregate.aggr_expr.clone())?
            .build()?;
        let actual = if aggregate.group_expr.is_empty() {
            // Keep the correlation directly above the terminal aggregate so
            // scalar validation can prove its per-binding cardinality.
            let correlation = conjunction(bindings.iter().map(|binding| {
                datafusion_expr::binary_expr(
                    Expr::Column(binding.key.clone()),
                    Operator::IsNotDistinctFrom,
                    binding.reference.clone(),
                )
            }))
            .ok_or_else(|| datafusion_common::internal_datafusion_err!("empty grouping domain"))?;
            LogicalPlanBuilder::from(actual)
                .filter(correlation)?
                .build()?
        } else {
            actual
        };
        let columns = actual.schema().columns();
        let mut exprs = Vec::new();
        for (index, _) in keys.iter().enumerate() {
            exprs.push(
                match branch.active.iter().position(|&active| active == index) {
                    Some(position) => Expr::Column(columns[bindings.len() + position].clone()),
                    None => datafusion_expr::lit(ScalarValue::try_from(
                        aggregate.schema.field(index).data_type(),
                    )?),
                },
            );
        }
        if let Some(id) = &branch.id {
            exprs.push(datafusion_expr::lit(id.clone()));
        }
        exprs.extend(
            columns[bindings.len() + branch.active.len()..]
                .iter()
                .cloned()
                .map(Expr::Column),
        );
        let named = name_original_outputs(exprs, &aggregate.schema);
        let mut named = named;
        named.extend(binding_exprs(bindings));
        outputs.push(Arc::new(
            LogicalPlanBuilder::from(actual).project(named)?.build()?,
        ));

        // FIXME: Align empty-input mixed/duplicate grouping sets with Spark
        // (SPARK-29701). Spark emits no rows, while DataFusion preserves empty
        // grand totals. Keep correlated and literal-bound Sail evaluation
        // consistent until both paths can change together; a downstream
        // explode(array(count, count + 1)) can currently yield 1 instead of NULL.
        // Native mixed grouping sets emit an empty grand total. Adding domain
        // keys would lose it, so complete missing *input* domains before HAVING.
        // The terminal global SUM/MAX needs no row: its missing scalar is NULL.
        if branch.active.is_empty() && branch.id.is_some() {
            let present = LogicalPlanBuilder::from(input.clone())
                .project(bindings.iter().map(|binding| {
                    Expr::Column(binding.key.clone())
                        .alias(format!("{}__present", binding.key.name))
                }))?
                .distinct()?
                .build()?;
            let predicates = bindings
                .iter()
                .zip(present.schema().columns())
                .map(|(binding, column)| {
                    datafusion_expr::binary_expr(
                        Expr::Column(binding.key.clone()),
                        Operator::IsNotDistinctFrom,
                        Expr::Column(column),
                    )
                })
                .collect::<Vec<_>>();
            let missing = LogicalPlanBuilder::from(domain.clone())
                .join_on(present, JoinType::LeftAnti, predicates)?
                .build()?;
            let mut defaults = keys
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    ScalarValue::try_from(aggregate.schema.field(index).data_type())
                        .map(datafusion_expr::lit)
                })
                .collect::<Result<Vec<_>>>()?;
            defaults.push(datafusion_expr::lit(branch.id.clone().ok_or_else(
                || datafusion_common::internal_datafusion_err!("missing grouping id"),
            )?));
            for expr in &aggregate.aggr_expr {
                let Expr::AggregateFunction(function) = expr.clone().unalias() else {
                    return datafusion_common::internal_err!("unsupported grouping aggregate");
                };
                defaults.push(datafusion_expr::lit(
                    function
                        .func
                        .default_value(&expr.get_type(aggregate.input.schema())?)?,
                ));
            }
            let mut defaults = name_original_outputs(defaults, &aggregate.schema);
            defaults.extend(binding_exprs(bindings));
            outputs.push(Arc::new(
                LogicalPlanBuilder::from(missing)
                    .project(defaults)?
                    .build()?,
            ));
        }
    }
    if outputs.len() == 1 {
        return Ok(Arc::unwrap_or_clone(outputs.remove(0)));
    }
    let union = LogicalPlan::Union(Union::try_new(outputs)?);
    restore_bound_output(union, &aggregate.schema, bindings)
}

fn name_original_outputs(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
    exprs
        .into_iter()
        .zip(schema.iter())
        .map(|(expr, (qualifier, field))| expr.alias_qualified(qualifier.cloned(), field.name()))
        .collect()
}

/// Evaluate the scalar aggregate's projection on empty input, retaining defaults
/// such as COUNT = 0 without replacing legitimate null results of nonempty groups.
fn empty_scalar_value(plan: &LogicalPlan) -> Result<Expr> {
    fn substitute(expr: Expr, input: &LogicalPlan, values: &[Expr]) -> Result<Expr> {
        expr.transform_up(|expr| {
            if let Expr::Column(column) = &expr {
                let index = input.schema().index_of_column(column)?;
                Ok(Transformed::yes(values[index].clone()))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .map(|result| result.data.unalias())
    }

    // Keep row existence separate from values: a projection such as COALESCE
    // must not turn a row rejected by HAVING into a non-null scalar result.
    fn values(plan: &LogicalPlan) -> Result<(Vec<Expr>, Option<Expr>)> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => aggregate
                .aggr_expr
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform_up(|expr| {
                            if let Expr::AggregateFunction(agg) = &expr {
                                let data_type = expr.get_type(aggregate.input.schema())?;
                                Ok(Transformed::yes(Expr::Literal(
                                    agg.func.default_value(&data_type)?,
                                    None,
                                )))
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })
                        .map(|result| result.data.unalias())
                })
                .collect::<Result<Vec<_>>>()
                .map(|values| (values, None)),
            LogicalPlan::Projection(projection) => {
                let (input, predicate) = values(&projection.input)?;
                let values = projection
                    .expr
                    .iter()
                    .map(|expr| substitute(expr.clone(), &projection.input, &input))
                    .collect::<Result<Vec<_>>>()?;
                Ok((values, predicate))
            }
            LogicalPlan::Filter(filter) => {
                let (input, predicate) = values(&filter.input)?;
                let condition = substitute(filter.predicate.clone(), &filter.input, &input)?;
                let predicate = Some(match predicate {
                    Some(predicate) => predicate.and(condition),
                    None => condition,
                });
                Ok((input, predicate))
            }
            LogicalPlan::SubqueryAlias(alias) => values(&alias.input),
            LogicalPlan::Sort(sort) if sort.fetch.is_none() => values(&sort.input),
            _ => datafusion_common::not_impl_err!("empty result of scalar correlation"),
        }
    }
    let (values, predicate) = values(plan)?;
    let value = values
        .into_iter()
        .next()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("empty scalar output"))?;
    match predicate {
        Some(predicate) => datafusion_expr::when(predicate, value)
            .otherwise(datafusion_expr::lit(datafusion_common::ScalarValue::Null)),
        None => Ok(value),
    }
}

fn native_equality(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op != Operator::Eq {
        return false;
    }
    let inner_key = |expr: &Expr| {
        matches!(expr, Expr::Column(_))
            || matches!(expr, Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_)))
    };
    (inner_key(&binary.left) && !binary.right.any_column_refs())
        || (inner_key(&binary.right) && !binary.left.any_column_refs())
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::functions_aggregate::expr_fn::{count, max, sum};
    use datafusion::functions_nested::expr_fn::make_array;
    use datafusion::optimizer::OptimizerContext;
    use datafusion_common::cast::as_int64_array;
    use datafusion_common::internal_datafusion_err;
    use datafusion_expr::{col, lit, out_ref_col, scalar_subquery};

    use super::*;

    fn input(name: &str) -> Result<LogicalPlan> {
        LogicalPlanBuilder::values(vec![
            vec![lit(1i64)],
            vec![lit(2i64)],
            vec![lit(1i64)],
            vec![lit(ScalarValue::Int64(None))],
        ])?
        .project(vec![col("column1").alias("k")])?
        .alias(name)?
        .build()
    }

    fn prototype(generator: bool) -> Result<LogicalPlan> {
        let inner = LogicalPlanBuilder::from(input("i")?)
            .filter(col("i.k").eq(out_ref_col(DataType::Int64, "o.k")))?
            .filter(col("i.k").eq(lit(1i64)))?;
        let inner = if generator {
            inner
                .aggregate(
                    vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                        vec![col("i.k")],
                        vec![],
                    ]))],
                    vec![count(lit(1i64)).alias("c")],
                )?
                .project(vec![
                    make_array(vec![col("c"), col("c") + lit(1i64)]).alias("z"),
                ])?
                .unnest_column("z")?
                .alias("generated")?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![max(col("generated.z")).alias("result")],
                )?
        } else {
            let lower = inner
                .aggregate(vec![col("i.k")], vec![count(lit(1i64)).alias("c")])?
                .project(vec![col("i.k").alias("k"), col("c")])?
                .alias("lower")?
                .filter(col("lower.k").eq(out_ref_col(DataType::Int64, "o.k")))?;
            lower
                .aggregate(
                    vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                        vec![col("lower.k")],
                        vec![col("lower.k")],
                        vec![],
                    ]))],
                    Vec::<Expr>::new(),
                )?
                .project(vec![
                    datafusion_expr::cast(
                        datafusion_expr::bitwise_and(
                            col(Aggregate::INTERNAL_GROUPING_ID),
                            lit(1u8),
                        ),
                        DataType::Int64,
                    )
                    .alias("z"),
                ])?
                .alias("bits")?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("bits.z")).alias("result")])?
        }
        .project(vec![col("result")])?
        .build()?;
        LogicalPlanBuilder::from(input("o")?)
            .project(vec![
                col("o.k"),
                scalar_subquery(Arc::new(inner)).alias("result"),
            ])?
            .build()
    }

    #[tokio::test]
    async fn grouped_domain_prototypes() -> Result<()> {
        for generator in [false, true] {
            let original = prototype(generator)?;
            let context = OptimizerContext::new();
            let rewritten = NormalizeScalarSubqueryDomain.rewrite(original.clone(), &context)?;
            assert!(rewritten.transformed);
            let repeated =
                NormalizeScalarSubqueryDomain.rewrite(rewritten.data.clone(), &context)?;
            assert!(!repeated.transformed);
            assert_eq!(repeated.data, rewritten.data);
            let mut unions = 0;
            rewritten.data.apply_with_subqueries(|node| {
                if matches!(node, LogicalPlan::Union(_) | LogicalPlan::Unnest(_)) {
                    assert!(node.all_out_ref_exprs().is_empty());
                    unions += usize::from(matches!(node, LogicalPlan::Union(_)));
                }
                if let LogicalPlan::Unnest(unnest) = node {
                    let rebuilt = Unnest::try_new(
                        Arc::clone(&unnest.input),
                        unnest.exec_columns.clone(),
                        unnest.options.clone(),
                    )?;
                    assert_eq!(rebuilt.schema, unnest.schema);
                    assert_eq!(rebuilt.dependency_indices, unnest.dependency_indices);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(unions > 0);
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_optimizer_rules(crate::default_optimizer_rules())
                .build();
            let batches = DataFrame::new(state, original).collect().await?;
            let mut values = Vec::new();
            for batch in batches {
                let keys = as_int64_array(batch.column(0).as_ref())?;
                let results = as_int64_array(batch.column(1).as_ref())?;
                for row in 0..batch.num_rows() {
                    assert!(!results.is_null(row));
                    values.push((
                        (!keys.is_null(row)).then(|| keys.value(row)),
                        results.value(row),
                    ));
                }
            }
            values.sort();
            let one = if generator { 3 } else { 1 };
            assert_eq!(
                values,
                vec![(None, 1), (Some(1), one), (Some(1), one), (Some(2), 1)]
            );
        }
        Ok(())
    }

    fn grouping_chain(depth: usize, empty: bool) -> Result<LogicalPlan> {
        let mut inner = LogicalPlanBuilder::from(input("i")?)
            .filter(col("i.k").eq(out_ref_col(DataType::Int64, "o.k")))?
            .build()?;
        for _ in 0..depth {
            inner = LogicalPlanBuilder::from(inner)
                .aggregate(
                    vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                        vec![col("i.k")],
                        if empty { vec![] } else { vec![col("i.k")] },
                    ]))],
                    vec![count(lit(1i64)).alias("c")],
                )?
                .project(vec![col("i.k"), col("c")])?
                .build()?;
        }
        let inner = LogicalPlanBuilder::from(inner)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("c")).alias("result")])?
            .project(vec![col("result")])?
            .build()?;
        LogicalPlanBuilder::from(input("o")?)
            .project(vec![scalar_subquery(Arc::new(inner)).alias("result")])?
            .build()
    }

    #[tokio::test]
    async fn native_grouping_chains_stay_linear() -> Result<()> {
        for depth in [2, 4, 8, 12] {
            let original = grouping_chain(depth, false)?;
            let rewritten = NormalizeScalarSubqueryDomain
                .rewrite(original.clone(), &OptimizerContext::new())?;
            assert!(!rewritten.transformed);
            assert_eq!(rewritten.data, original);
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_optimizer_rules(crate::default_optimizer_rules())
                .build();
            let frame = DataFrame::new(state, original);
            let optimized = frame.clone().into_optimized_plan()?;
            let (mut nodes, mut sources, mut groups) = (0, 0, 0);
            optimized.apply_with_subqueries(|node| {
                nodes += 1;
                sources += usize::from(matches!(node, LogicalPlan::Values(_)));
                if let LogicalPlan::Aggregate(aggregate) = node {
                    groups += usize::from(matches!(
                        aggregate.group_expr.as_slice(),
                        [Expr::GroupingSet(_)]
                    ));
                }
                assert!(!matches!(node, LogicalPlan::Union(_)));
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(sources, 2);
            assert_eq!(groups, depth);
            assert!(nodes <= 4 * depth + 16, "depth={depth}, nodes={nodes}");
            for batch in frame.collect().await? {
                let values = as_int64_array(batch.column(0).as_ref())?;
                for row in 0..batch.num_rows() {
                    if !values.is_null(row) {
                        assert_eq!(values.value(row), 4);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn grouped_budget_counts_completion_and_declines_atomically() -> Result<()> {
        assert!(aggregate_work(usize::MAX, 1, 1, 1, 1, 2, 1).is_none());
        assert!(aggregate_work(1, usize::MAX, 1, 1, 1, 2, 2).is_none());
        assert!(aggregate_work(1, 1, 1, usize::MAX, 1, 2, 1).is_none());
        assert!(aggregate_work(1, 1, 1, 1, 1, usize::MAX, 1).is_none());
        let mut accepted = 0;
        let mut declined = 0;
        for depth in [1, 2, 4, 8, 12] {
            let original = grouping_chain(depth, true)?;
            let LogicalPlan::Projection(projection) = &original else {
                unreachable!()
            };
            let Expr::ScalarSubquery(query) = projection.expr[0].clone().unalias() else {
                unreachable!()
            };
            assert!(grouped_pipeline(&query.subquery)?);
            let budget = grouped_work(&query.subquery, &projection.input, 1)?;
            let rewritten = NormalizeScalarSubqueryDomain
                .rewrite(original.clone(), &OptimizerContext::new())?;
            if let Some(budget) = budget {
                accepted += 1;
                assert!(rewritten.transformed);
                assert!(plan_work(&rewritten.data)?.is_some_and(|actual| actual <= budget));
                // An empty set has a second input occurrence for completion,
                // as well as another occurrence of the outer domain itself.
                let mut sources = 0usize;
                rewritten.data.apply_with_subqueries(|node| {
                    sources += usize::from(matches!(node, LogicalPlan::Values(_)));
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert!(sources > 2usize.pow(depth as u32 + 1));
            } else {
                declined += 1;
                assert!(!rewritten.transformed);
                assert_eq!(rewritten.data, original);
            }
        }
        assert!(accepted > 0 && declined > 0);
        Ok(())
    }

    fn grouping_aggregate(width: usize, grouping: GroupingSet) -> Result<Aggregate> {
        let input = LogicalPlanBuilder::values(vec![vec![lit(1i64); width.max(1)]])?
            .project(
                (0..width.max(1)).map(|i| col(format!("column{}", i + 1)).alias(format!("c{i}"))),
            )?
            .alias("i")?
            .filter(col("i.c0").eq(out_ref_col(DataType::Int64, "o.k")))?
            .build()?;
        Aggregate::try_new(
            Arc::new(input),
            vec![Expr::GroupingSet(grouping)],
            vec![count(lit(1i64)).alias("c")],
        )
    }

    #[test]
    fn rollup_preflight_rejects_width_before_materialization() -> Result<()> {
        for width in [64, 65, 128] {
            let aggregate = grouping_aggregate(
                width,
                GroupingSet::Rollup((0..width).map(|i| col(format!("i.c{i}"))).collect()),
            )?;
            let before = GROUPING_SET_MATERIALIZATIONS.get();
            let branches = grouping_branches(&aggregate)?;
            assert_eq!(
                GROUPING_SET_MATERIALIZATIONS.get() - before,
                usize::from(width == 64)
            );
            if width == 64 {
                let branches = branches
                    .ok_or_else(|| internal_datafusion_err!("64-key rollup must be supported"))?;
                assert_eq!(branches.len(), 65);
                assert!(branches[0].active.is_empty());
                assert_eq!(branches[0].id, Some(ScalarValue::UInt64(Some(u64::MAX))));
                assert_eq!(branches[64].active, (0..64).collect::<Vec<_>>());
                assert_eq!(branches[64].id, Some(ScalarValue::UInt64(Some(0))));
            } else {
                assert!(branches.is_none());
            }
            let inner = LogicalPlanBuilder::from(LogicalPlan::Aggregate(aggregate))
                .aggregate(Vec::<Expr>::new(), vec![sum(col("c")).alias("result")])?
                .build()?;
            let before = GROUPING_SET_MATERIALIZATIONS.get();
            assert_eq!(grouped_pipeline(&inner)?, width == 64);
            assert_eq!(
                GROUPING_SET_MATERIALIZATIONS.get() - before,
                usize::from(width == 64)
            );
            if width > 64 {
                let original = LogicalPlanBuilder::from(input("o")?)
                    .project(vec![scalar_subquery(Arc::new(inner)).alias("result")])?
                    .build()?;
                let rewritten = NormalizeScalarSubqueryDomain
                    .rewrite(original.clone(), &OptimizerContext::new())?;
                assert!(!rewritten.transformed);
                assert_eq!(rewritten.data, original);
                assert_eq!(GROUPING_SET_MATERIALIZATIONS.get(), before);
            }
        }
        Ok(())
    }

    #[test]
    fn grouping_preflight_preserves_cardinality_boundaries() -> Result<()> {
        for (width, grouping, expected) in [
            (0, GroupingSet::GroupingSets(vec![]), None),
            (0, GroupingSet::GroupingSets(vec![vec![]; 4096]), Some(4096)),
            (0, GroupingSet::GroupingSets(vec![vec![]; 4097]), None),
            (0, GroupingSet::Rollup(vec![]), Some(1)),
            (0, GroupingSet::Cube(vec![]), Some(1)),
            (
                12,
                GroupingSet::Cube((0..12).map(|i| col(format!("i.c{i}"))).collect()),
                Some(4096),
            ),
            (
                13,
                GroupingSet::Cube((0..13).map(|i| col(format!("i.c{i}"))).collect()),
                None,
            ),
            (
                65,
                GroupingSet::GroupingSets(vec![(0..65).map(|i| col(format!("i.c{i}"))).collect()]),
                None,
            ),
        ] {
            let aggregate = grouping_aggregate(width, grouping)?;
            let before = GROUPING_SET_MATERIALIZATIONS.get();
            let branches = grouping_branches(&aggregate)?;
            assert_eq!(branches.as_ref().map(Vec::len), expected);
            assert_eq!(
                GROUPING_SET_MATERIALIZATIONS.get() - before,
                usize::from(expected.is_some())
            );
        }
        // Ordinary GROUP BY has no grouping ID and must not inherit its width limit.
        let mut aggregate = grouping_aggregate(
            65,
            GroupingSet::Rollup((0..65).map(|i| col(format!("i.c{i}"))).collect()),
        )?;
        aggregate = Aggregate::try_new(
            aggregate.input,
            (0..65).map(|i| col(format!("i.c{i}"))).collect(),
            aggregate.aggr_expr,
        )?;
        let branches = grouping_branches(&aggregate)?
            .ok_or_else(|| internal_datafusion_err!("ordinary GROUP BY must be supported"))?;
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].active.len(), 65);
        assert!(branches[0].id.is_none());
        Ok(())
    }

    #[test]
    fn grouping_id_widths_and_ordinals() -> Result<()> {
        for (width, data_type) in [
            (8, DataType::UInt8),
            (16, DataType::UInt16),
            (32, DataType::UInt32),
            (64, DataType::UInt64),
        ] {
            let active = vec![false; width];
            let value = grouping_id(&active, 0, &data_type)
                .ok_or_else(|| internal_datafusion_err!("grouping ID must fit {data_type}"))?;
            assert_eq!(value.data_type(), data_type);
            assert_eq!(
                value,
                ScalarValue::UInt64(Some(if width == 64 {
                    u64::MAX
                } else {
                    (1u64 << width) - 1
                }))
                .cast_to(&data_type)?
            );
            assert!(grouping_id(&active, 1, &data_type).is_none());
            let active = vec![true; width - 1];
            assert_eq!(
                grouping_id(&active, 1, &data_type),
                Some(ScalarValue::UInt64(Some(1u64 << (width - 1))).cast_to(&data_type)?)
            );
        }
        assert_eq!(
            grouping_id(&[true, false, true], 2, &DataType::UInt8),
            Some(ScalarValue::UInt8(Some(18)))
        );
        Ok(())
    }

    #[test]
    fn unsupported_grouped_pipeline_is_atomic() -> Result<()> {
        let original = prototype(true)?;
        let LogicalPlan::Projection(projection) = &original else {
            unreachable!()
        };
        let Expr::ScalarSubquery(query) = projection.expr[1].clone().unalias() else {
            unreachable!()
        };
        for candidate in [
            LogicalPlanBuilder::from(Arc::clone(&query.subquery))
                .limit(0, Some(1))?
                .build()?,
            LogicalPlanBuilder::from(Arc::clone(&query.subquery))
                .project(vec![datafusion::functions::expr_fn::random()])?
                .build()?,
            LogicalPlanBuilder::from(Arc::clone(&query.subquery))
                .union(Arc::unwrap_or_clone(Arc::clone(&query.subquery)))?
                .build()?,
        ] {
            assert!(!grouped_pipeline(&candidate)?);
        }
        Ok(())
    }
}
