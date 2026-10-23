use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::{Cast, GroupingSet};
use datafusion_expr::expr_rewriter::{NamePreserver, strip_outer_reference};
use datafusion_expr::logical_plan::{Aggregate, Filter, Projection};
use datafusion_expr::utils::{
    collect_subquery_cols, conjunction, enumerate_grouping_sets, grouping_set_to_exprlist,
    split_conjunction,
};
use datafusion_expr::{Expr, JoinType, LogicalPlan, Operator, Volatility};

/// Group native correlated scalar aggregates by the value used in cast equality.
#[derive(Debug)]
pub struct NormalizeScalarSubqueryKeys;

impl OptimizerRule for NormalizeScalarSubqueryKeys {
    fn name(&self) -> &str {
        "normalize_scalar_subquery_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Filter(_)) {
            return Ok(Transformed::no(plan));
        }
        let names = NamePreserver::new(&plan);
        let mut reserved = plan.schema().field_names().into_iter().collect();
        plan.map_expressions(|expression| {
            let name = names.save(&expression);
            expression
                .transform_up(|expression| {
                    let Expr::ScalarSubquery(mut query) = expression else {
                        return Ok(Transformed::no(expression));
                    };
                    if query.outer_ref_columns.is_empty() {
                        return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                    }
                    normalize_keys(&query.subquery, config.alias_generator(), &mut reserved).map(
                        |result| {
                            if result.transformed {
                                query.subquery = Arc::new(result.data);
                            }
                            Transformed::new_transformed(
                                Expr::ScalarSubquery(query),
                                result.transformed,
                            )
                        },
                    )
                })
                .map(|result| result.update_data(|expression| name.restore(expression)))
        })
    }
}

fn native_equality(expression: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::BinaryExpr(binary) = expression else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let inner_key = |expression: &Expr| {
        matches!(expression, Expr::Column(_))
            || matches!(expression, Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_)))
    };
    let (left, right) = (
        strip_identity_cast(&binary.left),
        strip_identity_cast(&binary.right),
    );
    if inner_key(left) && !binary.right.any_column_refs() && binary.right.contains_outer() {
        Some((left, &binary.right))
    } else if inner_key(right) && !binary.left.any_column_refs() && binary.left.contains_outer() {
        Some((right, &binary.left))
    } else {
        None
    }
}

/// See through the same-type cast that compares a lifted key, which filter
/// pushdown inlines as `CAST(CAST(column AS t) AS t)` in a later optimizer pass.
fn strip_identity_cast(expression: &Expr) -> &Expr {
    match expression {
        Expr::Cast(cast) => match cast.expr.as_ref() {
            input @ Expr::Cast(inner) if inner.field.data_type() == cast.field.data_type() => input,
            _ => expression,
        },
        _ => expression,
    }
}

fn normalize_keys(
    plan: &LogicalPlan,
    aliases: &AliasGenerator,
    reserved: &mut BTreeSet<String>,
) -> Result<Transformed<LogicalPlan>> {
    let mut eligible = true;
    let mut has_aggregate = false;
    let mut has_cast = false;
    let mut reads_grouping_id = false;
    // An inner column compared to an outer value already selects one group per
    // outer row, so casts of that column need no separate key.
    let mut pinned = HashSet::new();
    plan.apply(|node| {
        reserved.extend(node.schema().field_names());
        has_aggregate |= matches!(node, LogicalPlan::Aggregate(_));
        reads_grouping_id |= node.expressions().iter().any(|expression| {
            expression
                .column_refs()
                .iter()
                .any(|column| column.name == Aggregate::INTERNAL_GROUPING_ID)
        });
        if matches!(node, LogicalPlan::Subquery(_))
            || (!matches!(node, LogicalPlan::Filter(_)) && node.contains_outer_reference())
            || (matches!(
                node,
                LogicalPlan::Union(_)
                    | LogicalPlan::Sort(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Extension(_)
            ) && !node.all_out_ref_exprs().is_empty())
        {
            eligible = false;
        }
        if let LogicalPlan::Join(join) = node {
            // Pulling a filter from an outer/semi/anti join's non-preserved side
            // changes unmatched rows; native decorrelation cannot carry that key.
            eligible &= (join.left.all_out_ref_exprs().is_empty()
                || matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti
                ))
                && (join.right.all_out_ref_exprs().is_empty()
                    || matches!(
                        join.join_type,
                        JoinType::Inner
                            | JoinType::Right
                            | JoinType::RightSemi
                            | JoinType::RightAnti
                    ));
        }
        for expression in node.expressions() {
            eligible &= !expression.exists(|part| {
                Ok(match part {
                    Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_) => true,
                    Expr::ScalarFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::HigherOrderFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::AggregateFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::WindowFunction(function) => {
                        function.fun.signature().volatility == Volatility::Volatile
                    }
                    _ => false,
                })
            })?;
        }
        if let LogicalPlan::Filter(filter) = node {
            for expression in split_conjunction(&filter.predicate)
                .into_iter()
                .filter(|expression| expression.contains_outer())
            {
                match native_equality(expression) {
                    Some((Expr::Column(column), _)) => {
                        // Floating-point equality merges -0.0 and 0.0, whose casts differ.
                        let floating = filter
                            .input
                            .schema()
                            .qualified_field_from_column(column)
                            .is_ok_and(|(_, field)| field.data_type().is_floating());
                        if !floating {
                            pinned.insert(column.name.clone());
                        }
                    }
                    Some(_) => has_cast = true,
                    None => eligible = false,
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !eligible || !has_aggregate || !has_cast {
        return Ok(Transformed::no(plan.clone()));
    }

    // Preserve grouped producers when replacing a cast would violate native's
    // requirement that every original grouping column occurs in a correlation.
    // Like DataFusion's `check_aggregation_in_scalar_subquery`, this checks only
    // the outermost aggregate; aggregates below it may group by anything.
    let (mut correlations, mut keyed) = (Vec::new(), Vec::new());
    if !lifted_scope(
        plan,
        &mut Vec::new(),
        &pinned,
        &mut correlations,
        &mut keyed,
    ) {
        return Ok(Transformed::no(plan.clone()));
    }
    for aggregate in keyed {
        let correlated = collect_subquery_cols(&correlations, aggregate.input.schema())?;
        if aggregate
            .group_expr
            .iter()
            .flat_map(Expr::column_refs)
            .any(|column| !correlated.contains(column))
        {
            return Ok(Transformed::no(plan.clone()));
        }
    }

    let mut lift = Lift {
        aliases,
        reserved,
        pinned: &pinned,
        hidden: HashSet::new(),
        reads_grouping_id,
        grouping_ids_fit: true,
    };
    // Even without a lifted key, carrying the correlated columns partitions windows
    // and refreshes cached schemas for native pull-up.
    let (carried, pending, _) = lift.carry(plan.clone(), false)?;
    // Every lifted equality has an aggregate above it, which consumes its key.
    debug_assert!(pending.is_empty());
    if carried == *plan || !lift.grouping_ids_fit {
        return Ok(Transformed::no(plan.clone()));
    }
    let schema = Arc::clone(plan.schema());
    Ok(Transformed::yes(if carried.schema() == &schema {
        carried
    } else {
        // The scalar result stays one column until native decorrelation consumes
        // its correlation keys as join inputs.
        LogicalPlan::Projection(Projection::new_from_schema(Arc::new(carried), schema))
    }))
}

/// The cast, its column, and the outer expression of `CAST(column) = outer`.
type CastEquality<'a> = (&'a Cast, &'a Column, &'a Expr);

/// Return the cast equality that a filter below an aggregate hands to that aggregate.
fn lifted_cast<'a>(expression: &'a Expr, pinned: &HashSet<String>) -> Option<CastEquality<'a>> {
    let (Expr::Cast(cast), outer) = native_equality(expression)? else {
        return None;
    };
    let Expr::Column(column) = cast.expr.as_ref() else {
        return None;
    };
    (!pinned.contains(&column.name)).then_some((cast, column, outer))
}

/// Split a filter's conjuncts into the cast equalities lifted to an aggregate above
/// it and the conjuncts it keeps, along with the outer correlations it keeps.
fn split_lifted<'a>(
    predicate: &'a Expr,
    aggregated: bool,
    pinned: &HashSet<String>,
) -> (Vec<CastEquality<'a>>, Vec<&'a Expr>, Vec<Expr>) {
    let (mut lifted, mut retained) = (Vec::new(), Vec::new());
    for expression in split_conjunction(predicate) {
        match lifted_cast(expression, pinned).filter(|_| aggregated) {
            Some(equality) => lifted.push(equality),
            None => retained.push(expression),
        }
    }
    let correlations = retained
        .iter()
        .filter(|expression| expression.contains_outer())
        .map(|expression| strip_outer_reference((*expression).clone()))
        .collect();
    (lifted, retained, correlations)
}

/// Collect the correlations that stay in their filters after cast equalities are
/// lifted, and the outermost aggregate above each lifted equality. Return false
/// if native pull-up could not carry a lifted key to the subquery's output.
fn lifted_scope<'a>(
    plan: &'a LogicalPlan,
    ancestors: &mut Vec<&'a LogicalPlan>,
    pinned: &HashSet<String>,
    correlations: &mut Vec<Expr>,
    keyed: &mut Vec<&'a Aggregate>,
) -> bool {
    let mut supported = true;
    if let LogicalPlan::Filter(filter) = plan {
        let nearest = ancestors
            .iter()
            .rposition(|node| matches!(node, LogicalPlan::Aggregate(_)));
        let (lifted, _, retained) = split_lifted(&filter.predicate, nearest.is_some(), pinned);
        correlations.extend(retained);
        if let Some(nearest) = nearest.filter(|_| !lifted.is_empty()) {
            // Native pull-up adds the key to the nearest aggregate, but above
            // it rebuilds only projections, filters and aggregates; any other
            // node would keep a cached schema without the key.
            supported &= ancestors[..nearest].iter().all(|node| match node {
                LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => true,
                // It appends the key to every grouping set there, which can shift
                // the grouping-ID bits that GROUPING was resolved to read.
                LogicalPlan::Aggregate(aggregate) => !has_grouping_set(&aggregate.group_expr),
                _ => false,
            });
            keyed.extend(ancestors.iter().find_map(|node| match *node {
                LogicalPlan::Aggregate(aggregate) => Some(aggregate),
                _ => None,
            }));
        }
    }
    ancestors.push(plan);
    for input in plan.inputs() {
        supported &= lifted_scope(input, ancestors, pinned, correlations, keyed);
    }
    ancestors.pop();
    supported
}

/// A cast equality lifted from its filter to the first window or aggregate above it.
struct Pending {
    cast: Cast,
    column: Column,
    outer: Expr,
}

/// Keep correlation inputs available until native pull-up adds them to aggregation.
struct Lift<'a> {
    aliases: &'a AliasGenerator,
    reserved: &'a mut BTreeSet<String>,
    pinned: &'a HashSet<String>,
    /// Names of the keys computed for lifted cast equalities.
    hidden: HashSet<String>,
    /// Whether GROUPING or GROUPING_ID reads a grouping ID in the subquery.
    reads_grouping_id: bool,
    /// Whether the grouping ID of every grouping set given a key still fits.
    grouping_ids_fit: bool,
}

impl Lift<'_> {
    fn carry(
        &mut self,
        plan: LogicalPlan,
        aggregated: bool,
    ) -> Result<(LogicalPlan, Vec<Pending>, BTreeSet<Column>)> {
        let mut pending = Vec::new();
        let mut keys = BTreeSet::new();
        let below = aggregated || matches!(plan, LogicalPlan::Aggregate(_));
        let plan = plan
            .map_children(|input| {
                let (input, input_pending, input_keys) = self.carry(input, below)?;
                pending.extend(input_pending);
                keys.extend(input_keys);
                Ok(Transformed::yes(input))
            })?
            .data;
        let names = NamePreserver::new(&plan);
        let plan = match plan {
            LogicalPlan::Filter(filter) => {
                // Only an aggregate above can consume a lifted equality; filters
                // above every aggregate keep their predicates for native pull-up.
                let (lifted, retained, correlations) =
                    split_lifted(&filter.predicate, aggregated, self.pinned);
                keys.extend(collect_subquery_cols(&correlations, filter.input.schema())?);
                if lifted.is_empty() {
                    LogicalPlan::Filter(filter)
                } else {
                    pending.extend(lifted.into_iter().map(|(cast, column, outer)| Pending {
                        cast: cast.clone(),
                        column: column.clone(),
                        outer: outer.clone(),
                    }));
                    match conjunction(retained.into_iter().cloned()) {
                        Some(predicate) => {
                            LogicalPlan::Filter(Filter::try_new(predicate, filter.input)?)
                        }
                        None => Arc::unwrap_or_clone(filter.input),
                    }
                }
            }
            LogicalPlan::Projection(mut projection) => {
                let carried = keys
                    .iter()
                    .chain(pending.iter().map(|pending| &pending.column))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                for key in carried {
                    if !projection.schema.has_column(&key) {
                        projection.expr.push(Expr::Column(key));
                    }
                }
                LogicalPlan::Projection(projection)
            }
            LogicalPlan::Window(mut window) => {
                if !pending.is_empty() {
                    window.input = self.materialize(window.input, &mut pending, &mut keys)?;
                }
                // Removing the filter makes the window see all correlation groups.
                // Add their compared keys without replacing the user's partitions.
                window.window_expr = window
                    .window_expr
                    .into_iter()
                    .map(|expression| {
                        let name = names.save(&expression);
                        expression
                            .transform_up(|expression| {
                                let Expr::WindowFunction(mut function) = expression else {
                                    return Ok(Transformed::no(expression));
                                };
                                for key in &keys {
                                    let key = Expr::Column(key.clone());
                                    if !function.params.partition_by.contains(&key) {
                                        function.params.partition_by.push(key);
                                    }
                                }
                                Ok(Transformed::yes(Expr::WindowFunction(function)))
                            })
                            .map(|expression| name.restore(expression.data))
                    })
                    .collect::<Result<Vec<_>>>()?;
                LogicalPlan::Window(window)
            }
            LogicalPlan::SubqueryAlias(alias) => {
                keys = keys
                    .into_iter()
                    .map(|key| Column::new(Some(alias.alias.clone()), key.name))
                    .collect();
                for pending in &mut pending {
                    pending.column =
                        Column::new(Some(alias.alias.clone()), pending.column.name.clone());
                }
                LogicalPlan::SubqueryAlias(alias)
            }
            LogicalPlan::Aggregate(mut aggregate) => {
                if !pending.is_empty() {
                    aggregate.input = self.materialize(aggregate.input, &mut pending, &mut keys)?;
                }
                if has_grouping_set(&aggregate.group_expr) {
                    // A grouping set has no empty-input COUNT to compensate. Native
                    // pull-up appends each key to every set, after the first set's
                    // columns, shifting the grouping-ID bits that GROUPING reads for
                    // those columns. Prefixed keys leave every bit in place.
                    let grouped = grouping_set_to_exprlist(&aggregate.group_expr)?;
                    let mut group_expr = keys
                        .iter()
                        .filter(|key| self.hidden.contains(&key.name))
                        .map(|key| Expr::Column(key.clone()))
                        .filter(|key| !grouped.contains(&key))
                        .collect::<Vec<_>>();
                    if !group_expr.is_empty() {
                        // Native pull-up also groups by correlated columns from below an
                        // inner aggregate, which `keys` no longer holds there.
                        self.grouping_ids_fit &= !correlated_below_aggregate(&aggregate.input)?
                            && grouping_id_fits(&aggregate, &keys, self.reads_grouping_id);
                        group_expr.append(&mut aggregate.group_expr);
                        aggregate.group_expr = group_expr;
                    }
                }
                // Leave the original grouping intact: native pull-up must still see
                // an ungrouped aggregate to compensate COUNT on an empty input.
                keys.clear();
                LogicalPlan::Aggregate(aggregate)
            }
            plan => plan,
        };
        // Window, Unnest, joins and aliases cache their output schema. Native
        // pull-up otherwise retains the old schema after adding a hidden key below.
        Ok((plan.recompute_schema()?, pending, keys))
    }

    /// Compute lifted cast keys just below the first window or aggregate above their
    /// filter, after joins and generators below it have removed their rows. Spark may
    /// cast rows an inner join removes, depending on which filters its optimizer
    /// infers; casting only the rows that reach the producer keeps errors consistent.
    fn materialize(
        &mut self,
        input: Arc<LogicalPlan>,
        pending: &mut Vec<Pending>,
        keys: &mut BTreeSet<Column>,
    ) -> Result<Arc<LogicalPlan>> {
        // Keep every original field; native pull-up carries the hidden key to its aggregate.
        let mut expressions = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let mut computed: Vec<(Expr, Column)> = Vec::new();
        let mut predicates = Vec::new();
        for Pending {
            cast,
            column,
            outer,
        } in pending.drain(..)
        {
            let data_type = cast.field.data_type().clone();
            let key = Expr::Cast(Cast {
                expr: Box::new(Expr::Column(column)),
                ..cast
            });
            let column = if let Some((_, column)) = computed.iter().find(|(expr, _)| expr == &key) {
                column.clone()
            } else {
                let name = loop {
                    let name = self.aliases.next("__sail_correlation_key");
                    if self.reserved.insert(name.clone()) {
                        break name;
                    }
                };
                let column = Column::from_name(name);
                expressions.push(key.clone().alias(column.name.clone()));
                computed.push((key, column.clone()));
                self.hidden.insert(column.name.clone());
                column
            };
            // Compare through a same-type cast, as native compares `CAST(column)`.
            // A column pair would let filter pushdown infer outer predicates onto
            // the key and evaluate the cast on rows that local predicates exclude.
            let compared = Expr::Cast(Cast::new(Box::new(Expr::Column(column.clone())), data_type));
            predicates.push(compared.eq(outer));
            keys.insert(column);
        }
        // Native pull-up retains local filters below aggregation. Evaluate casts
        // after those filters so excluded rows cannot introduce cast failures.
        let input = LogicalPlan::Projection(Projection::try_new(expressions, input)?);
        Ok(Arc::new(match conjunction(predicates) {
            Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?),
            None => input,
        }))
    }
}

fn has_grouping_set(group_expr: &[Expr]) -> bool {
    group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
}

fn correlated_below_aggregate(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| match node {
        LogicalPlan::Aggregate(aggregate) => aggregate
            .input
            .exists(|node| Ok(node.contains_outer_reference())),
        _ => Ok(false),
    })
}

/// Whether native pull-up can also group a grouping set by the correlation keys
/// within the 64 bits DataFusion supports for its grouping ID. An ID that GROUPING
/// or GROUPING_ID reads must also keep its type, which GROUPING literals were
/// resolved with, and a clear top bit, since GROUPING_ID casts it to BIGINT.
fn grouping_id_fits(aggregate: &Aggregate, keys: &BTreeSet<Column>, read: bool) -> bool {
    let mut prospective = aggregate.group_expr.clone();
    prospective.extend(keys.iter().cloned().map(Expr::Column));
    let Ok(expanded) = enumerate_grouping_sets(prospective) else {
        return false;
    };
    let (Ok(groups), [Expr::GroupingSet(GroupingSet::GroupingSets(sets))]) =
        (grouping_set_to_exprlist(&expanded), expanded.as_slice())
    else {
        return false;
    };
    // The physical plan numbers duplicate sets by which expressions they group,
    // regardless of order, and packs that ordinal above the grouping bits.
    let mut duplicates = HashMap::new();
    for set in sets {
        let mask = groups
            .iter()
            .map(|expr| set.contains(expr))
            .collect::<Vec<_>>();
        *duplicates.entry(mask).or_insert(0_usize) += 1;
    }
    let ordinal = duplicates.into_values().max().unwrap_or(1) - 1;
    let bits = groups.len() + (usize::BITS - ordinal.leading_zeros()) as usize;
    if !read {
        // DataFusion shifts the ordinal past the grouping bits, which overflows in
        // debug builds when there are exactly 64 of them.
        return groups.len() < 64 && bits <= 64;
    }
    bits < 64
        && aggregate
            .schema
            .field_with_unqualified_name(Aggregate::INTERNAL_GROUPING_ID)
            .is_ok_and(|field| {
                Aggregate::grouping_id_type(groups.len(), ordinal) == *field.data_type()
            })
}
