use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Cast, GroupingSet};
use datafusion_expr::expr_rewriter::{NamePreserver, strip_outer_reference};
use datafusion_expr::logical_plan::{Aggregate, Filter, Projection};
use datafusion_expr::utils::{
    collect_subquery_cols, conjunction, enumerate_grouping_sets, grouping_set_to_exprlist,
    split_conjunction,
};
use datafusion_expr::{
    Expr, ExprSchemable, JoinType, LogicalPlan, Operator, ScalarUDF, Volatility, bitwise_and,
};
use projection::project_with_keys;
use sail_function::scalar::normalize_float_keys::NormalizeFloatKeys;

mod projection;

const CORRELATION_KEY_PREFIX: &str = "__sail_correlation_key";

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
        if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Filter(_))
            || !expressions_exist(&plan, |expression| match expression {
                Expr::ScalarSubquery(query) => !query.outer_ref_columns.is_empty(),
                _ => false,
            })?
        {
            return Ok(Transformed::no(plan));
        }
        let names = NamePreserver::new(&plan);
        let mut reserved = BTreeSet::new();
        reserve_key_names(&mut reserved, plan.schema());
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

/// Whether any expression of the node, at any depth, matches, without cloning them.
fn expressions_exist(plan: &LogicalPlan, mut matches: impl FnMut(&Expr) -> bool) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expression| {
        found = expression.exists(|expression| Ok(matches(expression)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// Reserve the field names that a generated key name could collide with.
fn reserve_key_names(reserved: &mut BTreeSet<String>, schema: &DFSchema) {
    reserved.extend(
        schema
            .fields()
            .iter()
            .map(|field| field.name())
            .filter(|name| name.starts_with(CORRELATION_KEY_PREFIX))
            .cloned(),
    );
}

/// Whether grouping the input is equivalent to grouping the cast result.
/// Spark's up-casts include lossy integer-to-float conversions, so they are not
/// sufficient here: distinct inputs must not collide, nor equal inputs split.
fn injective_cast(from: &DataType, to: &DataType) -> bool {
    // Decimal digits needed by every value of each signed integer type.
    let digits = |data_type: &DataType| match data_type {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(19),
        _ => None,
    };
    let width = |precision: &u8, scale: &i8| i16::from(*precision) - i16::from(*scale);
    match (from, to) {
        _ if from == to => true,
        (DataType::Null, _) => true,
        (_, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            digits(from).is_some()
                || matches!(
                    from,
                    DataType::Decimal128(..)
                        | DataType::Boolean
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                )
        }
        (DataType::Boolean, _) => digits(to).is_some(),
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            width(p2, s2) >= width(p1, s1) && s2 >= s1
        }
        (DataType::Decimal128(precision, scale), _) => {
            *scale == 0
                && match to {
                    DataType::Int8 => *precision <= 2,
                    DataType::Int16 => *precision <= 4,
                    DataType::Int32 => *precision <= 9,
                    DataType::Int64 => *precision <= 18,
                    _ => false,
                }
        }
        (_, DataType::Decimal128(precision, scale)) => {
            digits(from).is_some_and(|needed| *scale >= 0 && width(precision, scale) >= needed)
        }
        (DataType::Int8 | DataType::Int16, DataType::Float32)
        | (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Float64) => true,
        _ => digits(from)
            .zip(digits(to))
            .is_some_and(|(needed, wider)| wider > needed),
    }
}

fn normalize_keys(
    plan: &LogicalPlan,
    aliases: &AliasGenerator,
    reserved: &mut BTreeSet<String>,
) -> Result<Transformed<LogicalPlan>> {
    let mut has_aggregate = false;
    let mut has_cast = false;
    let mut has_window = false;
    let mut has_grouping_sets = false;
    let mut has_column = false;
    // Most correlated subqueries need no key; find out before the full scan below.
    plan.apply(|node| {
        reserve_key_names(reserved, node.schema());
        has_aggregate |= matches!(node, LogicalPlan::Aggregate(_));
        has_window |= matches!(node, LogicalPlan::Window(_));
        has_grouping_sets |= matches!(node, LogicalPlan::Aggregate(aggregate) if has_grouping_set(&aggregate.group_expr));
        if let LogicalPlan::Filter(filter) = node {
            for expression in split_conjunction(&filter.predicate) {
                match native_equality(expression).filter(|_| expression.contains_outer()) {
                    Some((Expr::Column(_), _)) => has_column = true,
                    Some(_) => has_cast = true,
                    None => {}
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !has_aggregate || !(has_cast || has_window || (has_grouping_sets && has_column)) {
        return Ok(Transformed::no(plan.clone()));
    }
    let mut eligible = true;
    let mut reads_grouping_id = false;
    // An inner column compared to an outer value already selects one group per
    // outer row, so casts of that column need no separate key.
    let mut pinned = HashSet::new();
    let (mut grouping_columns, mut compared) = (HashSet::new(), HashSet::new());
    let mut always_grouped: Option<HashSet<Column>> = None;
    plan.apply(|node| {
        reads_grouping_id |= expressions_exist(node, is_grouping_id)?;
        if let LogicalPlan::Aggregate(aggregate) = node
            && has_grouping_set(&aggregate.group_expr)
        {
            // Native pull-up adds no grouping for a column that every set of every
            // grouping-set aggregate already groups.
            let (columns, always) = grouping_set_columns(&aggregate.group_expr)?;
            grouping_columns.extend(columns);
            always_grouped = Some(match always_grouped.take() {
                Some(previous) => previous.intersection(&always).cloned().collect(),
                None => always,
            });
        }
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
        eligible &= !expressions_exist(node, |part| match part {
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
        })?;
        if let LogicalPlan::Filter(filter) = node {
            for expression in split_conjunction(&filter.predicate)
                .into_iter()
                .filter(|expression| expression.contains_outer())
            {
                match native_equality(expression) {
                    Some((Expr::Column(column), _)) => {
                        compared.insert(column.clone());
                        // Floating equality merges values whose casts differ, including
                        // floating leaves of arrays and structs.
                        let floating = filter
                            .input
                            .schema()
                            .qualified_field_from_column(column)
                            .is_ok_and(|(_, field)| {
                                NormalizeFloatKeys::has_float(field.data_type())
                            });
                        if !floating {
                            pinned.insert(column.name.clone());
                        }
                    }
                    Some(_) => {}
                    None => eligible = false,
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !eligible {
        return Ok(Transformed::no(plan.clone()));
    }
    // Native pull-up would also group every grouping set by a grouping column compared
    // with the outer query, including sets that omit it, which GROUPING reads. Give it
    // a prefixed key instead, as for a window domain.
    let regrouped = if reads_grouping_id {
        compared
            .iter()
            .filter(|column| {
                grouping_columns.contains(*column)
                    && !always_grouped
                        .as_ref()
                        .is_some_and(|always| always.contains(*column))
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    for column in &regrouped {
        pinned.remove(&column.name);
    }

    // Preserve grouped producers when replacing a cast would violate native's
    // requirement that every original grouping column occurs in a correlation.
    // Like DataFusion's `check_aggregation_in_scalar_subquery`, this checks only
    // the outermost aggregate; aggregates below it may group by anything.
    let (mut correlations, mut keyed) = (Vec::new(), Vec::new());
    let mut domains = BTreeSet::new();
    // Discover the complete set before validating retained predicates: a deeper
    // window may require lifting the same column from an earlier filter too.
    let windowed_correlation = collect_window_domains(plan, false, false, &mut domains);
    domains.extend(regrouped.iter().cloned());
    if !lifted_scope(
        plan,
        &mut Vec::new(),
        &pinned,
        &mut correlations,
        &mut keyed,
        &domains,
    ) {
        return Ok(Transformed::no(plan.clone()));
    }
    // Without a cast, carry correlations only to windows over non-floating keys: Sail's
    // equality keeps NaN payloads apart, so floating partitions or peers would split them.
    if !has_cast
        && regrouped.is_empty()
        && (!windowed_correlation || !domains.is_empty() || has_floating_window_key(plan)?)
    {
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
        domains: &domains,
        hidden: HashSet::new(),
        reads_grouping_id,
        grouping_ids_fit: true,
        open_mask: None,
        masked: false,
        grouping_set_ancestors: 0,
    };
    // Even without a lifted key, carrying the correlated columns partitions windows
    // and refreshes cached schemas for native pull-up.
    let (carried, pending, _) = lift.carry(plan.clone(), false)?;
    // Every lifted equality has an aggregate above it, which consumes its key.
    debug_assert!(pending.is_empty());
    // Native pull-up cannot decorrelate a masked grouping ID compared with the outer query.
    if carried == *plan
        || !lift.grouping_ids_fit
        || (lift.masked
            && carried.exists(|node| {
                Ok(node.contains_outer_reference() && expressions_exist(node, is_grouping_id)?)
            })?)
    {
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

/// The optional cast, inner column and outer comparison value.
type KeyEquality<'a> = (Option<&'a Cast>, &'a Column, &'a Expr);

/// Return the cast equality that a filter below an aggregate hands to that aggregate.
fn lifted_key<'a>(
    expression: &'a Expr,
    pinned: &HashSet<String>,
    domains: &BTreeSet<Column>,
) -> Option<KeyEquality<'a>> {
    let (inner, outer) = native_equality(expression)?;
    match inner {
        Expr::Column(column) if domains.contains(column) => Some((None, column, outer)),
        Expr::Cast(cast) => {
            let Expr::Column(column) = cast.expr.as_ref() else {
                return None;
            };
            (!pinned.contains(&column.name)).then_some((Some(cast), column, outer))
        }
        _ => None,
    }
}

/// Split a filter's conjuncts into the cast equalities lifted to an aggregate above
/// it and the conjuncts it keeps, along with the outer correlations it keeps.
fn split_lifted<'a>(
    predicate: &'a Expr,
    aggregated: bool,
    pinned: &HashSet<String>,
    domains: &BTreeSet<Column>,
) -> (Vec<KeyEquality<'a>>, Vec<&'a Expr>, Vec<Expr>) {
    let (mut lifted, mut retained) = (Vec::new(), Vec::new());
    for expression in split_conjunction(predicate) {
        match lifted_key(expression, pinned, domains).filter(|_| aggregated) {
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

/// Merge -0.0 and 0.0 in a floating window key, including floating leaves of arrays and structs.
fn normalize_window_key(key: &mut Expr, schema: &DFSchema) -> Result<()> {
    if NormalizeFloatKeys::has_float(&key.get_type(schema)?)
        && !matches!(key, Expr::ScalarFunction(function) if function.func.inner().is::<NormalizeFloatKeys>())
    {
        *key = ScalarUDF::from(NormalizeFloatKeys::new()).call(vec![key.clone()]);
    }
    Ok(())
}

/// Find the floating domains whose filters reach a window before an aggregate.
fn collect_window_domains(
    plan: &LogicalPlan,
    aggregated: bool,
    windowed: bool,
    domains: &mut BTreeSet<Column>,
) -> bool {
    let (aggregated, windowed) = match plan {
        LogicalPlan::Aggregate(_) => (true, false),
        LogicalPlan::Window(_) => (aggregated, aggregated || windowed),
        _ => (aggregated, windowed),
    };
    let mut found = false;
    if windowed && let LogicalPlan::Filter(filter) = plan {
        found = filter.predicate.contains_outer();
        for expression in split_conjunction(&filter.predicate) {
            if let Some((Expr::Column(column), _)) = native_equality(expression)
                && filter
                    .input
                    .schema()
                    .qualified_field_from_column(column)
                    .is_ok_and(|(_, field)| NormalizeFloatKeys::has_float(field.data_type()))
            {
                domains.insert(column.clone());
            }
        }
    }
    for input in plan.inputs() {
        found |= collect_window_domains(input, aggregated, windowed, domains);
    }
    found
}

/// Whether a window partitions or orders by a floating value.
fn has_floating_window_key(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        let LogicalPlan::Window(window) = node else {
            return Ok(false);
        };
        let schema = window.input.schema();
        window
            .window_expr
            .iter()
            .try_fold(false, |found, expression| {
                Ok(found
                    || expression.exists(|part| {
                        let Expr::WindowFunction(function) = part else {
                            return Ok(false);
                        };
                        Ok(function
                            .params
                            .partition_by
                            .iter()
                            .chain(function.params.order_by.iter().map(|sort| &sort.expr))
                            .any(|key| {
                                key.get_type(schema).map_or(true, |data_type| {
                                    NormalizeFloatKeys::has_float(&data_type)
                                })
                            }))
                    })?)
            })
    })
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
    domains: &BTreeSet<Column>,
) -> bool {
    let mut supported = true;
    if let LogicalPlan::Filter(filter) = plan {
        let nearest = ancestors
            .iter()
            .rposition(|node| matches!(node, LogicalPlan::Aggregate(_)));
        let (lifted, _, retained) =
            split_lifted(&filter.predicate, nearest.is_some(), pinned, domains);
        correlations.extend(retained);
        if let Some(nearest) = nearest.filter(|_| !lifted.is_empty()) {
            // Native pull-up adds the key to the nearest aggregate, but above
            // it rebuilds only projections, filters and aggregates; any other
            // node would keep a cached schema without the key.
            // A grouping set there is given the carried key first, as below, so native
            // pull-up need not append it and shift its grouping-ID bits. An ungrouped
            // aggregate between them leaves the key for native pull-up to append.
            let mut plain = matches!(
                ancestors[nearest],
                LogicalPlan::Aggregate(aggregate) if aggregate.group_expr.is_empty()
            );
            supported &= ancestors[..nearest].iter().rev().all(|node| match node {
                LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => true,
                LogicalPlan::Aggregate(aggregate) => {
                    let grouping = has_grouping_set(&aggregate.group_expr);
                    let carried = !(grouping && plain);
                    plain |= aggregate.group_expr.is_empty();
                    carried
                }
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
        supported &= lifted_scope(input, ancestors, pinned, correlations, keyed, domains);
    }
    ancestors.pop();
    supported
}

/// A cast equality lifted from its filter to the first window or aggregate above it.
struct Pending {
    cast: Option<Cast>,
    column: Column,
    outer: Expr,
}

/// Keep correlation inputs available until native pull-up adds them to aggregation.
struct Lift<'a> {
    aliases: &'a AliasGenerator,
    reserved: &'a mut BTreeSet<String>,
    pinned: &'a HashSet<String>,
    domains: &'a BTreeSet<Column>,
    /// Names of the keys computed for lifted cast equalities.
    hidden: HashSet<String>,
    /// Whether GROUPING or GROUPING_ID reads a grouping ID in the subquery.
    reads_grouping_id: bool,
    /// Whether the grouping ID of every grouping set given a key still fits.
    grouping_ids_fit: bool,
    /// The user's grouping-bit count and grouping-ID type of the aggregate below the
    /// current node whose duplicate sets were given a key, until another aggregate.
    open_mask: Option<(usize, DataType)>,
    /// Whether any grouping-ID read was masked.
    masked: bool,
    /// How many grouping-set aggregates are above the current node.
    grouping_set_ancestors: usize,
}

impl Lift<'_> {
    fn carry(
        &mut self,
        plan: LogicalPlan,
        aggregated: bool,
    ) -> Result<(LogicalPlan, Vec<Pending>, BTreeSet<Column>)> {
        let mut pending = Vec::new();
        let mut keys = BTreeSet::new();
        // Rebuilding a schema looks up every expression's field, so only rebuild
        // the nodes whose inputs or expressions change.
        let mut changed = false;
        let below = aggregated || matches!(plan, LogicalPlan::Aggregate(_));
        let grouping = matches!(&plan, LogicalPlan::Aggregate(aggregate) if has_grouping_set(&aggregate.group_expr));
        self.grouping_set_ancestors += usize::from(grouping);
        let mut masks = Vec::new();
        let plan = plan
            .map_children(|input| {
                let schema = Arc::clone(input.schema());
                let (input, input_pending, input_keys) = self.carry(input, below)?;
                changed |= &schema != input.schema();
                pending.extend(input_pending);
                keys.extend(input_keys);
                masks.extend(self.open_mask.take());
                Ok(Transformed::yes(input))
            })?
            .data;
        self.grouping_set_ancestors -= usize::from(grouping);
        // Spark's GROUPING_ID holds only the user's grouping bits. A read above the
        // aggregate that was given keys, up to the next aggregate, sees its grouping ID.
        let open = match masks.len() {
            0 => None,
            1 => masks.pop(),
            _ => {
                self.grouping_ids_fit = false;
                None
            }
        };
        let plan = match &open {
            Some((bits, data_type)) => mask_grouping_id(plan, *bits, data_type)?,
            None => plan,
        };
        let mut opened = None;
        let names = NamePreserver::new(&plan);
        let plan = match plan {
            LogicalPlan::Filter(filter) => {
                // Only an aggregate above can consume a lifted equality; filters
                // above every aggregate keep their predicates for native pull-up.
                let (lifted, retained, correlations) =
                    split_lifted(&filter.predicate, aggregated, self.pinned, self.domains);
                keys.extend(collect_subquery_cols(&correlations, filter.input.schema())?);
                if lifted.is_empty() {
                    LogicalPlan::Filter(filter)
                } else {
                    pending.extend(lifted.into_iter().map(|(cast, column, outer)| Pending {
                        cast: cast.cloned(),
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
                        changed = true;
                    }
                }
                LogicalPlan::Projection(projection)
            }
            LogicalPlan::Window(mut window) => {
                if !pending.is_empty() {
                    window.input = self.materialize(window.input, &mut pending, &mut keys, true)?;
                }
                changed |= !keys.is_empty();
                let schema = Arc::clone(window.input.schema());
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
                                // Like the appended keys, the user's floating partitions and
                                // ordering peers merge -0.0 and 0.0.
                                if !keys.is_empty() {
                                    for key in function.params.partition_by.iter_mut().chain(
                                        function
                                            .params
                                            .order_by
                                            .iter_mut()
                                            .map(|sort| &mut sort.expr),
                                    ) {
                                        normalize_window_key(key, &schema)?;
                                    }
                                }
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
                    aggregate.input =
                        self.materialize(aggregate.input, &mut pending, &mut keys, false)?;
                    changed = true;
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
                    let bits = grouped.len();
                    if !group_expr.is_empty() {
                        // Native pull-up also groups by correlated columns from below an
                        // inner aggregate, which `keys` no longer holds there.
                        let carried = keys
                            .iter()
                            .filter(|key| self.hidden.contains(&key.name))
                            .map(|key| key.name.as_str())
                            .collect::<HashSet<_>>();
                        let ordinal = if correlated_below_aggregate(&aggregate.input, &carried)? {
                            None
                        } else {
                            grouping_id_fits(&aggregate, &keys, self.reads_grouping_id)
                        };
                        self.grouping_ids_fit &= ordinal.is_some();
                        // Keys add grouping bits below the ordinal of the user's duplicate
                        // sets, which a whole grouping-ID read would then see. Native pull-up
                        // also regroups by raw correlated columns, so those keep its layout.
                        if self.reads_grouping_id
                            && ordinal.is_some()
                            && keys.iter().all(|key| self.hidden.contains(&key.name))
                            && grouping_id_fits(&aggregate, &BTreeSet::new(), false)
                                .is_some_and(|ordinal| ordinal > 0)
                        {
                            if let Ok(field) = aggregate
                                .schema
                                .field_with_unqualified_name(Aggregate::INTERNAL_GROUPING_ID)
                            {
                                opened = Some((bits, field.data_type().clone()));
                                self.masked = true;
                            }
                        }
                        group_expr.append(&mut aggregate.group_expr);
                        aggregate.group_expr = group_expr;
                        changed = true;
                    }
                }
                // Leave the original grouping intact: native pull-up must still see
                // an ungrouped aggregate to compensate COUNT on an empty input. Below
                // a grouping set, a grouped aggregate keeps its keys for that one to
                // prefix; a plain one groups by them too, as native pull-up would.
                let carries = self.grouping_set_ancestors > 0
                    && (grouping || !aggregate.group_expr.is_empty());
                if carries && !grouping {
                    let missing = keys
                        .iter()
                        .filter(|key| self.hidden.contains(&key.name))
                        .map(|key| Expr::Column(key.clone()))
                        .filter(|key| !aggregate.group_expr.contains(key))
                        .collect::<Vec<_>>();
                    if !missing.is_empty() {
                        aggregate.group_expr.splice(0..0, missing);
                        changed = true;
                    }
                }
                if carries {
                    keys.retain(|key| self.hidden.contains(&key.name));
                } else {
                    keys.clear();
                }
                LogicalPlan::Aggregate(aggregate)
            }
            plan => plan,
        };
        self.open_mask = if matches!(plan, LogicalPlan::Aggregate(_)) {
            opened
        } else {
            open
        };
        // Window, Unnest, joins and aliases cache their output schema. Native
        // pull-up otherwise retains the old schema after adding a hidden key below.
        let plan = if changed {
            plan.recompute_schema()?
        } else {
            plan
        };
        Ok((plan, pending, keys))
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
        windowed: bool,
    ) -> Result<Arc<LogicalPlan>> {
        let mut expressions = Vec::new();
        let mut computed: Vec<(Expr, Column)> = Vec::new();
        let mut predicates = Vec::new();
        for Pending {
            cast,
            column,
            outer,
        } in pending.drain(..)
        {
            let data_type = match &cast {
                Some(cast) => cast.field.data_type().clone(),
                None => input
                    .schema()
                    .qualified_field_from_column(&column)?
                    .1
                    .data_type()
                    .clone(),
            };
            // Window partitions, aggregate groups and the outer join must use the
            // same floating equality classes; keep raw payloads in their own fields.
            let canonical =
                (cast.is_none() || windowed) && NormalizeFloatKeys::has_float(&data_type);
            let outer = if canonical {
                ScalarUDF::from(NormalizeFloatKeys::new()).call(vec![outer])
            } else {
                outer
            };
            // Like Spark, group by the input of an injective cast rather than
            // casting every row; the comparison below applies the cast instead.
            let key = if !canonical
                && input
                    .schema()
                    .qualified_field_from_column(&column)
                    .is_ok_and(|(_, field)| injective_cast(field.data_type(), &data_type))
            {
                Expr::Column(column)
            } else {
                let value = match cast {
                    Some(cast) => Expr::Cast(Cast {
                        expr: Box::new(Expr::Column(column)),
                        ..cast
                    }),
                    None => Expr::Column(column),
                };
                if canonical {
                    ScalarUDF::from(NormalizeFloatKeys::new()).call(vec![value])
                } else {
                    value
                }
            };
            let column = if let Some((_, column)) = computed.iter().find(|(expr, _)| expr == &key) {
                column.clone()
            } else {
                let name = loop {
                    let name = self.aliases.next(CORRELATION_KEY_PREFIX);
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
            // Compare through a same-type cast, as native compares `CAST(column)`,
            // or through the cast itself for an injective key's raw input.
            // A column pair would let filter pushdown infer outer predicates onto
            // the key and evaluate the cast on rows that local predicates exclude.
            let compared = Expr::Cast(Cast::new(Box::new(Expr::Column(column.clone())), data_type));
            predicates.push(compared.eq(outer));
            keys.insert(column);
        }
        // Native pull-up retains local filters below aggregation. Evaluate casts
        // after those filters so excluded rows cannot introduce cast failures.
        let input = LogicalPlan::Projection(project_with_keys(input, expressions)?);
        Ok(Arc::new(match conjunction(predicates) {
            Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?),
            None => input,
        }))
    }
}

/// The columns that grouping expressions read, and the grouping columns of every set.
fn grouping_set_columns(group_expr: &[Expr]) -> Result<(Vec<Column>, HashSet<Column>)> {
    let expanded = enumerate_grouping_sets(group_expr.to_vec())?;
    let groups = grouping_set_to_exprlist(&expanded)?;
    let columns = groups
        .iter()
        .flat_map(|group| group.column_refs())
        .cloned()
        .collect();
    // Some rollup or cube set omits every expression.
    let always = match expanded.as_slice() {
        [Expr::GroupingSet(GroupingSet::GroupingSets(sets))] => groups
            .iter()
            .filter(|group| sets.iter().all(|set| set.contains(group)))
            .filter_map(|group| match group {
                Expr::Column(column) => Some(column.clone()),
                _ => None,
            })
            .collect(),
        _ => HashSet::new(),
    };
    Ok((columns, always))
}

fn has_grouping_set(group_expr: &[Expr]) -> bool {
    group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
}

/// Whether native pull-up would group by a correlated column from below an inner
/// aggregate other than the carried keys, which the aggregate already groups by.
fn correlated_below_aggregate(plan: &LogicalPlan, carried: &HashSet<&str>) -> Result<bool> {
    plan.exists(|node| match node {
        LogicalPlan::Aggregate(aggregate) => aggregate.input.exists(|node| {
            Ok(match node {
                LogicalPlan::Filter(filter) => split_conjunction(&filter.predicate)
                    .into_iter()
                    .filter(|expression| expression.contains_outer())
                    .flat_map(Expr::column_refs)
                    .any(|column| !carried.contains(column.name.as_str())),
                node => node.contains_outer_reference(),
            })
        }),
        _ => Ok(false),
    })
}

/// Whether native pull-up can also group a grouping set by the correlation keys
/// within the 64 bits DataFusion supports for its grouping ID. An ID that GROUPING
/// or GROUPING_ID reads must also keep its type, which GROUPING literals were
/// resolved with, and a clear top bit, since GROUPING_ID casts it to BIGINT.
/// Returns the largest duplicate-set ordinal if it fits.
fn grouping_id_fits(aggregate: &Aggregate, keys: &BTreeSet<Column>, read: bool) -> Option<usize> {
    let mut prospective = aggregate.group_expr.clone();
    prospective.extend(keys.iter().cloned().map(Expr::Column));
    let Ok(expanded) = enumerate_grouping_sets(prospective) else {
        return None;
    };
    let (Ok(groups), [Expr::GroupingSet(GroupingSet::GroupingSets(sets))]) =
        (grouping_set_to_exprlist(&expanded), expanded.as_slice())
    else {
        return None;
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
        return (groups.len() < 64 && bits <= 64).then_some(ordinal);
    }
    (bits < 64
        && aggregate
            .schema
            .field_with_unqualified_name(Aggregate::INTERNAL_GROUPING_ID)
            .is_ok_and(|field| {
                Aggregate::grouping_id_type(groups.len(), ordinal) == *field.data_type()
            }))
    .then_some(ordinal)
}

fn is_grouping_id(expression: &Expr) -> bool {
    matches!(expression, Expr::Column(column) if column.name == Aggregate::INTERNAL_GROUPING_ID)
}

/// Keep only the user's grouping bits of every whole grouping-ID read in a node, dropping
/// the key bits and the duplicate-set ordinal above them. GROUPING already reads one bit.
fn mask_grouping_id(plan: LogicalPlan, bits: usize, data_type: &DataType) -> Result<LogicalPlan> {
    let mask = Expr::Literal(
        ScalarValue::UInt64(Some((1_u64 << bits) - 1)).cast_to(data_type)?,
        None,
    );
    let names = NamePreserver::new(&plan);
    Ok(plan
        .map_expressions(|expression| {
            let name = names.save(&expression);
            Ok(expression
                .transform_down(|expression| {
                    Ok(match expression {
                        Expr::BinaryExpr(BinaryExpr {
                            ref left,
                            op: Operator::BitwiseAnd,
                            ref right,
                        }) if is_grouping_id(left)
                            && matches!(right.as_ref(), Expr::Literal(..)) =>
                        {
                            Transformed::new(expression, false, TreeNodeRecursion::Jump)
                        }
                        expression if is_grouping_id(&expression) => Transformed::new(
                            bitwise_and(expression, mask.clone()),
                            true,
                            TreeNodeRecursion::Jump,
                        ),
                        expression => Transformed::no(expression),
                    })
                })?
                .update_data(|expression| name.restore(expression)))
        })?
        .data)
}
