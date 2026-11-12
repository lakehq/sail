use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::SessionState;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::{OptimizerContext, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, TableReference};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    Distinct, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, SubqueryAlias,
    TableProviderFilterPushDown, TableScan, TableScanBuilder, TableSource,
};
use indexmap::IndexMap;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan_reusing_projection;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::seeded_random::SeededRandomNode;
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

/// A CTE definition whose references can share one evaluation of its body.
#[derive(Debug)]
pub struct CteDefinition {
    /// The position of the definition among the CTE definitions of the query.
    /// A definition only refers to definitions at earlier positions.
    index: usize,
    name: TableReference,
    schema: SchemaRef,
    /// The definition as it was resolved.
    resolved: Arc<LogicalPlan>,
    /// The definition once the query is resolved, where every reference to another
    /// shareable definition is a scan.
    plan: OnceLock<Arc<LogicalPlan>>,
}

impl CteDefinition {
    pub fn new(index: usize, name: TableReference, plan: Arc<LogicalPlan>) -> Self {
        Self {
            index,
            name,
            schema: Arc::clone(plan.schema().inner()),
            resolved: plan,
            plan: OnceLock::new(),
        }
    }

    /// Returns the definition as it was resolved.
    pub fn resolved(&self) -> &Arc<LogicalPlan> {
        &self.resolved
    }

    pub fn plan(&self) -> &Arc<LogicalPlan> {
        self.plan.get().unwrap_or(&self.resolved)
    }

    /// Sets the definition once the query is resolved.
    pub fn set_plan(&self, plan: LogicalPlan) {
        let _ = self.plan.set(Arc::new(plan));
    }

    /// Returns a new reference to the definition. The output has the names and the
    /// schema of the definition, so that the definition can replace the reference.
    pub fn scan(self: &Arc<Self>) -> Result<LogicalPlan> {
        let source = Arc::new(CteReference {
            definition: Arc::clone(self),
            body: None,
        });
        let mut scan = TableScanBuilder::new(self.name.clone(), source).build()?;
        scan.projected_schema = Arc::clone(self.resolved.schema());
        Ok(LogicalPlan::TableScan(scan))
    }
}

/// The source of a reference to a CTE definition.
///
/// The optimizer records what each reference reads in its scan: the columns, the
/// filters, and the number of rows. References that read the same rows share one
/// body, which applies the filters, and each of the other references is replaced by
/// the definition, as if it was never a scan.
#[derive(Debug)]
pub struct CteReference {
    definition: Arc<CteDefinition>,
    body: Option<Arc<SharedCteBody>>,
}

impl CteReference {
    pub fn try_from_scan(scan: &TableScan) -> Option<&Self> {
        scan.source.downcast_ref::<Self>()
    }

    pub fn definition(&self) -> &Arc<CteDefinition> {
        &self.definition
    }

    /// Returns the body that the reference shares, or `None` if the reference was
    /// not optimized together with the other references to its definition.
    pub fn body(&self) -> Option<&Arc<SharedCteBody>> {
        self.body.as_ref()
    }
}

impl TableSource for CteReference {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.definition.schema)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// The optimized body of a CTE definition that is shared by several references.
#[derive(Debug)]
pub struct SharedCteBody {
    name: TableReference,
    plan: LogicalPlan,
    /// The columns of the definition that the body computes, or `None` for all columns.
    columns: Option<Vec<usize>>,
}

impl SharedCteBody {
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns the name of the CTE.
    pub fn name(&self) -> &TableReference {
        &self.name
    }

    /// Returns the positions in the body output of the columns of the definition.
    pub fn project(&self, projection: Option<&[usize]>) -> Option<Vec<usize>> {
        match (&self.columns, projection) {
            (None, projection) => projection.map(|x| x.to_vec()),
            (Some(columns), None) => Some((0..columns.len()).collect()),
            (Some(columns), Some(projection)) => Some(
                projection
                    .iter()
                    .filter_map(|column| columns.binary_search(column).ok())
                    .collect(),
            ),
        }
    }
}

/// Analyzes a resolved plan in which the references to shareable CTE definitions are
/// scans, for the schema of the plan. The references are replaced by their definitions
/// if the analysis changes the schema of a definition.
pub fn analyze_shared_ctes(
    plan: LogicalPlan,
    analyze: &mut dyn FnMut(LogicalPlan) -> Result<LogicalPlan>,
) -> Result<LogicalPlan> {
    let mut definitions: IndexMap<usize, Arc<CteDefinition>> = IndexMap::new();
    fn collect(
        plan: &LogicalPlan,
        definitions: &mut IndexMap<usize, Arc<CteDefinition>>,
    ) -> Result<()> {
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(reference) = CteReference::try_from_scan(scan)
                && !definitions.contains_key(&reference.definition.index)
            {
                definitions.insert(
                    reference.definition.index,
                    Arc::clone(&reference.definition),
                );
                collect(reference.definition.plan(), definitions)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    collect(&plan, &mut definitions)?;
    if !definitions.is_empty() && reads_input_layout(&plan)? {
        return analyze(inline_references(plan, &|_| true)?);
    }
    for definition in definitions.values() {
        let stable = analyze(definition.plan().as_ref().clone())
            .is_ok_and(|analyzed| analyzed.schema().inner() == &definition.schema);
        if !stable {
            return analyze(inline_references(plan, &|_| true)?);
        }
    }
    analyze(plan)
}

/// Returns the session state for optimizing the query again, after a CTE reference is
/// replaced by its definition, so that the optimizer generates the aliases that it
/// generates for the query with its CTEs inlined.
pub fn session_state_for_restart(state: &SessionState) -> SessionState {
    let mut state = state.clone();
    state.execution_props_mut().alias_generator = Arc::new(AliasGenerator::new());
    state
}

/// Returns whether the predicate holds for a node of the plan or of the CTE definitions
/// that the plan refers to, as if each reference was its definition.
pub fn exists_in_plan_or_cte_definitions(
    plan: &LogicalPlan,
    predicate: &mut dyn FnMut(&LogicalPlan) -> Result<bool>,
) -> Result<bool> {
    fn exists(
        plan: &LogicalPlan,
        predicate: &mut dyn FnMut(&LogicalPlan) -> Result<bool>,
        visited: &mut HashSet<usize>,
    ) -> Result<bool> {
        plan.exists(|plan| {
            if predicate(plan)? {
                return Ok(true);
            }
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(reference) = CteReference::try_from_scan(scan)
                && visited.insert(reference.definition.index)
            {
                return exists(&reference.definition.resolved, predicate, visited);
            }
            Ok(false)
        })
    }
    exists(plan, predicate, &mut HashSet::new())
}

/// The rows that a reference reads from its definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Demand {
    /// The filters in the order in which they are evaluated.
    filters: Vec<Expr>,
    /// The types of the literals in the filters, since literal equality ignores some
    /// type properties (e.g. the time zone of a timestamp).
    literal_types: Vec<DataType>,
}

impl Demand {
    /// Returns `None` if the reference reads a limited number of rows, or if a filter
    /// depends on more than the columns of the reference.
    fn try_new(scan: &TableScan) -> Option<Self> {
        if scan.fetch.is_some() {
            return None;
        }
        let mut literal_types = vec![];
        let mut closed = true;
        for filter in &scan.filters {
            filter
                .apply(|expr| {
                    match expr {
                        Expr::Literal(value, _) => literal_types.push(value.data_type()),
                        Expr::ScalarSubquery(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::OuterReferenceColumn(_, _) => {
                            closed = false;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        _ => {}
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .ok()?;
        }
        closed.then(|| Self {
            filters: scan.filters.clone(),
            literal_types,
        })
    }

    /// Returns the definition as the references with this demand read it.
    fn apply(&self, plan: LogicalPlan, columns: Option<&[usize]>) -> Result<LogicalPlan> {
        let mut builder = LogicalPlanBuilder::from(plan);
        if let Some(predicate) = conjunction(self.filters.iter().cloned()) {
            builder = builder.filter(predicate)?;
        }
        if let Some(columns) = columns {
            builder = builder.select(columns.iter().copied())?;
        }
        builder.build()
    }
}

/// Identifies a reference in the resolved query by the address of its source.
type Site = usize;

fn site(scan: &TableScan) -> Site {
    Arc::as_ptr(&scan.source).cast::<()>() as Site
}

struct Member {
    site: Site,
    /// `None` if the reference cannot share a body.
    demand: Option<Demand>,
    /// Whether the consumer of the reference ignores duplicate rows.
    distinct: bool,
    projection: Option<Vec<usize>>,
}

/// The references to each definition, by the position of the definition.
type Members = BTreeMap<usize, (Arc<CteDefinition>, Vec<Member>)>;

/// Optimizes a resolved plan in which the references to shareable CTE definitions
/// are scans, and decides for each reference whether it shares a body.
///
/// References share a body when the optimizer records the same filters for them, no
/// filter is left above them, and they are not read through a limit. The shared body
/// is optimized once under these filters, and computes the columns that any of the
/// references reads. A reference that shares with no other reference is replaced by
/// its definition in the resolved plan, which is then optimized again, so that the
/// plan of a query without shared bodies is the plan of the query with all its CTEs
/// inlined. The same holds for a query that the optimizer fails on with references
/// in it.
///
/// The `optimize` function receives the plan and whether it is the plan of the query.
pub fn optimize_shared_ctes(
    plan: LogicalPlan,
    optimize: &mut dyn FnMut(LogicalPlan, bool) -> Result<LogicalPlan>,
) -> Result<LogicalPlan> {
    if !contains_reference(&plan)? {
        return optimize(plan, true);
    }
    if reads_input_layout(&plan)? {
        return optimize(inline_references(plan, &|_| true)?, true);
    }
    let mut inlined = HashSet::new();
    loop {
        let root = inline_unshared_references(&plan, &mut inlined)?;
        let has_references = contains_reference(&root)?;
        let root = match optimize(root, true) {
            Ok(root) => root,
            // The query is optimized as if its CTEs were never shared. The optimizer
            // can fail on a query that it simplifies, and then accepts, when the CTEs are
            // in the place of their references (e.g. when a CTE is empty).
            Err(error) if has_references => {
                log::warn!("optimizing the query without sharing its CTEs: {error}");
                return optimize(inline_references(plan, &|_| true)?, true);
            }
            Err(error) => return Err(error),
        };
        match share_references(root, &inlined, optimize)? {
            Sharing::Shared(plan) => return Ok(plan),
            Sharing::Inline(sites) => inlined.extend(sites),
            // The optimizer fails on a shared body, as it can on the query above.
            Sharing::Failed(error) => {
                log::warn!("optimizing the query without sharing its CTEs: {error}");
                return optimize(inline_references(plan, &|_| true)?, true);
            }
        }
    }
}

fn contains_reference(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::TableScan(scan) = plan
            && CteReference::try_from_scan(scan).is_some()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// Returns whether the query reads the files or the partitions of its input, which a
/// reference does not expose.
fn reads_input_layout(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::Extension(extension) = plan {
            let node = extension.node.as_any();
            found = node.is::<SeededRandomNode>()
                || node.is::<MonotonicIdNode>()
                || node.is::<SparkPartitionIdNode>();
        }
        if !found {
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::ScalarFunction(function) = expr
                        && is_input_file_metadata_function(&function.func)
                    {
                        found = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
        }
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// Returns the names that a repeated reference to a CTE gives to the columns of its scan:
/// the scan is renamed by a projection below an alias, as the definition is renamed
/// when it is in the place of the reference.
fn renamed_reference(alias: &SubqueryAlias) -> Option<(&TableScan, Vec<String>)> {
    let LogicalPlan::Projection(projection) = alias.input.as_ref() else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        return None;
    };
    CteReference::try_from_scan(scan)?;
    let schema = &scan.projected_schema;
    if alias.alias != scan.table_name || projection.expr.len() != schema.fields().len() {
        return None;
    }
    let names = projection
        .expr
        .iter()
        .zip(schema.iter())
        .map(|(expr, (qualifier, field))| match expr {
            Expr::Alias(alias)
                if alias.metadata.is_none()
                    && alias.relation.as_ref() == qualifier
                    && matches!(alias.expr.as_ref(), Expr::Column(column)
                        if column.relation.as_ref() == qualifier
                            && &column.name == field.name()) =>
            {
                Some(alias.name.clone())
            }
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some((scan, names))
}

/// Replaces the selected references by their definitions, as the resolver does for
/// the references to a CTE that is not shareable.
pub fn inline_references(
    plan: LogicalPlan,
    selected: &dyn Fn(Site) -> bool,
) -> Result<LogicalPlan> {
    plan.transform_down_with_subqueries(|plan| {
        let inlined = match &plan {
            LogicalPlan::SubqueryAlias(alias) => match renamed_reference(alias) {
                Some((scan, names)) if selected(site(scan)) => {
                    let definition = definition(scan);
                    Some(rename_logical_plan_reusing_projection(
                        definition.plan().as_ref().clone(),
                        &names,
                    )?)
                }
                _ => None,
            },
            LogicalPlan::TableScan(scan)
                if CteReference::try_from_scan(scan).is_some() && selected(site(scan)) =>
            {
                Some(definition(scan).plan().as_ref().clone())
            }
            _ => None,
        };
        Ok(match inlined {
            Some(plan) => Transformed::yes(plan),
            None => Transformed::no(plan),
        })
    })
    .data()
}

fn definition(scan: &TableScan) -> Arc<CteDefinition> {
    match CteReference::try_from_scan(scan) {
        Some(reference) => Arc::clone(&reference.definition),
        None => unreachable!("the scan is a CTE reference"),
    }
}

/// Replaces the references that do not share a body by their definitions, including
/// the only reference to a definition.
fn inline_unshared_references(
    plan: &LogicalPlan,
    inlined: &mut HashSet<Site>,
) -> Result<LogicalPlan> {
    loop {
        let root = inline_references(plan.clone(), &|site| inlined.contains(&site))?;
        let mut sites: HashMap<usize, Vec<Site>> = HashMap::new();
        count_references(&root, inlined, &mut sites)?;
        let single = sites
            .into_values()
            .filter(|sites| sites.len() == 1)
            .flatten()
            .collect::<Vec<_>>();
        if single.is_empty() {
            return Ok(root);
        }
        inlined.extend(single);
    }
}

/// Counts the references to each definition in the plan and in the definitions that it
/// refers to, where a definition is counted once for all the references that share it.
fn count_references(
    plan: &LogicalPlan,
    inlined: &HashSet<Site>,
    sites: &mut HashMap<usize, Vec<Site>>,
) -> Result<()> {
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::TableScan(scan) = plan
            && let Some(reference) = CteReference::try_from_scan(scan)
        {
            let definition = &reference.definition;
            if inlined.contains(&site(scan)) {
                count_references(definition.plan(), inlined, sites)?;
            } else {
                let first = !sites.contains_key(&definition.index);
                sites.entry(definition.index).or_default().push(site(scan));
                if first {
                    count_references(definition.plan(), inlined, sites)?;
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// How a plan is read by its consumer.
#[derive(Clone, Copy, Default)]
struct Context {
    /// Whether the plan is read through a limit, which can stop reading it early, or
    /// through a filter that is left above a reference, which the scan does not record.
    partial: bool,
    /// Whether the consumer ignores duplicate rows of the plan, as the filter side of
    /// a semi or anti join, or the input of a duplicate elimination.
    distinct: bool,
}

/// Collects the references in the plan with what they read.
fn collect_members(plan: &LogicalPlan, context: Context, members: &mut Members) -> Result<()> {
    plan.apply_subqueries(|subquery| {
        collect_members(subquery, Context::default(), members)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let LogicalPlan::TableScan(scan) = plan
        && let Some(reference) = CteReference::try_from_scan(scan)
    {
        let definition = &reference.definition;
        members
            .entry(definition.index)
            .or_insert_with(|| (Arc::clone(definition), vec![]))
            .1
            .push(Member {
                site: site(scan),
                demand: if context.partial {
                    None
                } else {
                    Demand::try_new(scan)
                },
                distinct: context.distinct,
                projection: scan.projection.clone(),
            });
        return Ok(());
    }
    // A filter above a reference, through the nodes that only rename its columns.
    let filtered = {
        let mut input = plan;
        let mut filtered = false;
        loop {
            match input {
                LogicalPlan::Filter(filter) => {
                    filtered = true;
                    input = &filter.input;
                }
                LogicalPlan::Projection(projection) if filtered => input = &projection.input,
                LogicalPlan::SubqueryAlias(alias) if filtered => input = &alias.input,
                _ => break,
            }
        }
        filtered
            && matches!(input, LogicalPlan::TableScan(scan) if CteReference::try_from_scan(scan).is_some())
    };
    let partial = filtered
        || match plan {
            LogicalPlan::Limit(_) => true,
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_) => context.partial,
            _ => false,
        };
    // The same nodes as in the join elimination of the optimizer.
    let distinct = match plan {
        LogicalPlan::Distinct(_) => vec![true],
        LogicalPlan::Aggregate(aggregate)
            if !aggregate.group_expr.is_empty() && aggregate.aggr_expr.is_empty() =>
        {
            vec![true]
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => vec![context.distinct],
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Repartition(_) => vec![context.distinct],
        LogicalPlan::Join(join) => match join.join_type {
            JoinType::Inner => vec![context.distinct; 2],
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                vec![context.distinct, true]
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                vec![true, context.distinct]
            }
            JoinType::Left | JoinType::Right | JoinType::Full => vec![false; 2],
        },
        _ => vec![],
    };
    for (i, input) in plan.inputs().into_iter().enumerate() {
        let distinct = distinct.get(i).copied().unwrap_or(false);
        collect_members(input, Context { partial, distinct }, members)?;
    }
    Ok(())
}

/// Returns the references that cannot share a body or that read different rows than
/// every other reference.
fn single_references(references: &[Member]) -> Vec<Site> {
    let mut counts: HashMap<&Demand, usize> = HashMap::new();
    for demand in references
        .iter()
        .filter_map(|member| member.demand.as_ref())
    {
        *counts.entry(demand).or_default() += 1;
    }
    references
        .iter()
        .filter(|member| match &member.demand {
            Some(demand) => counts[demand] == 1,
            None => true,
        })
        .map(|member| member.site)
        .collect()
}

/// Returns the positions of the definitions that the given definitions refer to,
/// directly or through other definitions.
fn referenced_definitions<'a>(
    definitions: impl Iterator<Item = &'a Arc<CteDefinition>>,
) -> Result<HashSet<usize>> {
    fn visit(definition: &CteDefinition, referenced: &mut HashSet<usize>) -> Result<()> {
        definition.plan().apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(reference) = CteReference::try_from_scan(scan)
                && referenced.insert(reference.definition.index)
            {
                visit(&reference.definition, referenced)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    let mut referenced = HashSet::new();
    for definition in definitions {
        visit(definition, &mut referenced)?;
    }
    Ok(referenced)
}

/// The outcome of sharing the references of an optimized plan.
enum Sharing {
    /// The plan in which every reference shares a body.
    Shared(LogicalPlan),
    /// The references that share with no other reference.
    Inline(Vec<Site>),
    /// The error of optimizing a shared body.
    Failed(datafusion_common::DataFusionError),
}

/// Gives the references of the optimized plan that read the same rows of a definition
/// one shared body. Returns the references that share with no other reference instead,
/// if there are any.
fn share_references(
    plan: LogicalPlan,
    inlined: &HashSet<Site>,
    optimize: &mut dyn FnMut(LogicalPlan, bool) -> Result<LogicalPlan>,
) -> Result<Sharing> {
    let mut members = Members::new();
    collect_members(&plan, Context::default(), &mut members)?;
    let mut bodies = vec![];
    // The references to a definition are in the plan and in the bodies of later
    // definitions, so the definitions are visited from the last one.
    while let Some((_, (definition, references))) = members.pop_last() {
        let mut single = single_references(&references);
        let mut groups: IndexMap<Demand, Vec<Member>> = IndexMap::new();
        for member in references {
            if let Some(demand) = &member.demand {
                groups.entry(demand.clone()).or_default().push(member);
            }
        }
        if !single.is_empty() {
            // No more references to a definition appear when other references are
            // replaced by their definitions, unless a definition refers to it. So the
            // references to the other definitions that share with no other reference
            // are known as well.
            let nested = referenced_definitions(
                members
                    .values()
                    .map(|(definition, _)| definition)
                    .chain(std::iter::once(&definition)),
            )?;
            for (index, (_, references)) in &members {
                if !nested.contains(index) {
                    single.extend(single_references(references));
                }
            }
            return Ok(Sharing::Inline(single));
        }
        let sites = groups
            .values()
            .flatten()
            .map(|member| member.site)
            .collect::<Vec<_>>();
        for (demand, group) in groups {
            let columns = group
                .iter()
                .map(|member| member.projection.as_deref())
                .collect::<Option<Vec<_>>>()
                .map(|projections| {
                    let mut columns = projections.concat();
                    columns.sort_unstable();
                    columns.dedup();
                    columns
                });
            let body = inline_references(definition.plan().as_ref().clone(), &|site| {
                inlined.contains(&site)
            })?;
            let body = demand.apply(body, columns.as_deref())?;
            let schema = Arc::clone(body.schema());
            let body = match optimize(body, false) {
                Ok(body) => body,
                Err(error) => return Ok(Sharing::Failed(error)),
            };
            // The references read the body with the types of the definition.
            let same_types = schema.fields().len() == body.schema().fields().len()
                && schema
                    .fields()
                    .iter()
                    .zip(body.schema().fields())
                    .all(|(x, y)| x.data_type() == y.data_type());
            // The optimizer turns a join of the definition into a semi join, or removes
            // it, only if the definition is in the place of a consumer that ignores
            // duplicate rows.
            if group.iter().any(|member| member.distinct) {
                let distinct = LogicalPlan::Distinct(Distinct::All(Arc::new(body.clone())));
                if EliminateJoin::new()
                    .rewrite(distinct, &OptimizerContext::new())?
                    .transformed
                {
                    return Ok(Sharing::Inline(sites));
                }
            }
            // The optimizer simplifies the consumers of an empty definition only if it
            // is in their place.
            if !same_types || matches!(body, LogicalPlan::EmptyRelation(_)) {
                return Ok(Sharing::Inline(
                    group.iter().map(|member| member.site).collect(),
                ));
            }
            collect_members(&body, Context::default(), &mut members)?;
            bodies.push((Arc::clone(&definition), demand, columns, body));
        }
    }
    // A body refers to the bodies of earlier definitions.
    let mut sources: HashMap<(usize, Demand), Arc<dyn TableSource>> = HashMap::new();
    let mut missing = vec![];
    for (definition, demand, columns, body) in bodies.into_iter().rev() {
        let plan = bind_references(body, &sources, &mut missing)?;
        let source = Arc::new(CteReference {
            definition: Arc::clone(&definition),
            body: Some(Arc::new(SharedCteBody {
                name: definition.name.clone(),
                plan,
                columns,
            })),
        });
        sources.insert((definition.index, demand), source);
    }
    let plan = bind_references(plan, &sources, &mut missing)?;
    Ok(if missing.is_empty() {
        Sharing::Shared(plan)
    } else {
        Sharing::Inline(missing)
    })
}

/// Replaces the source of each reference by the source of the body that it shares.
fn bind_references(
    plan: LogicalPlan,
    sources: &HashMap<(usize, Demand), Arc<dyn TableSource>>,
    missing: &mut Vec<Site>,
) -> Result<LogicalPlan> {
    plan.transform_down_with_subqueries(|plan| {
        let LogicalPlan::TableScan(mut scan) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(reference) = CteReference::try_from_scan(&scan) else {
            return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
        };
        let source = Demand::try_new(&scan)
            .and_then(|demand| sources.get(&(reference.definition.index, demand)));
        match source {
            Some(source) => scan.source = Arc::clone(source),
            None => missing.push(site(&scan)),
        }
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .data()
}
