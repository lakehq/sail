use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::SessionState;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::{OptimizerContext, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::utils::{conjunction, disjunction};
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
    /// Whether each evaluation of the definition can return different rows, so that
    /// all its references must read one evaluation.
    volatile: bool,
    /// The schema of the definition with the nullability of the optimized definition,
    /// once it is known.
    optimized_schema: OnceLock<SchemaRef>,
}

impl CteDefinition {
    pub fn new(index: usize, name: TableReference, plan: Arc<LogicalPlan>, volatile: bool) -> Self {
        Self {
            index,
            name,
            schema: Arc::clone(plan.schema().inner()),
            resolved: plan,
            plan: OnceLock::new(),
            volatile,
            optimized_schema: OnceLock::new(),
        }
    }

    /// Returns a copy of the definition at another position, with the given plan, in which
    /// the references refer to copies of the definitions as well.
    fn renew(&self, index: usize, resolved: LogicalPlan) -> Self {
        Self {
            index,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            resolved: Arc::new(resolved),
            plan: OnceLock::new(),
            volatile: self.volatile,
            optimized_schema: OnceLock::new(),
        }
    }

    /// Returns the schema that the references read.
    fn source_schema(&self) -> &SchemaRef {
        self.optimized_schema.get().unwrap_or(&self.schema)
    }

    /// Sets the schema that the references read to the schema of the definition with
    /// the nullability of the optimized definition. Returns whether a column that is
    /// nullable in the definition is not nullable in the optimized definition.
    fn set_optimized_schema(&self, optimized: &SchemaRef) -> bool {
        let fields = self.schema.fields();
        let schema = if fields.len() == optimized.fields().len()
            && fields
                .iter()
                .zip(optimized.fields())
                .all(|(x, y)| x.data_type() == y.data_type())
        {
            let fields = fields
                .iter()
                .zip(optimized.fields())
                .map(|(x, y)| {
                    let nullable = x.is_nullable() && y.is_nullable();
                    Arc::new(x.as_ref().clone().with_nullable(nullable))
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(
                fields,
                self.schema.metadata().clone(),
            ))
        } else {
            Arc::clone(&self.schema)
        };
        let narrowed = schema != self.schema;
        let _ = self.optimized_schema.set(schema);
        narrowed
    }

    /// Returns whether all the references must read one evaluation of the definition.
    pub fn volatile(&self) -> bool {
        self.volatile
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
        scan.projected_schema = self.projected_schema(None)?;
        Ok(LogicalPlan::TableScan(scan))
    }

    /// Returns the schema of a reference with the given projection: the schema of the
    /// definition as it was resolved, with the nullability that the references read.
    fn projected_schema(&self, projection: Option<&[usize]>) -> Result<DFSchemaRef> {
        let resolved = self.resolved.schema();
        let source = self.source_schema();
        if projection.is_none() && source == &self.schema {
            return Ok(Arc::clone(resolved));
        }
        let indices = match projection {
            Some(projection) => projection.to_vec(),
            None => (0..resolved.fields().len()).collect(),
        };
        let fields = indices
            .iter()
            .map(|&i| {
                let (qualifier, field) = resolved.qualified_field(i);
                let nullable = field.is_nullable() && source.field(i).is_nullable();
                (
                    qualifier.cloned(),
                    Arc::new(field.as_ref().clone().with_nullable(nullable)),
                )
            })
            .collect();
        let dependencies = resolved
            .functional_dependencies()
            .project_functional_dependencies(&indices, indices.len());
        Ok(Arc::new(
            DFSchema::new_with_metadata(fields, resolved.metadata().clone())?
                .with_functional_dependencies(dependencies)?,
        ))
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
        Arc::clone(self.definition.source_schema())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // The references to a volatile definition read the same rows, so they keep their
        // filters, while the shared body skips the rows that no reference reads.
        let pushdown = if self.definition.volatile {
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Exact
        };
        Ok(vec![pushdown; filters.len()])
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

/// Returns the plan of a stored view in which the references refer to copies of their
/// definitions, so that each query that reads the view evaluates the CTEs of the view
/// with its own CTEs. The references to definitions that are not volatile are replaced by
/// the definitions unless `deterministic` is set. `next_index` returns the next position
/// among the CTE definitions of the query, and `register` receives each copy, after the
/// copies that it refers to.
pub fn renew_cte_definitions(
    plan: LogicalPlan,
    deterministic: bool,
    next_index: &mut dyn FnMut() -> usize,
    register: &mut dyn FnMut(Arc<CteDefinition>),
) -> Result<LogicalPlan> {
    fn collect(
        plan: &LogicalPlan,
        definitions: &mut BTreeMap<usize, Arc<CteDefinition>>,
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
                collect(&reference.definition.resolved, definitions)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    fn bind(
        plan: LogicalPlan,
        renewed: &HashMap<usize, Arc<CteDefinition>>,
    ) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(definition) = CteReference::try_from_scan(&scan)
                .and_then(|reference| renewed.get(&reference.definition.index))
            else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            scan.source = Arc::new(CteReference {
                definition: Arc::clone(definition),
                body: None,
            });
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })
        .data()
    }
    let mut definitions = BTreeMap::new();
    collect(&plan, &mut definitions)?;
    let mut renewed = HashMap::new();
    // A definition refers only to the definitions at earlier positions.
    let inline = |plan| match deterministic {
        true => Ok(plan),
        false => inline_deterministic_references(plan),
    };
    for (index, definition) in definitions {
        let resolved = inline(bind(definition.resolved.as_ref().clone(), &renewed)?)?;
        let copy = Arc::new(definition.renew(next_index(), resolved));
        register(Arc::clone(&copy));
        renewed.insert(index, copy);
    }
    inline(bind(plan, &renewed)?)
}

/// Replaces the references to definitions that are not volatile by their definitions,
/// for a plan that outlives the query, such as the plan of a view. The references to
/// volatile definitions remain, so that they read one evaluation.
pub fn inline_deterministic_references(plan: LogicalPlan) -> Result<LogicalPlan> {
    let sites = deterministic_references(&plan)?
        .into_iter()
        .collect::<HashSet<_>>();
    inline_references(plan, &|site| sites.contains(&site))
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
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
        Self::filtered(&scan.filters)
    }

    /// Returns `None` if a filter depends on more than the columns of the reference.
    fn filtered(filters: &[Expr]) -> Option<Self> {
        let mut literal_types = vec![];
        let mut closed = true;
        for filter in filters {
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
            filters: filters.to_vec(),
            literal_types,
        })
    }

    /// Returns the demand of a body that is shared by references with different demands,
    /// which reads the rows that any of the references reads, or all the rows if a
    /// reference reads all the rows.
    fn any<'a>(demands: impl IntoIterator<Item = Option<&'a Demand>>) -> Self {
        let mut distinct: Vec<&Demand> = vec![];
        for demand in demands {
            let Some(demand) = demand.filter(|x| !x.filters.is_empty()) else {
                return Self::default();
            };
            if !distinct.contains(&demand) {
                distinct.push(demand);
            }
        }
        let predicates = distinct
            .iter()
            .filter_map(|x| conjunction(x.filters.iter().cloned()));
        Self {
            filters: disjunction(predicates).into_iter().collect(),
            literal_types: vec![],
        }
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
    /// The rows that a reference to a volatile definition reads, or `None` if they are
    /// not known.
    filters: Option<Demand>,
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
    // The references to volatile definitions still share one evaluation, which a
    // reference reads as it reads a source.
    let mut inlined = HashSet::new();
    if reads_input_layout(&plan)? {
        inlined.extend(deterministic_references(&plan)?);
    }
    loop {
        let root = with_optimized_schemas(inline_unshared_references(&plan, &mut inlined)?)?;
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

/// Returns the references to definitions that are not volatile, in the plan and in the
/// definitions that it refers to.
fn deterministic_references(plan: &LogicalPlan) -> Result<Vec<Site>> {
    fn collect(
        plan: &LogicalPlan,
        visited: &mut HashSet<usize>,
        sites: &mut Vec<Site>,
    ) -> Result<()> {
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(reference) = CteReference::try_from_scan(scan)
            {
                if !reference.definition.volatile {
                    sites.push(site(scan));
                }
                if visited.insert(reference.definition.index) {
                    collect(reference.definition.plan(), visited, sites)?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    let mut sites = vec![];
    collect(plan, &mut HashSet::new(), &mut sites)?;
    Ok(sites)
}

/// Returns the plan in which the references read the optimized schemas of their
/// definitions, where they are known.
fn with_optimized_schemas(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        let LogicalPlan::TableScan(mut scan) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(definition) = CteReference::try_from_scan(&scan)
            .map(|reference| Arc::clone(&reference.definition))
            .filter(|definition| {
                definition
                    .optimized_schema
                    .get()
                    .is_some_and(|schema| schema != &definition.schema)
            })
        else {
            return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
        };
        scan.projected_schema = definition.projected_schema(scan.projection.as_deref())?;
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .data()
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

/// Returns the demand by which the reference shares a body, or `None` if the reference
/// cannot share a body. All the references to a volatile definition share one body,
/// whatever they read, since each evaluation of the definition can return different rows.
fn reference_demand(scan: &TableScan, context: Context) -> Option<Demand> {
    match CteReference::try_from_scan(scan) {
        Some(reference) if reference.definition.volatile => Some(Demand::default()),
        _ if context.partial => None,
        _ => Demand::try_new(scan),
    }
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
                demand: reference_demand(scan, context),
                filters: definition
                    .volatile
                    .then(|| Demand::filtered(&scan.filters))
                    .flatten(),
                distinct: context.distinct && !definition.volatile,
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
    let mut narrowed = false;
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
            let body = with_optimized_schemas(inline_references(
                definition.plan().as_ref().clone(),
                &|site| inlined.contains(&site),
            )?)?;
            let body = if definition.volatile {
                Demand::any(group.iter().map(|member| member.filters.as_ref()))
                    .apply(body, columns.as_deref())?
            } else {
                demand.apply(body, columns.as_deref())?
            };
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
            // The optimizer simplifies the consumers of a column that it finds not nullable
            // only if the definition is in their place, which the references read with
            // the nullability of the definition as it is optimized without them.
            let positions = columns
                .clone()
                .unwrap_or_else(|| (0..definition.schema.fields().len()).collect());
            let source = definition.source_schema();
            if definition.optimized_schema.get().is_none()
                && body
                    .schema()
                    .fields()
                    .iter()
                    .zip(&positions)
                    .any(|(field, &i)| !field.is_nullable() && source.field(i).is_nullable())
            {
                let plan = with_optimized_schemas(inline_references(
                    definition.plan().as_ref().clone(),
                    &|site| inlined.contains(&site),
                )?)?;
                match optimize(plan, false) {
                    Ok(plan) => narrowed |= definition.set_optimized_schema(plan.schema().inner()),
                    Err(error) => return Ok(Sharing::Failed(error)),
                }
            }
            collect_members(&body, Context::default(), &mut members)?;
            bodies.push((Arc::clone(&definition), demand, columns, body));
        }
    }
    // The query is optimized again with the references that read the optimized schemas.
    if narrowed {
        return Ok(Sharing::Inline(vec![]));
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
        let source = reference_demand(&scan, Context::default())
            .and_then(|demand| sources.get(&(reference.definition.index, demand)));
        match source {
            Some(source) => scan.source = Arc::clone(source),
            None => missing.push(site(&scan)),
        }
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .data()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::lit;

    use super::*;

    #[test]
    fn test_renewed_deterministic_definitions_are_inlined_unless_shared() -> Result<()> {
        let values = LogicalPlanBuilder::values(vec![vec![lit(1)]])?.build()?;
        let alias = |plan: LogicalPlan, name: &str| -> Result<Arc<LogicalPlan>> {
            Ok(Arc::new(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), name)?,
            )))
        };
        let d = Arc::new(CteDefinition::new(
            0,
            "d".into(),
            alias(values, "d")?,
            false,
        ));
        let v = Arc::new(CteDefinition::new(
            1,
            "v".into(),
            alias(d.scan()?, "v")?,
            true,
        ));
        for (deterministic, expected) in [(true, 1), (false, 0)] {
            let mut index = 10;
            let mut copies = vec![];
            renew_cte_definitions(
                v.scan()?,
                deterministic,
                &mut || {
                    index += 1;
                    index
                },
                &mut |copy| copies.push(copy),
            )?;
            let mut references = 0;
            let resolved = copies.iter().filter(|x| x.volatile).map(|x| x.resolved());
            assert_eq!(resolved.clone().count(), 1);
            for plan in resolved {
                plan.apply_with_subqueries(|plan| {
                    if let LogicalPlan::TableScan(scan) = plan
                        && CteReference::try_from_scan(scan).is_some()
                    {
                        references += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            assert_eq!(references, expected);
        }
        Ok(())
    }
}
