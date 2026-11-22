// SPDX-License-Identifier: Apache-2.0

//! The definition of a temporary view created with a SQL query.
//!
//! Such a view stores its unresolved query and the context that the query was created in.
//! Each statement that reads the view resolves the query once in that context, with the
//! catalog table lookups of the statement, and matches the output to the columns that the
//! view was created with. The rules are those of Spark for a view with schema binding
//! (`SessionCatalog.fromCatalogTable` and `UpCastRule.canUpCast`).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::{Column, DFSchemaRef, Result, TableReference};
use datafusion_expr::expr::Cast;
use datafusion_expr::{
    Expr, Extension, LogicalPlan, Projection, SubqueryAlias, UserDefinedLogicalNodeCore,
};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::manager::table::SessionCatalogTable;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::variant::VARIANT_METADATA_MARKER_KEY;

use super::temp_view::{logical_type_change, user_metadata_differs, view_name};
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::shared_cte::inline_deterministic_references;

/// The maximum number of nested views, as the default of Spark's
/// `spark.sql.view.maxNestedViewDepth`.
const MAX_NESTED_VIEW_DEPTH: usize = 100;

/// The number of nested view queries above which a view that reads its own name stores
/// its resolved plan, so that replacing a view by a query over itself in a loop does not
/// resolve every earlier version of the view at each read.
const MAX_SELF_READING_VIEW_NESTING: usize = 4;

/// The stack space that the resolution of a view query needs without a new stack segment.
const VIEW_BODY_RED_ZONE: usize = 2 * 1024 * 1024;
/// The size of a new stack segment for the resolution of a view query.
const VIEW_BODY_STACK_SIZE: usize = 16 * 1024 * 1024;

/// The name of a temporary view: whether the view is global, and its name.
pub(in crate::resolver) type TemporaryViewName = (bool, String);

/// What a temporary view created with a SQL query stores.
pub(in crate::resolver) struct ViewDefinition {
    /// The unresolved query.
    pub query: spec::QueryPlan,
    /// The configuration that the query was created with.
    pub config: Arc<PlanConfig>,
    /// The current catalog when the view was created.
    pub catalog: String,
    /// The current database when the view was created.
    pub database: Vec<String>,
    /// The temporary views that the query reads, with the names as the query wrote them.
    pub temp_views: HashMap<TemporaryViewName, Vec<String>>,
    /// The session functions that the query calls.
    pub functions: HashSet<String>,
    /// The output column names of the query when the view was created.
    pub query_columns: Vec<String>,
    /// The view that this view replaced, under its name, if the query reads that name.
    pub replaced: Option<(TemporaryViewName, TableStatus)>,
    /// The number of nested view queries, including this one.
    pub nesting: usize,
    /// The session that created a global temporary view, whose temporary views, session
    /// functions, and session-local catalogs the view reads from every session.
    pub session: Option<Arc<CatalogManager>>,
}

/// The names that a view query sees while it is resolved.
pub(in crate::resolver) struct ViewBodyContext {
    pub catalog: String,
    pub database: Vec<String>,
    pub temp_views: HashMap<TemporaryViewName, Vec<String>>,
    pub functions: HashSet<String>,
    /// The session whose names the query reads, if it is not the session of the statement.
    pub session: Option<Arc<CatalogManager>>,
    /// The views that names refer to instead of the current view of that name.
    pub pinned: HashMap<TemporaryViewName, TableStatus>,
}

impl std::fmt::Debug for ViewDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewDefinition")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("query_columns", &self.query_columns)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ViewBodyContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewBodyContext")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl ViewDefinition {
    /// Returns whether the view stores its resolved plan instead of this definition.
    pub fn stores_plan(&self) -> bool {
        self.replaced.is_some() && self.nesting > MAX_SELF_READING_VIEW_NESTING
    }

    fn context(&self, parent: Option<&ViewBodyContext>) -> ViewBodyContext {
        let mut pinned = parent.map(|x| x.pinned.clone()).unwrap_or_default();
        if let Some((name, status)) = &self.replaced {
            pinned.insert(name.clone(), status.clone());
        }
        ViewBodyContext {
            catalog: self.catalog.clone(),
            database: self.database.clone(),
            temp_views: self.temp_views.clone(),
            functions: self.functions.clone(),
            session: self
                .session
                .clone()
                .or_else(|| parent.and_then(|x| x.session.clone())),
            pinned,
        }
    }
}

/// Identifies the resolution of a view query in a statement. A query that is read in
/// another context can resolve to another plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::resolver) struct ViewBodyKey {
    definition: usize,
    session: Option<usize>,
    pinned: Vec<(TemporaryViewName, usize)>,
}

impl ViewBodyKey {
    fn new(definition: &Arc<ViewDefinition>, context: &ViewBodyContext) -> Self {
        let mut pinned = context
            .pinned
            .iter()
            .map(|(name, status)| (name.clone(), stored_plan_address(status)))
            .collect::<Vec<_>>();
        pinned.sort();
        Self {
            definition: Arc::as_ptr(definition) as usize,
            session: context
                .session
                .as_ref()
                .map(|x| Arc::as_ptr(x) as *const () as usize),
            pinned,
        }
    }
}

fn stored_plan(status: &TableStatus) -> Option<&Arc<LogicalPlan>> {
    match &status.kind {
        TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
            Some(plan)
        }
        _ => None,
    }
}

fn stored_plan_address(status: &TableStatus) -> usize {
    stored_plan(status).map_or(0, |plan| Arc::as_ptr(plan) as *const () as usize)
}

/// The stored plan of a temporary view created with a SQL query. It is replaced when the
/// view is read, so it is never optimized or executed.
#[derive(Clone)]
pub(in crate::resolver) struct TemporaryViewDefinitionNode {
    schema: DFSchemaRef,
    definition: Arc<ViewDefinition>,
}

impl TemporaryViewDefinitionNode {
    pub fn new(schema: DFSchemaRef, definition: ViewDefinition) -> Self {
        Self {
            schema,
            definition: Arc::new(definition),
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn try_from_plan(plan: &LogicalPlan) -> Option<&Self> {
        match plan {
            LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref::<Self>(),
            _ => None,
        }
    }
}

impl std::fmt::Debug for TemporaryViewDefinitionNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporaryViewDefinitionNode")
            .finish_non_exhaustive()
    }
}

impl PartialEq for TemporaryViewDefinitionNode {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.definition, &other.definition)
    }
}

impl Eq for TemporaryViewDefinitionNode {}

impl PartialOrd for TemporaryViewDefinitionNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Arc::as_ptr(&self.definition).partial_cmp(&Arc::as_ptr(&other.definition))
    }
}

impl Hash for TemporaryViewDefinitionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.definition).hash(state);
    }
}

impl UserDefinedLogicalNodeCore for TemporaryViewDefinitionNode {
    fn name(&self) -> &str {
        "TemporaryViewDefinition"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TemporaryViewDefinition")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(self.clone())
    }
}

/// A view query to resolve, in the order of the nesting of the views.
struct ViewBodyFrame {
    definition: Arc<ViewDefinition>,
    schema: DFSchemaRef,
    status: TableStatus,
    context: Arc<ViewBodyContext>,
    key: ViewBodyKey,
    depth: usize,
}

impl PlanResolver<'_> {
    /// Returns the temporary view that `reference` names for the query being resolved.
    /// In a view query, a name refers to a temporary view only if the query read that
    /// view when the view was created.
    pub(in crate::resolver) async fn lookup_temporary_view(
        manager: &CatalogManager,
        reference: &[String],
        state: &mut PlanResolverState,
    ) -> Result<Option<TableStatus>, CatalogError> {
        let name = manager.temporary_view_name(reference)?;
        let found = match state.view_body().cloned() {
            None => manager.find_temporary_view(reference).await?,
            Some(context) => {
                Self::find_temporary_view_in(manager, &context, reference, name.as_ref()).await?
            }
        };
        if found.is_some()
            && let Some(name) = name
        {
            // A session view of another session is not a view of this session of that name.
            let is_own_view = name.0
                || state
                    .view_body()
                    .and_then(|x| x.session.as_ref())
                    .is_none_or(|session| manager.is_same_session(session));
            state.record_temporary_view_read(name, reference, is_own_view);
        }
        Ok(found)
    }

    async fn find_temporary_view_in(
        manager: &CatalogManager,
        context: &ViewBodyContext,
        reference: &[String],
        name: Option<&TemporaryViewName>,
    ) -> Result<Option<TableStatus>, CatalogError> {
        let Some(name) = name else {
            return Ok(None);
        };
        if let Some(status) = context.pinned.get(name) {
            return Ok(Some(status.clone()));
        }
        if !name.0 && !context.temp_views.contains_key(name) {
            return Ok(None);
        }
        context
            .session
            .as_deref()
            .unwrap_or(manager)
            .find_temporary_view(reference)
            .await
    }

    /// Returns the fully qualified name of an object for the query being resolved.
    pub(in crate::resolver) fn qualify_object_name(
        manager: &CatalogManager,
        reference: &[String],
        state: &PlanResolverState,
    ) -> Result<Vec<String>, CatalogError> {
        match state.view_body() {
            Some(context) => context
                .session
                .as_deref()
                .unwrap_or(manager)
                .resolve_fully_qualified_object_name_in(
                    reference,
                    &context.catalog,
                    &context.database,
                ),
            None => manager.resolve_fully_qualified_object_name(reference),
        }
    }

    /// Returns the session that a view query reads the table `name` from, and the table,
    /// if the table is in a session-local catalog of a session other than this one.
    pub(in crate::resolver) fn view_session_catalog_table(
        manager: &CatalogManager,
        name: &[String],
        state: &PlanResolverState,
    ) -> Result<Option<(Arc<CatalogManager>, SessionCatalogTable)>, CatalogError> {
        let Some(session) = state.view_body().and_then(|x| x.session.as_ref()) else {
            return Ok(None);
        };
        if manager.is_same_session(session) {
            return Ok(None);
        }
        match session.session_catalog_table(name)? {
            Some(table) if !manager.has_session_catalog_table(&table)? => {
                Ok(Some((Arc::clone(session), table)))
            }
            _ => Ok(None),
        }
    }

    /// Returns the session function `name` for the query being resolved. In a view query,
    /// a name refers to a session function only if the query called that function when
    /// the view was created, so a later function cannot take the place of a built-in.
    pub(in crate::resolver) fn lookup_session_function(
        &self,
        name: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<datafusion_expr::ScalarUDF>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let function = match state.view_body() {
            Some(context) if !context.functions.contains(name) => None,
            Some(context) => context
                .session
                .as_deref()
                .unwrap_or(&manager)
                .get_function(name)?,
            None => manager.get_function(name)?,
        };
        if function.is_some() {
            state.record_session_function(name);
        }
        Ok(function)
    }

    /// Returns the definition that a temporary view created with `query` stores.
    pub(in crate::resolver) async fn create_view_definition(
        &self,
        query: spec::QueryPlan,
        query_columns: Vec<String>,
        view: &str,
        is_global: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<ViewDefinition> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let (catalog, database) = manager.current_catalog_and_database()?;
        let name = (is_global, view.to_string());
        // A view that reads its own name, directly or through other views, keeps reading
        // the view that it replaces.
        let replaced = if state.has_read_temporary_view(&name) {
            let status = if is_global {
                manager.get_global_temporary_view(view).await
            } else {
                manager.get_temporary_view(view).await
            };
            match status {
                Ok(status) => Some((name, status)),
                Err(CatalogError::NotFound(_, _)) => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        let (temp_views, functions) = state.take_referred_names();
        let mut nesting = 1;
        for reference in temp_views.values() {
            if let Some(status) = manager.find_temporary_view(reference).await?
                && let Some(node) =
                    stored_plan(&status).and_then(|x| TemporaryViewDefinitionNode::try_from_plan(x))
            {
                nesting = nesting.max(node.definition.nesting + 1);
            }
        }
        Ok(ViewDefinition {
            query,
            config: Arc::clone(&self.config),
            catalog,
            database,
            temp_views,
            functions,
            query_columns,
            replaced,
            nesting,
            session: is_global.then(|| Arc::new(manager.session_handle())),
        })
    }

    /// Returns the plan of the query of a view in this statement. The nested views are
    /// resolved first, so that the depth of the nesting does not use the stack.
    pub(super) async fn resolve_view_definition(
        &self,
        node: &TemporaryViewDefinitionNode,
        status: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<Arc<LogicalPlan>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let parent = state.view_body().cloned();
        let context = Arc::new(node.definition.context(parent.as_deref()));
        let key = ViewBodyKey::new(&node.definition, &context);
        if let Some(plan) = state.view_body_plan(&key) {
            return Ok(plan);
        }
        let depth = state.view_depth() + 1;
        check_view_depth(depth, status)?;
        let mut frames = vec![ViewBodyFrame {
            definition: Arc::clone(&node.definition),
            schema: node.schema.clone(),
            status: status.clone(),
            context,
            key,
            depth,
        }];
        while let Some(frame) = frames.last() {
            let mut next = None;
            for (name, reference) in &frame.definition.temp_views {
                let is_replaced = frame.context.pinned.contains_key(name);
                // An error is raised by the query itself when it reads the name.
                let Ok(Some(inner)) =
                    Self::find_temporary_view_in(&manager, &frame.context, reference, Some(name))
                        .await
                else {
                    continue;
                };
                let Some(inner_node) =
                    stored_plan(&inner).and_then(|x| TemporaryViewDefinitionNode::try_from_plan(x))
                else {
                    continue;
                };
                let context = Arc::new(inner_node.definition.context(Some(&frame.context)));
                let key = ViewBodyKey::new(&inner_node.definition, &context);
                if state.view_body_plan(&key).is_some() {
                    continue;
                }
                if state.is_resolving_view_body(&key) || frames.iter().any(|x| x.key == key) {
                    let cycle = frames
                        .iter()
                        .map(|x| view_name(&x.status))
                        .chain([view_name(&inner)])
                        .collect::<Vec<_>>();
                    return Err(PlanError::AnalysisError(format!(
                        "[RECURSIVE_VIEW] Recursive view {} detected (cycle: {}).",
                        view_name(&inner),
                        cycle.join(" -> "),
                    )));
                }
                // The views that a view replaced are versions of one view.
                let depth = frame.depth + usize::from(!is_replaced);
                check_view_depth(depth, &inner)?;
                next = Some(ViewBodyFrame {
                    definition: Arc::clone(&inner_node.definition),
                    schema: inner_node.schema.clone(),
                    status: inner.clone(),
                    context,
                    key,
                    depth,
                });
                break;
            }
            if let Some(next) = next {
                frames.push(next);
                continue;
            }
            let Some(frame) = frames.pop() else {
                break;
            };
            let plan = poll_on_large_stack(self.resolve_view_body(&frame, state)).await?;
            state.insert_view_body_plan(frame.key, Arc::clone(&frame.definition), Arc::new(plan));
        }
        let key = ViewBodyKey::new(
            &node.definition,
            &node.definition.context(parent.as_deref()),
        );
        state
            .view_body_plan(&key)
            .ok_or_else(|| PlanError::internal("view query was not resolved"))
    }

    /// Resolves a view query in its own resolver state, which shares only the catalog
    /// lookups and the table sources of the statement. The query sees nothing of the query
    /// that reads the view, and its plan is the plan that the view was created from.
    async fn resolve_view_body(
        &self,
        frame: &ViewBodyFrame,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let definition = &frame.definition;
        let mut body =
            state.enter_view_body(Arc::clone(&frame.context), frame.key.clone(), frame.depth);
        let resolver = PlanResolver {
            ctx: self.ctx,
            config: Arc::clone(&definition.config),
            share_ctes: self.share_ctes,
        };
        let plan = resolver
            .resolve_query_plan(definition.query.clone(), &mut body)
            .await;
        let plan = plan.and_then(|plan| {
            let plan = inline_deterministic_references(plan)?;
            resolver.fit_view_body(plan, definition, &frame.schema, &frame.status, &mut body)
        });
        state.exit_view_body(body);
        plan
    }

    /// Returns the plan with the columns that the view was created with. Each column is
    /// found by its name, and by its position among the columns of that name, and cast
    /// up to its stored type. The nullability of a column follows the query.
    fn fit_view_body(
        &self,
        plan: LogicalPlan,
        definition: &ViewDefinition,
        stored: &DFSchemaRef,
        status: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let case_sensitive = definition.config.case_sensitive;
        let fold = |name: &str| {
            if case_sensitive {
                name.to_string()
            } else {
                name.to_lowercase()
            }
        };
        let names = Self::get_field_names(plan.schema(), state)?;
        let names = names.iter().map(|x| fold(x)).collect::<Vec<_>>();
        let query_columns = definition
            .query_columns
            .iter()
            .map(|x| fold(x))
            .collect::<Vec<_>>();
        let mut expressions = Vec::with_capacity(query_columns.len());
        // The plan keeps the shape that the view was created with when its output is the
        // same.
        let mut same_output = names.len() == query_columns.len();
        for (i, (name, target)) in query_columns.iter().zip(stored.fields()).enumerate() {
            let count = query_columns.iter().filter(|x| *x == name).count();
            let ordinal = query_columns[..i].iter().filter(|x| *x == name).count();
            let candidates = names
                .iter()
                .enumerate()
                .filter(|(_, x)| *x == name)
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if candidates.len() != count {
                let actual = candidates
                    .iter()
                    .map(|&i| names[i].as_str())
                    .collect::<Vec<_>>();
                return Err(PlanError::AnalysisError(format!(
                    "[INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view {} has an \
                    incompatible schema change and column {} cannot be resolved. Expected \
                    {count} columns named {} but got [{}].\nPlease try to re-create the view \
                    by running: CREATE OR REPLACE TEMPORARY VIEW.",
                    view_name(status),
                    definition.query_columns[i],
                    definition.query_columns[i],
                    actual.join(","),
                )));
            }
            let index = candidates[ordinal];
            same_output &= index == i;
            let (qualifier, field) = plan.schema().qualified_field(index);
            let column = Expr::Column(Column::from((qualifier, field)));
            let same_type = same_view_type(field, target);
            if !same_type && !can_up_cast(field, target) {
                return Err(PlanError::AnalysisError(format!(
                    "[CANNOT_UP_CAST_DATATYPE] Cannot up cast {} from \"{}\" to \"{}\".\nThe \
                    type path of the target object is:\n\nYou can either add an explicit cast \
                    to the input data or choose a higher precision type of the field in the \
                    target object",
                    definition.query_columns[i],
                    self.view_type_name(field.data_type()),
                    self.view_type_name(target.data_type()),
                )));
            }
            let expression = if same_type || field.data_type() == target.data_type() {
                column
            } else {
                self.cast_resolved_expression(
                    column,
                    field,
                    target.data_type().clone(),
                    None,
                    None,
                    false,
                    plan.schema(),
                )?
            };
            let keeps_field = same_type
                && !user_metadata_differs(target, field)
                && logical_type_change(target, field).is_none();
            same_output &= keeps_field;
            let expression = if user_metadata_differs(target, field)
                || logical_type_change(target, field).is_some()
            {
                let target = target.as_ref().clone().with_nullable(field.is_nullable());
                Expr::Cast(Cast::new_from_field(Box::new(expression), Arc::new(target)))
            } else {
                expression
            };
            let id = state.register_field_name(target.name());
            expressions.push(expression.alias(id));
        }
        let plan = if same_output {
            plan
        } else {
            LogicalPlan::Projection(Projection::try_new(expressions, Arc::new(plan))?)
        };
        let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            TableReference::Bare {
                table: status.name.clone().into(),
            },
        )?);
        let names = stored
            .fields()
            .iter()
            .map(|x| x.name().clone())
            .collect::<Vec<_>>();
        Ok(rename_logical_plan(plan, &names)?)
    }

    fn view_type_name(&self, data_type: &DataType) -> String {
        let name = self
            .ctx
            .extension::<PlanService>()
            .ok()
            .and_then(|service| {
                service
                    .plan_formatter()
                    .data_type_to_simple_string(data_type)
                    .ok()
            })
            .unwrap_or_else(|| data_type.to_string());
        if data_type.is_nested() {
            name
        } else {
            name.to_uppercase()
        }
    }
}

/// Polls a future on a stack with room for the resolution of a query inside a query.
async fn poll_on_large_stack<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    std::future::poll_fn(|cx| {
        stacker::maybe_grow(VIEW_BODY_RED_ZONE, VIEW_BODY_STACK_SIZE, || {
            future.as_mut().poll(cx)
        })
    })
    .await
}

fn check_view_depth(depth: usize, status: &TableStatus) -> PlanResult<()> {
    if depth > MAX_NESTED_VIEW_DEPTH {
        return Err(PlanError::AnalysisError(format!(
            "[VIEW_EXCEED_MAX_NESTED_DEPTH] The depth of view {} exceeds the maximum view \
            resolution depth ({MAX_NESTED_VIEW_DEPTH}). Analysis is aborted to avoid errors.",
            view_name(status),
        )));
    }
    Ok(())
}

fn is_variant(field: &Field) -> bool {
    field.metadata().contains_key(VARIANT_METADATA_MARKER_KEY)
}

/// Returns whether two fields have one type, with the nullability of their nested fields.
/// The nullability of the fields themselves, their metadata, and the names of the fields
/// that Spark does not name are not compared.
fn same_view_type(a: &Field, b: &Field) -> bool {
    if logical_type_change(a, b).is_some() {
        return false;
    }
    let nested = |a: &Field, b: &Field| a.is_nullable() == b.is_nullable() && same_view_type(a, b);
    match (a.data_type(), b.data_type()) {
        (DataType::Struct(x), DataType::Struct(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(a, b)| a.name() == b.name() && nested(a, b))
        }
        (DataType::List(x), DataType::List(y))
        | (DataType::LargeList(x), DataType::LargeList(y))
        | (DataType::ListView(x), DataType::ListView(y))
        | (DataType::LargeListView(x), DataType::LargeListView(y)) => nested(x, y),
        (DataType::FixedSizeList(x, n), DataType::FixedSizeList(y, m)) => n == m && nested(x, y),
        (DataType::Map(x, s), DataType::Map(y, t)) => {
            s == t
                && match (x.data_type(), y.data_type()) {
                    (DataType::Struct(x), DataType::Struct(y)) if x.len() == 2 && y.len() == 2 => {
                        nested(&x[0], &y[0]) && nested(&x[1], &y[1])
                    }
                    _ => false,
                }
        }
        (x, y) => x == y,
    }
}

fn integral_digits(data_type: &DataType) -> Option<i16> {
    match data_type {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(20),
        _ => None,
    }
}

fn numeric_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(1),
        DataType::Int16 => Some(2),
        DataType::Int32 => Some(3),
        DataType::Int64 => Some(4),
        DataType::Float32 => Some(5),
        DataType::Float64 => Some(6),
        _ => None,
    }
}

fn unsigned_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::UInt8 => Some(1),
        DataType::UInt16 => Some(2),
        DataType::UInt32 => Some(3),
        DataType::UInt64 => Some(4),
        _ => None,
    }
}

fn decimal_parts(data_type: &DataType) -> Option<(i16, i16)> {
    match data_type {
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) => Some((*p as i16, *s as i16)),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Some((*p as i16, *s as i16)),
        _ => None,
    }
}

fn time_unit_precision(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

/// Returns whether a value of a type converts to a string with no loss of meaning.
fn is_atomic(data_type: &DataType) -> bool {
    data_type.is_numeric()
        || is_string(data_type)
        || is_binary(data_type)
        || is_date(data_type)
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::FixedSizeBinary(_)
                | DataType::Timestamp(_, _)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
        )
}

fn list_element(data_type: &DataType) -> Option<&Arc<Field>> {
    match data_type {
        DataType::List(x)
        | DataType::LargeList(x)
        | DataType::ListView(x)
        | DataType::LargeListView(x)
        | DataType::FixedSizeList(x, _) => Some(x),
        _ => None,
    }
}

/// Returns whether a value of one field can be read as another field without losing
/// information, by the table of Spark's `UpCastRule.canUpCast`. Arrow types that Spark
/// does not have follow the Spark type that they represent.
fn can_up_cast(from: &Field, to: &Field) -> bool {
    if same_view_type(from, to) {
        return true;
    }
    if is_variant(from) || is_variant(to) {
        return false;
    }
    let nested = |from: &Field, to: &Field| {
        (!from.is_nullable() || to.is_nullable()) && can_up_cast(from, to)
    };
    let (source, target) = (from.data_type(), to.data_type());
    if matches!(source, DataType::Null) {
        return true;
    }
    if let (Some(digits), Some((p, s))) = (integral_digits(source), decimal_parts(target)) {
        return p - s >= digits && s >= 0;
    }
    if let (Some((p1, s1)), Some((p2, s2))) = (decimal_parts(source), decimal_parts(target)) {
        return p2 - s2 >= p1 - s1 && s2 >= s1;
    }
    if let Some((p, s)) = decimal_parts(source)
        && let Some(digits) = integral_digits(target)
    {
        // A decimal with as many digits as the largest value of the type can overflow it.
        let limit = if matches!(target, DataType::Int64) {
            18
        } else {
            digits - 1
        };
        return s == 0 && p <= limit;
    }
    if let (Some(x), Some(y)) = (numeric_rank(source), numeric_rank(target)) {
        return x < y;
    }
    if let (Some(x), Some(y)) = (unsigned_rank(source), unsigned_rank(target)) {
        return x < y;
    }
    if is_string(target) {
        return is_atomic(source);
    }
    if let (Some(x), Some(y)) = (list_element(source), list_element(target)) {
        if let DataType::FixedSizeList(_, m) = target
            && !matches!(source, DataType::FixedSizeList(_, n) if n == m)
        {
            return false;
        }
        return nested(x, y);
    }
    match (source, target) {
        (x, DataType::Timestamp(_, _)) if is_date(x) => true,
        (x, y) if is_date(x) && is_date(y) => true,
        (DataType::Timestamp(x, _), DataType::Timestamp(y, _)) => {
            time_unit_precision(y) >= time_unit_precision(x)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, Some(_)), DataType::Int64) => true,
        (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, Some(_))) => true,
        (x, y) if is_binary(x) && is_binary(y) => true,
        (DataType::FixedSizeBinary(_), y) if is_binary(y) => true,
        (DataType::Struct(x), DataType::Struct(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| nested(a, b))
        }
        (DataType::Map(x, _), DataType::Map(y, _)) => match (x.data_type(), y.data_type()) {
            (DataType::Struct(x), DataType::Struct(y)) if x.len() == 2 && y.len() == 2 => {
                nested(&x[0], &y[0]) && nested(&x[1], &y[1])
            }
            _ => false,
        },
        (DataType::Interval(x), DataType::Interval(y)) => x == y,
        (DataType::Duration(x), DataType::Duration(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

    use super::{can_up_cast, same_view_type};

    fn up(from: DataType, to: DataType) -> bool {
        can_up_cast(&Field::new("c", from, true), &Field::new("c", to, true))
    }

    fn list(element: Field) -> DataType {
        DataType::List(element.into())
    }

    fn fields(fields: Vec<Field>) -> DataType {
        DataType::Struct(fields.into())
    }

    #[test]
    fn test_integral_and_float_up_casts_follow_the_order_of_the_types() {
        let order = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for (i, from) in order.iter().enumerate() {
            for (j, to) in order.iter().enumerate() {
                assert_eq!(up(from.clone(), to.clone()), i <= j, "{from} to {to}");
            }
        }
    }

    #[test]
    fn test_decimal_up_casts() {
        assert!(up(DataType::Int32, DataType::Decimal128(10, 0)));
        assert!(!up(DataType::Int32, DataType::Decimal128(9, 0)));
        assert!(up(DataType::Int32, DataType::Decimal128(12, 2)));
        assert!(!up(DataType::Int32, DataType::Decimal128(11, 2)));
        assert!(up(DataType::Int64, DataType::Decimal128(20, 0)));
        assert!(!up(DataType::Int64, DataType::Decimal128(19, 0)));
        assert!(up(DataType::Int64, DataType::Decimal128(38, 18)));
        assert!(!up(DataType::Int64, DataType::Decimal128(38, 19)));
        assert!(up(DataType::Decimal128(10, 2), DataType::Decimal128(12, 2)));
        assert!(up(DataType::Decimal128(10, 2), DataType::Decimal128(12, 4)));
        assert!(up(DataType::Decimal128(10, 2), DataType::Decimal128(11, 3)));
        assert!(!up(
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 3)
        ));
        assert!(!up(
            DataType::Decimal128(12, 2),
            DataType::Decimal128(10, 2)
        ));
        assert!(up(DataType::Decimal128(9, 0), DataType::Int32));
        assert!(!up(DataType::Decimal128(10, 0), DataType::Int32));
        assert!(!up(DataType::Decimal128(9, 1), DataType::Int32));
        assert!(up(DataType::Decimal128(18, 0), DataType::Int64));
        // SPARK-60943: a value of 19 digits does not always fit a BIGINT.
        assert!(!up(DataType::Decimal128(19, 0), DataType::Int64));
        for float in [DataType::Float32, DataType::Float64] {
            assert!(!up(float.clone(), DataType::Decimal128(38, 2)));
            assert!(!up(DataType::Decimal128(10, 2), float));
        }
    }

    #[test]
    fn test_datetime_up_casts() {
        let ltz = |unit| DataType::Timestamp(unit, Some("UTC".into()));
        let ntz = |unit| DataType::Timestamp(unit, None);
        assert!(up(DataType::Date32, ltz(TimeUnit::Microsecond)));
        assert!(up(DataType::Date32, ntz(TimeUnit::Microsecond)));
        assert!(!up(ltz(TimeUnit::Microsecond), DataType::Date32));
        assert!(up(ltz(TimeUnit::Microsecond), ntz(TimeUnit::Microsecond)));
        assert!(up(ntz(TimeUnit::Microsecond), ltz(TimeUnit::Microsecond)));
        assert!(up(ntz(TimeUnit::Millisecond), ltz(TimeUnit::Nanosecond)));
        assert!(!up(ntz(TimeUnit::Nanosecond), ltz(TimeUnit::Microsecond)));
        assert!(up(ltz(TimeUnit::Microsecond), DataType::Int64));
        assert!(up(DataType::Int64, ltz(TimeUnit::Microsecond)));
        assert!(!up(DataType::Int32, ltz(TimeUnit::Microsecond)));
        assert!(!up(ntz(TimeUnit::Microsecond), DataType::Int64));
        assert!(!up(DataType::Int64, ntz(TimeUnit::Microsecond)));
        assert!(!up(DataType::Date32, DataType::Int32));
    }

    #[test]
    fn test_string_up_casts() {
        for from in [
            DataType::Int32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Boolean,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::LargeUtf8,
        ] {
            assert!(up(from.clone(), DataType::Utf8), "{from}");
        }
        for to in [
            DataType::Int32,
            DataType::Float64,
            DataType::Boolean,
            DataType::Date32,
            DataType::Binary,
        ] {
            assert!(!up(DataType::Utf8, to.clone()), "{to}");
        }
        assert!(!up(
            list(Field::new("i", DataType::Int32, true)),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_nested_up_casts() {
        let a = |t, n| Field::new("a", t, n);
        assert!(up(
            list(a(DataType::Int16, true)),
            list(a(DataType::Int32, true))
        ));
        assert!(!up(
            list(a(DataType::Int32, true)),
            list(a(DataType::Int16, true))
        ));
        // A part that can hold NULL is not read into a part that cannot.
        assert!(!up(
            list(a(DataType::Int32, true)),
            list(a(DataType::Int32, false))
        ));
        assert!(up(
            list(a(DataType::Int32, false)),
            list(a(DataType::Int32, true))
        ));
        // Fields of a struct match by position, whatever their names are.
        assert!(up(
            fields(vec![Field::new("a", DataType::Int32, true)]),
            fields(vec![Field::new("x", DataType::Int32, true)])
        ));
        assert!(!up(
            fields(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true)
            ]),
            fields(vec![Field::new("a", DataType::Int32, true)])
        ));
    }

    #[test]
    fn test_same_type_includes_nested_nullability_only() {
        let field = |nullable| {
            Field::new(
                "c",
                list(Field::new("item", DataType::Int32, nullable)),
                true,
            )
        };
        assert!(same_view_type(&field(true), &field(true)));
        assert!(!same_view_type(&field(true), &field(false)));
        assert!(same_view_type(
            &Field::new("c", DataType::Int32, true),
            &Field::new("c", DataType::Int32, false)
        ));
    }

    #[test]
    fn test_each_arrow_type_is_up_cast_to_itself() {
        let item = || Arc::new(Field::new("item", DataType::Int32, true));
        for data_type in [
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float64,
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(4),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::ListView(item()),
            DataType::LargeListView(item()),
            DataType::FixedSizeList(item(), 2),
            fields(vec![Field::new("a", DataType::Int32, true)]),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                item(),
            ),
        ] {
            assert!(up(data_type.clone(), data_type.clone()), "{data_type}");
        }
    }

    #[test]
    fn test_other_types_are_up_cast_only_to_themselves() {
        assert!(!up(DataType::Boolean, DataType::Int32));
        assert!(!up(DataType::Binary, DataType::Int32));
        assert!(up(DataType::Null, DataType::Int32));
        assert!(!up(DataType::Int32, DataType::Null));
        assert!(!up(
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Duration(TimeUnit::Microsecond)
        ));
        assert!(up(DataType::UInt16, DataType::UInt32));
        assert!(!up(DataType::UInt32, DataType::UInt16));
    }
}
