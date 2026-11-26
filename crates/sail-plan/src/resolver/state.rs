// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion_common::arrow::datatypes::{Field, FieldRef, Schema};
use datafusion_common::{DFSchemaRef, HashMap, HashSet, ParamValues, ScalarValue, TableReference};
use datafusion_expr::{LogicalPlan, TableSource};
use sail_catalog::lakehouse::LakehouseResolvedTable;
use sail_common::spec;
use sail_common_datafusion::catalog::TableStatus;
use sail_common_datafusion::datasource::LakeSnapshotPins;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::query::{
    CteInfo, CteKind, MissingInputBoundaries, MissingInputResolution, TableOrigin,
};
use crate::shared_cte::CteDefinition;

/// Identifies what a catalog table read scans.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct TableSourceKey {
    pub catalog: Option<String>,
    pub database: Vec<String>,
    pub name: String,
    pub format: String,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
    pub options: Vec<(String, String)>,
    pub temporal_options: Vec<(String, String)>,
    /// How the source matches the letter case of column names, for a source that matches
    /// the columns of the table with the columns of its files.
    pub read_case_sensitive: Option<bool>,
    /// The catalog instance of the table, for a table that a global temporary view reads
    /// in a catalog of the session that created the view.
    pub session_catalog: Option<usize>,
}

#[derive(Clone)]
struct SharedTableSource(Arc<dyn TableSource>);

/// Identifies what a read of a Delta Lake or Iceberg table by its path scans.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct LakePathSourceKey {
    /// The format in lower case.
    pub format: String,
    pub paths: Vec<String>,
    /// The options of the read, sorted by key.
    pub options: Vec<(String, String)>,
    pub schema: Option<Schema>,
}

impl std::fmt::Debug for SharedTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedTableSource").finish_non_exhaustive()
    }
}

/// The field information for fields in the logical plan.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    /// The set of plan IDs, if any, that reference this field.
    plan_ids: HashSet<i64>,
    /// Attribute identity, preserved by identity projections.
    origin: usize,
    /// The user-facing name of the field.
    name: String,
    /// Whether this is a hidden field that should be excluded from the
    /// final logical plan.
    /// A hidden field is helpful for filtering or sorting plans using columns
    /// of its child plans (e.g. a key column of the left/right plans in an outer join).
    hidden: bool,
}

impl FieldInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the attribute identity, shared by the identity projections of the field.
    pub fn origin(&self) -> usize {
        self.origin
    }

    pub fn plan_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.plan_ids.iter().copied()
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    pub fn matches(&self, name: &str, plan_id: Option<i64>) -> bool {
        self.name.eq_ignore_ascii_case(name)
            && match plan_id {
                Some(plan_id) => self.plan_ids.contains(&plan_id),
                None => true,
            }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct PlanResolverStateConfig {
    pub arrow_allow_large_var_types: bool,
    /// Keep source columns in COUNT arguments while resolving SQL PIVOT aggregates.
    /// Implicit pivot grouping needs those references before the aggregate is rewritten.
    pub preserve_count_argument_columns: bool,
}

#[derive(Debug, Default)]
pub(super) enum AggregateState {
    Lateral {
        projections: Vec<NamedExpr>,
    },
    /// The expressions in the `GROUP BY` clause is being resolved.
    Grouping {
        projections: Vec<NamedExpr>,
    },
    /// The expressions in the `HAVING` clause is being resolved.
    Having {
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
    },
    /// There is no aggregate being resolved.
    #[default]
    None,
}

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    next_lateral_id: usize,
    next_origin: usize,
    /// Multipart relation names represented by opaque DataFusion qualifiers.
    relation_names: HashMap<TableReference, Vec<String>>,
    /// A map from the generated opaque field ID to field information.
    fields: HashMap<String, FieldInfo>,
    /// An output schema of each DataFrame plan ID, for checking references against it.
    plan_schemas: HashMap<i64, DFSchemaRef>,
    /// The outer query schema for the current subquery.
    outer_query_schema: Option<DFSchemaRef>,
    /// The type-checking schema and ordered name-resolution schemas for expressions
    /// that can recover missing inputs (e.g. a filter predicate).
    missing_input_resolution: Option<MissingInputResolution>,
    /// Outputs whose descendants cannot participate in missing-reference resolution.
    missing_input_boundaries: MissingInputBoundaries,
    /// The aggregate state for the current query.
    aggregate_state: AggregateState,
    /// The CTEs for the current query.
    ctes: HashMap<TableReference, Arc<CteInfo>>,
    /// Output attributes of the CTE definitions referenced so far; later references renew them.
    cte_reference_origins: HashSet<usize>,
    /// The number of CTE definitions resolved so far.
    next_cte_definition_index: usize,
    /// The CTE definitions with references that are scans.
    shared_cte_definitions: Vec<Arc<CteDefinition>>,
    /// Output attributes of the parameter views referenced so far, including renewed ones.
    parameter_view_origins: HashSet<usize>,
    /// Unresolved subquery references from a WithRelations node, keyed by plan_id.
    subquery_references: HashMap<i64, spec::QueryPlan>,
    config: PlanResolverStateConfig,
    /// Named parameter values available while resolving a `WithParameters` query node.
    /// These provide placeholder types and support early `IDENTIFIER` evaluation.
    param_values: std::collections::HashMap<String, ScalarValue>,
    /// Positional parameter values available alongside `param_values`.
    positional_param_values: Vec<ScalarValue>,
    /// Stack of in-scope lambda parameter frames (innermost last).
    /// Each frame holds the declared parameter names of one enclosing lambda
    /// function, along with the parameter field when the enclosing
    /// higher-order function provides it.
    lambda_param_scopes: Vec<Vec<(String, Option<FieldRef>)>>,
    /// The named windows defined in the current query, keyed by window name.
    windows: std::collections::HashMap<String, spec::Window>,
    /// The sources of the catalog tables read so far, shared by repeated references to
    /// the same table within the query.
    table_sources: HashMap<TableSourceKey, SharedTableSource>,
    /// The sources of the Delta Lake and Iceberg tables read by path so far, so that the
    /// reads of a path in the query read one version of the table.
    lake_path_sources: HashMap<LakePathSourceKey, SharedTableSource>,
    /// The versions of the lake tables read so far, so that the reads of a table in the
    /// query read one version of it, by name or by path and with any read options.
    lake_snapshot_pins: Arc<LakeSnapshotPins>,
    /// The successful catalog table lookups so far, shared by repeated references to
    /// the same table within the query. The key is the catalog instance of another session
    /// for a table that a global temporary view reads in a catalog of the session that
    /// created it (see `SessionCatalogTable::catalog_id`), and the fully qualified name
    /// of the table (the catalog, the database, and the table), which is the same for all
    /// the ways to write the name of the table. Names that the catalog returned the same
    /// table for have the same lookup, see [`PlanResolverState::share_catalog_table`].
    catalog_table_lookups: HashMap<(Option<usize>, Vec<String>), CatalogTableLookup>,
    /// What the definition of a temporary view reads, while the definition is resolved.
    view_definition: Option<ViewDefinitionReads>,
}

/// What the definition of a temporary view reads, which is recorded while the definition is
/// resolved.
#[derive(Debug, Default, Clone)]
pub(super) struct ViewDefinitionReads {
    /// What the table sources of the scans read, by the address of the source, for the
    /// sources that the view obtains again at each read.
    table_origins: HashMap<usize, (SharedTableSource, Arc<TableOrigin>)>,
    /// The columns that the definition reads although its plan does not refer to them: the
    /// columns of the arguments of a function that its expression no longer refers to,
    /// such as the argument of `typeof`.
    pub folded_columns: HashSet<String>,
    /// The columns that the temporary views that the definition reads read of their tables,
    /// by the address of the source and the positions of the columns in the source.
    pub view_reads: HashMap<usize, HashSet<usize>>,
    /// Whether the definition expands a star inside the arguments of a function, which
    /// expands to the columns that the tables have when the definition is resolved.
    pub star_in_function: bool,
    /// Whether the definition reads a persistent view, which can be defined again.
    pub reads_persistent_view: bool,
}

impl ViewDefinitionReads {
    /// Records what the source of a scan reads. The first record of a source remains.
    pub fn record_table_origin(&mut self, source: &Arc<dyn TableSource>, origin: Arc<TableOrigin>) {
        self.table_origins
            .entry(table_source_address(source))
            .or_insert_with(|| (SharedTableSource(Arc::clone(source)), origin));
    }

    /// Returns the addresses of the sources that the definition read.
    pub fn sources(&self) -> impl Iterator<Item = usize> + '_ {
        self.table_origins.keys().copied()
    }

    /// Returns what the source of a scan reads, if the view obtains it again.
    pub fn table_origin(&self, source: &Arc<dyn TableSource>) -> Option<&Arc<TableOrigin>> {
        self.table_origins
            .get(&table_source_address(source))
            .map(|(_, origin)| origin)
    }
}

/// Returns the address of a table source, which identifies the source while it is alive.
pub(super) fn table_source_address(source: &Arc<dyn TableSource>) -> usize {
    Arc::as_ptr(source) as *const () as usize
}

/// A catalog table or view, with the lakehouse resolution of a catalog table.
pub(super) type CatalogTableLookup = (TableStatus, Option<LakehouseResolvedTable>);

impl Default for PlanResolverState {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanResolverState {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            next_lateral_id: 0,
            next_origin: 0,
            relation_names: HashMap::new(),
            fields: HashMap::new(),
            plan_schemas: HashMap::new(),
            outer_query_schema: None,
            missing_input_resolution: None,
            missing_input_boundaries: MissingInputBoundaries::default(),
            aggregate_state: AggregateState::default(),
            ctes: HashMap::new(),
            cte_reference_origins: HashSet::new(),
            next_cte_definition_index: 0,
            shared_cte_definitions: Vec::new(),
            parameter_view_origins: HashSet::new(),
            subquery_references: HashMap::new(),
            config: PlanResolverStateConfig::default(),
            param_values: std::collections::HashMap::new(),
            positional_param_values: Vec::new(),
            lambda_param_scopes: Vec::new(),
            windows: std::collections::HashMap::new(),
            table_sources: HashMap::new(),
            lake_path_sources: HashMap::new(),
            lake_snapshot_pins: Arc::default(),
            catalog_table_lookups: HashMap::new(),
            view_definition: None,
        }
    }

    pub fn register_relation_name(&mut self, name: Vec<String>) -> TableReference {
        let reference = TableReference::bare(format!("#relation:{}", uuid::Uuid::new_v4()));
        self.relation_names.insert(reference.clone(), name);
        reference
    }

    pub fn relation_name<'a>(&'a self, reference: &'a TableReference) -> Vec<&'a str> {
        if let Some(name) = self.relation_names.get(reference) {
            return name.iter().map(String::as_str).collect();
        }
        match reference {
            TableReference::Bare { table } => vec![table],
            TableReference::Partial { schema, table } => vec![schema, table],
            TableReference::Full {
                catalog,
                schema,
                table,
            } => vec![catalog, schema, table],
        }
    }

    pub fn is_multipart_relation(&self, reference: &TableReference) -> bool {
        self.relation_names.contains_key(reference)
    }

    pub fn next_field_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("#{id}")
    }

    /// Reserve a typed lateral binding without consuming output field IDs.
    /// Most select lists never use these bindings, so keep their namespace separate.
    pub fn register_lateral_field(&mut self, source: Option<&str>) -> PlanResult<String> {
        let (origin, plan_ids) = match source {
            Some(source) => {
                let info = self.get_field_info(source)?;
                (info.origin, info.plan_ids.clone())
            }
            None => (self.next_origin(), HashSet::new()),
        };
        let field_id = format!("#lateral{}", self.next_lateral_id);
        self.next_lateral_id += 1;
        self.fields.insert(
            field_id.clone(),
            FieldInfo {
                plan_ids,
                origin,
                name: String::new(),
                // Temporary bindings must not participate in wildcard expansion.
                hidden: true,
            },
        );
        Ok(field_id)
    }

    /// Returns a new attribute identity.
    pub fn next_origin(&mut self) -> usize {
        let origin = self.next_origin;
        self.next_origin += 1;
        origin
    }

    fn register_field_info(&mut self, name: impl Into<String>, hidden: bool) -> String {
        let origin = self.next_origin();
        self.register_field_with_origin(name.into(), hidden, origin, HashSet::new())
    }

    /// Registers a field of the attribute identity and returns its generated field ID.
    pub fn register_field_with_origin(
        &mut self,
        name: String,
        hidden: bool,
        origin: usize,
        plan_ids: HashSet<i64>,
    ) -> String {
        let field_id = self.next_field_id();
        let info = FieldInfo {
            plan_ids,
            origin,
            name,
            hidden,
        };
        self.fields.insert(field_id.clone(), info);
        field_id
    }

    /// Registers a field and returns a generated opaque string ID for the field.
    /// The field ID is unique within the plan resolver state.
    /// No assumption should be made about the format of the field ID.
    pub fn register_field_name(&mut self, name: impl Into<String>) -> String {
        self.register_field_info(name, false)
    }

    /// Registers a hidden field and returns a generated opaque string ID for the field.
    /// This is similar to [`Self::register_field_name`] but the field is marked as hidden.
    pub fn register_hidden_field_name(&mut self, name: impl Into<String>) -> String {
        self.register_field_info(name, true)
    }

    /// Sets the display name of a materialized field, registering an internal field
    /// when it becomes referenceable (e.g. an unnested `window` grouping column).
    pub fn set_field_name(&mut self, field_id: &str, name: impl Into<String>) {
        let name = name.into();
        if let Some(info) = self.fields.get_mut(field_id) {
            info.name = name;
        } else {
            let info = FieldInfo {
                plan_ids: HashSet::new(),
                origin: self.next_origin(),
                name,
                hidden: false,
            };
            self.fields.insert(field_id.to_string(), info);
        }
    }

    pub fn register_field(&mut self, field: impl AsRef<Field>) -> String {
        self.register_field_info(field.as_ref().name(), false)
    }

    /// Registers each field and returns unique internal names to avoid column name collisions.
    pub fn register_fields(
        &mut self,
        fields: impl IntoIterator<Item = impl AsRef<Field>>,
    ) -> Vec<String> {
        fields
            .into_iter()
            .map(|field| self.register_field(field))
            .collect()
    }

    /// Registers each name and returns unique internal IDs, like `register_fields` but for plain strings.
    pub fn register_field_names(
        &mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Vec<String> {
        names
            .into_iter()
            .map(|name| self.register_field_name(name))
            .collect()
    }

    pub fn clear_field_plan_ids(&mut self, field_id: &str) -> PlanResult<()> {
        let field_info = self
            .fields
            .get_mut(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))?;
        field_info.plan_ids.clear();
        Ok(())
    }

    pub fn register_plan_id_for_field(&mut self, field_id: &str, plan_id: i64) -> PlanResult<()> {
        let field_info = self
            .fields
            .get_mut(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))?;
        field_info.plan_ids.insert(plan_id);
        Ok(())
    }

    pub fn register_plan_schema(&mut self, schema: &DFSchemaRef, plan_id: i64) -> PlanResult<()> {
        // Every instance has the same output names. Keep one to resolve roots against.
        self.plan_schemas
            .entry(plan_id)
            .or_insert_with(|| Arc::clone(schema));
        for field in schema.fields() {
            self.register_plan_id_for_field(field.name(), plan_id)?;
        }
        Ok(())
    }

    pub fn get_plan_schema(&self, plan_id: i64) -> Option<&DFSchemaRef> {
        self.plan_schemas.get(&plan_id)
    }

    pub fn get_field_info(&self, field_id: &str) -> PlanResult<&FieldInfo> {
        self.fields
            .get(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))
    }

    pub fn get_outer_query_schema(&self) -> Option<&DFSchemaRef> {
        self.outer_query_schema.as_ref()
    }

    /// Returns the missing-input resolution whose type-checking schema is `schema`.
    pub fn missing_input(&self, schema: &DFSchemaRef) -> Option<&MissingInputResolution> {
        self.missing_input_resolution
            .as_ref()
            .filter(|input| input.applies_to(schema))
    }

    pub fn missing_input_mut(
        &mut self,
        schema: &DFSchemaRef,
    ) -> Option<&mut MissingInputResolution> {
        self.missing_input_resolution
            .as_mut()
            .filter(|input| input.applies_to(schema))
    }

    /// Returns the registered missing-input boundaries.
    pub fn missing_input_boundaries(&self) -> &MissingInputBoundaries {
        &self.missing_input_boundaries
    }

    pub fn register_missing_input_boundary(&mut self, plan: &LogicalPlan) {
        self.missing_input_boundaries.register(plan);
    }

    pub fn enter_missing_input_scope(
        &mut self,
        resolution: MissingInputResolution,
    ) -> MissingInputScope<'_> {
        let previous = self.missing_input_resolution.replace(resolution);
        MissingInputScope {
            state: self,
            previous,
        }
    }

    pub fn get_lateral_projections(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Lateral { projections } => projections,
            _ => &[],
        }
    }

    pub fn get_projections_for_grouping(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Grouping { projections } => projections.as_ref(),
            _ => &[],
        }
    }

    pub fn get_projections_for_having(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Having { projections, .. } => projections.as_ref(),
            _ => &[],
        }
    }

    pub fn get_grouping_for_having(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Having { grouping, .. } => grouping.as_ref(),
            _ => &[],
        }
    }

    pub fn enter_query_scope(&mut self, schema: DFSchemaRef) -> QueryScope<'_> {
        QueryScope::new(self, schema)
    }

    pub fn enter_aggregate_scope(&mut self, aggregate_state: AggregateState) -> AggregateScope<'_> {
        AggregateScope::new(self, aggregate_state)
    }

    pub fn enter_cte_scope(&mut self) -> CteScope<'_> {
        CteScope::new(self)
    }

    pub fn get_cte(&self, table_ref: &TableReference) -> Option<Arc<CteInfo>> {
        self.ctes.get(table_ref).cloned()
    }

    pub fn insert_cte(
        &mut self,
        table_ref: TableReference,
        plan: LogicalPlan,
        kind: CteKind,
    ) -> PlanResult<()> {
        let cte = CteInfo::try_new(plan, kind, self)?;
        self.ctes.insert(table_ref, Arc::new(cte));
        Ok(())
    }

    pub fn insert_cte_info(&mut self, table_ref: TableReference, cte: CteInfo) {
        self.ctes.insert(table_ref, Arc::new(cte));
    }

    pub fn next_cte_definition_index(&mut self) -> usize {
        let index = self.next_cte_definition_index;
        self.next_cte_definition_index += 1;
        index
    }

    pub fn register_shared_cte_definition(&mut self, definition: Arc<CteDefinition>) {
        self.shared_cte_definitions.push(definition);
    }

    pub fn shared_cte_definitions(&self) -> &[Arc<CteDefinition>] {
        &self.shared_cte_definitions
    }

    pub fn ctes(&self) -> impl Iterator<Item = &Arc<CteInfo>> {
        self.ctes.values()
    }

    pub fn get_table_source(&self, key: &TableSourceKey) -> Option<Arc<dyn TableSource>> {
        self.table_sources
            .get(key)
            .map(|source| Arc::clone(&source.0))
    }

    pub fn insert_table_source(&mut self, key: TableSourceKey, source: Arc<dyn TableSource>) {
        self.table_sources.insert(key, SharedTableSource(source));
    }

    pub fn get_lake_path_source(&self, key: &LakePathSourceKey) -> Option<Arc<dyn TableSource>> {
        self.lake_path_sources
            .get(key)
            .map(|source| Arc::clone(&source.0))
    }

    pub fn insert_lake_path_source(
        &mut self,
        key: LakePathSourceKey,
        source: Arc<dyn TableSource>,
    ) {
        self.lake_path_sources
            .insert(key, SharedTableSource(source));
    }

    pub fn lake_snapshot_pins(&self) -> Arc<LakeSnapshotPins> {
        Arc::clone(&self.lake_snapshot_pins)
    }

    pub fn get_catalog_table_lookup(
        &self,
        session_catalog: Option<usize>,
        table: &[String],
    ) -> Option<CatalogTableLookup> {
        self.catalog_table_lookups
            .get(&(session_catalog, table.to_vec()))
            .cloned()
    }

    pub fn insert_catalog_table_lookup(
        &mut self,
        session_catalog: Option<usize>,
        table: Vec<String>,
        lookup: CatalogTableLookup,
    ) {
        self.catalog_table_lookups
            .insert((session_catalog, table), lookup);
    }

    /// Records what the definition of a temporary view reads, while the definition is resolved.
    pub fn begin_view_definition(&mut self) {
        self.view_definition = Some(ViewDefinitionReads::default());
    }

    /// Returns what the definition of a temporary view read, once it is resolved.
    pub fn end_view_definition(&mut self) -> ViewDefinitionReads {
        self.view_definition.take().unwrap_or_default()
    }

    /// Returns what the definition of a temporary view reads, while it is resolved.
    pub fn view_definition_mut(&mut self) -> Option<&mut ViewDefinitionReads> {
        self.view_definition.as_mut()
    }

    /// Records that the definition of a temporary view, if one is resolved, expands a star
    /// inside the arguments of a function.
    pub fn record_star_in_function(&mut self) {
        if let Some(reads) = self.view_definition.as_mut() {
            reads.star_in_function = true;
        }
    }

    /// Swaps the parts of the state that belong to the whole statement with those of another
    /// state: the generators of IDs, the fields, the sources and lookups of the tables that the
    /// statement reads, its shared CTE definitions, and what the definition of a view that the
    /// statement creates reads. The query of a temporary view is resolved again in a new
    /// state that has these parts of the state of the query, and none of its scopes, such as
    /// its CTEs or its outer query.
    pub fn swap_statement(&mut self, other: &mut Self) {
        macro_rules! swap {
            ($($field:ident),*) => { $(std::mem::swap(&mut self.$field, &mut other.$field);)* };
        }
        swap!(
            next_id,
            next_lateral_id,
            next_origin,
            relation_names,
            fields,
            next_cte_definition_index,
            shared_cte_definitions,
            config,
            table_sources,
            lake_path_sources,
            lake_snapshot_pins,
            catalog_table_lookups,
            view_definition
        );
    }

    /// Stores what the catalog returned for the table `name` and returns the lookup that
    /// the query uses for it. A catalog can return one table for names that are
    /// written differently, such as names that differ in letter case in a catalog that
    /// ignores it. Such a table uses its first lookup in the query, so that all the
    /// references to the table read the same table metadata. A table is the same as
    /// an earlier one when both are in the same catalog instance and the catalog returned
    /// the same database, name, table ID, and location for both.
    pub fn share_catalog_table(
        &mut self,
        session_catalog: Option<usize>,
        name: Vec<String>,
        status: TableStatus,
        resolved: LakehouseResolvedTable,
    ) -> (TableStatus, LakehouseResolvedTable) {
        let first =
            self.catalog_table_lookups
                .iter()
                .find_map(|((first_catalog, first_name), lookup)| match lookup {
                    (first, Some(first_resolved))
                        if *first_catalog == session_catalog
                            && first_name.first() == name.first()
                            && first.database == status.database
                            && first.name == status.name
                            && first_resolved.execution.table_identity
                                == resolved.execution.table_identity =>
                    {
                        Some((first.clone(), first_resolved.clone()))
                    }
                    _ => None,
                });
        let (status, resolved) = first.unwrap_or((status, resolved));
        self.catalog_table_lookups.insert(
            (session_catalog, name),
            (status.clone(), Some(resolved.clone())),
        );
        (status, resolved)
    }

    /// Returns the output attributes of the CTE definitions referenced so far.
    pub fn cte_reference_origins_mut(&mut self) -> &mut HashSet<usize> {
        &mut self.cte_reference_origins
    }

    /// Returns the output attributes of the parameter views referenced so far,
    /// including renewed ones.
    pub fn parameter_view_origins_mut(&mut self) -> &mut HashSet<usize> {
        &mut self.parameter_view_origins
    }

    /// Registers a field of the same attribute as the source field.
    pub fn register_identity_field(&mut self, name: String, source: &str) -> PlanResult<String> {
        let source = self.get_field_info(source)?;
        let (origin, plan_ids) = (source.origin, source.plan_ids.clone());
        Ok(self.register_field_with_origin(name, false, origin, plan_ids))
    }

    /// Returns a subquery reference plan from state by plan_id.
    pub fn get_subquery_reference(&self, plan_id: i64) -> Option<spec::QueryPlan> {
        self.subquery_references.get(&plan_id).cloned()
    }

    /// Stores a subquery reference plan in state, returning the previous value if any.
    pub fn insert_subquery_reference(
        &mut self,
        plan_id: i64,
        plan: spec::QueryPlan,
    ) -> Option<spec::QueryPlan> {
        self.subquery_references.insert(plan_id, plan)
    }

    pub fn enter_with_relations_scope(&mut self) -> WithRelationsScope<'_> {
        WithRelationsScope::new(self)
    }

    pub fn enter_config_scope(&mut self) -> ConfigScope<'_> {
        ConfigScope::new(self)
    }

    pub fn set_windows(
        &mut self,
        windows: std::collections::HashMap<String, spec::Window>,
    ) -> std::collections::HashMap<String, spec::Window> {
        std::mem::replace(&mut self.windows, windows)
    }

    pub fn get_window(&self, name: &str) -> Option<&spec::Window> {
        self.windows.get(name)
    }

    // TODO:
    //  1. It's unclear which `PySparkUdfType`s rely on the `arrow_use_large_var_types` config.
    //     While searching through the Spark codebase provides insight into this config's usage,
    //      the relationship remains unclear since we use Arrow for all UDFs.
    //      For now, we're applying this config to all UDFs.
    //      https://github.com/search?q=repo%3Aapache%2Fspark%20%22useLargeVarTypes%22&type=code
    //  2. We are likely overly liberal in setting this flag to `true`.
    //     Evaluate if we are unnecessarily setting this flag to `true` anywhere.

    pub fn config(&self) -> &PlanResolverStateConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut PlanResolverStateConfig {
        &mut self.config
    }

    /// Returns the named parameter value for the given name, if any.
    pub fn get_param_value(&self, name: &str) -> Option<&ScalarValue> {
        self.param_values.get(name)
    }

    /// Returns the positional parameter value at the given 0-based index, if any.
    pub fn get_positional_param_value(&self, index: usize) -> Option<&ScalarValue> {
        self.positional_param_values.get(index)
    }

    /// Enters a scope where the given lambda function parameters are in scope.
    /// The frame is popped when the scope is dropped.
    pub fn enter_lambda_scope(
        &mut self,
        params: Vec<(String, Option<FieldRef>)>,
    ) -> LambdaScope<'_> {
        LambdaScope::new(self, params)
    }

    /// Resolves a name against the in-scope lambda parameters, innermost first.
    /// Returns the declared spelling of the parameter so that the emitted
    /// lambda variable matches the lambda parameter list exactly, along with
    /// the parameter field if known.
    pub fn resolve_lambda_parameter(&self, name: &str) -> Option<(&str, Option<&FieldRef>)> {
        self.lambda_param_scopes
            .iter()
            .rev()
            .find_map(|frame| frame.iter().find(|(p, _)| p.eq_ignore_ascii_case(name)))
            .map(|(p, f)| (p.as_str(), f.as_ref()))
    }

    pub fn in_lambda_scope(&self) -> bool {
        !self.lambda_param_scopes.is_empty()
    }

    /// Replaces the parameters in the plan by the parameter values in scope.
    pub fn bind_param_values(&self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        let plan = if self.positional_param_values.is_empty() {
            plan
        } else {
            plan.with_param_values(ParamValues::from(self.positional_param_values.clone()))?
        };
        if self.param_values.is_empty() {
            Ok(plan)
        } else {
            Ok(plan.with_param_values(ParamValues::from(self.param_values.clone()))?)
        }
    }

    /// Enters a scope where named and positional parameter values are set.
    /// The previous parameter values are restored when the scope is dropped.
    pub fn enter_param_values_scope(
        &mut self,
        named: std::collections::HashMap<String, ScalarValue>,
        positional: Vec<ScalarValue>,
    ) -> ParamValuesScope<'_> {
        ParamValuesScope::new(self, named, positional)
    }
}

/// Scope for parameter values used by IDENTIFIER clause evaluation.
pub(crate) struct ParamValuesScope<'a> {
    state: &'a mut PlanResolverState,
    previous_param_values: std::collections::HashMap<String, ScalarValue>,
    previous_positional_param_values: Vec<ScalarValue>,
}

impl<'a> ParamValuesScope<'a> {
    fn new(
        state: &'a mut PlanResolverState,
        named: std::collections::HashMap<String, ScalarValue>,
        positional: Vec<ScalarValue>,
    ) -> Self {
        let previous_param_values = std::mem::replace(&mut state.param_values, named);
        let previous_positional_param_values =
            std::mem::replace(&mut state.positional_param_values, positional);
        Self {
            state,
            previous_param_values,
            previous_positional_param_values,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for ParamValuesScope<'_> {
    fn drop(&mut self) {
        self.state.param_values = std::mem::take(&mut self.previous_param_values);
        self.state.positional_param_values =
            std::mem::take(&mut self.previous_positional_param_values);
    }
}

pub(crate) struct MissingInputScope<'a> {
    state: &'a mut PlanResolverState,
    previous: Option<MissingInputResolution>,
}

impl MissingInputScope<'_> {
    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for MissingInputScope<'_> {
    fn drop(&mut self) {
        self.state.missing_input_resolution = self.previous.take();
    }
}

pub(crate) struct QueryScope<'a> {
    state: &'a mut PlanResolverState,
    previous_outer_query_schema: Option<DFSchemaRef>,
    previous_aggregate_state: AggregateState,
}

impl<'a> QueryScope<'a> {
    fn new(state: &'a mut PlanResolverState, schema: DFSchemaRef) -> Self {
        // Subqueries cannot recover missing local inputs solely for correlation.
        // TODO: Resolve the filter's local references before its subqueries so that
        // directly recovered inputs become visible to correlation in either order.
        let schema = PlanResolver::local_schema(&schema, state);
        let previous_outer_query_schema = state.outer_query_schema.replace(schema);
        // Select aliases and grouping bindings belong to their own query scope.
        let previous_aggregate_state = std::mem::take(&mut state.aggregate_state);
        Self {
            state,
            previous_outer_query_schema,
            previous_aggregate_state,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for QueryScope<'_> {
    fn drop(&mut self) {
        self.state.outer_query_schema = self.previous_outer_query_schema.take();
        self.state.aggregate_state = std::mem::take(&mut self.previous_aggregate_state);
    }
}

pub(crate) struct AggregateScope<'a> {
    state: &'a mut PlanResolverState,
    previous_aggregate_state: AggregateState,
}

impl<'a> AggregateScope<'a> {
    fn new(state: &'a mut PlanResolverState, aggregate_state: AggregateState) -> Self {
        let previous_aggregate_state =
            std::mem::replace(&mut state.aggregate_state, aggregate_state);
        Self {
            state,
            previous_aggregate_state,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for AggregateScope<'_> {
    fn drop(&mut self) {
        self.state.aggregate_state = std::mem::take(&mut self.previous_aggregate_state);
    }
}

pub(crate) struct CteScope<'a> {
    state: &'a mut PlanResolverState,
    previous_ctes: HashMap<TableReference, Arc<CteInfo>>,
}

impl<'a> CteScope<'a> {
    fn new(state: &'a mut PlanResolverState) -> Self {
        let previous_ctes = state.ctes.clone();
        Self {
            state,
            previous_ctes,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for CteScope<'_> {
    fn drop(&mut self) {
        self.state.ctes = std::mem::take(&mut self.previous_ctes);
    }
}

pub(crate) struct ConfigScope<'a> {
    state: &'a mut PlanResolverState,
    previous_config: PlanResolverStateConfig,
}

impl<'a> ConfigScope<'a> {
    fn new(state: &'a mut PlanResolverState) -> Self {
        let previous_config = state.config.clone();
        Self {
            state,
            previous_config,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for ConfigScope<'_> {
    fn drop(&mut self) {
        self.state.config = std::mem::take(&mut self.previous_config);
    }
}

/// Scope for the parameter names of a lambda function being resolved.
pub(crate) struct LambdaScope<'a> {
    state: &'a mut PlanResolverState,
}

impl<'a> LambdaScope<'a> {
    fn new(state: &'a mut PlanResolverState, params: Vec<(String, Option<FieldRef>)>) -> Self {
        state.lambda_param_scopes.push(params);
        Self { state }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for LambdaScope<'_> {
    fn drop(&mut self) {
        self.state.lambda_param_scopes.pop();
    }
}

/// Scope for WithRelations subquery references.
pub(crate) struct WithRelationsScope<'a> {
    state: &'a mut PlanResolverState,
    previous_subquery_references: HashMap<i64, spec::QueryPlan>,
}

impl<'a> WithRelationsScope<'a> {
    fn new(state: &'a mut PlanResolverState) -> Self {
        let previous_subquery_references = std::mem::take(&mut state.subquery_references);
        Self {
            state,
            previous_subquery_references,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for WithRelationsScope<'_> {
    fn drop(&mut self) {
        self.state.subquery_references = std::mem::take(&mut self.previous_subquery_references);
    }
}
