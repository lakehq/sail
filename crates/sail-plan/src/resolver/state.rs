use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::{Field, FieldRef};
use datafusion_common::{DFSchemaRef, HashSet, ScalarValue, TableReference};
use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;

pub(super) type PlanAttributeRoots =
    datafusion_common::HashMap<String, Vec<(Option<TableReference>, usize)>>;

/// The field information for fields in the logical plan.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    /// The sorted DataFrame plan IDs, if any, that reference this field.
    /// A contiguous list keeps identity-projection clones cheap.
    plan_ids: Vec<i64>,
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

    fn register_plan_id(&mut self, plan_id: i64) {
        if let Err(index) = self.plan_ids.binary_search(&plan_id) {
            self.plan_ids.insert(index, plan_id);
        }
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    pub fn matches(&self, name: &str, plan_id: Option<i64>) -> bool {
        self.name.eq_ignore_ascii_case(name)
            && match plan_id {
                Some(plan_id) => self.plan_ids.binary_search(&plan_id).is_ok(),
                None => true,
            }
    }
}

#[derive(Debug)]
pub(super) struct CteInfo {
    pub plan: Arc<LogicalPlan>,
    pub definition: bool,
    origins: Vec<usize>,
    bindings: datafusion_common::HashMap<usize, Vec<i64>>,
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
    /// The expressions in the `GROUP BY` clause is being resolved.
    Grouping { projections: Vec<NamedExpr> },
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
struct MissingInputResolution {
    schema: DFSchemaRef,
    local_schema: DFSchemaRef,
    schemas: Vec<DFSchemaRef>,
    resolve_sort_inputs: bool,
}

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    next_origin: usize,
    /// A map from the generated opaque field ID to field information.
    fields: datafusion_common::HashMap<String, FieldInfo>,
    plan_schemas: HashMap<i64, DFSchemaRef>,
    plan_attribute_roots: HashMap<(i64, bool), PlanAttributeRoots>,
    /// The outer query schema for the current subquery.
    outer_query_schema: Option<DFSchemaRef>,
    /// The type-checking schema and ordered name-resolution schemas for expressions
    /// that can recover missing inputs (e.g. a filter predicate).
    missing_input_resolution: Option<MissingInputResolution>,
    /// Outputs whose descendants cannot participate in missing-reference resolution.
    /// Each output is paired with its input so that a pass-through projection that
    /// reproduces the output over a wider input (e.g. a join) is not a boundary.
    missing_input_boundaries: Vec<(DFSchemaRef, Option<DFSchemaRef>)>,
    /// The aggregate state for the current query.
    aggregate_state: AggregateState,
    /// The CTEs for the current query.
    ctes: HashMap<TableReference, Arc<CteInfo>>,
    cte_reference_origins: HashSet<usize>,
    parameter_view_origins: HashSet<usize>,
    /// Unresolved subquery references from a WithRelations node, keyed by plan_id.
    subquery_references: HashMap<i64, spec::QueryPlan>,
    config: PlanResolverStateConfig,
    /// Named parameter values available while resolving a `WithParameters` query node.
    /// These provide placeholder types and support early `IDENTIFIER` evaluation.
    param_values: HashMap<String, ScalarValue>,
    /// Positional parameter values available alongside `param_values`.
    positional_param_values: Vec<ScalarValue>,
    /// Stack of in-scope lambda parameter frames (innermost last).
    /// Each frame holds the declared parameter names of one enclosing lambda
    /// function, along with the parameter field when the enclosing
    /// higher-order function provides it.
    lambda_param_scopes: Vec<Vec<(String, Option<FieldRef>)>>,
    /// The named windows defined in the current query, keyed by window name.
    windows: HashMap<String, spec::Window>,
}

impl Default for PlanResolverState {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanResolverState {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            next_origin: 0,
            fields: datafusion_common::HashMap::new(),
            plan_schemas: HashMap::new(),
            plan_attribute_roots: HashMap::new(),
            outer_query_schema: None,
            missing_input_resolution: None,
            missing_input_boundaries: vec![],
            aggregate_state: AggregateState::default(),
            ctes: HashMap::new(),
            cte_reference_origins: HashSet::new(),
            parameter_view_origins: HashSet::new(),
            subquery_references: HashMap::new(),
            config: PlanResolverStateConfig::default(),
            param_values: HashMap::new(),
            positional_param_values: Vec::new(),
            lambda_param_scopes: Vec::new(),
            windows: HashMap::new(),
        }
    }

    pub fn next_field_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("#{id}")
    }

    fn register_field_info(&mut self, name: impl Into<String>, hidden: bool) -> String {
        let field_id = self.next_field_id();
        let origin = self.next_origin;
        self.next_origin += 1;
        let info = FieldInfo {
            plan_ids: vec![],
            origin,
            name: name.into(),
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
        let info = self.fields.entry(field_id.to_string()).or_insert_with(|| {
            let origin = self.next_origin;
            self.next_origin += 1;
            FieldInfo {
                plan_ids: vec![],
                origin,
                name: String::new(),
                hidden: false,
            }
        });
        info.name = name.into();
        self.plan_attribute_roots.clear();
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
        let info = self
            .fields
            .get_mut(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))?;
        info.register_plan_id(plan_id);
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

    /// Index original roots only when a bound missing-input expression needs
    /// them. Repeated validation must not rescan a wide source for every field.
    pub fn get_plan_attribute_roots(
        &mut self,
        plan_id: i64,
        case_sensitive: bool,
    ) -> PlanResult<Option<&PlanAttributeRoots>> {
        let key = (plan_id, case_sensitive);
        if !self.plan_attribute_roots.contains_key(&key) {
            let Some(schema) = self.plan_schemas.get(&plan_id) else {
                return Ok(None);
            };
            let mut roots = PlanAttributeRoots::with_capacity(schema.fields().len());
            for (qualifier, field) in schema.iter() {
                let info = self.get_field_info(field.name())?;
                if !info.hidden {
                    let name = if case_sensitive {
                        info.name.clone()
                    } else {
                        info.name.to_ascii_lowercase()
                    };
                    roots
                        .entry(name)
                        .or_default()
                        .push((qualifier.cloned(), info.origin));
                }
            }
            self.plan_attribute_roots.insert(key, roots);
        }
        Ok(self.plan_attribute_roots.get(&key))
    }

    pub fn get_field_info(&self, field_id: &str) -> PlanResult<&FieldInfo> {
        self.fields
            .get(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))
    }

    pub fn get_outer_query_schema(&self) -> Option<&DFSchemaRef> {
        self.outer_query_schema.as_ref()
    }

    pub fn get_missing_input_schemas(&self, schema: &DFSchemaRef) -> Option<&[DFSchemaRef]> {
        self.missing_input_resolution
            .as_ref()
            .filter(|input| Arc::ptr_eq(&input.schema, schema))
            .map(|input| input.schemas.as_slice())
    }

    /// Returns the operator's own input schema when `schema` is the type-checking schema
    /// for missing-input resolution. Expansions such as `*` only see this schema.
    pub fn get_local_schema(&self, schema: &DFSchemaRef) -> DFSchemaRef {
        self.missing_input_resolution
            .as_ref()
            .filter(|input| Arc::ptr_eq(&input.schema, schema))
            .map_or_else(
                || Arc::clone(schema),
                |input| Arc::clone(&input.local_schema),
            )
    }

    /// Discards the bindings to one output when resolution against it fails.
    /// Sorts can discard their own output; other operators can only discard descendants.
    /// Returns whether the output is discarded.
    pub fn discard_missing_input_schema(&mut self, schema: &DFSchemaRef, index: usize) -> bool {
        if let Some(input) = &mut self.missing_input_resolution
            && Arc::ptr_eq(&input.schema, schema)
            && (index > 0 || input.resolve_sort_inputs)
            && index < input.schemas.len()
        {
            input.schemas.remove(index);
            return true;
        }
        false
    }

    /// Discards the bindings to one output and all deeper outputs.
    pub fn discard_missing_input_schemas_from(&mut self, schema: &DFSchemaRef, index: usize) {
        if let Some(input) = &mut self.missing_input_resolution
            && Arc::ptr_eq(&input.schema, schema)
            && (index > 0 || input.resolve_sort_inputs)
        {
            input.schemas.truncate(index);
        }
    }

    pub fn register_missing_input_boundary(&mut self, plan: &LogicalPlan) {
        let mut plan = plan;
        // Empty outputs can share a schema with unrelated plans. Stop recovery at
        // the first nonempty input instead, whose field IDs distinguish the boundary.
        while plan.schema().fields().is_empty() {
            let Some(child) = PlanResolver::missing_input_child(plan, self) else {
                return;
            };
            if !child.schema().fields().is_empty() {
                break;
            }
            plan = child;
        }
        self.missing_input_boundaries.push((
            Arc::clone(plan.schema()),
            plan.inputs()
                .first()
                .map(|input| Arc::clone(input.schema())),
        ));
    }

    pub fn is_missing_input_boundary(&self, plan: &LogicalPlan) -> bool {
        // Rewriters can rebuild schemas with different types or nullability.
        // Retain the original schemas cheaply, but compare only column identities.
        let matches = |left: &DFSchemaRef, right: &DFSchemaRef| {
            Arc::ptr_eq(left, right)
                || (left.fields().len() == right.fields().len()
                    && left
                        .iter()
                        .map(|(qualifier, field)| (qualifier, field.name()))
                        .eq(right
                            .iter()
                            .map(|(qualifier, field)| (qualifier, field.name()))))
        };
        self.missing_input_boundaries.iter().any(|(output, input)| {
            matches(output, plan.schema())
                && match (input, plan.inputs().first()) {
                    (Some(schema), Some(child)) => matches(schema, child.schema()),
                    (None, None) => true,
                    _ => false,
                }
        })
    }

    pub fn enter_missing_input_scope(
        &mut self,
        schema: DFSchemaRef,
        schemas: Vec<DFSchemaRef>,
        resolve_sort_inputs: bool,
    ) -> MissingInputScope<'_> {
        let local_schema = schemas
            .first()
            .cloned()
            .unwrap_or_else(|| Arc::clone(&schema));
        let previous = self
            .missing_input_resolution
            .replace(MissingInputResolution {
                schema,
                local_schema,
                schemas,
                resolve_sort_inputs,
            });
        MissingInputScope {
            state: self,
            previous,
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
        definition: bool,
    ) -> PlanResult<()> {
        let mut origins = Vec::with_capacity(plan.schema().fields().len());
        let mut bindings = datafusion_common::HashMap::new();
        for field in plan.schema().fields() {
            let info = self.get_field_info(field.name())?;
            origins.push(info.origin);
            if !info.plan_ids.is_empty() {
                if bindings.is_empty() {
                    bindings.reserve(plan.schema().fields().len());
                }
                bindings.insert(info.origin, info.plan_ids.clone());
            }
        }
        self.ctes.insert(
            table_ref,
            Arc::new(CteInfo {
                plan: Arc::new(plan),
                definition,
                origins,
                bindings,
            }),
        );
        Ok(())
    }

    /// Spark renews repeated CTERelationRef and parameter-view attributes
    /// separately. Identity projections preserve the original attribute IDs.
    pub fn renew_cte_reference(&mut self, cte: &CteInfo) -> PlanResult<Option<Vec<String>>> {
        let origins = if cte.definition {
            &mut self.cte_reference_origins
        } else {
            &mut self.parameter_view_origins
        };
        if !cte.origins.iter().any(|origin| origins.contains(origin)) {
            // SQL outputs may acquire a DataFrame plan ID only after this query
            // finishes. Their repeated CTE references already need fresh identities.
            origins.extend(cte.origins.iter().copied());
            return Ok(None);
        }
        // One saved binding entry per output proves all origins are distinct.
        // Bound CTEs and parameter views usually satisfy this without another scan.
        let distinct = cte.bindings.len() == cte.origins.len();
        let mut renewed =
            datafusion_common::HashMap::with_capacity(if distinct { 0 } else { cte.origins.len() });
        let mut names = Vec::with_capacity(cte.origins.len());
        for (field, &original) in cte.plan.schema().fields().iter().zip(&cte.origins) {
            let info = self.get_field_info(field.name())?;
            let (name, hidden) = (info.name.clone(), info.hidden);
            // Spark's CTERelationRef.newInstance preserves duplicate attributes
            // within one output while giving the reference fresh identities.
            let origin = if distinct {
                let origin = self.next_origin;
                self.next_origin += 1;
                origin
            } else {
                *renewed.entry(original).or_insert_with(|| {
                    let origin = self.next_origin;
                    self.next_origin += 1;
                    origin
                })
            };
            let plan_ids = if cte.definition {
                vec![]
            } else {
                cte.bindings.get(&original).cloned().unwrap_or_default()
            };
            let field_id = self.next_field_id();
            self.fields.insert(
                field_id.clone(),
                FieldInfo {
                    name,
                    hidden,
                    origin,
                    plan_ids,
                },
            );
            if !cte.definition {
                self.parameter_view_origins.insert(origin);
            }
            names.push(field_id);
        }
        Ok(Some(names))
    }

    /// A Union is a plan-ID lookup leaf, but WithCTE also exposes its definitions.
    /// Restore only bindings whose original attributes survive in the output.
    pub fn restore_cte_output_bindings(&mut self, plan: &LogicalPlan) -> PlanResult<()> {
        let definitions = self
            .ctes
            .values()
            .filter(|cte| cte.definition && !cte.bindings.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        if definitions.is_empty() {
            return Ok(());
        }
        for field in plan.schema().fields() {
            let info = self
                .fields
                .get_mut(field.name())
                .ok_or_else(|| PlanError::internal(format!("unknown field: {}", field.name())))?;
            for cte in &definitions {
                for &plan_id in cte.bindings.get(&info.origin).into_iter().flatten() {
                    info.register_plan_id(plan_id);
                }
            }
        }
        Ok(())
    }

    pub fn register_identity_field(&mut self, name: String, source: &str) -> PlanResult<String> {
        let source = self.get_field_info(source)?;
        let info = FieldInfo {
            name,
            plan_ids: source.plan_ids.clone(),
            origin: source.origin,
            hidden: false,
        };
        let field_id = self.next_field_id();
        self.fields.insert(field_id.clone(), info);
        Ok(field_id)
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
        windows: HashMap<String, spec::Window>,
    ) -> HashMap<String, spec::Window> {
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

    /// Enters a scope where named and positional parameter values are set.
    /// The previous parameter values are restored when the scope is dropped.
    pub fn enter_param_values_scope(
        &mut self,
        named: HashMap<String, ScalarValue>,
        positional: Vec<ScalarValue>,
    ) -> ParamValuesScope<'_> {
        ParamValuesScope::new(self, named, positional)
    }
}

/// Scope for parameter values used by IDENTIFIER clause evaluation.
pub(crate) struct ParamValuesScope<'a> {
    state: &'a mut PlanResolverState,
    previous_param_values: HashMap<String, ScalarValue>,
    previous_positional_param_values: Vec<ScalarValue>,
}

impl<'a> ParamValuesScope<'a> {
    fn new(
        state: &'a mut PlanResolverState,
        named: HashMap<String, ScalarValue>,
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
}

impl<'a> QueryScope<'a> {
    fn new(state: &'a mut PlanResolverState, schema: DFSchemaRef) -> Self {
        // Subqueries cannot recover missing local inputs solely for correlation.
        // TODO: Resolve the filter's local references before its subqueries so that
        // directly recovered inputs become visible to correlation in either order.
        let schema = state.get_local_schema(&schema);
        let previous_outer_query_schema = state.outer_query_schema.replace(schema);
        Self {
            state,
            previous_outer_query_schema,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for QueryScope<'_> {
    fn drop(&mut self) {
        self.state.outer_query_schema = self.previous_outer_query_schema.take();
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
