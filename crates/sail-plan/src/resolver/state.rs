use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::arrow::datatypes::Field;
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_expr::LogicalPlan;

use crate::error::{PlanError, PlanResult};

/// The field information for fields in the logical plan.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    /// The set of plan IDs, if any, that reference this field.
    plan_ids: HashSet<i64>,
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

    pub fn plan_ids(&self) -> Vec<i64> {
        self.plan_ids.iter().copied().collect()
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
}

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    /// A map from the generated opaque field ID to field information.
    fields: HashMap<String, FieldInfo>,
    /// The outer query schema for the current subquery.
    outer_query_schema: Option<DFSchemaRef>,
    /// The CTEs for the current query.
    ctes: HashMap<TableReference, Arc<LogicalPlan>>,
    config: PlanResolverStateConfig,
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
            fields: HashMap::new(),
            outer_query_schema: None,
            ctes: HashMap::new(),
            config: PlanResolverStateConfig::default(),
        }
    }

    fn next_field_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("#{id}")
    }

    fn register_field_info(&mut self, name: impl Into<String>, hidden: bool) -> String {
        let field_id = self.next_field_id();
        let info = FieldInfo {
            plan_ids: HashSet::new(),
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

    pub fn register_field(&mut self, field: impl AsRef<Field>) -> String {
        self.register_field_info(field.as_ref().name(), false)
    }

    pub fn register_fields(
        &mut self,
        fields: impl IntoIterator<Item = impl AsRef<Field>>,
    ) -> Vec<String> {
        fields
            .into_iter()
            .map(|field| self.register_field(field))
            .collect()
    }

    pub fn register_plan_id_for_field(&mut self, field_id: &str, plan_id: i64) -> PlanResult<()> {
        let field_info = self
            .fields
            .get_mut(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))?;
        field_info.plan_ids.insert(plan_id);
        Ok(())
    }

    pub fn get_field_info(&self, field_id: &str) -> PlanResult<&FieldInfo> {
        self.fields
            .get(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))
    }

    pub fn get_outer_query_schema(&self) -> Option<&DFSchemaRef> {
        self.outer_query_schema.as_ref()
    }

    pub fn enter_query_scope(&mut self, schema: DFSchemaRef) -> QueryScope {
        QueryScope::new(self, schema)
    }

    pub fn enter_cte_scope(&mut self) -> CteScope {
        CteScope::new(self)
    }

    pub fn get_cte(&self, table_ref: &TableReference) -> Option<&LogicalPlan> {
        self.ctes.get(table_ref).map(|cte| cte.as_ref())
    }

    pub fn insert_cte(&mut self, table_ref: TableReference, plan: LogicalPlan) {
        self.ctes.insert(table_ref, Arc::new(plan));
    }

    pub fn enter_config_scope(&mut self) -> ConfigScope {
        ConfigScope::new(self)
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
}

pub(crate) struct QueryScope<'a> {
    state: &'a mut PlanResolverState,
    previous_outer_query_schema: Option<DFSchemaRef>,
}

impl<'a> QueryScope<'a> {
    fn new(state: &'a mut PlanResolverState, schema: DFSchemaRef) -> Self {
        let previous_outer_query_schema =
            std::mem::replace(&mut state.outer_query_schema, Some(schema));
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

pub(crate) struct CteScope<'a> {
    state: &'a mut PlanResolverState,
    previous_ctes: HashMap<TableReference, Arc<LogicalPlan>>,
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
