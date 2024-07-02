use std::collections::HashMap;

use serde_arrow::_impl::arrow::_raw::schema::SchemaRef;

use crate::error::{PlanError, PlanResult};

pub(super) type PlanId = i64;
pub(super) type FieldName = String;
pub(super) type ResolvedFieldName = String;

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    fields: HashMap<ResolvedFieldName, FieldName>,
    attributes: HashMap<(PlanId, FieldName), ResolvedFieldName>,
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
            attributes: HashMap::new(),
        }
    }

    pub fn next_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Registers a field and returns a generated opaque name for the field.
    /// The generated name is unique within the plan resolver state.
    /// No assumption should be made about the format of the name.
    pub fn register_field(&mut self, name: impl Into<FieldName>) -> ResolvedFieldName {
        let resolved = format!("#{}", self.next_id());
        self.fields.insert(resolved.clone(), name.into());
        resolved
    }

    pub fn register_anonymous_field(&mut self) -> ResolvedFieldName {
        self.register_field("")
    }

    pub fn register_fields(&mut self, schema: &SchemaRef) -> Vec<ResolvedFieldName> {
        schema
            .fields()
            .iter()
            .map(|field| self.register_field(field.name()))
            .collect()
    }

    pub fn register_attribute(
        &mut self,
        plan_id: PlanId,
        name: FieldName,
        resolved: ResolvedFieldName,
    ) {
        self.attributes.insert((plan_id, name), resolved);
    }

    pub fn get_field_name(&self, resolved: &str) -> PlanResult<&FieldName> {
        self.fields
            .get(resolved)
            .ok_or_else(|| PlanError::internal(format!("unknown resolved field: {resolved}")))
    }

    pub fn get_field_names(&self, schema: &SchemaRef) -> PlanResult<Vec<FieldName>> {
        schema
            .fields()
            .iter()
            .map(|field| Ok(self.get_field_name(field.name())?.to_string()))
            .collect::<PlanResult<Vec<_>>>()
    }

    pub fn get_resolved_field_name_in_plan(
        &self,
        plan_id: PlanId,
        name: &str,
    ) -> PlanResult<&ResolvedFieldName> {
        self.attributes
            .get(&(plan_id, name.to_string()))
            .ok_or_else(|| {
                PlanError::internal(format!("unknown attribute in plan {plan_id}: {name}"))
            })
    }
}
