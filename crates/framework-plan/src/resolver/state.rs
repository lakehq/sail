use std::collections::HashMap;

use serde_arrow::_impl::arrow::_raw::schema::SchemaRef;

use crate::error::{PlanError, PlanResult};

#[derive(Debug, Clone)]
pub(super) struct FieldDescriptor {
    pub name: String,
}

impl FieldDescriptor {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    fields: HashMap<String, FieldDescriptor>,
    tables: HashMap<i64, String>,
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
            tables: HashMap::new(),
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
    pub fn register_field(&mut self, descriptor: FieldDescriptor) -> String {
        let name = format!("#{}", self.next_id());
        self.fields.insert(name.clone(), descriptor);
        name
    }

    pub fn register_anonymous_field(&mut self) -> String {
        format!("#{}", self.next_id())
    }

    pub fn register_schema(&mut self, schema: &SchemaRef) -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|field| self.register_field(FieldDescriptor::new(field.name())))
            .collect()
    }

    pub fn register_anonymous_table(&mut self) -> String {
        format!("#{}", self.next_id())
    }

    pub fn register_table(&mut self, plan_id: i64) -> String {
        let name = format!("#{}", self.next_id());
        self.tables.insert(plan_id, name.clone());
        name
    }

    pub fn field(&self, name: &str) -> Option<&FieldDescriptor> {
        self.fields.get(name)
    }

    pub fn field_or_err(&self, name: &str) -> PlanResult<&str> {
        Ok(self
            .fields
            .get(name)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {name}")))?
            .name
            .as_str())
    }

    pub fn schema_field_names(&self, schema: &SchemaRef) -> PlanResult<Vec<String>> {
        schema
            .fields()
            .iter()
            .map(|field| Ok(self.field_or_err(field.name())?.to_string()))
            .collect::<PlanResult<Vec<_>>>()
    }

    pub fn table(&self, plan_id: i64) -> Option<&str> {
        self.tables.get(&plan_id).map(|s| s.as_str())
    }
}
