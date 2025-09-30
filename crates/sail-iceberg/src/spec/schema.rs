use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::datatypes::{NestedFieldRef, PrimitiveType, StructType, Type};

/// Type alias for schema id.
pub type SchemaId = i32;
/// Reference to [`Schema`].
pub type SchemaRef = Arc<Schema>;
/// Default schema id.
pub const DEFAULT_SCHEMA_ID: SchemaId = 0;

/// Defines schema in iceberg.
#[derive(Debug, Serialize, Clone)]
pub struct Schema {
    #[serde(rename = "type")]
    schema_type: String,
    #[serde(rename = "schema-id")]
    schema_id: SchemaId,
    #[serde(rename = "fields")]
    fields: Vec<NestedFieldRef>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    identifier_field_ids: Option<Vec<i32>>,

    // Internal indexes (not serialized)
    #[serde(skip)]
    struct_type: StructType,
    #[serde(skip)]
    highest_field_id: i32,
    #[serde(skip)]
    id_to_field: HashMap<i32, NestedFieldRef>,
    #[serde(skip)]
    name_to_id: HashMap<String, i32>,
    #[serde(skip)]
    id_to_name: HashMap<i32, String>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.schema_id == other.schema_id
            && self.fields == other.fields
            && self.identifier_field_ids == other.identifier_field_ids
    }
}

impl Eq for Schema {}

#[derive(Deserialize)]
struct SchemaData {
    #[serde(rename = "type")]
    schema_type: String,
    #[serde(rename = "schema-id")]
    schema_id: SchemaId,
    #[serde(rename = "fields")]
    fields: Vec<NestedFieldRef>,
    #[serde(rename = "identifier-field-ids")]
    identifier_field_ids: Option<Vec<i32>>,
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = SchemaData::deserialize(deserializer)?;

        let struct_type = StructType::new(data.fields.clone());
        let mut id_to_field = HashMap::new();
        SchemaBuilder::index_fields_recursive(struct_type.fields(), &mut id_to_field);

        let mut name_to_id = HashMap::new();
        let mut id_to_name = HashMap::new();
        SchemaBuilder::index_names_recursive(
            struct_type.fields(),
            "",
            &mut name_to_id,
            &mut id_to_name,
        );

        let highest_field_id = id_to_field.keys().max().cloned().unwrap_or(0);

        Ok(Schema {
            schema_type: data.schema_type,
            schema_id: data.schema_id,
            fields: data.fields,
            identifier_field_ids: data.identifier_field_ids,
            struct_type,
            highest_field_id,
            id_to_field,
            name_to_id,
            id_to_name,
        })
    }
}

/// Schema builder.
#[derive(Debug)]
pub struct SchemaBuilder {
    schema_id: i32,
    fields: Vec<NestedFieldRef>,
    identifier_field_ids: HashSet<i32>,
}

impl SchemaBuilder {
    /// Add fields to schema builder.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = NestedFieldRef>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Set schema id.
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set identifier field ids.
    pub fn with_identifier_field_ids(mut self, ids: impl IntoIterator<Item = i32>) -> Self {
        self.identifier_field_ids.extend(ids);
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Result<Schema, String> {
        let struct_type = StructType::new(self.fields.clone());
        let id_to_field = self.build_id_to_field_index(&struct_type);

        self.validate_identifier_ids(&id_to_field)?;

        let (name_to_id, id_to_name) = self.build_name_indexes(&struct_type);
        let highest_field_id = id_to_field.keys().max().cloned().unwrap_or(0);

        let identifier_field_ids = if self.identifier_field_ids.is_empty() {
            None
        } else {
            Some(self.identifier_field_ids.into_iter().collect())
        };

        Ok(Schema {
            schema_type: "struct".to_string(),
            schema_id: self.schema_id,
            fields: self.fields,
            identifier_field_ids,
            struct_type,
            highest_field_id,
            id_to_field,
            name_to_id,
            id_to_name,
        })
    }

    fn build_id_to_field_index(&self, struct_type: &StructType) -> HashMap<i32, NestedFieldRef> {
        let mut id_to_field = HashMap::new();
        Self::index_fields_recursive(struct_type.fields(), &mut id_to_field);
        id_to_field
    }

    fn index_fields_recursive(
        fields: &[NestedFieldRef],
        id_to_field: &mut HashMap<i32, NestedFieldRef>,
    ) {
        for field in fields {
            id_to_field.insert(field.id, field.clone());

            match field.field_type.as_ref() {
                Type::Struct(struct_type) => {
                    Self::index_fields_recursive(struct_type.fields(), id_to_field);
                }
                Type::List(list_type) => {
                    id_to_field.insert(list_type.element_field.id, list_type.element_field.clone());
                    if let Type::Struct(struct_type) = list_type.element_field.field_type.as_ref() {
                        Self::index_fields_recursive(struct_type.fields(), id_to_field);
                    }
                }
                Type::Map(map_type) => {
                    id_to_field.insert(map_type.key_field.id, map_type.key_field.clone());
                    id_to_field.insert(map_type.value_field.id, map_type.value_field.clone());
                    if let Type::Struct(struct_type) = map_type.key_field.field_type.as_ref() {
                        Self::index_fields_recursive(struct_type.fields(), id_to_field);
                    }
                    if let Type::Struct(struct_type) = map_type.value_field.field_type.as_ref() {
                        Self::index_fields_recursive(struct_type.fields(), id_to_field);
                    }
                }
                _ => {}
            }
        }
    }

    fn build_name_indexes(
        &self,
        struct_type: &StructType,
    ) -> (HashMap<String, i32>, HashMap<i32, String>) {
        let mut name_to_id = HashMap::new();
        let mut id_to_name = HashMap::new();
        Self::index_names_recursive(struct_type.fields(), "", &mut name_to_id, &mut id_to_name);
        (name_to_id, id_to_name)
    }

    fn index_names_recursive(
        fields: &[NestedFieldRef],
        prefix: &str,
        name_to_id: &mut HashMap<String, i32>,
        id_to_name: &mut HashMap<i32, String>,
    ) {
        for field in fields {
            let full_name = if prefix.is_empty() {
                field.name.clone()
            } else {
                format!("{}.{}", prefix, field.name)
            };

            name_to_id.insert(full_name.clone(), field.id);
            id_to_name.insert(field.id, full_name.clone());

            match field.field_type.as_ref() {
                Type::Struct(struct_type) => {
                    Self::index_names_recursive(
                        struct_type.fields(),
                        &full_name,
                        name_to_id,
                        id_to_name,
                    );
                }
                Type::List(list_type) => {
                    let element_name = format!("{}.element", full_name);
                    name_to_id.insert(element_name.clone(), list_type.element_field.id);
                    id_to_name.insert(list_type.element_field.id, element_name);

                    if let Type::Struct(struct_type) = list_type.element_field.field_type.as_ref() {
                        Self::index_names_recursive(
                            struct_type.fields(),
                            &full_name,
                            name_to_id,
                            id_to_name,
                        );
                    }
                }
                Type::Map(map_type) => {
                    let key_name = format!("{}.key", full_name);
                    let value_name = format!("{}.value", full_name);

                    name_to_id.insert(key_name.clone(), map_type.key_field.id);
                    id_to_name.insert(map_type.key_field.id, key_name);

                    name_to_id.insert(value_name.clone(), map_type.value_field.id);
                    id_to_name.insert(map_type.value_field.id, value_name.clone());

                    if let Type::Struct(struct_type) = map_type.value_field.field_type.as_ref() {
                        Self::index_names_recursive(
                            struct_type.fields(),
                            &value_name,
                            name_to_id,
                            id_to_name,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn validate_identifier_ids(
        &self,
        id_to_field: &HashMap<i32, NestedFieldRef>,
    ) -> Result<(), String> {
        for identifier_field_id in &self.identifier_field_ids {
            let field = id_to_field.get(identifier_field_id).ok_or_else(|| {
                format!("Cannot add identifier field {identifier_field_id}: field does not exist")
            })?;

            if !field.required {
                return Err(format!(
                    "Cannot add identifier field: {} is an optional field",
                    field.name
                ));
            }

            if let Type::Primitive(p) = field.field_type.as_ref() {
                if matches!(p, PrimitiveType::Double | PrimitiveType::Float) {
                    return Err(format!(
                        "Cannot add identifier field {}: cannot be a float or double type",
                        field.name
                    ));
                }
            } else {
                return Err(format!(
                    "Cannot add field {} as an identifier field: not a primitive type field",
                    field.name
                ));
            }
        }

        Ok(())
    }
}

impl Schema {
    /// Create a schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            schema_id: DEFAULT_SCHEMA_ID,
            fields: vec![],
            identifier_field_ids: HashSet::default(),
        }
    }

    /// Get field by field id.
    pub fn field_by_id(&self, field_id: i32) -> Option<&NestedFieldRef> {
        self.id_to_field.get(&field_id)
    }

    /// Get field by field name.
    pub fn field_by_name(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.name_to_id
            .get(field_name)
            .and_then(|id| self.field_by_id(*id))
    }

    /// Returns [`highest_field_id`].
    #[inline]
    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }

    /// Returns [`schema_id`].
    #[inline]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns the struct type representation of this schema.
    pub fn as_struct(&self) -> &StructType {
        &self.struct_type
    }

    /// Returns [`identifier_field_ids`].
    pub fn identifier_field_ids(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.identifier_field_ids
            .as_ref()
            .map(|ids| ids.iter().copied())
            .unwrap_or_else(|| [].iter().copied())
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Get field id by full name.
    pub fn field_id_by_name(&self, name: &str) -> Option<i32> {
        self.name_to_id.get(name).copied()
    }

    /// Get full name by field id.
    pub fn name_by_field_id(&self, field_id: i32) -> Option<&str> {
        self.id_to_name.get(&field_id).map(String::as_str)
    }

    /// Get all fields in the schema.
    pub fn fields(&self) -> &[NestedFieldRef] {
        &self.fields
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "table {{")?;
        for field in &self.fields {
            writeln!(f, "  {}", field)?;
        }
        writeln!(f, "}}")
    }
}
