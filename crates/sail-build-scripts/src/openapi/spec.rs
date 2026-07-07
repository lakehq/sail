//! Rust types for the OpenAPI specification.
//!
//! The types are compatible with a subset of OpenAPI 3.0 and 3.1 (which has a few breaking changes
//! between them). The types are designed to support client code generation for REST services
//! used in the codebase (e.g., Unity Catalog and Iceberg REST catalog).
//! The types can be extended if more OpenAPI features are needed in the future.
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::de::{self, IgnoredAny, Visitor};
use serde::Deserialize;

use crate::error::BuildResult;

pub fn load_spec(path: impl AsRef<Path>) -> BuildResult<OpenApi> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

pub type ObjectMap<T> = BTreeMap<String, T>;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenApi {
    pub openapi: String,
    pub info: Info,
    #[serde(default)]
    pub servers: Vec<Server>,
    #[serde(default)]
    pub tags: Vec<Tag>,
    #[serde(default)]
    pub paths: ObjectMap<MaybeRef<PathItem>>,
    #[serde(default)]
    pub components: Components,
    #[serde(default)]
    pub security: Option<AnyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Info {
    pub title: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub license: Option<License>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct License {
    pub name: String,
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub url: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub variables: ObjectMap<ServerVariable>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerVariable {
    pub default: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tag {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Components {
    #[serde(default)]
    pub schemas: ObjectMap<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub parameters: ObjectMap<MaybeRef<Parameter>>,
    #[serde(default)]
    pub responses: ObjectMap<MaybeRef<Response>>,
    #[serde(default)]
    pub examples: ObjectMap<AnyValue>,
    #[serde(default, rename = "securitySchemes")]
    pub security_schemes: ObjectMap<AnyValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathItem {
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Vec<MaybeRef<Parameter>>,
    #[serde(default)]
    pub get: Option<Operation>,
    #[serde(default)]
    pub put: Option<Operation>,
    #[serde(default)]
    pub post: Option<Operation>,
    #[serde(default)]
    pub delete: Option<Operation>,
    #[serde(default)]
    pub options: Option<Operation>,
    #[serde(default)]
    pub head: Option<Operation>,
    #[serde(default)]
    pub patch: Option<Operation>,
    #[serde(default)]
    pub trace: Option<Operation>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, rename = "operationId")]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub parameters: Vec<MaybeRef<Parameter>>,
    #[serde(default, rename = "requestBody")]
    pub request_body: Option<MaybeRef<RequestBody>>,
    #[serde(default, deserialize_with = "deserialize_string_key_map")]
    pub responses: ObjectMap<MaybeRef<Response>>,
    #[serde(default)]
    pub deprecated: Option<bool>,
    #[serde(default)]
    pub security: Option<AnyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "in")]
    pub location: ParameterLocation,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required: Option<bool>,
    #[serde(default)]
    pub schema: Option<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub style: Option<String>,
    #[serde(default)]
    pub explode: Option<bool>,
    #[serde(default, rename = "allowEmptyValue")]
    pub allow_empty_value: Option<bool>,
    #[serde(default)]
    pub example: Option<AnyValue>,
    #[serde(default)]
    pub examples: ObjectMap<AnyValue>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterLocation {
    Query,
    Header,
    Path,
    Cookie,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBody {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub content: ObjectMap<MediaType>,
    #[serde(default)]
    pub required: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub description: String,
    #[serde(default)]
    pub content: ObjectMap<MediaType>,
    #[serde(default)]
    pub headers: ObjectMap<MaybeRef<Header>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required: Option<bool>,
    #[serde(default)]
    pub deprecated: Option<bool>,
    #[serde(default, rename = "allowEmptyValue")]
    pub allow_empty_value: Option<bool>,
    #[serde(default)]
    pub style: Option<String>,
    #[serde(default)]
    pub explode: Option<bool>,
    #[serde(default)]
    pub schema: Option<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub example: Option<AnyValue>,
    #[serde(default)]
    pub examples: ObjectMap<AnyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaType {
    #[serde(default)]
    pub schema: Option<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub example: Option<AnyValue>,
    #[serde(default)]
    pub examples: ObjectMap<AnyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    #[serde(
        default,
        rename = "type",
        deserialize_with = "deserialize_schema_types"
    )]
    pub r#type: Option<Vec<SchemaType>>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub default: Option<AnyValue>,
    #[serde(default)]
    pub example: Option<AnyValue>,
    #[serde(default, rename = "enum")]
    pub enum_values: Vec<String>,
    #[serde(default, rename = "const")]
    pub const_value: Option<AnyValue>,
    #[serde(default)]
    pub properties: ObjectMap<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub items: Option<Box<MaybeRef<Schema, SchemaReference>>>,
    #[serde(default, rename = "additionalProperties")]
    pub additional_properties: Option<AdditionalProperties>,
    #[serde(default, rename = "allOf")]
    pub all_of: Vec<MaybeRef<Schema, SchemaReference>>,
    #[serde(default, rename = "oneOf")]
    pub one_of: Vec<MaybeRef<Schema, SchemaReference>>,
    #[serde(default, rename = "anyOf")]
    pub any_of: Vec<MaybeRef<Schema, SchemaReference>>,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default)]
    pub discriminator: Option<Discriminator>,
    #[serde(default)]
    pub deprecated: Option<bool>,
    #[serde(default, rename = "readOnly")]
    pub read_only: Option<bool>,
    #[serde(default)]
    pub minimum: Option<AnyValue>,
    #[serde(default)]
    pub maximum: Option<AnyValue>,
    #[serde(default, rename = "minLength")]
    pub min_length: Option<u64>,
    #[serde(default, rename = "maxLength")]
    pub max_length: Option<u64>,
    #[serde(default, rename = "maxItems")]
    pub max_items: Option<u64>,
    #[serde(default, rename = "uniqueItems")]
    pub unique_items: Option<bool>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default, rename = "contentEncoding")]
    pub content_encoding: Option<String>,
    #[serde(default, rename = "x-enum-descriptions")]
    pub x_enum_descriptions: Option<AnyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AdditionalProperties {
    Bool(bool),
    Schema(Box<MaybeRef<Schema, SchemaReference>>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discriminator {
    #[serde(rename = "propertyName")]
    pub property_name: String,
    #[serde(default)]
    pub mapping: ObjectMap<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    Null,
    Boolean,
    Object,
    Array,
    Number,
    String,
    Integer,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MaybeRef<T, R = Reference> {
    Ref(R),
    Value(T),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    #[serde(rename = "$ref")]
    pub reference: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaReference {
    #[serde(rename = "$ref")]
    pub reference: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, rename = "const")]
    pub const_value: Option<AnyValue>,
    #[serde(default, rename = "enum")]
    pub enum_values: Vec<String>,
}

/// A placeholder type for any value that we do not care about during deserialization.
#[derive(Debug, Clone)]
pub struct AnyValue;

impl<'de> Deserialize<'de> for AnyValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        IgnoredAny::deserialize(deserializer)?;
        Ok(Self)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

fn deserialize_schema_types<'de, D>(deserializer: D) -> Result<Option<Vec<SchemaType>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(
        Option::<OneOrMany<SchemaType>>::deserialize(deserializer)?.map(|value| match value {
            OneOrMany::One(value) => vec![value],
            OneOrMany::Many(value) => value,
        }),
    )
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct StringKey(String);

impl<'de> Deserialize<'de> for StringKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StringKeyVisitor)
    }
}

struct StringKeyVisitor;

impl Visitor<'_> for StringKeyVisitor {
    type Value = StringKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string or integer map key")
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringKey(value.to_string()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringKey(value.to_string()))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringKey(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringKey(value))
    }
}

fn deserialize_string_key_map<'de, D, T>(deserializer: D) -> Result<ObjectMap<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    let values = BTreeMap::<StringKey, T>::deserialize(deserializer)?;
    Ok(values
        .into_iter()
        .map(|(StringKey(key), value)| (key, value))
        .collect())
}
