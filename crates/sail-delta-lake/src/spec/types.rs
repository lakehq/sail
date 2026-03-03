// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::iter::{DoubleEndedIterator, FusedIterator};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::error::{DeltaError as DeltaTableError, DeltaResult};

pub type Schema = StructType;
/// Arrow-native schema reference, replacing the previous `Arc<StructType>`.
pub type SchemaRef = ArrowSchemaRef;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged)]
pub enum MetadataValue {
    Number(i64),
    String(String),
    Boolean(bool),
    Other(serde_json::Value),
}

impl Display for MetadataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Other(v) => write!(f, "{v}"),
        }
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for MetadataValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for MetadataValue {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

#[derive(Debug)]
pub enum ColumnMetadataKey {
    ColumnMappingId,
    ColumnMappingPhysicalName,
    ParquetFieldId,
    GenerationExpression,
    IdentityStart,
    IdentityStep,
    IdentityHighWaterMark,
    IdentityAllowExplicitInsert,
    InternalColumn,
    Invariants,
    MetadataSpec,
}

impl AsRef<str> for ColumnMetadataKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ColumnMappingId => "delta.columnMapping.id",
            Self::ColumnMappingPhysicalName => "delta.columnMapping.physicalName",
            Self::ParquetFieldId => "parquet.field.id",
            Self::GenerationExpression => "delta.generationExpression",
            Self::IdentityAllowExplicitInsert => "delta.identity.allowExplicitInsert",
            Self::IdentityHighWaterMark => "delta.identity.highWaterMark",
            Self::IdentityStart => "delta.identity.start",
            Self::IdentityStep => "delta.identity.step",
            Self::InternalColumn => "delta.isInternalColumn",
            Self::Invariants => "delta.invariants",
            Self::MetadataSpec => "delta.metadataSpec",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    #[default]
    None,
    Name,
    Id,
}

impl AsRef<str> for ColumnMappingMode {
    fn as_ref(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Name => "name",
            Self::Id => "id",
        }
    }
}

impl TryFrom<&str> for ColumnMappingMode {
    type Error = DeltaTableError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "name" => Ok(Self::Name),
            "id" => Ok(Self::Id),
            other => Err(DeltaTableError::generic(format!(
                "Invalid column mapping mode: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ColumnName {
    path: Vec<String>,
}

impl ColumnName {
    pub fn new<A>(iter: impl IntoIterator<Item = A>) -> Self
    where
        Self: FromIterator<A>,
    {
        iter.into_iter().collect()
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }

    pub fn into_inner(self) -> Vec<String> {
        self.path
    }

    pub fn parse_column_name_list(names: impl AsRef<str>) -> DeltaResult<Vec<ColumnName>> {
        let mut result = Vec::new();
        let raw = names.as_ref().trim();
        if raw.is_empty() {
            return Ok(result);
        }

        for column in raw.split(',') {
            let column = column.trim();
            if column.is_empty() {
                continue;
            }
            let path = column
                .split('.')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|segment| {
                    segment
                        .trim_matches('`')
                        .replace("``", "`")
                        .trim()
                        .to_string()
                })
                .collect::<Vec<_>>();
            if path.is_empty() {
                return Err(DeltaTableError::generic(format!(
                    "invalid column name list: {raw}"
                )));
            }
            result.push(Self { path });
        }
        Ok(result)
    }
}

impl<A: Into<String>> FromIterator<A> for ColumnName {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let path = iter.into_iter().map(Into::into).collect();
        Self { path }
    }
}

impl FromIterator<ColumnName> for ColumnName {
    fn from_iter<T: IntoIterator<Item = ColumnName>>(iter: T) -> Self {
        let path = iter.into_iter().flat_map(|c| c.into_iter()).collect();
        Self { path }
    }
}

impl IntoIterator for ColumnName {
    type Item = String;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl AsRef<[String]> for ColumnName {
    fn as_ref(&self) -> &[String] {
        &self.path
    }
}

impl Deref for ColumnName {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

impl DecimalType {
    pub fn try_new(precision: u8, scale: u8) -> DeltaResult<Self> {
        if !(1..=38).contains(&precision) {
            return Err(DeltaTableError::generic(format!(
                "precision must be in range 1..=38, found {precision}"
            )));
        }
        if scale > precision {
            return Err(DeltaTableError::generic(format!(
                "scale must be <= precision ({precision}), found {scale}"
            )));
        }
        Ok(Self { precision, scale })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    Boolean,
    Binary,
    Date,
    Timestamp,
    #[serde(rename = "timestamp_ntz")]
    TimestampNtz,
    #[serde(
        serialize_with = "serialize_decimal",
        deserialize_with = "deserialize_decimal",
        untagged
    )]
    Decimal(DecimalType),
}

impl PrimitiveType {
    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(Self::Decimal(DecimalType::try_new(precision, scale)?))
    }

    pub fn data_type(&self) -> DataType {
        DataType::Primitive(self.clone())
    }
}

fn serialize_decimal<S: serde::Serializer>(
    dtype: &DecimalType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("decimal({},{})", dtype.precision(), dtype.scale()))
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<DecimalType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if !(value.starts_with("decimal(") && value.ends_with(')')) {
        return Err(serde::de::Error::custom(format!(
            "Invalid decimal: {value}"
        )));
    }
    let mut parts = value[8..value.len() - 1].split(',');
    let precision = parts
        .next()
        .and_then(|v| v.trim().parse::<u8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid precision in decimal: {value}"))
        })?;
    let scale = parts
        .next()
        .and_then(|v| v.trim().parse::<u8>().ok())
        .ok_or_else(|| serde::de::Error::custom(format!("Invalid scale in decimal: {value}")))?;
    DecimalType::try_new(precision, scale).map_err(serde::de::Error::custom)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StructField {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, MetadataValue>,
}

impl StructField {
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    pub fn nullable(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, true)
    }

    pub fn not_null(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, false)
    }

    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    pub fn add_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata
            .extend(metadata.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    pub fn get_config_value(&self, key: &ColumnMetadataKey) -> Option<&MetadataValue> {
        self.metadata.get(key.as_ref())
    }

    pub fn physical_name(&self, column_mapping_mode: ColumnMappingMode) -> &str {
        match column_mapping_mode {
            ColumnMappingMode::None => &self.name,
            ColumnMappingMode::Id | ColumnMappingMode::Name => self
                .metadata
                .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                .and_then(|v| match v {
                    MetadataValue::String(v) => Some(v.as_str()),
                    _ => None,
                })
                .unwrap_or(&self.name),
        }
    }

    pub fn with_name(&self, new_name: impl Into<String>) -> Self {
        Self {
            name: new_name.into(),
            data_type: self.data_type.clone(),
            nullable: self.nullable,
            metadata: self.metadata.clone(),
        }
    }

    #[inline]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, MetadataValue> {
        &self.metadata
    }

    pub fn make_physical(&self, column_mapping_mode: ColumnMappingMode) -> Self {
        make_physical_field(self, column_mapping_mode)
    }
}

impl Display for StructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (is nullable: {})",
            self.name, self.data_type, self.nullable
        )
    }
}

fn make_physical_field(field: &StructField, column_mapping_mode: ColumnMappingMode) -> StructField {
    let data_type = match &field.data_type {
        DataType::Struct(inner) => DataType::from(inner.make_physical(column_mapping_mode)),
        other => other.clone(),
    };

    let mut metadata = field.metadata().clone();
    let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    let field_id_key = ColumnMetadataKey::ColumnMappingId.as_ref();
    let parquet_field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();

    match column_mapping_mode {
        ColumnMappingMode::Id => {
            if let Some(MetadataValue::Number(fid)) = metadata.get(field_id_key) {
                let fid = *fid;
                metadata.insert(parquet_field_id_key.to_string(), MetadataValue::Number(fid));
            }
        }
        ColumnMappingMode::Name => {
            metadata.remove(field_id_key);
            metadata.remove(parquet_field_id_key);
        }
        ColumnMappingMode::None => {
            metadata.remove(physical_name_key);
            metadata.remove(field_id_key);
            metadata.remove(parquet_field_id_key);
        }
    }

    let name = match column_mapping_mode {
        ColumnMappingMode::None => field.name().to_owned(),
        ColumnMappingMode::Id | ColumnMappingMode::Name => {
            field.physical_name(column_mapping_mode).to_owned()
        }
    };

    StructField {
        name,
        data_type,
        nullable: field.nullable,
        metadata,
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct StructType {
    type_name: String,
    fields: IndexMap<String, StructField>,
}

impl StructType {
    pub fn try_new(fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        let mut field_map = IndexMap::new();
        for field in fields {
            if let Some(dup) = field_map.insert(field.name.clone(), field) {
                return Err(DeltaTableError::schema(format!(
                    "Duplicate field name: {}",
                    dup.name
                )));
            }
        }
        Ok(Self {
            type_name: "struct".into(),
            fields: field_map,
        })
    }

    pub fn try_from_results<E: Into<DeltaTableError>>(
        fields: impl IntoIterator<Item = Result<StructField, E>>,
    ) -> DeltaResult<Self> {
        let fields = fields
            .into_iter()
            .map(|f| f.map_err(Into::into))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(fields)
    }

    pub fn new_unchecked(fields: impl IntoIterator<Item = StructField>) -> Self {
        let mut field_map = IndexMap::new();
        for field in fields {
            field_map.insert(field.name.clone(), field);
        }
        Self {
            type_name: "struct".into(),
            fields: field_map,
        }
    }

    pub fn field(&self, name: impl AsRef<str>) -> Option<&StructField> {
        self.fields.get(name.as_ref())
    }

    pub fn fields(
        &self,
    ) -> impl ExactSizeIterator<Item = &StructField> + DoubleEndedIterator + FusedIterator {
        self.fields.values()
    }

    pub fn into_fields(
        self,
    ) -> impl ExactSizeIterator<Item = StructField> + DoubleEndedIterator + FusedIterator {
        self.fields.into_values()
    }

    pub fn make_physical(&self, column_mapping_mode: ColumnMappingMode) -> Self {
        let fields = self
            .fields()
            .map(|field| field.make_physical(column_mapping_mode));
        Self::new_unchecked(fields)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructTypeSerDeHelper {
    #[serde(rename = "type")]
    type_name: String,
    fields: Vec<StructField>,
}

impl Serialize for StructType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StructTypeSerDeHelper {
            type_name: self.type_name.clone(),
            fields: self.fields.values().cloned().collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = StructTypeSerDeHelper::deserialize(deserializer)?;
        StructType::try_new(helper.fields).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    #[serde(rename = "type")]
    pub type_name: String,
    pub element_type: DataType,
    pub contains_null: bool,
}

impl ArrayType {
    pub fn new(element_type: DataType, contains_null: bool) -> Self {
        Self {
            type_name: "array".into(),
            element_type,
            contains_null,
        }
    }

    #[inline]
    pub const fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[inline]
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: String,
    pub key_type: DataType,
    pub value_type: DataType,
    #[serde(default = "default_true")]
    pub value_contains_null: bool,
}

impl MapType {
    pub fn new(
        key_type: impl Into<DataType>,
        value_type: impl Into<DataType>,
        value_contains_null: bool,
    ) -> Self {
        Self {
            type_name: "map".into(),
            key_type: key_type.into(),
            value_type: value_type.into(),
            value_contains_null,
        }
    }

    #[inline]
    pub const fn key_type(&self) -> &DataType {
        &self.key_type
    }

    #[inline]
    pub const fn value_type(&self) -> &DataType {
        &self.value_type
    }

    #[inline]
    pub const fn value_contains_null(&self) -> bool {
        self.value_contains_null
    }
}

fn default_true() -> bool {
    true
}

fn serialize_variant<S: serde::Serializer>(
    _: &StructType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("variant")
}

fn deserialize_variant<'de, D>(deserializer: D) -> Result<Box<StructType>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value != "variant" {
        return Err(serde::de::Error::custom(format!(
            "Invalid variant: {value}"
        )));
    }
    match DataType::unshredded_variant() {
        DataType::Variant(st) => Ok(st),
        _ => Err(serde::de::Error::custom(
            "unable to construct variant schema".to_string(),
        )),
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
pub enum DataType {
    Primitive(PrimitiveType),
    Array(Box<ArrayType>),
    Struct(Box<StructType>),
    Map(Box<MapType>),
    #[serde(
        serialize_with = "serialize_variant",
        deserialize_with = "deserialize_variant"
    )]
    Variant(Box<StructType>),
}

impl DataType {
    pub const STRING: Self = Self::Primitive(PrimitiveType::String);
    pub const LONG: Self = Self::Primitive(PrimitiveType::Long);
    pub const INTEGER: Self = Self::Primitive(PrimitiveType::Integer);
    pub const SHORT: Self = Self::Primitive(PrimitiveType::Short);
    pub const BYTE: Self = Self::Primitive(PrimitiveType::Byte);
    pub const FLOAT: Self = Self::Primitive(PrimitiveType::Float);
    pub const DOUBLE: Self = Self::Primitive(PrimitiveType::Double);
    pub const BOOLEAN: Self = Self::Primitive(PrimitiveType::Boolean);
    pub const BINARY: Self = Self::Primitive(PrimitiveType::Binary);
    pub const DATE: Self = Self::Primitive(PrimitiveType::Date);
    pub const TIMESTAMP: Self = Self::Primitive(PrimitiveType::Timestamp);
    pub const TIMESTAMP_NTZ: Self = Self::Primitive(PrimitiveType::TimestampNtz);

    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(PrimitiveType::decimal(precision, scale)?.into())
    }

    pub fn try_struct_type(fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        Ok(StructType::try_new(fields)?.into())
    }

    pub fn try_struct_type_from_results<E: Into<DeltaTableError>>(
        fields: impl IntoIterator<Item = Result<StructField, E>>,
    ) -> DeltaResult<Self> {
        StructType::try_from_results(fields).map(Self::from)
    }

    pub fn struct_type_unchecked(fields: impl IntoIterator<Item = StructField>) -> Self {
        StructType::new_unchecked(fields).into()
    }

    pub fn unshredded_variant() -> Self {
        Self::Variant(Box::new(StructType::new_unchecked([
            StructField::not_null("metadata", Self::BINARY),
            StructField::not_null("value", Self::BINARY),
        ])))
    }
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::Long => write!(f, "long"),
            Self::Integer => write!(f, "integer"),
            Self::Short => write!(f, "short"),
            Self::Byte => write!(f, "byte"),
            Self::Float => write!(f, "float"),
            Self::Double => write!(f, "double"),
            Self::Boolean => write!(f, "boolean"),
            Self::Binary => write!(f, "binary"),
            Self::Date => write!(f, "date"),
            Self::Timestamp => write!(f, "timestamp"),
            Self::TimestampNtz => write!(f, "timestamp_ntz"),
            Self::Decimal(dtype) => write!(f, "decimal({},{})", dtype.precision(), dtype.scale()),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primitive(p) => write!(f, "{p}"),
            Self::Array(a) => write!(f, "array<{}>", a.element_type),
            Self::Struct(s) => {
                write!(f, "struct<")?;
                for (i, field) in s.fields().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            Self::Map(m) => write!(f, "map<{}, {}>", m.key_type, m.value_type),
            Self::Variant(_) => write!(f, "variant"),
        }
    }
}

impl From<DecimalType> for PrimitiveType {
    fn from(dtype: DecimalType) -> Self {
        Self::Decimal(dtype)
    }
}

impl From<DecimalType> for DataType {
    fn from(dtype: DecimalType) -> Self {
        PrimitiveType::from(dtype).into()
    }
}

impl From<PrimitiveType> for DataType {
    fn from(ptype: PrimitiveType) -> Self {
        Self::Primitive(ptype)
    }
}

impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        Self::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        Self::Struct(Box::new(struct_type))
    }
}

impl From<ArrayType> for DataType {
    fn from(array_type: ArrayType) -> Self {
        Self::Array(Box::new(array_type))
    }
}

impl TryFrom<SchemaRef> for DataType {
    type Error = DeltaTableError;

    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        let struct_type = StructType::try_from(schema)?;
        Ok(DataType::Struct(Box::new(struct_type)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Format {
    provider: String,
    options: HashMap<String, String>,
}

impl Format {
    fn default_parquet() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
}

impl Metadata {
    pub fn try_new(
        name: Option<String>,
        description: Option<String>,
        schema: StructType,
        partition_columns: Vec<String>,
        created_time: i64,
        configuration: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description,
            format: Format::default_parquet(),
            schema_string: serde_json::to_string(&schema)?,
            partition_columns,
            created_time: Some(created_time),
            configuration,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    pub fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    /// Parse the schema string into a `StructType` (Delta JSON format).
    pub fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    /// Parse the schema string and convert to an Arrow `Schema`.
    pub fn parse_schema_arrow(&self) -> DeltaResult<ArrowSchema> {
        let struct_type: StructType = serde_json::from_str(&self.schema_string)?;
        ArrowSchema::try_from(&struct_type)
            .map_err(|e| DeltaTableError::generic(format!("Failed to convert schema: {e}")))
    }

    pub fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
    }

    pub fn with_table_id(self, table_id: String) -> Metadata {
        Metadata {
            id: table_id,
            ..self
        }
    }

    pub fn with_name(self, name: String) -> Metadata {
        Metadata {
            name: Some(name),
            ..self
        }
    }

    pub fn with_description(self, description: String) -> Metadata {
        Metadata {
            description: Some(description),
            ..self
        }
    }

    pub fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata> {
        Ok(Metadata {
            schema_string: serde_json::to_string(schema)?,
            ..self
        })
    }

    pub fn add_config_key(self, key: String, value: String) -> Metadata {
        let mut configuration = self.configuration;
        configuration.insert(key, value);
        Metadata {
            configuration,
            ..self
        }
    }

    pub fn remove_config_key(self, key: &str) -> Metadata {
        let mut configuration = self.configuration;
        configuration.remove(key);
        Metadata {
            configuration,
            ..self
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TableFeature {
    AppendOnly,
    Invariants,
    CheckConstraints,
    ChangeDataFeed,
    GeneratedColumns,
    IdentityColumns,
    ColumnMapping,
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    min_reader_version: i32,
    min_writer_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<TableFeature>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<TableFeature>>,
}

impl Protocol {
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    pub fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    pub fn reader_features(&self) -> Option<&[TableFeature]> {
        self.reader_features.as_deref()
    }

    pub fn writer_features(&self) -> Option<&[TableFeature]> {
        self.writer_features.as_deref()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataSkippingNumIndexedCols {
    AllColumns,
    NumColumns(u64),
}

impl TryFrom<&str> for DataSkippingNumIndexedCols {
    type Error = DeltaTableError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let num: i64 = value.parse().map_err(|_| {
            DeltaTableError::generic("couldn't parse DataSkippingNumIndexedCols to an integer")
        })?;
        match num {
            -1 => Ok(Self::AllColumns),
            x if x >= 0 => Ok(Self::NumColumns(x as u64)),
            _ => Err(DeltaTableError::generic(
                "couldn't parse DataSkippingNumIndexedCols to positive integer",
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    Serializable,
    WriteSerializable,
    SnapshotIsolation,
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(DeltaTableError::generic(format!(
                "Invalid string for IsolationLevel: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct TableProperties {
    pub append_only: Option<bool>,
    pub checkpoint_interval: Option<NonZeroU64>,
    pub checkpoint_write_stats_as_json: Option<bool>,
    pub checkpoint_write_stats_as_struct: Option<bool>,
    pub column_mapping_mode: Option<ColumnMappingMode>,
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,
    pub deleted_file_retention_duration: Option<Duration>,
    pub isolation_level: Option<IsolationLevel>,
    pub log_retention_duration: Option<Duration>,
    pub enable_expired_log_cleanup: Option<bool>,
    pub unknown_properties: HashMap<String, String>,
}

impl<K, V, I> From<I> for TableProperties
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from(unparsed: I) -> Self {
        let mut props = TableProperties::default();
        let unparsed = unparsed.into_iter().filter(|(k, v)| {
            try_parse_table_property(&mut props, k.as_ref(), v.as_ref()).is_none()
        });
        props.unknown_properties = unparsed.map(|(k, v)| (k.into(), v.into())).collect();
        props
    }
}

const DEFAULT_LOG_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_DELETED_FILE_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_CHECKPOINT_INTERVAL: NonZeroU64 =
    NonZeroU64::new(100).expect("non-zero checkpoint interval");

impl TableProperties {
    pub fn append_only(&self) -> bool {
        self.append_only.unwrap_or(false)
    }

    pub fn log_retention_duration(&self) -> Duration {
        self.log_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_LOG_RETENTION_SECS))
    }

    pub fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup.unwrap_or(true)
    }

    pub fn checkpoint_interval(&self) -> NonZeroU64 {
        self.checkpoint_interval
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL)
    }

    pub fn deleted_file_retention_duration(&self) -> Duration {
        self.deleted_file_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_DELETED_FILE_RETENTION_SECS))
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }
}

fn try_parse_table_property(props: &mut TableProperties, key: &str, value: &str) -> Option<()> {
    match key {
        "delta.appendOnly" => props.append_only = Some(parse_bool(value)?),
        "delta.checkpointInterval" => props.checkpoint_interval = Some(parse_positive_int(value)?),
        "delta.checkpoint.writeStatsAsJson" => {
            props.checkpoint_write_stats_as_json = Some(parse_bool(value)?)
        }
        "delta.checkpoint.writeStatsAsStruct" => {
            props.checkpoint_write_stats_as_struct = Some(parse_bool(value)?)
        }
        "delta.columnMapping.mode" => {
            props.column_mapping_mode = ColumnMappingMode::try_from(value).ok()
        }
        "delta.dataSkippingNumIndexedCols" => {
            props.data_skipping_num_indexed_cols = DataSkippingNumIndexedCols::try_from(value).ok()
        }
        "delta.dataSkippingStatsColumns" => {
            props.data_skipping_stats_columns = ColumnName::parse_column_name_list(value).ok()
        }
        "delta.deletedFileRetentionDuration" => {
            props.deleted_file_retention_duration = parse_interval(value)
        }
        "delta.isolationLevel" => {
            props.isolation_level = IsolationLevel::from_str(value).ok();
        }
        "delta.logRetentionDuration" => props.log_retention_duration = parse_interval(value),
        "delta.enableExpiredLogCleanup" => {
            props.enable_expired_log_cleanup = Some(parse_bool(value)?)
        }
        _ => return None,
    }
    Some(())
}

fn parse_positive_int(s: &str) -> Option<NonZeroU64> {
    let n: i64 = s.parse().ok()?;
    if n <= 0 {
        return None;
    }
    NonZeroU64::new(n as u64)
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_interval(s: &str) -> Option<Duration> {
    const SECONDS_PER_MINUTE: u64 = 60;
    const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
    const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
    const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

    let mut it = s.split_whitespace();
    if it.next() != Some("interval") {
        return None;
    }
    let number = it.next()?.parse::<i64>().ok()?;
    if number < 0 {
        return None;
    }
    let number = number as u64;
    match it.next()? {
        "nanosecond" | "nanoseconds" => Some(Duration::from_nanos(number)),
        "microsecond" | "microseconds" => Some(Duration::from_micros(number)),
        "millisecond" | "milliseconds" => Some(Duration::from_millis(number)),
        "second" | "seconds" => Some(Duration::from_secs(number)),
        "minute" | "minutes" => Some(Duration::from_secs(number * SECONDS_PER_MINUTE)),
        "hour" | "hours" => Some(Duration::from_secs(number * SECONDS_PER_HOUR)),
        "day" | "days" => Some(Duration::from_secs(number * SECONDS_PER_DAY)),
        "week" | "weeks" => Some(Duration::from_secs(number * SECONDS_PER_WEEK)),
        _ => None,
    }
}

impl TryFrom<&StructType> for ArrowSchema {
    type Error = ArrowError;
    fn try_from(s: &StructType) -> Result<Self, ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(ArrowField::try_from).try_collect()?;
        Ok(Self::new(fields))
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;
    fn try_from(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match val {
                MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<Result<HashMap<_, _>, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        Ok(ArrowField::new(
            f.name(),
            ArrowDataType::try_from(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata))
    }
}

impl TryFrom<&ArrayType> for ArrowField {
    type Error = ArrowError;
    fn try_from(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "element",
            ArrowDataType::try_from(a.element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFrom<&MapType> for ArrowField {
    type Error = ArrowError;
    fn try_from(m: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("key", ArrowDataType::try_from(m.key_type())?, false),
                    ArrowField::new(
                        "value",
                        ArrowDataType::try_from(m.value_type())?,
                        m.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false,
        ))
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = ArrowError;
    fn try_from(t: &DataType) -> Result<Self, ArrowError> {
        match t {
            DataType::Primitive(p) => match p {
                PrimitiveType::String => Ok(Self::Utf8),
                PrimitiveType::Long => Ok(Self::Int64),
                PrimitiveType::Integer => Ok(Self::Int32),
                PrimitiveType::Short => Ok(Self::Int16),
                PrimitiveType::Byte => Ok(Self::Int8),
                PrimitiveType::Float => Ok(Self::Float32),
                PrimitiveType::Double => Ok(Self::Float64),
                PrimitiveType::Boolean => Ok(Self::Boolean),
                PrimitiveType::Binary => Ok(Self::Binary),
                PrimitiveType::Decimal(dtype) => {
                    Ok(Self::Decimal128(dtype.precision(), dtype.scale() as i8))
                }
                PrimitiveType::Date => Ok(Self::Date32),
                PrimitiveType::Timestamp => {
                    Ok(Self::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
                }
                PrimitiveType::TimestampNtz => Ok(Self::Timestamp(TimeUnit::Microsecond, None)),
            },
            DataType::Struct(s) => Ok(Self::Struct(
                s.fields()
                    .map(ArrowField::try_from)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(Self::List(Arc::new(ArrowField::try_from(a.as_ref())?))),
            DataType::Map(m) => Ok(Self::Map(
                Arc::new(ArrowField::try_from(m.as_ref())?),
                false,
            )),
            DataType::Variant(s) => {
                if *t == DataType::unshredded_variant() {
                    Ok(Self::Struct(
                        s.fields()
                            .map(ArrowField::try_from)
                            .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                            .into(),
                    ))
                } else {
                    Err(ArrowError::SchemaError(
                        "Incorrect Variant Schema: only unshredded variant is supported"
                            .to_string(),
                    ))
                }
            }
        }
    }
}

impl TryFrom<&ArrowSchema> for StructType {
    type Error = ArrowError;
    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        StructType::try_from_results(
            arrow_schema
                .fields()
                .iter()
                .map(|field| StructField::try_from(field.as_ref())),
        )
        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
    }
}

impl TryFrom<ArrowSchemaRef> for StructType {
    type Error = ArrowError;
    fn try_from(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        StructType::try_from(arrow_schema.as_ref())
    }
}

impl TryFrom<&ArrowField> for StructField {
    type Error = ArrowError;
    fn try_from(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(
            arrow_field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), parse_metadata_value(v))),
        ))
    }
}

fn parse_metadata_value(v: &str) -> MetadataValue {
    match serde_json::from_str::<serde_json::Value>(v) {
        Ok(serde_json::Value::Number(n)) => n
            .as_i64()
            .map(MetadataValue::Number)
            .unwrap_or_else(|| MetadataValue::String(v.to_string())),
        Ok(serde_json::Value::Bool(b)) => MetadataValue::Boolean(b),
        Ok(serde_json::Value::String(s)) => MetadataValue::String(s),
        Ok(other) => MetadataValue::Other(other),
        Err(_) => MetadataValue::String(v.to_string()),
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ArrowError;
    fn try_from(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                Ok(DataType::STRING)
            }
            ArrowDataType::Int64 | ArrowDataType::UInt64 => Ok(DataType::LONG),
            ArrowDataType::Int32 | ArrowDataType::UInt32 => Ok(DataType::INTEGER),
            ArrowDataType::Int16 | ArrowDataType::UInt16 => Ok(DataType::SHORT),
            ArrowDataType::Int8 | ArrowDataType::UInt8 => Ok(DataType::BYTE),
            ArrowDataType::Float32 => Ok(DataType::FLOAT),
            ArrowDataType::Float64 => Ok(DataType::DOUBLE),
            ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
            ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::LargeBinary
            | ArrowDataType::BinaryView => Ok(DataType::BINARY),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(ArrowError::SchemaError(
                        "Negative scales are not supported in Delta".to_string(),
                    ));
                }
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
            }
            ArrowDataType::Date32 | ArrowDataType::Date64 => Ok(DataType::DATE),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::TIMESTAMP)
            }
            ArrowDataType::Struct(fields) => DataType::try_struct_type_from_results(
                fields
                    .iter()
                    .map(|field| StructField::try_from(field.as_ref())),
            )
            .map_err(|e| ArrowError::from_external_error(Box::new(e))),
            ArrowDataType::List(field)
            | ArrowDataType::ListView(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::LargeListView(field)
            | ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                DataType::try_from(field.data_type())?,
                field.is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from(struct_fields[1].data_type())?;
                    Ok(MapType::new(key_type, value_type, struct_fields[1].is_nullable()).into())
                } else {
                    Err(ArrowError::SchemaError(
                        "DataType::Map should contain a struct field child".to_string(),
                    ))
                }
            }
            ArrowDataType::Dictionary(_, value_type) => {
                Ok(DataType::try_from(value_type.as_ref())?)
            }
            unsupported => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {unsupported}"
            ))),
        }
    }
}
