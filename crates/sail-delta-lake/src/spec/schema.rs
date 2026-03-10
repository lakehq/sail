// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
//
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
use std::ops::Deref;
use std::str::FromStr;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

pub type Schema = StructType;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged)]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L42-L92>
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L95-L125>
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_features/column_mapping.rs#L15-L26>
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/expressions/column_names.rs#L9-L134>
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L1231-L1262>
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L1265-L1380>
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
    let value = std::string::String::deserialize(deserializer)?;
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L180-L536>
pub struct StructField {
    pub name: std::string::String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<std::string::String, MetadataValue>,
}

impl StructField {
    pub fn new(
        name: impl Into<std::string::String>,
        data_type: impl Into<DataType>,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    pub fn nullable(name: impl Into<std::string::String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, true)
    }

    pub fn not_null(name: impl Into<std::string::String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, false)
    }

    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<std::string::String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    pub fn add_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<std::string::String>, impl Into<MetadataValue>)>,
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

    pub fn with_name(&self, new_name: impl Into<std::string::String>) -> Self {
        Self {
            name: new_name.into(),
            data_type: self.data_type.clone(),
            nullable: self.nullable,
            metadata: self.metadata.clone(),
        }
    }

    #[inline]
    pub fn name(&self) -> &std::string::String {
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
    pub const fn metadata(&self) -> &HashMap<std::string::String, MetadataValue> {
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L537-L1141>
pub struct StructType {
    type_name: std::string::String,
    fields: IndexMap<std::string::String, StructField>,
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
    type_name: std::string::String,
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L1145-L1173>
pub struct ArrayType {
    #[serde(rename = "type")]
    pub type_name: std::string::String,
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L1176-L1224>
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: std::string::String,
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
    let value = std::string::String::deserialize(deserializer)?;
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
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/schema/mod.rs#L1384-L1465>
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

impl FromStr for ColumnMappingMode {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}
