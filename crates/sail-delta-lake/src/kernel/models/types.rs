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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::iter::{DoubleEndedIterator, FusedIterator};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::kernel::{DeltaResult, DeltaTableError};

pub type Schema = StructType;
pub type SchemaRef = Arc<StructType>;

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

    pub fn parse_scalar(&self, raw: &str) -> DeltaResult<Scalar> {
        use PrimitiveType::*;

        if raw.is_empty() {
            return Ok(Scalar::Null(self.data_type()));
        }

        match self {
            String => Ok(Scalar::String(raw.to_string())),
            Binary => Ok(Scalar::Binary(raw.as_bytes().to_vec())),
            Byte => parse_str_as_scalar(raw, Scalar::Byte)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Short => parse_str_as_scalar(raw, Scalar::Short)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Integer => parse_str_as_scalar(raw, Scalar::Integer)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Long => parse_str_as_scalar(raw, Scalar::Long)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Float => parse_str_as_scalar(raw, Scalar::Float)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Double => parse_str_as_scalar(raw, Scalar::Double)
                .ok_or_else(|| parse_error(raw, self.data_type())),
            Boolean => {
                if raw.eq_ignore_ascii_case("true") {
                    Ok(Scalar::Boolean(true))
                } else if raw.eq_ignore_ascii_case("false") {
                    Ok(Scalar::Boolean(false))
                } else {
                    Err(parse_error(raw, self.data_type()))
                }
            }
            Date => {
                let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .ok_or_else(|| parse_error(raw, self.data_type()))?;
                let date = Utc.from_utc_datetime(&date);
                let days = date.signed_duration_since(DateTime::UNIX_EPOCH).num_days() as i32;
                Ok(Scalar::Date(days))
            }
            TimestampNtz | Timestamp => {
                let mut parsed = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f");
                if parsed.is_err() && matches!(self, Timestamp) {
                    parsed = NaiveDateTime::parse_from_str(raw, "%+");
                }
                let ts = parsed.map_err(|_| parse_error(raw, self.data_type()))?;
                let ts = Utc.from_utc_datetime(&ts);
                let micros = ts
                    .signed_duration_since(DateTime::UNIX_EPOCH)
                    .num_microseconds()
                    .ok_or_else(|| parse_error(raw, self.data_type()))?;
                match self {
                    Timestamp => Ok(Scalar::Timestamp(micros)),
                    TimestampNtz => Ok(Scalar::TimestampNtz(micros)),
                    _ => unreachable!(),
                }
            }
            Decimal(dtype) => parse_decimal(raw, *dtype),
        }
    }

    fn data_type(&self) -> DataType {
        DataType::Primitive(self.clone())
    }
}

fn parse_str_as_scalar<T: std::str::FromStr>(
    raw: &str,
    f: impl FnOnce(T) -> Scalar,
) -> Option<Scalar> {
    raw.parse().ok().map(f)
}

fn parse_error(raw: &str, data_type: DataType) -> DeltaTableError {
    DeltaTableError::generic(format!("Failed to parse value '{raw}' as '{data_type}'"))
}

fn parse_decimal(raw: &str, dtype: DecimalType) -> DeltaResult<Scalar> {
    let (base, exp): (&str, i128) = match raw.find(['e', 'E']) {
        None => (raw, 0),
        Some(pos) => {
            let (base, exp) = raw.split_at(pos);
            (
                base,
                exp[1..]
                    .parse()
                    .map_err(|_| parse_error(raw, PrimitiveType::Decimal(dtype).data_type()))?,
            )
        }
    };
    if base.is_empty() {
        return Err(parse_error(raw, PrimitiveType::Decimal(dtype).data_type()));
    }

    let (int_part, frac_part, frac_digits) = match base.find('.') {
        None => (base, None, 0),
        Some(pos) if pos == base.len() - 1 => (&base[..pos], None, 0),
        Some(pos) => {
            let (int_part, frac_part) = (&base[..pos], &base[pos + 1..]);
            (int_part, Some(frac_part), frac_part.len() as i128)
        }
    };

    let scale: u8 = (frac_digits - exp)
        .try_into()
        .map_err(|_| parse_error(raw, PrimitiveType::Decimal(dtype).data_type()))?;
    if scale != dtype.scale() {
        return Err(parse_error(raw, PrimitiveType::Decimal(dtype).data_type()));
    }
    let int: i128 = match frac_part {
        None => int_part
            .parse()
            .map_err(|_| parse_error(raw, PrimitiveType::Decimal(dtype).data_type()))?,
        Some(frac_part) => format!("{int_part}{frac_part}")
            .parse()
            .map_err(|_| parse_error(raw, PrimitiveType::Decimal(dtype).data_type()))?,
    };
    Ok(Scalar::Decimal(DecimalData::try_new(int, dtype)?))
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
        struct MakePhysical {
            column_mapping_mode: ColumnMappingMode,
        }

        impl<'a> SchemaTransform<'a> for MakePhysical {
            fn transform_struct_field(
                &mut self,
                field: &'a StructField,
            ) -> Option<Cow<'a, StructField>> {
                let field = self.recurse_into_struct_field(field)?;

                let mut metadata = field.metadata().clone();
                let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
                let field_id_key = ColumnMetadataKey::ColumnMappingId.as_ref();
                let parquet_field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();

                match self.column_mapping_mode {
                    ColumnMappingMode::Id => {
                        if let Some(MetadataValue::Number(fid)) = metadata.get(field_id_key) {
                            metadata.insert(
                                parquet_field_id_key.to_string(),
                                MetadataValue::Number(*fid),
                            );
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

                let name = match self.column_mapping_mode {
                    ColumnMappingMode::None => field.name().to_owned(),
                    ColumnMappingMode::Id | ColumnMappingMode::Name => {
                        field.physical_name(self.column_mapping_mode).to_owned()
                    }
                };

                Some(Cow::Owned(field.with_name(name).with_metadata(metadata)))
            }
        }

        MakePhysical {
            column_mapping_mode,
        }
        .transform_struct_field(self)
        .map(Cow::into_owned)
        .unwrap_or_else(|| self.clone())
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

impl From<SchemaRef> for DataType {
    fn from(schema: SchemaRef) -> Self {
        Arc::unwrap_or_clone(schema).into()
    }
}

pub trait SchemaTransform<'a> {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Borrowed(ptype))
    }

    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.recurse_into_struct(stype)
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.recurse_into_struct_field(field)
    }

    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.recurse_into_array(atype)
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.recurse_into_map(mtype)
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    fn transform_variant(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.recurse_into_struct(stype)
    }

    fn transform(&mut self, data_type: &'a DataType) -> Option<Cow<'a, DataType>> {
        let result = match data_type {
            DataType::Primitive(ptype) => self
                .transform_primitive(ptype)?
                .map_owned_or_else(data_type, DataType::from),
            DataType::Array(atype) => self
                .transform_array(atype)?
                .map_owned_or_else(data_type, DataType::from),
            DataType::Struct(stype) => self
                .transform_struct(stype)?
                .map_owned_or_else(data_type, DataType::from),
            DataType::Map(mtype) => self
                .transform_map(mtype)?
                .map_owned_or_else(data_type, DataType::from),
            DataType::Variant(stype) => self
                .transform_variant(stype)?
                .map_owned_or_else(data_type, |s| DataType::Variant(Box::new(s))),
        };
        Some(result)
    }

    fn recurse_into_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> Option<Cow<'a, StructField>> {
        let result = self.transform(&field.data_type)?;
        let f = |new_data_type| StructField {
            name: field.name.clone(),
            data_type: new_data_type,
            nullable: field.nullable,
            metadata: field.metadata.clone(),
        };
        Some(result.map_owned_or_else(field, f))
    }

    fn recurse_into_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        use Cow::*;
        let mut num_borrowed = 0;
        let fields: Vec<_> = stype
            .fields()
            .filter_map(|field| self.transform_struct_field(field))
            .inspect(|field| {
                if let Borrowed(_) = field {
                    num_borrowed += 1;
                }
            })
            .collect();

        if fields.is_empty() {
            None
        } else if num_borrowed < stype.fields.len() {
            Some(Owned(StructType::new_unchecked(
                fields.into_iter().map(Cow::into_owned),
            )))
        } else {
            Some(Borrowed(stype))
        }
    }

    fn recurse_into_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        let result = self.transform_array_element(&atype.element_type)?;
        let f = |element_type| ArrayType {
            type_name: atype.type_name.clone(),
            element_type,
            contains_null: atype.contains_null,
        };
        Some(result.map_owned_or_else(atype, f))
    }

    fn recurse_into_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        let key_type = self.transform_map_key(&mtype.key_type)?;
        let value_type = self.transform_map_value(&mtype.value_type)?;
        if matches!(key_type, Cow::Borrowed(_)) && matches!(value_type, Cow::Borrowed(_)) {
            Some(Cow::Borrowed(mtype))
        } else {
            Some(Cow::Owned(MapType {
                type_name: mtype.type_name.clone(),
                key_type: key_type.into_owned(),
                value_type: value_type.into_owned(),
                value_contains_null: mtype.value_contains_null,
            }))
        }
    }
}

trait CowExt<'a, B: ?Sized + ToOwned> {
    fn map_owned_or_else<C: ToOwned<Owned = C>>(
        self,
        borrowed: &'a C,
        map: impl FnOnce(B::Owned) -> C::Owned,
    ) -> Cow<'a, C>;
}

impl<'a, B: ?Sized + ToOwned> CowExt<'a, B> for Cow<'a, B> {
    fn map_owned_or_else<C: ToOwned<Owned = C>>(
        self,
        borrowed: &'a C,
        map: impl FnOnce(B::Owned) -> C::Owned,
    ) -> Cow<'a, C> {
        match self {
            Cow::Borrowed(_) => Cow::Borrowed(borrowed),
            Cow::Owned(owned) => Cow::Owned(map(owned)),
        }
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

    pub fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    pub fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
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

#[derive(Debug, Clone, PartialEq)]
pub struct DecimalData {
    bits: i128,
    ty: DecimalType,
}

impl DecimalData {
    pub fn try_new(bits: impl Into<i128>, ty: DecimalType) -> DeltaResult<Self> {
        let bits = bits.into();
        if ty.precision() < get_decimal_precision(bits) {
            return Err(DeltaTableError::generic(format!(
                "Decimal value {bits} exceeds precision {}",
                ty.precision()
            )));
        }
        Ok(Self { bits, ty })
    }

    pub fn bits(&self) -> i128 {
        self.bits
    }

    pub fn ty(&self) -> &DecimalType {
        &self.ty
    }

    pub fn precision(&self) -> u8 {
        self.ty.precision()
    }

    pub fn scale(&self) -> u8 {
        self.ty.scale()
    }
}

fn get_decimal_precision(value: i128) -> u8 {
    value.unsigned_abs().checked_ilog10().map_or(0, |p| p + 1) as u8
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArrayData {
    tpe: ArrayType,
    elements: Vec<Scalar>,
}

impl ArrayData {
    pub fn try_new(
        tpe: ArrayType,
        elements: impl IntoIterator<Item = impl Into<Scalar>>,
    ) -> DeltaResult<Self> {
        let elements = elements
            .into_iter()
            .map(|v| {
                let v = v.into();
                if !tpe.contains_null() && v.is_null() {
                    Err(DeltaTableError::schema(
                        "Array element cannot be null for non-nullable array".to_string(),
                    ))
                } else if *tpe.element_type() != v.data_type() {
                    Err(DeltaTableError::schema(format!(
                        "Array scalar type mismatch: expected {}, got {}",
                        tpe.element_type(),
                        v.data_type()
                    )))
                } else {
                    Ok(v)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { tpe, elements })
    }

    pub fn array_type(&self) -> &ArrayType {
        &self.tpe
    }

    pub fn array_elements(&self) -> &[Scalar] {
        &self.elements
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MapData {
    data_type: MapType,
    pairs: Vec<(Scalar, Scalar)>,
}

impl MapData {
    pub fn try_new(
        data_type: MapType,
        values: impl IntoIterator<Item = (impl Into<Scalar>, impl Into<Scalar>)>,
    ) -> DeltaResult<Self> {
        let key_type = data_type.key_type();
        let val_type = data_type.value_type();
        let pairs = values
            .into_iter()
            .map(|(key, val)| {
                let (k, v) = (key.into(), val.into());
                if k.data_type() != *key_type {
                    Err(DeltaTableError::schema(format!(
                        "Map scalar type mismatch: expected key type {}, got key type {}",
                        key_type,
                        k.data_type()
                    )))
                } else if k.is_null() {
                    Err(DeltaTableError::schema(
                        "Map key cannot be null".to_string(),
                    ))
                } else if v.data_type() != *val_type {
                    Err(DeltaTableError::schema(format!(
                        "Map scalar type mismatch: expected value type {}, got value type {}",
                        val_type,
                        v.data_type()
                    )))
                } else if v.is_null() && !data_type.value_contains_null {
                    Err(DeltaTableError::schema(
                        "Null map value disallowed if map value_contains_null is false".to_string(),
                    ))
                } else {
                    Ok((k, v))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { data_type, pairs })
    }

    pub fn pairs(&self) -> &[(Scalar, Scalar)] {
        &self.pairs
    }

    pub fn map_type(&self) -> &MapType {
        &self.data_type
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructData {
    fields: Vec<StructField>,
    values: Vec<Scalar>,
}

impl StructData {
    pub fn try_new(fields: Vec<StructField>, values: Vec<Scalar>) -> DeltaResult<Self> {
        if fields.len() != values.len() {
            return Err(DeltaTableError::generic(format!(
                "Incorrect number of values for Struct fields, expected {} got {}",
                fields.len(),
                values.len()
            )));
        }

        for (f, a) in fields.iter().zip(&values) {
            if f.data_type() != &a.data_type() {
                return Err(DeltaTableError::generic(format!(
                    "Incorrect datatype for Struct field {:?}, expected {} got {}",
                    f.name(),
                    f.data_type(),
                    a.data_type()
                )));
            }
            if !f.is_nullable() && a.is_null() {
                return Err(DeltaTableError::generic(format!(
                    "Value for non-nullable field {:?} cannot be null",
                    f.name()
                )));
            }
        }

        Ok(Self { fields, values })
    }

    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }

    pub fn values(&self) -> &[Scalar] {
        &self.values
    }
}

#[derive(Debug, Clone)]
pub enum Scalar {
    Integer(i32),
    Long(i64),
    Short(i16),
    Byte(i8),
    Float(f32),
    Double(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64),
    TimestampNtz(i64),
    Date(i32),
    Binary(Vec<u8>),
    Decimal(DecimalData),
    Null(DataType),
    Struct(StructData),
    Array(ArrayData),
    Map(MapData),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::INTEGER,
            Self::Long(_) => DataType::LONG,
            Self::Short(_) => DataType::SHORT,
            Self::Byte(_) => DataType::BYTE,
            Self::Float(_) => DataType::FLOAT,
            Self::Double(_) => DataType::DOUBLE,
            Self::String(_) => DataType::STRING,
            Self::Boolean(_) => DataType::BOOLEAN,
            Self::Timestamp(_) => DataType::TIMESTAMP,
            Self::TimestampNtz(_) => DataType::TIMESTAMP_NTZ,
            Self::Date(_) => DataType::DATE,
            Self::Binary(_) => DataType::BINARY,
            Self::Decimal(d) => DataType::from(*d.ty()),
            Self::Null(data_type) => data_type.clone(),
            Self::Struct(data) => DataType::struct_type_unchecked(data.fields.clone()),
            Self::Array(data) => data.tpe.clone().into(),
            Self::Map(data) => data.data_type.clone().into(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    pub fn decimal(bits: impl Into<i128>, precision: u8, scale: u8) -> DeltaResult<Self> {
        let dtype = DecimalType::try_new(precision, scale)?;
        let dval = DecimalData::try_new(bits, dtype)?;
        Ok(Self::Decimal(dval))
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{i}"),
            Self::Long(i) => write!(f, "{i}"),
            Self::Short(i) => write!(f, "{i}"),
            Self::Byte(i) => write!(f, "{i}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "'{v}'"),
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Timestamp(v) | Self::TimestampNtz(v) => write!(f, "{v}"),
            Self::Date(v) => write!(f, "{v}"),
            Self::Binary(v) => write!(f, "{v:?}"),
            Self::Decimal(v) => write!(f, "{}(p={},s={})", v.bits(), v.precision(), v.scale()),
            Self::Null(_) => write!(f, "null"),
            Self::Struct(data) => {
                write!(f, "{{")?;
                for (i, (value, field)) in data.values.iter().zip(data.fields.iter()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {value}", field.name)?;
                }
                write!(f, "}}")
            }
            Self::Array(data) => {
                write!(f, "(")?;
                for (i, value) in data.elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{value}")?;
                }
                write!(f, ")")
            }
            Self::Map(data) => {
                write!(f, "{{")?;
                for (i, (k, v)) in data.pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Scalar) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use Scalar::*;
        match (self, other) {
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Long(a), Long(b)) => a.partial_cmp(b),
            (Short(a), Short(b)) => a.partial_cmp(b),
            (Byte(a), Byte(b)) => a.partial_cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (String(a), String(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (TimestampNtz(a), TimestampNtz(b)) => a.partial_cmp(b),
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            (Decimal(d1), Decimal(d2)) => (d1.ty() == d2.ty())
                .then(|| d1.bits().partial_cmp(&d2.bits()))
                .flatten(),
            _ => None,
        }
    }
}

pub trait TryIntoArrow<ArrowType> {
    fn try_into_arrow(self) -> Result<ArrowType, ArrowError>;
}

pub trait TryFromArrow<ArrowType>: Sized {
    fn try_from_arrow(t: ArrowType) -> Result<Self, ArrowError>;
}

pub trait TryIntoKernel<KernelType> {
    fn try_into_kernel(self) -> Result<KernelType, ArrowError>;
}

pub trait TryFromKernel<KernelType>: Sized {
    fn try_from_kernel(t: KernelType) -> Result<Self, ArrowError>;
}

impl<KernelType, ArrowType> TryIntoArrow<ArrowType> for KernelType
where
    ArrowType: TryFromKernel<KernelType>,
{
    fn try_into_arrow(self) -> Result<ArrowType, ArrowError> {
        ArrowType::try_from_kernel(self)
    }
}

impl<KernelType, ArrowType> TryIntoKernel<KernelType> for ArrowType
where
    KernelType: TryFromArrow<ArrowType>,
{
    fn try_into_kernel(self) -> Result<KernelType, ArrowError> {
        KernelType::try_from_arrow(self)
    }
}

impl TryFromKernel<&StructType> for ArrowSchema {
    fn try_from_kernel(s: &StructType) -> Result<Self, ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(|f| f.try_into_arrow()).try_collect()?;
        Ok(Self::new(fields))
    }
}

impl TryFromKernel<&StructField> for ArrowField {
    fn try_from_kernel(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match val {
                MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<Result<HashMap<_, _>, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        Ok(
            ArrowField::new(f.name(), f.data_type().try_into_arrow()?, f.is_nullable())
                .with_metadata(metadata),
        )
    }
}

impl TryFromKernel<&ArrayType> for ArrowField {
    fn try_from_kernel(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "element",
            a.element_type().try_into_arrow()?,
            a.contains_null(),
        ))
    }
}

impl TryFromKernel<&MapType> for ArrowField {
    fn try_from_kernel(m: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("key", m.key_type().try_into_arrow()?, false),
                    ArrowField::new(
                        "value",
                        m.value_type().try_into_arrow()?,
                        m.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false,
        ))
    }
}

impl TryFromKernel<&DataType> for ArrowDataType {
    fn try_from_kernel(t: &DataType) -> Result<Self, ArrowError> {
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
                    .map(TryIntoArrow::try_into_arrow)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(Self::List(Arc::new(a.as_ref().try_into_arrow()?))),
            DataType::Map(m) => Ok(Self::Map(Arc::new(m.as_ref().try_into_arrow()?), false)),
            DataType::Variant(s) => {
                if *t == DataType::unshredded_variant() {
                    Ok(Self::Struct(
                        s.fields()
                            .map(TryIntoArrow::try_into_arrow)
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

impl TryFromArrow<&ArrowSchema> for StructType {
    fn try_from_arrow(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        StructType::try_from_results(
            arrow_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().try_into_kernel()),
        )
        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
    }
}

impl TryFromArrow<ArrowSchemaRef> for StructType {
    fn try_from_arrow(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        arrow_schema.as_ref().try_into_kernel()
    }
}

impl TryFromArrow<&ArrowField> for StructField {
    fn try_from_arrow(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_arrow(arrow_field.data_type())?,
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

impl TryFromArrow<&ArrowDataType> for DataType {
    fn try_from_arrow(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
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
                fields.iter().map(|field| field.as_ref().try_into_kernel()),
            )
            .map_err(|e| ArrowError::from_external_error(Box::new(e))),
            ArrowDataType::List(field)
            | ArrowDataType::ListView(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::LargeListView(field)
            | ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                field.data_type().try_into_kernel()?,
                field.is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from_arrow(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from_arrow(struct_fields[1].data_type())?;
                    Ok(MapType::new(key_type, value_type, struct_fields[1].is_nullable()).into())
                } else {
                    Err(ArrowError::SchemaError(
                        "DataType::Map should contain a struct field child".to_string(),
                    ))
                }
            }
            ArrowDataType::Dictionary(_, value_type) => {
                Ok(value_type.as_ref().try_into_kernel()?)
            }
            unsupported => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {unsupported}"
            ))),
        }
    }
}
