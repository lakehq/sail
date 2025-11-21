// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/datatypes.rs

pub mod values;

use std::collections::HashMap;
use std::fmt;
use std::ops::Index;
use std::sync::{Arc, OnceLock};

use ordered_float::OrderedFloat;
use serde::de::{Error, IntoDeserializer, MapAccess, Visitor};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
pub use values::*;

/// Field name for list type.
pub const ICEBERG_LIST_FIELD_NAME: &str = "element";
/// Field name for map type's key.
pub const ICEBERG_MAP_KEY_FIELD_NAME: &str = "key";
/// Field name for map type's value.
pub const ICEBERG_MAP_VALUE_FIELD_NAME: &str = "value";

pub(crate) const MAX_DECIMAL_BYTES: u32 = 24;
pub(crate) const MAX_DECIMAL_PRECISION: u32 = 38;

mod _decimal {
    use once_cell::sync::Lazy;

    use super::{MAX_DECIMAL_BYTES, MAX_DECIMAL_PRECISION};

    // Max precision of bytes, starts from 1
    pub(super) static MAX_PRECISION: Lazy<[u32; MAX_DECIMAL_BYTES as usize]> = Lazy::new(|| {
        let mut ret: [u32; 24] = [0; 24];
        for (i, prec) in ret.iter_mut().enumerate() {
            *prec = 2f64.powi((8 * (i + 1) - 1) as i32).log10().floor() as u32;
        }

        ret
    });

    //  Required bytes of precision, starts from 1
    pub(super) static REQUIRED_LENGTH: Lazy<[u32; MAX_DECIMAL_PRECISION as usize]> =
        Lazy::new(|| {
            let mut ret: [u32; MAX_DECIMAL_PRECISION as usize] =
                [0; MAX_DECIMAL_PRECISION as usize];

            for (i, required_len) in ret.iter_mut().enumerate() {
                for j in 0..MAX_PRECISION.len() {
                    if MAX_PRECISION[j] >= ((i + 1) as u32) {
                        *required_len = (j + 1) as u32;
                        break;
                    }
                }
            }

            ret
        });
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// All data types are either primitives or nested types, which are maps, lists, or structs.
pub enum Type {
    /// Primitive types
    Primitive(PrimitiveType),
    /// Struct type
    Struct(StructType),
    /// List type.
    List(ListType),
    /// Map type
    Map(MapType),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Primitive(primitive) => write!(f, "{}", primitive),
            Type::Struct(s) => write!(f, "{}", s),
            Type::List(_) => write!(f, "list"),
            Type::Map(_) => write!(f, "map"),
        }
    }
}

impl Type {
    /// Whether the type is primitive type.
    #[inline(always)]
    pub fn is_primitive(&self) -> bool {
        matches!(self, Type::Primitive(_))
    }

    /// Whether the type is struct type.
    #[inline(always)]
    pub fn is_struct(&self) -> bool {
        matches!(self, Type::Struct(_))
    }

    /// Whether the type is nested type.
    #[inline(always)]
    pub fn is_nested(&self) -> bool {
        matches!(self, Type::Struct(_) | Type::List(_) | Type::Map(_))
    }

    /// Convert Type to reference of PrimitiveType
    pub fn as_primitive_type(&self) -> Option<&PrimitiveType> {
        if let Type::Primitive(primitive_type) = self {
            Some(primitive_type)
        } else {
            None
        }
    }

    /// Convert Type to StructType
    pub fn into_struct_type(self) -> Option<StructType> {
        if let Type::Struct(struct_type) = self {
            Some(struct_type)
        } else {
            None
        }
    }

    /// Return max precision for decimal given [`num_bytes`] bytes.
    #[inline(always)]
    pub fn decimal_max_precision(num_bytes: u32) -> Result<u32, String> {
        if num_bytes == 0 || num_bytes > MAX_DECIMAL_BYTES {
            return Err(format!(
                "Decimal length larger than {MAX_DECIMAL_BYTES} is not supported: {num_bytes}"
            ));
        }
        Ok(_decimal::MAX_PRECISION[num_bytes as usize - 1])
    }

    /// Returns minimum bytes required for decimal with [`precision`].
    #[inline(always)]
    pub fn decimal_required_bytes(precision: u32) -> Result<u32, String> {
        if precision == 0 || precision > MAX_DECIMAL_PRECISION {
            return Err(format!(
                "Decimals with precision larger than {MAX_DECIMAL_PRECISION} are not supported: {precision}"
            ));
        }
        Ok(_decimal::REQUIRED_LENGTH[precision as usize - 1])
    }

    /// Creates  decimal type.
    #[inline(always)]
    pub fn decimal(precision: u32, scale: u32) -> Result<Self, String> {
        if precision == 0 || precision > MAX_DECIMAL_PRECISION {
            return Err(format!(
                "Decimals with precision larger than {MAX_DECIMAL_PRECISION} are not supported: {precision}"
            ));
        }
        Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
    }

    /// Check if it's float or double type.
    #[inline(always)]
    pub fn is_floating_type(&self) -> bool {
        matches!(
            self,
            Type::Primitive(PrimitiveType::Float) | Type::Primitive(PrimitiveType::Double)
        )
    }
}

impl From<PrimitiveType> for Type {
    fn from(value: PrimitiveType) -> Self {
        Self::Primitive(value)
    }
}

impl From<StructType> for Type {
    fn from(value: StructType) -> Self {
        Type::Struct(value)
    }
}

impl From<ListType> for Type {
    fn from(value: ListType) -> Self {
        Type::List(value)
    }
}

impl From<MapType> for Type {
    fn from(value: MapType) -> Self {
        Type::Map(value)
    }
}

/// Primitive data types
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
#[serde(rename_all = "lowercase", remote = "Self")]
pub enum PrimitiveType {
    /// True or False
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating point.
    Float,
    /// 64-bit IEEE 754 floating point.
    Double,
    /// Fixed point decimal
    Decimal {
        /// Precision, must be 38 or less
        precision: u32,
        /// Scale
        scale: u32,
    },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day in microsecond precision, without date or timezone.
    Time,
    /// Timestamp in microsecond precision, without timezone
    Timestamp,
    /// Timestamp in microsecond precision, with timezone
    Timestamptz,
    /// Timestamp in nanosecond precision, without timezone
    #[serde(rename = "timestamp_ns")]
    TimestampNs,
    /// Timestamp in nanosecond precision with timezone
    #[serde(rename = "timestamptz_ns")]
    TimestamptzNs,
    /// Arbitrary-length character sequences encoded in utf-8
    String,
    /// Universally Unique Identifiers, should use 16-byte fixed
    Uuid,
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}

impl PrimitiveType {
    /// Check whether literal is compatible with the type.
    pub fn compatible(&self, literal: &PrimitiveLiteral) -> bool {
        matches!(
            (self, literal),
            (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(_))
                | (PrimitiveType::Int, PrimitiveLiteral::Int(_))
                | (PrimitiveType::Long, PrimitiveLiteral::Long(_))
                | (PrimitiveType::Float, PrimitiveLiteral::Float(_))
                | (PrimitiveType::Double, PrimitiveLiteral::Double(_))
                | (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(_))
                | (PrimitiveType::Date, PrimitiveLiteral::Int(_))
                | (PrimitiveType::Time, PrimitiveLiteral::Long(_))
                | (PrimitiveType::Timestamp, PrimitiveLiteral::Long(_))
                | (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(_))
                | (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(_))
                | (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(_))
                | (PrimitiveType::String, PrimitiveLiteral::String(_))
                | (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(_))
                | (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(_))
                | (PrimitiveType::Binary, PrimitiveLiteral::Binary(_))
        )
    }

    /// Decode a PrimitiveLiteral from the serialized bound bytes that appear in manifests.
    pub fn literal_from_bytes(&self, bytes: &[u8]) -> Result<PrimitiveLiteral, String> {
        use crate::spec::types::values::PrimitiveLiteral as PL;

        let literal = match self {
            PrimitiveType::Boolean => {
                let val = !(bytes.len() == 1 && bytes.first().copied().unwrap_or(0) == 0);
                PL::Boolean(val)
            }
            PrimitiveType::Int | PrimitiveType::Date => {
                let val = i32::from_le_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| "Invalid i32 bytes".to_string())?,
                );
                PL::Int(val)
            }
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs => {
                let val = if bytes.len() == 4 {
                    i32::from_le_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| "Invalid i32 bytes".to_string())?,
                    ) as i64
                } else {
                    i64::from_le_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| "Invalid i64 bytes".to_string())?,
                    )
                };
                PL::Long(val)
            }
            PrimitiveType::Float => {
                let val = f32::from_le_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| "Invalid f32 bytes for float bound".to_string())?,
                );
                PL::Float(OrderedFloat(val))
            }
            PrimitiveType::Double => {
                let val = if bytes.len() == 4 {
                    f32::from_le_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| "Invalid f32 bytes for double bound".to_string())?,
                    ) as f64
                } else {
                    f64::from_le_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| "Invalid f64 bytes for double bound".to_string())?,
                    )
                };
                PL::Double(OrderedFloat(val))
            }
            PrimitiveType::String => {
                let val = std::str::from_utf8(bytes)
                    .map_err(|_| "Invalid UTF-8 in bound bytes".to_string())?
                    .to_string();
                PL::String(val)
            }
            PrimitiveType::Uuid => {
                return Err("uuid bound decoding not supported".to_string());
            }
            PrimitiveType::Fixed(_) | PrimitiveType::Binary => PL::Binary(bytes.to_vec()),
            PrimitiveType::Decimal { .. } => {
                return Err("decimal bound decoding not supported".to_string());
            }
        };

        Ok(literal)
    }
}

impl Serialize for Type {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let type_serde = _serde::SerdeType::from(self);
        type_serde.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let type_serde = _serde::SerdeType::deserialize(deserializer)?;
        Ok(Type::from(type_serde))
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("decimal") {
            deserialize_decimal(s.into_deserializer())
        } else if s.starts_with("fixed") {
            deserialize_fixed(s.into_deserializer())
        } else {
            PrimitiveType::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PrimitiveType::Decimal { precision, scale } => {
                serialize_decimal(precision, scale, serializer)
            }
            PrimitiveType::Fixed(l) => serialize_fixed(l, serializer),
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> std::result::Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let (precision, scale) = s
        .trim_start_matches(r"decimal(")
        .trim_end_matches(')')
        .split_once(',')
        .ok_or_else(|| D::Error::custom(format!("Decimal requires precision and scale: {s}")))?;

    Ok(PrimitiveType::Decimal {
        precision: precision.trim().parse().map_err(D::Error::custom)?,
        scale: scale.trim().parse().map_err(D::Error::custom)?,
    })
}

fn serialize_decimal<S>(
    precision: &u32,
    scale: &u32,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("decimal({precision},{scale})"))
}

fn deserialize_fixed<'de, D>(deserializer: D) -> std::result::Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let fixed = String::deserialize(deserializer)?
        .trim_start_matches(r"fixed[")
        .trim_end_matches(']')
        .to_owned();

    fixed
        .parse()
        .map(PrimitiveType::Fixed)
        .map_err(D::Error::custom)
}

fn serialize_fixed<S>(value: &u64, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("fixed[{value}]"))
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Int => write!(f, "int"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Decimal { precision, scale } => {
                write!(f, "decimal({},{})", precision, scale)
            }
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Time => write!(f, "time"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Timestamptz => write!(f, "timestamptz"),
            PrimitiveType::TimestampNs => write!(f, "timestamp_ns"),
            PrimitiveType::TimestamptzNs => write!(f, "timestamptz_ns"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Fixed(size) => write!(f, "fixed({})", size),
            PrimitiveType::Binary => write!(f, "binary"),
        }
    }
}

/// DataType for a specific struct
#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename = "struct", tag = "type")]
pub struct StructType {
    /// Struct fields
    fields: Vec<NestedFieldRef>,
    /// Lookup for index by field id
    #[serde(skip_serializing)]
    id_lookup: OnceLock<HashMap<i32, usize>>,
    #[serde(skip_serializing)]
    name_lookup: OnceLock<HashMap<String, usize>>,
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Fields,
        }

        struct StructTypeVisitor;

        impl<'de> Visitor<'de> for StructTypeVisitor {
            type Value = StructType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StructType, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => (),
                        Field::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                    }
                }
                let fields: Vec<NestedFieldRef> =
                    fields.ok_or_else(|| de::Error::missing_field("fields"))?;

                Ok(StructType::new(fields))
            }
        }

        const FIELDS: &[&str] = &["type", "fields"];
        deserializer.deserialize_struct("struct", FIELDS, StructTypeVisitor)
    }
}

impl StructType {
    /// Creates a struct type with the given fields.
    pub fn new(fields: Vec<NestedFieldRef>) -> Self {
        Self {
            fields,
            id_lookup: OnceLock::new(),
            name_lookup: OnceLock::new(),
        }
    }

    /// Get struct field with certain id
    pub fn field_by_id(&self, id: i32) -> Option<&NestedFieldRef> {
        self.field_id_to_index(id).map(|idx| &self.fields[idx])
    }

    fn field_id_to_index(&self, field_id: i32) -> Option<usize> {
        self.id_lookup
            .get_or_init(|| {
                HashMap::from_iter(self.fields.iter().enumerate().map(|(i, x)| (x.id, i)))
            })
            .get(&field_id)
            .copied()
    }

    /// Get struct field with certain field name
    pub fn field_by_name(&self, name: &str) -> Option<&NestedFieldRef> {
        self.field_name_to_index(name).map(|idx| &self.fields[idx])
    }

    fn field_name_to_index(&self, name: &str) -> Option<usize> {
        self.name_lookup
            .get_or_init(|| {
                HashMap::from_iter(
                    self.fields
                        .iter()
                        .enumerate()
                        .map(|(i, x)| (x.name.clone(), i)),
                )
            })
            .get(name)
            .copied()
    }

    /// Get fields.
    pub fn fields(&self) -> &[NestedFieldRef] {
        &self.fields
    }
}

impl PartialEq for StructType {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for StructType {}

impl Index<usize> for StructType {
    type Output = NestedField;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

impl fmt::Display for StructType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "struct<")?;
        for field in &self.fields {
            write!(f, "{}", field.field_type)?;
        }
        write!(f, ">")
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, Clone)]
#[serde(from = "SerdeNestedField", into = "SerdeNestedField")]
/// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
/// Fields may have an optional comment or doc string. Fields can have default values.
pub struct NestedField {
    /// Id unique in table schema
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required
    pub required: bool,
    /// Datatype
    pub field_type: Box<Type>,
    /// Fields may have an optional comment or doc string.
    pub doc: Option<String>,
    /// Used to populate the field's value for all records that were written before the field was added to the schema
    pub initial_default: Option<Literal>,
    /// Used to populate the field's value for any records written after the field was added to the schema, if the writer does not supply the field's value
    pub write_default: Option<Literal>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
struct SerdeNestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: Box<Type>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_default: Option<JsonValue>,
}

impl From<SerdeNestedField> for NestedField {
    fn from(value: SerdeNestedField) -> Self {
        NestedField {
            id: value.id,
            name: value.name,
            required: value.required,
            initial_default: value.initial_default.and_then(|x| {
                Literal::try_from_json(x, &value.field_type)
                    .ok()
                    .and_then(|x| x)
            }),
            write_default: value.write_default.and_then(|x| {
                Literal::try_from_json(x, &value.field_type)
                    .ok()
                    .and_then(|x| x)
            }),
            field_type: value.field_type,
            doc: value.doc,
        }
    }
}

impl From<NestedField> for SerdeNestedField {
    fn from(value: NestedField) -> Self {
        let initial_default = value
            .initial_default
            .and_then(|x| x.try_into_json(&value.field_type).ok());
        let write_default = value
            .write_default
            .and_then(|x| x.try_into_json(&value.field_type).ok());
        SerdeNestedField {
            id: value.id,
            name: value.name,
            required: value.required,
            field_type: value.field_type,
            doc: value.doc,
            initial_default,
            write_default,
        }
    }
}

/// Reference to nested field.
pub type NestedFieldRef = Arc<NestedField>;

impl NestedField {
    /// Construct a new field.
    pub fn new(id: i32, name: impl ToString, field_type: Type, required: bool) -> Self {
        Self {
            id,
            name: name.to_string(),
            required,
            field_type: Box::new(field_type),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Construct a required field.
    pub fn required(id: i32, name: impl ToString, field_type: Type) -> Self {
        Self::new(id, name, field_type, true)
    }

    /// Construct an optional field.
    pub fn optional(id: i32, name: impl ToString, field_type: Type) -> Self {
        Self::new(id, name, field_type, false)
    }

    /// Construct list type's element field.
    pub fn list_element(id: i32, field_type: Type, required: bool) -> Self {
        Self::new(id, ICEBERG_LIST_FIELD_NAME, field_type, required)
    }

    /// Construct map type's key field.
    pub fn map_key_element(id: i32, field_type: Type) -> Self {
        Self::required(id, ICEBERG_MAP_KEY_FIELD_NAME, field_type)
    }

    /// Construct map type's value field.
    pub fn map_value_element(id: i32, field_type: Type, required: bool) -> Self {
        Self::new(id, ICEBERG_MAP_VALUE_FIELD_NAME, field_type, required)
    }

    /// Set the field's doc.
    pub fn with_doc(mut self, doc: impl ToString) -> Self {
        self.doc = Some(doc.to_string());
        self
    }

    /// Set the field's initial default value.
    pub fn with_initial_default(mut self, value: Literal) -> Self {
        self.initial_default = Some(value);
        self
    }

    /// Set the field's initial default value.
    pub fn with_write_default(mut self, value: Literal) -> Self {
        self.write_default = Some(value);
        self
    }

    /// Set the id of the field.
    #[allow(unused)]
    pub(crate) fn with_id(mut self, id: i32) -> Self {
        self.id = id;
        self
    }
}

impl fmt::Display for NestedField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: ", self.id)?;
        write!(f, "{}: ", self.name)?;
        if self.required {
            write!(f, "required ")?;
        } else {
            write!(f, "optional ")?;
        }
        write!(f, "{} ", self.field_type)?;
        if let Some(doc) = &self.doc {
            write!(f, "{}", doc)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// A list is a collection of values with some element type. The element field has an integer id that is unique in the table schema.
/// Elements can be either optional or required. Element types may be any type.
pub struct ListType {
    /// Element field of list type.
    pub element_field: NestedFieldRef,
}

impl ListType {
    /// Construct a list type with the given element field.
    pub fn new(element_field: NestedFieldRef) -> Self {
        Self { element_field }
    }
}

/// Module for type serialization/deserialization.
pub(super) mod _serde {
    use std::borrow::Cow;

    use serde::{Deserialize, Serialize};

    use super::Type::Map;
    use super::{ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type};

    /// List type for serialization and deserialization
    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    pub(super) enum SerdeType<'a> {
        #[serde(rename_all = "kebab-case")]
        List {
            r#type: String,
            element_id: i32,
            element_required: bool,
            element: Cow<'a, Type>,
        },
        Struct {
            r#type: String,
            fields: Cow<'a, [NestedFieldRef]>,
        },
        #[serde(rename_all = "kebab-case")]
        Map {
            r#type: String,
            key_id: i32,
            key: Cow<'a, Type>,
            value_id: i32,
            value_required: bool,
            value: Cow<'a, Type>,
        },
        Primitive(PrimitiveType),
    }

    impl From<SerdeType<'_>> for Type {
        fn from(value: SerdeType) -> Self {
            match value {
                SerdeType::List {
                    r#type: _,
                    element_id,
                    element_required,
                    element,
                } => Self::List(ListType {
                    element_field: NestedField::list_element(
                        element_id,
                        element.into_owned(),
                        element_required,
                    )
                    .into(),
                }),
                SerdeType::Map {
                    r#type: _,
                    key_id,
                    key,
                    value_id,
                    value_required,
                    value,
                } => Map(MapType {
                    key_field: NestedField::map_key_element(key_id, key.into_owned()).into(),
                    value_field: NestedField::map_value_element(
                        value_id,
                        value.into_owned(),
                        value_required,
                    )
                    .into(),
                }),
                SerdeType::Struct { r#type: _, fields } => {
                    Self::Struct(StructType::new(fields.into_owned()))
                }
                SerdeType::Primitive(p) => Self::Primitive(p.clone()),
            }
        }
    }

    impl<'a> From<&'a Type> for SerdeType<'a> {
        fn from(value: &'a Type) -> Self {
            match value {
                Type::List(list) => SerdeType::List {
                    r#type: "list".to_string(),
                    element_id: list.element_field.id,
                    element_required: list.element_field.required,
                    element: Cow::Borrowed(&list.element_field.field_type),
                },
                Type::Map(map) => SerdeType::Map {
                    r#type: "map".to_string(),
                    key_id: map.key_field.id,
                    key: Cow::Borrowed(&map.key_field.field_type),
                    value_id: map.value_field.id,
                    value_required: map.value_field.required,
                    value: Cow::Borrowed(&map.value_field.field_type),
                },
                Type::Struct(s) => SerdeType::Struct {
                    r#type: "struct".to_string(),
                    fields: Cow::Borrowed(&s.fields),
                },
                Type::Primitive(p) => SerdeType::Primitive(p.clone()),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique in the table schema.
/// Map keys are required and map values can be either optional or required.
/// Both map keys and map values may be any type, including nested types.
pub struct MapType {
    /// Field for key.
    pub key_field: NestedFieldRef,
    /// Field for value.
    pub value_field: NestedFieldRef,
}

impl MapType {
    /// Construct a map type with the given key and value fields.
    pub fn new(key_field: NestedFieldRef, value_field: NestedFieldRef) -> Self {
        Self {
            key_field,
            value_field,
        }
    }
}
