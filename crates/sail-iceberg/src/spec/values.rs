use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Literal values used in Iceberg
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Literal {
    Primitive(PrimitiveLiteral),
    Struct(Vec<(String, Option<Literal>)>),
    List(Vec<Option<Literal>>),
    Map(Vec<(Literal, Option<Literal>)>),
}

/// Primitive literal values
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimitiveLiteral {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    Int128(i128),
    String(String),
    UInt128(u128),
    Binary(Vec<u8>),
}

impl Literal {
    // TODO: Type-aware JSON conversion
    pub fn try_from_json(
        value: JsonValue,
        _data_type: &crate::spec::Type,
    ) -> Result<Option<Self>, String> {
        match value {
            JsonValue::Null => Ok(None),
            _ => Ok(Some(Literal::Primitive(PrimitiveLiteral::String(
                value.to_string(),
            )))),
        }
    }

    // TODO: Type-aware JSON conversion
    pub fn try_into_json(&self, _data_type: &crate::spec::Type) -> Result<JsonValue, String> {
        match self {
            Literal::Primitive(p) => match p {
                PrimitiveLiteral::Boolean(v) => Ok(JsonValue::Bool(*v)),
                PrimitiveLiteral::Int(v) => Ok(JsonValue::Number((*v).into())),
                PrimitiveLiteral::Long(v) => Ok(JsonValue::Number((*v).into())),
                PrimitiveLiteral::String(v) => Ok(JsonValue::String(v.clone())),
                _ => Ok(JsonValue::String(format!("{:?}", p))),
            },
            _ => Ok(JsonValue::String(format!("{:?}", self))),
        }
    }
}
