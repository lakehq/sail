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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/values.rs

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Typed single-value used for lower/upper bounds
pub struct Datum {
    /// Primitive data type of the datum
    pub r#type: crate::spec::types::PrimitiveType,
    /// Primitive literal value
    pub literal: PrimitiveLiteral,
}

impl Datum {
    pub fn new(r#type: crate::spec::types::PrimitiveType, literal: PrimitiveLiteral) -> Self {
        Self { r#type, literal }
    }
}

impl Literal {
    // TODO: Type-aware JSON conversion
    pub fn try_from_json(
        value: JsonValue,
        _data_type: &crate::spec::types::Type,
    ) -> Result<Option<Self>, String> {
        match value {
            JsonValue::Null => Ok(None),
            _ => Ok(Some(Literal::Primitive(PrimitiveLiteral::String(
                value.to_string(),
            )))),
        }
    }

    // TODO: Type-aware JSON conversion
    pub fn try_into_json(
        &self,
        _data_type: &crate::spec::types::Type,
    ) -> Result<JsonValue, String> {
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
