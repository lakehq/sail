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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/transform.rs

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::types::values::Literal;
use super::types::{PrimitiveType, Type};

/// Transform is used to transform predicates to partition predicates,
/// in addition to transforming data values.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Transform {
    /// Source value, unmodified
    Identity,
    /// Hash of value, mod `N`.
    Bucket(u32),
    /// Value truncated to width `W`
    Truncate(u32),
    /// Extract a date or timestamp year, as years from 1970
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    Hour,
    /// Always produces `null`
    Void,
    /// Used to represent some customized transform that can't be recognized or supported now.
    Unknown,
}

impl Transform {
    pub fn to_human_string(self, field_type: &Type, value: Option<&Literal>) -> String {
        fn bytes_to_hex(bytes: &[u8]) -> String {
            let mut s = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                use std::fmt::Write as _;
                let _ = write!(&mut s, "{:02x}", b);
            }
            s
        }

        fn lit_str(l: &Literal) -> String {
            match l {
                Literal::Primitive(p) => match p {
                    super::types::values::PrimitiveLiteral::Boolean(v) => v.to_string(),
                    super::types::values::PrimitiveLiteral::Int(v) => v.to_string(),
                    super::types::values::PrimitiveLiteral::Long(v) => v.to_string(),
                    super::types::values::PrimitiveLiteral::Float(v) => v.0.to_string(),
                    super::types::values::PrimitiveLiteral::Double(v) => v.0.to_string(),
                    super::types::values::PrimitiveLiteral::Int128(v) => v.to_string(),
                    super::types::values::PrimitiveLiteral::String(v) => v.clone(),
                    super::types::values::PrimitiveLiteral::UInt128(v) => v.to_string(),
                    super::types::values::PrimitiveLiteral::Binary(b) => {
                        format!("0x{}", bytes_to_hex(b))
                    }
                },
                Literal::Struct(_) | Literal::List(_) | Literal::Map(_) => format!("{:?}", l),
            }
        }

        match value {
            None => "null".to_string(),
            Some(val) => match self {
                Transform::Identity => lit_str(val),
                Transform::Void => "null".to_string(),
                Transform::Truncate(w) => match (field_type, val) {
                    (
                        Type::Primitive(PrimitiveType::String),
                        Literal::Primitive(super::types::values::PrimitiveLiteral::String(s)),
                    ) => s.chars().take(w as usize).collect::<String>(),
                    (
                        Type::Primitive(PrimitiveType::Int),
                        Literal::Primitive(super::types::values::PrimitiveLiteral::Int(v)),
                    ) => {
                        let w = w as i32;
                        let rem = v.rem_euclid(w);
                        (v - rem).to_string()
                    }
                    (
                        Type::Primitive(PrimitiveType::Long),
                        Literal::Primitive(super::types::values::PrimitiveLiteral::Long(v)),
                    ) => {
                        let w = w as i64;
                        let rem = v.rem_euclid(w);
                        (v - rem).to_string()
                    }
                    _ => lit_str(val),
                },
                Transform::Bucket(n) => format!("bucket[{n}]({})", lit_str(val)),
                Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                    lit_str(val)
                }
                Transform::Unknown => lit_str(val),
            },
        }
    }

    /// Get the return type of transform given the input type.
    /// Returns `None` if it can't be transformed.
    pub fn result_type(&self, input_type: &Type) -> Result<Type, String> {
        match self {
            Transform::Identity => {
                if matches!(input_type, Type::Primitive(_)) {
                    Ok(input_type.clone())
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of identity transform"
                    ))
                }
            }
            Transform::Void => Ok(input_type.clone()),
            Transform::Unknown => Ok(Type::Primitive(PrimitiveType::String)),
            Transform::Bucket(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::Date
                        | PrimitiveType::Time
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::String
                        | PrimitiveType::Uuid
                        | PrimitiveType::Fixed(_)
                        | PrimitiveType::Binary => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of bucket transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of bucket transform"
                    ))
                }
            }
            Transform::Truncate(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::String => Ok(input_type.clone()),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of truncate transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of truncate transform"
                    ))
                }
            }
            Transform::Year | Transform::Month | Transform::Day => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Date
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of date transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of date transform"
                    ))
                }
            }
            Transform::Hour => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of hour transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of hour transform"
                    ))
                }
            }
        }
    }

    /// Whether the transform preserves the order of values.
    pub fn preserves_order(&self) -> bool {
        !matches!(
            self,
            Transform::Void | Transform::Bucket(_) | Transform::Unknown
        )
    }

    /// Unique transform name to deduplicate equivalent transforms in a builder.
    pub fn dedup_name(&self) -> String {
        match self {
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                "time".to_string()
            }
            _ => format!("{self}"),
        }
    }

    /// Whether ordering by this transform satisfies the ordering of another transform.
    pub fn satisfies_order_of(&self, other: &Self) -> bool {
        match self {
            Transform::Identity => other.preserves_order(),
            Transform::Hour => matches!(
                other,
                Transform::Hour | Transform::Day | Transform::Month | Transform::Year
            ),
            Transform::Day => matches!(other, Transform::Day | Transform::Month | Transform::Year),
            Transform::Month => matches!(other, Transform::Month | Transform::Year),
            _ => self == other,
        }
    }
}

impl Display for Transform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Bucket(n) => write!(f, "bucket[{}]", n),
            Transform::Truncate(w) => write!(f, "truncate[{}]", w),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
            Transform::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for Transform {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "identity" => Ok(Transform::Identity),
            "year" => Ok(Transform::Year),
            "month" => Ok(Transform::Month),
            "day" => Ok(Transform::Day),
            "hour" => Ok(Transform::Hour),
            "void" => Ok(Transform::Void),
            _ => {
                if let Some(bucket_str) =
                    s.strip_prefix("bucket[").and_then(|s| s.strip_suffix(']'))
                {
                    let n: u32 = bucket_str
                        .parse()
                        .map_err(|_| format!("Invalid bucket parameter: {}", bucket_str))?;
                    Ok(Transform::Bucket(n))
                } else if let Some(truncate_str) = s
                    .strip_prefix("truncate[")
                    .and_then(|s| s.strip_suffix(']'))
                {
                    let w: u32 = truncate_str
                        .parse()
                        .map_err(|_| format!("Invalid truncate parameter: {}", truncate_str))?;
                    Ok(Transform::Truncate(w))
                } else {
                    Ok(Transform::Unknown)
                }
            }
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Transform::from_str(&s).map_err(serde::de::Error::custom)
    }
}
