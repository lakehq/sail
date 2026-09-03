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

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{NaiveDate, NaiveTime, Timelike};
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
        fn decimal_string(value: i128, scale: u32) -> String {
            let negative = value < 0;
            let mut digits = value.unsigned_abs().to_string();
            let scale = scale as usize;
            let adjusted_exponent = digits.len() as i64 - scale as i64 - 1;
            if scale > 0 && adjusted_exponent >= -6 {
                if digits.len() <= scale {
                    digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
                }
                digits.insert(digits.len() - scale, '.');
            } else if adjusted_exponent < -6 {
                if digits.len() > 1 {
                    digits.insert(1, '.');
                }
                digits.push('E');
                if adjusted_exponent >= 0 {
                    digits.push('+');
                }
                digits.push_str(&adjusted_exponent.to_string());
            }
            if negative {
                digits.insert(0, '-');
            }
            digits
        }

        fn iso_local_time(time: NaiveTime) -> String {
            let prefix = if time.second() == 0 && time.nanosecond() == 0 {
                format!("{:02}:{:02}", time.hour(), time.minute())
            } else {
                format!(
                    "{:02}:{:02}:{:02}",
                    time.hour(),
                    time.minute(),
                    time.second()
                )
            };
            let nanos = time.nanosecond();
            if nanos == 0 {
                prefix
            } else if nanos.is_multiple_of(1_000_000) {
                format!("{prefix}.{:03}", nanos / 1_000_000)
            } else if nanos.is_multiple_of(1_000) {
                format!("{prefix}.{:06}", nanos / 1_000)
            } else {
                format!("{prefix}.{nanos:09}")
            }
        }

        fn timestamp_string(value: i64, nanos_per_unit: i64, with_zone: bool) -> Option<String> {
            let seconds = value.div_euclid(nanos_per_unit);
            let remainder = value.rem_euclid(nanos_per_unit);
            let nanos = if nanos_per_unit == 1_000_000 {
                u32::try_from(remainder).ok()?.checked_mul(1_000)?
            } else {
                u32::try_from(remainder).ok()?
            };
            let datetime = chrono::DateTime::from_timestamp(seconds, nanos)?.naive_utc();
            let mut output = format!("{}T{}", datetime.date(), iso_local_time(datetime.time()));
            if with_zone {
                output.push_str("+00:00");
            }
            Some(output)
        }

        fn primitive_string(field_type: &Type, value: &Literal) -> String {
            use super::types::values::PrimitiveLiteral;

            match (field_type, value) {
                (_, Literal::Primitive(PrimitiveLiteral::Boolean(value))) => value.to_string(),
                (
                    Type::Primitive(PrimitiveType::Date),
                    Literal::Primitive(PrimitiveLiteral::Int(value)),
                ) => {
                    #[expect(clippy::expect_used)]
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .expect("the Unix epoch is a valid date");
                    epoch
                        .checked_add_signed(chrono::Duration::days(i64::from(*value)))
                        .map_or_else(|| value.to_string(), |date| date.to_string())
                }
                (_, Literal::Primitive(PrimitiveLiteral::Int(value))) => value.to_string(),
                (
                    Type::Primitive(PrimitiveType::Time),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => {
                    let seconds = value.div_euclid(1_000_000);
                    let micros = value.rem_euclid(1_000_000) as u32;
                    u32::try_from(seconds)
                        .ok()
                        .and_then(|seconds| {
                            NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * 1_000)
                        })
                        .map_or_else(|| value.to_string(), iso_local_time)
                }
                (
                    Type::Primitive(PrimitiveType::Timestamp),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => {
                    timestamp_string(*value, 1_000_000, false).unwrap_or_else(|| value.to_string())
                }
                (
                    Type::Primitive(PrimitiveType::Timestamptz),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => timestamp_string(*value, 1_000_000, true).unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::TimestampNs),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => timestamp_string(*value, 1_000_000_000, false)
                    .unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => timestamp_string(*value, 1_000_000_000, true)
                    .unwrap_or_else(|| value.to_string()),
                (_, Literal::Primitive(PrimitiveLiteral::Long(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Float(value))) => value.0.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Double(value))) => value.0.to_string(),
                (
                    Type::Primitive(PrimitiveType::Decimal { scale, .. }),
                    Literal::Primitive(PrimitiveLiteral::Int128(value)),
                ) => decimal_string(*value, *scale),
                (_, Literal::Primitive(PrimitiveLiteral::Int128(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::String(value))) => value.clone(),
                (
                    Type::Primitive(PrimitiveType::Uuid),
                    Literal::Primitive(PrimitiveLiteral::UInt128(value)),
                ) => uuid::Uuid::from_u128(*value).to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::UInt128(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Binary(value))) => {
                    BASE64_STANDARD.encode(value)
                }
                (_, Literal::Struct(_) | Literal::List(_) | Literal::Map(_)) => "null".to_string(),
            }
        }

        let Some(value) = value else {
            return "null".to_string();
        };
        if matches!(self, Transform::Void) {
            return "null".to_string();
        }

        match (self, value) {
            (
                Transform::Year,
                Literal::Primitive(super::types::values::PrimitiveLiteral::Int(v)),
            ) => {
                format!("{:04}", 1970_i64 + i64::from(*v))
            }
            (
                Transform::Month,
                Literal::Primitive(super::types::values::PrimitiveLiteral::Int(v)),
            ) => {
                let year = 1970_i64 + i64::from(*v).div_euclid(12);
                let month = i64::from(*v).rem_euclid(12) + 1;
                format!("{year:04}-{month:02}")
            }
            (
                Transform::Day,
                Literal::Primitive(super::types::values::PrimitiveLiteral::Int(v)),
            ) => {
                #[expect(clippy::expect_used)]
                let epoch =
                    NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is a valid date");
                epoch
                    .checked_add_signed(chrono::Duration::days(i64::from(*v)))
                    .map_or_else(|| v.to_string(), |date| date.to_string())
            }
            (
                Transform::Hour,
                Literal::Primitive(super::types::values::PrimitiveLiteral::Int(v)),
            ) => chrono::DateTime::from_timestamp(i64::from(*v) * 3_600, 0).map_or_else(
                || v.to_string(),
                |datetime| datetime.format("%Y-%m-%d-%H").to_string(),
            ),
            _ => primitive_string(field_type, value),
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
                        | PrimitiveType::TimestamptzNs => {
                            let result_type = if matches!(self, Transform::Day) {
                                PrimitiveType::Date
                            } else {
                                PrimitiveType::Int
                            };
                            Ok(Type::Primitive(result_type))
                        }
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

#[cfg(test)]
mod tests {
    use super::{PrimitiveType, Transform, Type};
    use crate::spec::types::values::{Literal, PrimitiveLiteral};

    #[test]
    fn decimal_human_string_matches_big_decimal_notation() {
        let decimal_type = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 8,
        });
        for (unscaled, expected) in [
            (1, "1E-8"),
            (-1, "-1E-8"),
            (0, "0E-8"),
            (12_300, "0.00012300"),
        ] {
            let literal = Literal::Primitive(PrimitiveLiteral::Int128(unscaled));
            assert_eq!(
                Transform::Identity.to_human_string(&decimal_type, Some(&literal)),
                expected
            );
        }
    }

    #[test]
    fn day_transform_has_date_result_type() {
        let timestamp_type = Type::Primitive(PrimitiveType::Timestamp);
        assert_eq!(
            Transform::Day.result_type(&timestamp_type),
            Ok(Type::Primitive(PrimitiveType::Date))
        );
        assert_eq!(
            Transform::Month.result_type(&timestamp_type),
            Ok(Type::Primitive(PrimitiveType::Int))
        );
    }
}
