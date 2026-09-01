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

use base64::Engine;
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
        fn int_value(value: &Literal) -> Option<i32> {
            match value {
                Literal::Primitive(super::types::values::PrimitiveLiteral::Int(value)) => {
                    Some(*value)
                }
                _ => None,
            }
        }

        fn human_day(day: i32) -> Option<String> {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            epoch
                .checked_add_signed(chrono::Duration::days(i64::from(day)))
                .map(|date| date.format("%Y-%m-%d").to_string())
        }

        fn human_hour(hour: i32) -> Option<String> {
            let day = hour.div_euclid(24);
            let hour_of_day = hour.rem_euclid(24);
            human_day(day).map(|date| format!("{date}-{hour_of_day:02}"))
        }

        fn human_decimal(value: i128, scale: u32) -> String {
            if scale > 38 {
                return value.to_string();
            }
            let digits = value.unsigned_abs().to_string();
            let scale = scale as usize;
            let negative = value.is_negative();
            let sign = if negative { "-" } else { "" };
            if scale == 0 {
                return format!("{sign}{digits}");
            }
            let adjusted_exponent = digits.len() as i64 - scale as i64 - 1;
            if adjusted_exponent >= -6 {
                if digits.len() <= scale {
                    format!("{sign}0.{}{}", "0".repeat(scale - digits.len()), digits)
                } else {
                    let point = digits.len() - scale;
                    format!("{sign}{}.{}", &digits[..point], &digits[point..])
                }
            } else if digits.len() == 1 {
                format!("{sign}{digits}E{adjusted_exponent}")
            } else {
                format!(
                    "{sign}{}.{}E{adjusted_exponent}",
                    &digits[..1],
                    &digits[1..]
                )
            }
        }

        fn format_local_time(time: chrono::NaiveTime) -> String {
            use chrono::Timelike;

            let mut value = time.format("%H:%M").to_string();
            if time.second() != 0 || time.nanosecond() != 0 {
                value.push_str(&format!(":{:02}", time.second()));
            }
            if time.nanosecond() != 0 {
                let (width, divisor) = if time.nanosecond().is_multiple_of(1_000_000) {
                    (3_usize, 1_000_000_u32)
                } else if time.nanosecond().is_multiple_of(1_000) {
                    (6_usize, 1_000_u32)
                } else {
                    (9_usize, 1_u32)
                };
                value.push_str(&format!(".{:0width$}", time.nanosecond() / divisor));
            }
            value
        }

        fn java_float_repr(negative: bool, rust_scientific: &str) -> String {
            let Some((mantissa, exponent)) = rust_scientific.split_once('e') else {
                return rust_scientific.to_string();
            };
            let Ok(exponent) = exponent.parse::<i32>() else {
                return rust_scientific.to_string();
            };
            let digits = mantissa.replace('.', "");
            let sign = if negative { "-" } else { "" };
            if (-3..7).contains(&exponent) {
                let body = if exponent >= 0 {
                    let integer_digits = exponent as usize + 1;
                    if integer_digits >= digits.len() {
                        format!("{}{}.0", digits, "0".repeat(integer_digits - digits.len()))
                    } else {
                        format!(
                            "{}.{}",
                            &digits[..integer_digits],
                            &digits[integer_digits..]
                        )
                    }
                } else {
                    format!("0.{}{}", "0".repeat((-exponent - 1) as usize), digits)
                };
                format!("{sign}{body}")
            } else {
                let mantissa = if digits.len() == 1 {
                    format!("{digits}.0")
                } else {
                    format!("{}.{}", &digits[..1], &digits[1..])
                };
                format!("{sign}{mantissa}E{exponent}")
            }
        }

        fn human_float(value: f32) -> String {
            if value.is_nan() {
                "NaN".to_string()
            } else if value.is_infinite() {
                if value.is_sign_positive() {
                    "Infinity".to_string()
                } else {
                    "-Infinity".to_string()
                }
            } else if value == 0.0 {
                if value.is_sign_negative() {
                    "-0.0".to_string()
                } else {
                    "0.0".to_string()
                }
            } else {
                java_float_repr(value.is_sign_negative(), &format!("{:e}", value.abs()))
            }
        }

        fn human_double(value: f64) -> String {
            if value.is_nan() {
                "NaN".to_string()
            } else if value.is_infinite() {
                if value.is_sign_positive() {
                    "Infinity".to_string()
                } else {
                    "-Infinity".to_string()
                }
            } else if value == 0.0 {
                if value.is_sign_negative() {
                    "-0.0".to_string()
                } else {
                    "0.0".to_string()
                }
            } else {
                java_float_repr(value.is_sign_negative(), &format!("{:e}", value.abs()))
            }
        }

        fn human_time(micros: i64) -> Option<String> {
            let seconds = u32::try_from(micros.div_euclid(1_000_000)).ok()?;
            let nanos = u32::try_from(micros.rem_euclid(1_000_000) * 1_000).ok()?;
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)
                .map(format_local_time)
        }

        fn human_timestamp(value: i64, units_per_second: i64, with_zone: bool) -> Option<String> {
            let seconds = value.div_euclid(units_per_second);
            let nanos = value.rem_euclid(units_per_second) * (1_000_000_000 / units_per_second);
            let timestamp = chrono::DateTime::from_timestamp(seconds, u32::try_from(nanos).ok()?)?;
            let timestamp = timestamp.naive_utc();
            let value = format!(
                "{}T{}",
                timestamp.date().format("%Y-%m-%d"),
                format_local_time(timestamp.time())
            );
            Some(if with_zone {
                format!("{value}+00:00")
            } else {
                value
            })
        }

        fn literal_string(field_type: &Type, value: &Literal) -> String {
            use super::types::values::PrimitiveLiteral;

            match (field_type, value) {
                (
                    Type::Primitive(PrimitiveType::Date),
                    Literal::Primitive(PrimitiveLiteral::Int(value)),
                ) => human_day(*value).unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::Time),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => human_time(*value).unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::Timestamp),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => human_timestamp(*value, 1_000_000, false).unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::Timestamptz),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => human_timestamp(*value, 1_000_000, true).unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::TimestampNs),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => human_timestamp(*value, 1_000_000_000, false)
                    .unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                    Literal::Primitive(PrimitiveLiteral::Long(value)),
                ) => human_timestamp(*value, 1_000_000_000, true)
                    .unwrap_or_else(|| value.to_string()),
                (
                    Type::Primitive(PrimitiveType::Decimal { scale, .. }),
                    Literal::Primitive(PrimitiveLiteral::Int128(value)),
                ) => human_decimal(*value, *scale),
                (
                    Type::Primitive(PrimitiveType::Uuid),
                    Literal::Primitive(PrimitiveLiteral::UInt128(value)),
                ) => uuid::Uuid::from_u128(*value).to_string(),
                (
                    Type::Primitive(
                        PrimitiveType::Fixed(_)
                        | PrimitiveType::Binary
                        | PrimitiveType::Variant
                        | PrimitiveType::Geometry { .. }
                        | PrimitiveType::Geography { .. },
                    ),
                    Literal::Primitive(PrimitiveLiteral::Binary(value)),
                ) => base64::engine::general_purpose::STANDARD.encode(value),
                (_, Literal::Primitive(PrimitiveLiteral::Boolean(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Int(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Long(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Float(value))) => human_float(value.0),
                (_, Literal::Primitive(PrimitiveLiteral::Double(value))) => human_double(value.0),
                (_, Literal::Primitive(PrimitiveLiteral::Int128(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::String(value))) => value.clone(),
                (_, Literal::Primitive(PrimitiveLiteral::UInt128(value))) => value.to_string(),
                (_, Literal::Primitive(PrimitiveLiteral::Binary(value))) => {
                    base64::engine::general_purpose::STANDARD.encode(value)
                }
                (_, Literal::Struct(_) | Literal::List(_) | Literal::Map(_)) => {
                    format!("{value:?}")
                }
            }
        }

        match value {
            None => "null".to_string(),
            Some(val) => match self {
                Transform::Identity | Transform::Bucket(_) | Transform::Truncate(_) => {
                    literal_string(field_type, val)
                }
                Transform::Void => "null".to_string(),
                Transform::Year => int_value(val)
                    .and_then(|year| 1970_i32.checked_add(year))
                    .map(|year| format!("{year:04}"))
                    .unwrap_or_else(|| literal_string(field_type, val)),
                Transform::Month => int_value(val)
                    .and_then(|month| {
                        let year = 1970_i32.checked_add(month.div_euclid(12))?;
                        Some(format!("{year:04}-{:02}", month.rem_euclid(12) + 1))
                    })
                    .unwrap_or_else(|| literal_string(field_type, val)),
                Transform::Day => int_value(val)
                    .and_then(human_day)
                    .unwrap_or_else(|| literal_string(field_type, val)),
                Transform::Hour => int_value(val)
                    .and_then(human_hour)
                    .unwrap_or_else(|| literal_string(field_type, val)),
                Transform::Unknown => literal_string(field_type, val),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::values::PrimitiveLiteral;

    fn integer(value: i32) -> Literal {
        Literal::Primitive(PrimitiveLiteral::Int(value))
    }

    #[test]
    fn temporal_transforms_use_iceberg_human_strings() {
        let integer_type = Type::Primitive(PrimitiveType::Int);

        assert_eq!(
            Transform::Year.to_human_string(&integer_type, Some(&integer(47))),
            "2017"
        );
        assert_eq!(
            Transform::Month.to_human_string(&integer_type, Some(&integer(574))),
            "2017-11"
        );
        assert_eq!(
            Transform::Day.to_human_string(&integer_type, Some(&integer(17_486))),
            "2017-11-16"
        );
        assert_eq!(
            Transform::Hour.to_human_string(&integer_type, Some(&integer(419_686))),
            "2017-11-16-22"
        );
    }

    #[test]
    fn temporal_transforms_floor_negative_ordinals() {
        let integer_type = Type::Primitive(PrimitiveType::Int);

        assert_eq!(
            Transform::Year.to_human_string(&integer_type, Some(&integer(-1))),
            "1969"
        );
        assert_eq!(
            Transform::Month.to_human_string(&integer_type, Some(&integer(-1))),
            "1969-12"
        );
        assert_eq!(
            Transform::Day.to_human_string(&integer_type, Some(&integer(-1))),
            "1969-12-31"
        );
        assert_eq!(
            Transform::Hour.to_human_string(&integer_type, Some(&integer(-1))),
            "1969-12-31-23"
        );
    }

    #[test]
    fn identity_transform_uses_iceberg_human_strings() {
        let identity = Transform::Identity;

        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Date),
                Some(&integer(17_501))
            ),
            "2017-12-01"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Time),
                Some(&Literal::Primitive(PrimitiveLiteral::Long(36_775_038_194))),
            ),
            "10:12:55.038194"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Timestamp),
                Some(&Literal::Primitive(PrimitiveLiteral::Long(
                    1_512_123_175_038_194,
                ))),
            ),
            "2017-12-01T10:12:55.038194"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Timestamptz),
                Some(&Literal::Primitive(PrimitiveLiteral::Long(
                    1_512_151_975_038_194,
                ))),
            ),
            "2017-12-01T18:12:55.038194+00:00"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::TimestampNs),
                Some(&Literal::Primitive(PrimitiveLiteral::Long(1))),
            ),
            "1970-01-01T00:00:00.000000001"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
                Some(&Literal::Primitive(PrimitiveLiteral::Int128(-150))),
            ),
            "-1.50"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Binary),
                Some(&Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3]))),
            ),
            "AQID"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Timestamp),
                Some(&Literal::Primitive(PrimitiveLiteral::Long(0))),
            ),
            "1970-01-01T00:00"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Float),
                Some(&Literal::Primitive(PrimitiveLiteral::Float(
                    ordered_float::OrderedFloat(10_000_000.0),
                ))),
            ),
            "1.0E7"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Double),
                Some(&Literal::Primitive(PrimitiveLiteral::Double(
                    ordered_float::OrderedFloat(-0.0),
                ))),
            ),
            "-0.0"
        );
        assert_eq!(
            identity.to_human_string(
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 7,
                }),
                Some(&Literal::Primitive(PrimitiveLiteral::Int128(1))),
            ),
            "1E-7"
        );
    }

    #[test]
    fn bucket_and_truncate_format_the_transformed_value() {
        let integer_type = Type::Primitive(PrimitiveType::Int);

        assert_eq!(
            Transform::Bucket(16).to_human_string(&integer_type, Some(&integer(7))),
            "7"
        );
        assert_eq!(
            Transform::Truncate(10).to_human_string(&integer_type, Some(&integer(120))),
            "120"
        );
    }
}
