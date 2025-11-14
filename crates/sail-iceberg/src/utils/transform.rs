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

/// Partition transformation utilities for Iceberg
///
/// This module contains pure computational functions for applying Iceberg partition
/// transforms like bucket, truncate, year, month, day, and hour.
use chrono::Datelike;
use uuid::Uuid;

use crate::spec::transform::Transform;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};

/// Apply an Iceberg partition transform to a value.
///
/// Returns the transformed value according to the Iceberg specification.
pub fn apply_transform(
    transform: Transform,
    field_type: &Type,
    value: Option<Literal>,
) -> Option<Literal> {
    match transform {
        Transform::Identity | Transform::Unknown | Transform::Void => value,
        Transform::Truncate(w) => match value {
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                let taken = s.chars().take(w as usize).collect::<String>();
                Some(Literal::Primitive(PrimitiveLiteral::String(taken)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                let w = w as i32;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Int(v - rem)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => {
                let w = w as i64;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Long(v - rem)))
            }
            other => other,
        },
        Transform::Bucket(n) => match value {
            None => None,
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_int(v, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_long(v, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Int128(v))) => Some(Literal::Primitive(
                PrimitiveLiteral::Int(bucket_decimal(v, n)),
            )),
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_str(&s, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::UInt128(v))) => {
                let uuid = Uuid::from_u128(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_bytes(
                    uuid.as_bytes(),
                    n,
                ))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Binary(b))) => Some(Literal::Primitive(
                PrimitiveLiteral::Int(bucket_bytes(&b, n)),
            )),
            // Unsupported bucket types fallback to pass-through
            other => other,
        },
        // For time-based transforms, convert to integer offsets per Iceberg spec
        Transform::Day => match value {
            // If already days since epoch
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(v)))
            }
            // If timestamp in microseconds since epoch
            Some(Literal::Primitive(PrimitiveLiteral::Long(us))) => {
                let days = (us).div_euclid(86_400_000_000);
                // Safe to downcast within reasonable date ranges used in tests
                let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
                Some(Literal::Primitive(PrimitiveLiteral::Int(days_i32)))
            }
            other => other,
        },
        // Year: years since 1970 for date/timestamp
        Transform::Year => match (field_type, value.clone()) {
            (
                Type::Primitive(PrimitiveType::Date),
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // days -> year offset from 1970
                let year = days_to_year(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(year)))
            }
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let year = micros_to_year(micros);
                Some(Literal::Primitive(PrimitiveLiteral::Int(year)))
            }
            _ => value,
        },
        // Month: months since 1970-01 for date/timestamp
        Transform::Month => match (field_type, value.clone()) {
            (
                Type::Primitive(PrimitiveType::Date),
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                let months = days_to_months(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(months)))
            }
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let months = micros_to_months(micros);
                Some(Literal::Primitive(PrimitiveLiteral::Int(months)))
            }
            _ => value,
        },
        // Hour: hours since epoch for timestamp
        Transform::Hour => match (field_type, value.clone()) {
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let hours = micros.div_euclid(3_600_000_000);
                // safe downcast in typical ranges
                let hours_i32 = i32::try_from(hours).unwrap_or(i32::MAX);
                Some(Literal::Primitive(PrimitiveLiteral::Int(hours_i32)))
            }
            _ => value,
        },
    }
}

// ==== Helpers for temporal transforms ====
const UNIX_EPOCH_YEAR: i32 = 1970;

pub fn days_to_year(days: i32) -> i32 {
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Days::new(days as u64);
    date.year() - UNIX_EPOCH_YEAR
}

pub fn micros_to_year(micros: i64) -> i32 {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.year() - UNIX_EPOCH_YEAR)
        .unwrap_or(0)
}

pub fn days_to_months(days: i32) -> i32 {
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Days::new(days as u64);
    (date.year() - UNIX_EPOCH_YEAR) * 12 + (date.month0() as i32)
}

pub fn micros_to_months(micros: i64) -> i32 {
    let date = match chrono::DateTime::from_timestamp_micros(micros) {
        Some(dt) => dt,
        None => return 0,
    };
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::DateTime::from_timestamp_micros(0).unwrap();
    if date > epoch {
        (date.year() - UNIX_EPOCH_YEAR) * 12 + (date.month0() as i32)
    } else {
        let delta = (12 - date.month0() as i32) + 12 * (UNIX_EPOCH_YEAR - date.year() - 1);
        -delta
    }
}

// ==== Helpers for bucket transform (Murmur3) ====
#[inline]
#[allow(clippy::unwrap_used)]
fn hash_bytes(v: &[u8]) -> i32 {
    let mut rdr = v;
    murmur3::murmur3_32(&mut rdr, 0).unwrap() as i32
}

#[inline]
fn hash_int(v: i32) -> i32 {
    hash_long(v as i64)
}

#[inline]
fn hash_long(v: i64) -> i32 {
    hash_bytes(&v.to_le_bytes())
}

#[inline]
fn hash_decimal(v: i128) -> i32 {
    let bytes = v.to_be_bytes();
    if let Some(start) = bytes.iter().position(|&x| x != 0) {
        hash_bytes(&bytes[start..])
    } else {
        hash_bytes(&[0])
    }
}

#[inline]
fn bucket_n(hash: i32, n: u32) -> i32 {
    (hash & i32::MAX) % (n as i32)
}

#[inline]
pub fn bucket_int(v: i32, n: u32) -> i32 {
    bucket_n(hash_int(v), n)
}

#[inline]
pub fn bucket_long(v: i64, n: u32) -> i32 {
    bucket_n(hash_long(v), n)
}

#[inline]
pub fn bucket_decimal(v: i128, n: u32) -> i32 {
    bucket_n(hash_decimal(v), n)
}

#[inline]
pub fn bucket_str(s: &str, n: u32) -> i32 {
    bucket_n(hash_bytes(s.as_bytes()), n)
}

#[inline]
pub fn bucket_bytes(b: &[u8], n: u32) -> i32 {
    bucket_n(hash_bytes(b), n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_year() {
        // 0 days = 1970
        assert_eq!(days_to_year(0), 0);
        // 365 days = 1971
        assert_eq!(days_to_year(365), 1);
        // 730 days = 1972
        assert_eq!(days_to_year(730), 2);
    }

    #[test]
    fn test_bucket_int() {
        let result = bucket_int(42, 10);
        assert!((0..10).contains(&result));
    }

    #[test]
    fn test_bucket_str() {
        let result = bucket_str("test", 10);
        assert!((0..10).contains(&result));
    }
}
