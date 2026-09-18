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
        Transform::Identity | Transform::Unknown => value,
        Transform::Void => None,
        Transform::Truncate(w) => match value {
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                let taken = s.chars().take(w as usize).collect::<String>();
                Some(Literal::Primitive(PrimitiveLiteral::String(taken)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Binary(mut bytes))) => {
                bytes.truncate(w as usize);
                Some(Literal::Primitive(PrimitiveLiteral::Binary(bytes)))
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
            Some(Literal::Primitive(PrimitiveLiteral::Int128(v))) => {
                let width = i128::from(w);
                Some(Literal::Primitive(PrimitiveLiteral::Int128(
                    v - v.rem_euclid(width),
                )))
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
        Transform::Day => match (field_type, value.clone()) {
            (
                Type::Primitive(PrimitiveType::Date),
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
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
                        us_or_ns.div_euclid(1_000)
                    }
                    _ => us_or_ns,
                };
                let days = micros.div_euclid(86_400_000_000);
                // Safe to downcast within reasonable date ranges used in tests
                let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
                Some(Literal::Primitive(PrimitiveLiteral::Int(days_i32)))
            }
            _ => value,
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
                        us_or_ns.div_euclid(1_000)
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
                        us_or_ns.div_euclid(1_000)
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
                        us_or_ns.div_euclid(1_000)
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
    let (year, _) = year_month_from_days(days);
    year - UNIX_EPOCH_YEAR
}

pub fn micros_to_year(micros: i64) -> i32 {
    days_to_year(micros.div_euclid(86_400_000_000) as i32)
}

pub fn days_to_months(days: i32) -> i32 {
    let (year, month) = year_month_from_days(days);
    (year - UNIX_EPOCH_YEAR) * 12 + month - 1
}

pub fn micros_to_months(micros: i64) -> i32 {
    days_to_months(micros.div_euclid(86_400_000_000) as i32)
}

fn year_month_from_days(days: i32) -> (i32, i32) {
    // March-based Gregorian eras cover the full Date32 range, including dates
    // outside chrono's supported years. Euclidean division handles pre-epoch dates.
    let days = i64::from(days) + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let month = march_month + if march_month < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year as i32, month as i32)
}

// ==== Helpers for bucket transform (Murmur3) ====
#[inline]
#[expect(clippy::unwrap_used)]
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
    let mut start = 0;
    while start < bytes.len() - 1 {
        let redundant = (bytes[start] == 0 && bytes[start + 1] & 0x80 == 0)
            || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0);
        if !redundant {
            break;
        }
        start += 1;
    }
    hash_bytes(&bytes[start..])
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
    #[expect(clippy::expect_used)]
    fn temporal_transforms_match_signed_gregorian_dates() {
        use chrono::Datelike;

        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        for days in (-800_000..800_000).step_by(31).chain([-1, 0, 1, -3653]) {
            let date = epoch + chrono::TimeDelta::days(i64::from(days));
            let year = date.year() - 1970;
            let months = year * 12 + date.month0() as i32;
            assert_eq!(days_to_year(days), year);
            assert_eq!(days_to_months(days), months);
            assert_eq!(micros_to_year(i64::from(days) * 86_400_000_000), year);
            assert_eq!(micros_to_months(i64::from(days) * 86_400_000_000), months);
        }
        assert_eq!(micros_to_year(-1), -1);
        assert_eq!(micros_to_months(-1), -1);
        // The complete Date32 domain fits in year/month offsets.
        assert!(days_to_year(i32::MIN) < -5_000_000);
        assert!(days_to_months(i32::MAX) > 60_000_000);
    }

    #[test]
    fn decimal_bucket_preserves_twos_complement_sign() {
        for (value, bytes) in [
            (0, vec![0]),
            (127, vec![127]),
            (128, vec![0, 128]),
            (255, vec![0, 255]),
            (256, vec![1, 0]),
            (-1, vec![255]),
            (-128, vec![128]),
            (-129, vec![255, 127]),
        ] {
            assert_eq!(hash_decimal(value), hash_bytes(&bytes), "{value}");
        }
        assert_eq!(bucket_decimal(128, 100), 49);
    }

    #[test]
    fn decimal_truncate_uses_floor_on_unscaled_values() {
        let data_type = Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        });
        for (value, expected) in [(1065, 1050), (-1065, -1100), (-1050, -1050), (0, 0)] {
            assert_eq!(
                apply_transform(
                    Transform::Truncate(50),
                    &data_type,
                    Some(Literal::Primitive(PrimitiveLiteral::Int128(value)))
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Int128(expected)))
            );
        }
    }

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
