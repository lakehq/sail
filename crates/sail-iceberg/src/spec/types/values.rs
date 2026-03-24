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

impl PartialOrd for PrimitiveLiteral {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrimitiveLiteral {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        use PrimitiveLiteral::*;

        match (self, other) {
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Long(a), Long(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.cmp(b),
            (Double(a), Double(b)) => a.cmp(b),
            (Int128(a), Int128(b)) => a.cmp(b),
            (UInt128(a), UInt128(b)) => a.cmp(b),
            (String(a), String(b)) => a.cmp(b),
            (Binary(a), Binary(b)) => a.cmp(b),
            // For different types, use a consistent ordering based on variant index
            (Boolean(_), _) => Ordering::Less,
            (_, Boolean(_)) => Ordering::Greater,
            (Int(_), _) => Ordering::Less,
            (_, Int(_)) => Ordering::Greater,
            (Long(_), _) => Ordering::Less,
            (_, Long(_)) => Ordering::Greater,
            (Float(_), _) => Ordering::Less,
            (_, Float(_)) => Ordering::Greater,
            (Double(_), _) => Ordering::Less,
            (_, Double(_)) => Ordering::Greater,
            (Int128(_), _) => Ordering::Less,
            (_, Int128(_)) => Ordering::Greater,
            (String(_), _) => Ordering::Less,
            (_, String(_)) => Ordering::Greater,
            (UInt128(_), Binary(_)) => Ordering::Less,
            (Binary(_), UInt128(_)) => Ordering::Greater,
        }
    }
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
    pub fn try_from_json(
        value: JsonValue,
        data_type: &crate::spec::types::Type,
    ) -> Result<Option<Self>, String> {
        use chrono::{NaiveDate, NaiveTime, Timelike};
        use serde_json::Number;

        use crate::spec::types::{PrimitiveType, Type};

        fn number_to_i32(n: &Number) -> Result<i32, String> {
            n.as_i64()
                .ok_or_else(|| "Failed to convert json number to i32".to_string())
                .and_then(|v| i32::try_from(v).map_err(|e| e.to_string()))
        }
        fn number_to_i64(n: &Number) -> Result<i64, String> {
            n.as_i64()
                .ok_or_else(|| "Failed to convert json number to i64".to_string())
        }
        fn number_to_f32(n: &Number) -> Result<f32, String> {
            n.as_f64()
                .ok_or_else(|| "Failed to convert json number to f32".to_string())
                .map(|v| v as f32)
        }
        fn number_to_f64(n: &Number) -> Result<f64, String> {
            n.as_f64()
                .ok_or_else(|| "Failed to convert json number to f64".to_string())
        }

        fn parse_date_to_days(s: &str) -> Result<i32, String> {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| format!("Invalid date format: {}", e))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("Bad epoch")?;
            Ok((d - epoch).num_days() as i32)
        }
        fn parse_time_to_micros(s: &str) -> Result<i64, String> {
            // Accept up to nanosecond precision, truncate to microseconds
            let fmt_candidates = ["%H:%M:%S", "%H:%M:%S%.f"];
            let mut last_err: Option<String> = None;
            for fmt in &fmt_candidates {
                match NaiveTime::parse_from_str(s, fmt) {
                    Ok(t) => {
                        let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                            + (t.nanosecond() as i64);
                        return Ok(nanos / 1_000);
                    }
                    Err(e) => last_err = Some(e.to_string()),
                }
            }
            Err(last_err.unwrap_or_else(|| "Invalid time".to_string()))
        }
        fn parse_ts_to_micros(s: &str) -> Result<i64, String> {
            // Accept naive timestamp like 2020-01-01T12:34:56[.ffffff]
            let fmt_candidates = [
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
            ];
            let mut last_err: Option<String> = None;
            for fmt in &fmt_candidates {
                match chrono::NaiveDateTime::parse_from_str(s, fmt) {
                    Ok(dt) => {
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                            .ok_or("Bad epoch")?
                            .and_hms_nano_opt(0, 0, 0, 0)
                            .ok_or("Bad epoch")?;
                        let micros = (dt - epoch).num_microseconds().ok_or("overflow")?;
                        return Ok(micros);
                    }
                    Err(e) => last_err = Some(e.to_string()),
                }
            }
            // Try with timezone (treated as UTC)
            match chrono::DateTime::parse_from_rfc3339(s)
                .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z"))
            {
                Ok(dt) => Ok(dt.timestamp_micros()),
                Err(_) => Err(last_err.unwrap_or_else(|| "Invalid timestamp".to_string())),
            }
        }

        fn parse_uuid_to_u128(s: &str) -> Result<u128, String> {
            let u = uuid::Uuid::parse_str(s).map_err(|e| e.to_string())?;
            let bytes = u.as_bytes();
            let mut acc: u128 = 0;
            for b in bytes.iter() {
                acc = (acc << 8) | (*b as u128);
            }
            Ok(acc)
        }

        fn parse_decimal_to_i128(s: &str, scale: u32) -> Result<i128, String> {
            let s = s.trim();
            if s.is_empty() {
                return Err("empty decimal".to_string());
            }
            let negative = s.starts_with('-');
            let s = if negative || s.starts_with('+') {
                &s[1..]
            } else {
                s
            };
            let mut int_part: i128 = 0;
            let mut frac_part: i128 = 0;
            let mut frac_len: u32 = 0;
            let mut seen_dot = false;
            for ch in s.chars() {
                if ch == '.' {
                    if seen_dot {
                        return Err("multiple decimal points".to_string());
                    }
                    seen_dot = true;
                    continue;
                }
                if !ch.is_ascii_digit() {
                    return Err("invalid decimal".to_string());
                }
                let d = (ch as u8 - b'0') as i128;
                if !seen_dot {
                    int_part = int_part
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d))
                        .ok_or("overflow")?;
                } else if frac_len < scale {
                    frac_part = frac_part
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d))
                        .ok_or("overflow")?;
                    frac_len += 1;
                } else {
                    // truncate extra fractional digits beyond scale (rounding not applied)
                }
            }
            let pow10 = 10i128.pow(scale);
            let scaled = int_part
                .checked_mul(pow10)
                .and_then(|v| v.checked_add(frac_part * 10i128.pow(scale - frac_len)))
                .ok_or("overflow")?;
            Ok(if negative { -scaled } else { scaled })
        }

        Ok(match (data_type, value) {
            (_, JsonValue::Null) => None,
            (Type::Primitive(PrimitiveType::Boolean), JsonValue::Bool(v)) => {
                Some(Literal::Primitive(PrimitiveLiteral::Boolean(v)))
            }
            (Type::Primitive(PrimitiveType::Int), JsonValue::Number(n)) => Some(
                Literal::Primitive(PrimitiveLiteral::Int(number_to_i32(&n)?)),
            ),
            (Type::Primitive(PrimitiveType::Long), JsonValue::Number(n)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(number_to_i64(&n)?)),
            ),
            (Type::Primitive(PrimitiveType::Float), JsonValue::Number(n)) => Some(
                Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(number_to_f32(&n)?))),
            ),
            (Type::Primitive(PrimitiveType::Double), JsonValue::Number(n)) => Some(
                Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(number_to_f64(&n)?))),
            ),
            (Type::Primitive(PrimitiveType::Date), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Int(parse_date_to_days(&s)?)),
            ),
            (Type::Primitive(PrimitiveType::Time), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(parse_time_to_micros(&s)?)),
            ),
            (Type::Primitive(PrimitiveType::Timestamp), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(parse_ts_to_micros(&s)?)),
            ),
            (Type::Primitive(PrimitiveType::Timestamptz), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(parse_ts_to_micros(&s)?)),
            ),
            (Type::Primitive(PrimitiveType::TimestampNs), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(parse_ts_to_micros(&s)? * 1000)),
            ),
            (Type::Primitive(PrimitiveType::TimestamptzNs), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Long(parse_ts_to_micros(&s)? * 1000)),
            ),
            (Type::Primitive(PrimitiveType::String), JsonValue::String(s)) => {
                Some(Literal::Primitive(PrimitiveLiteral::String(s)))
            }
            (Type::Primitive(PrimitiveType::Uuid), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::UInt128(parse_uuid_to_u128(&s)?)),
            ),
            (Type::Primitive(PrimitiveType::Binary), JsonValue::String(s)) => {
                Some(Literal::Primitive(PrimitiveLiteral::Binary(s.into_bytes())))
            }
            (Type::Primitive(PrimitiveType::Fixed(_)), JsonValue::String(s)) => {
                Some(Literal::Primitive(PrimitiveLiteral::Binary(s.into_bytes())))
            }
            (Type::Primitive(PrimitiveType::Decimal { scale, .. }), JsonValue::String(s)) => Some(
                Literal::Primitive(PrimitiveLiteral::Int128(parse_decimal_to_i128(&s, *scale)?)),
            ),
            (Type::Struct(struct_ty), JsonValue::Object(mut map)) => {
                let mut out = Vec::with_capacity(struct_ty.fields().len());
                for field in struct_ty.fields() {
                    let key = field.id.to_string();
                    let v = map.remove(&key);
                    let val = match v {
                        Some(json) => Literal::try_from_json(json, &field.field_type)
                            .and_then(|opt| {
                                opt.ok_or_else(|| "Key of map cannot be null".to_string())
                            })
                            .ok(),
                        None => None,
                    };
                    out.push((key, val));
                }
                Some(Literal::Struct(out))
            }
            (Type::List(list_ty), JsonValue::Array(arr)) => {
                let mut out = Vec::with_capacity(arr.len());
                for item in arr.into_iter() {
                    let elem = Literal::try_from_json(item, &list_ty.element_field.field_type)?;
                    out.push(elem);
                }
                Some(Literal::List(out))
            }
            (Type::Map(map_ty), JsonValue::Object(mut obj)) => {
                let keys = obj.remove("keys").unwrap_or(JsonValue::Array(vec![]));
                let vals = obj.remove("values").unwrap_or(JsonValue::Array(vec![]));
                let (JsonValue::Array(keys), JsonValue::Array(vals)) = (keys, vals) else {
                    return Err("Invalid map JSON".to_string());
                };
                if keys.len() != vals.len() {
                    return Err("Keys and values length mismatch".to_string());
                }
                let mut out = Vec::with_capacity(keys.len());
                for (k, v) in keys.into_iter().zip(vals.into_iter()) {
                    let key = Literal::try_from_json(k, &map_ty.key_field.field_type)
                        .and_then(|opt| opt.ok_or_else(|| "Map key cannot be null".to_string()))?;
                    let val = Literal::try_from_json(v, &map_ty.value_field.field_type)?;
                    out.push((key, val));
                }
                Some(Literal::Map(out))
            }
            // Fallback: store as string for unsupported combinations
            (_, other) => Some(Literal::Primitive(PrimitiveLiteral::String(
                other.to_string(),
            ))),
        })
    }

    pub fn try_into_json(&self, data_type: &crate::spec::types::Type) -> Result<JsonValue, String> {
        use chrono::{NaiveDate, NaiveTime};
        use serde_json::Number;

        use crate::spec::types::{PrimitiveType, Type};

        fn days_to_date_str(days: i32) -> String {
            #[expect(clippy::expect_used)]
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("Creating date from constant should never fail");
            let d = epoch + chrono::Days::new(days as u64);
            d.to_string()
        }
        fn micros_to_time_str(us: i64) -> String {
            let secs = us.div_euclid(1_000_000);
            let rem = us.rem_euclid(1_000_000) as u32;
            #[expect(clippy::expect_used)]
            let t = NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem * 1000)
                .unwrap_or(
                    NaiveTime::from_hms_opt(0, 0, 0)
                        .expect("Creating time from constant should never fail"),
                );
            t.format("%H:%M:%S%.f").to_string()
        }
        fn micros_to_datetime_str(us: i64) -> String {
            let secs = us.div_euclid(1_000_000);
            let rem = us.rem_euclid(1_000_000) as u32;
            #[expect(clippy::expect_used)]
            let base = NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("Creating date from constant should never fail")
                .and_hms_nano_opt(0, 0, 0, 0)
                .expect("Creating time from constant should never fail");
            let dt = base
                .checked_add_signed(chrono::Duration::seconds(secs))
                .and_then(|d| {
                    d.checked_add_signed(chrono::Duration::nanoseconds((rem as i64) * 1000))
                })
                .unwrap_or(base);
            dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
        }

        match (self, data_type) {
            (Literal::Primitive(prim), Type::Primitive(prim_ty)) => match (prim_ty, prim) {
                (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => Ok(JsonValue::Bool(*v)),
                (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => {
                    Ok(JsonValue::Number((*v).into()))
                }
                (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::Number((*v).into()))
                }
                (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => Number::from_f64(v.0 as f64)
                    .map(JsonValue::Number)
                    .ok_or_else(|| "Invalid float".to_string()),
                (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => Number::from_f64(v.0)
                    .map(JsonValue::Number)
                    .ok_or_else(|| "Invalid double".to_string()),
                (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => {
                    Ok(JsonValue::String(days_to_date_str(*v)))
                }
                (PrimitiveType::Time, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::String(micros_to_time_str(*v)))
                }
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::String(micros_to_datetime_str(*v)))
                }
                (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::String(micros_to_datetime_str(*v)))
                }
                (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::String(micros_to_datetime_str(*v / 1000)))
                }
                (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                    Ok(JsonValue::String(micros_to_datetime_str(*v / 1000)))
                }
                (PrimitiveType::String, PrimitiveLiteral::String(s)) => {
                    Ok(JsonValue::String(s.clone()))
                }
                (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(u)) => {
                    let mut bytes = [0u8; 16];
                    let mut tmp = *u;
                    for i in (0..16).rev() {
                        bytes[i] = (tmp & 0xFF) as u8;
                        tmp >>= 8;
                    }
                    let u = uuid::Uuid::from_bytes(bytes);
                    Ok(JsonValue::String(u.to_string()))
                }
                (PrimitiveType::Decimal { scale, .. }, PrimitiveLiteral::Int128(v)) => {
                    // render scaled decimal as string
                    let neg = *v < 0;
                    let x = v.abs();
                    let scale = *scale as usize;
                    let mut s = if scale == 0 {
                        x.to_string()
                    } else {
                        let mut frac = String::with_capacity(scale);
                        let mut tmp = x;
                        for _ in 0..scale {
                            frac.insert(0, char::from(b'0' + (tmp % 10) as u8));
                            tmp /= 10;
                        }
                        let int_part = tmp.to_string();
                        format!("{}.{frac}", int_part)
                    };
                    if neg {
                        s.insert(0, '-');
                    }
                    Ok(JsonValue::String(s))
                }
                (PrimitiveType::Binary, PrimitiveLiteral::Binary(b)) => {
                    // store as UTF-8 string of bytes if valid; otherwise hex-ish
                    Ok(JsonValue::String(String::from_utf8_lossy(b).into_owned()))
                }
                (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(b)) => {
                    Ok(JsonValue::String(String::from_utf8_lossy(b).into_owned()))
                }
                // Fallback for mismatched pairs
                _ => Ok(JsonValue::Null),
            },
            (Literal::Struct(s), Type::Struct(struct_ty)) => {
                let mut map = serde_json::Map::with_capacity(struct_ty.fields().len());
                for ((id_str, val_opt), field) in s.iter().zip(struct_ty.fields()) {
                    let key = id_str.clone();
                    let json = match val_opt {
                        Some(l) => l.try_into_json(&field.field_type)?,
                        None => JsonValue::Null,
                    };
                    map.insert(key, json);
                }
                Ok(JsonValue::Object(map))
            }
            (Literal::List(list), Type::List(list_ty)) => {
                let mut arr = Vec::with_capacity(list.len());
                for opt in list.iter() {
                    match opt {
                        Some(l) => arr.push(l.try_into_json(&list_ty.element_field.field_type)?),
                        None => arr.push(JsonValue::Null),
                    }
                }
                Ok(JsonValue::Array(arr))
            }
            (Literal::Map(map), Type::Map(map_ty)) => {
                let mut keys = Vec::with_capacity(map.len());
                let mut vals = Vec::with_capacity(map.len());
                for (k, v) in map.iter() {
                    keys.push(k.try_into_json(&map_ty.key_field.field_type)?);
                    vals.push(match v {
                        Some(l) => l.try_into_json(&map_ty.value_field.field_type)?,
                        None => JsonValue::Null,
                    });
                }
                let mut obj = serde_json::Map::new();
                obj.insert("keys".to_string(), JsonValue::Array(keys));
                obj.insert("values".to_string(), JsonValue::Array(vals));
                Ok(JsonValue::Object(obj))
            }
            // Fallback
            _ => Ok(JsonValue::Null),
        }
    }
}

/// A lightweight, schema-agnostic representation of a struct literal that
/// serializes into an Avro record matching a provided `StructType`.
///
/// This is used for encoding the `partition` tuple in `DataFile` manifest
/// entries. It intentionally avoids carrying type information and focuses on
/// field-name to optional `Literal` mapping; type validation is deferred to
/// the caller that provides the `StructType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLiteral(pub Vec<(String, Option<Literal>)>);

impl RawLiteral {
    /// Build from a positional vector of values and a struct type; positions
    /// are matched to the struct fields' order.
    pub fn from_struct_values(
        values: &[Option<Literal>],
        ty: &crate::spec::types::StructType,
    ) -> Result<Self, String> {
        let fields = ty.fields();
        if values.len() != fields.len() {
            return Err(format!(
                "Partition values length {} does not match struct fields length {}",
                values.len(),
                fields.len()
            ));
        }
        let mut out = Vec::with_capacity(values.len());
        for (i, f) in fields.iter().enumerate() {
            out.push((f.name.clone(), values[i].clone()));
        }
        Ok(RawLiteral(out))
    }

    /// Convert back to positional vector based on the struct type's field order.
    pub fn into_struct_values(self, ty: &crate::spec::types::StructType) -> Vec<Option<Literal>> {
        let mut by_name = std::collections::HashMap::with_capacity(self.0.len());
        for (k, v) in self.0.into_iter() {
            by_name.insert(k, v);
        }
        ty.fields()
            .iter()
            .map(|f| by_name.remove(&f.name).unwrap_or(None))
            .collect()
    }
}

impl Serialize for RawLiteral {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut ss = serializer.serialize_struct("", self.0.len())?;
        for (k, v) in &self.0 {
            // Avro's serde requires &'static str for field names
            ss.serialize_field(Box::leak(k.clone().into_boxed_str()), v)?;
        }
        ss.end()
    }
}

impl<'de> Deserialize<'de> for RawLiteral {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};
        struct RLVisitor;
        impl<'de> Visitor<'de> for RLVisitor {
            type Value = RawLiteral;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("Avro record for RawLiteral")
            }
            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut out: Vec<(String, Option<Literal>)> = Vec::new();
                while let Some((k, v)) = map.next_entry::<String, Option<Literal>>()? {
                    out.push((k, v));
                }
                Ok(RawLiteral(out))
            }
        }
        deserializer.deserialize_map(RLVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_literal_ordering_same_type() {
        // Test Int ordering
        let a = PrimitiveLiteral::Int(10);
        let b = PrimitiveLiteral::Int(20);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, a);

        // Test Long ordering
        let a = PrimitiveLiteral::Long(100);
        let b = PrimitiveLiteral::Long(200);
        assert!(a < b);

        // Test String ordering
        let a = PrimitiveLiteral::String("apple".to_string());
        let b = PrimitiveLiteral::String("banana".to_string());
        assert!(a < b);

        // Test Boolean ordering
        let a = PrimitiveLiteral::Boolean(false);
        let b = PrimitiveLiteral::Boolean(true);
        assert!(a < b);

        // Test Float ordering
        let a = PrimitiveLiteral::Float(OrderedFloat(1.5));
        let b = PrimitiveLiteral::Float(OrderedFloat(2.5));
        assert!(a < b);

        // Test Double ordering
        let a = PrimitiveLiteral::Double(OrderedFloat(1.5));
        let b = PrimitiveLiteral::Double(OrderedFloat(2.5));
        assert!(a < b);
    }

    #[test]
    fn test_primitive_literal_ordering_different_types() {
        // Different types should have consistent ordering based on variant index
        let int_val = PrimitiveLiteral::Int(42);
        let long_val = PrimitiveLiteral::Long(42);
        let string_val = PrimitiveLiteral::String("42".to_string());

        // The ordering should be consistent (based on discriminant/variant order)
        // Boolean < Int < Long < Float < Double < Int128 < String < UInt128 < Binary
        assert!(int_val < long_val);
        assert!(long_val < string_val);
        assert!(int_val < string_val);
    }

    #[test]
    fn test_primitive_literal_equality() {
        let a = PrimitiveLiteral::Int(42);
        let b = PrimitiveLiteral::Int(42);
        let c = PrimitiveLiteral::Int(43);

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a >= b);
        assert!(a <= b);
    }

    #[test]
    fn test_primitive_literal_can_use_comparison_operators() {
        // This test verifies that we can use standard comparison operators
        // instead of custom lt_prim, gt_prim, eq_prim functions
        let a = PrimitiveLiteral::Int(10);
        let b = PrimitiveLiteral::Int(20);

        assert!(a < b);
        assert!(b > a);
        assert!(a <= b);
        assert!(b >= a);
        assert_eq!(a, a);
        assert_ne!(a, b);
    }

    #[test]
    fn test_primitive_literal_ordering_with_binary() {
        let a = PrimitiveLiteral::Binary(vec![1, 2, 3]);
        let b = PrimitiveLiteral::Binary(vec![1, 2, 4]);
        assert!(a < b);

        let c = PrimitiveLiteral::Binary(vec![1, 2, 3]);
        assert_eq!(a, c);
    }
}
