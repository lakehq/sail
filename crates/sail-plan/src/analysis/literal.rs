use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::Date32Type;
use chrono::{NaiveTime, Timelike};
use datafusion::arrow::array::{
    ArrayData, AsArray, FixedSizeListArray, LargeListArray, MapArray, StructArray, new_empty_array,
    new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes as adt;
use datafusion_common::ScalarValue;
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::utils::SingleRowListArrayBuilder;
use sail_common::spec::{
    self, Literal, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::{parse_date, parse_time, parse_timestamp};

use super::data_type::DataTypeResolver;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};

impl DataTypeResolver<'_> {
    pub fn resolve_literal(&self, literal: Literal) -> PlanResult<ScalarValue> {
        match literal {
            Literal::Null => Ok(ScalarValue::Null),
            Literal::Boolean { value } => Ok(ScalarValue::Boolean(value)),
            Literal::Int8 { value } => Ok(ScalarValue::Int8(value)),
            Literal::Int16 { value } => Ok(ScalarValue::Int16(value)),
            Literal::Int32 { value } => Ok(ScalarValue::Int32(value)),
            Literal::Int64 { value } => Ok(ScalarValue::Int64(value)),
            Literal::UInt8 { value } => Ok(ScalarValue::UInt8(value)),
            Literal::UInt16 { value } => Ok(ScalarValue::UInt16(value)),
            Literal::UInt32 { value } => Ok(ScalarValue::UInt32(value)),
            Literal::UInt64 { value } => Ok(ScalarValue::UInt64(value)),
            Literal::Float16 { value } => Ok(ScalarValue::Float16(value)),
            Literal::Float32 { value } => Ok(ScalarValue::Float32(value)),
            Literal::Float64 { value } => Ok(ScalarValue::Float64(value)),
            Literal::TimestampSecond {
                seconds,
                timestamp_type,
            } => {
                let timezone = self.resolve_timezone(&timestamp_type)?;
                Ok(ScalarValue::TimestampSecond(seconds, timezone))
            }
            Literal::TimestampMillisecond {
                milliseconds,
                timestamp_type,
            } => {
                let timezone = self.resolve_timezone(&timestamp_type)?;
                Ok(ScalarValue::TimestampMillisecond(milliseconds, timezone))
            }
            Literal::TimestampMicrosecond {
                microseconds,
                timestamp_type,
            } => {
                let timezone = self.resolve_timezone(&timestamp_type)?;
                Ok(ScalarValue::TimestampMicrosecond(microseconds, timezone))
            }
            Literal::TimestampNanosecond {
                nanoseconds,
                timestamp_type,
            } => {
                let timezone = self.resolve_timezone(&timestamp_type)?;
                Ok(ScalarValue::TimestampNanosecond(nanoseconds, timezone))
            }
            Literal::Date32 { days } => Ok(ScalarValue::Date32(days)),
            Literal::Date64 { milliseconds } => Ok(ScalarValue::Date64(milliseconds)),
            Literal::Time32Second { seconds } => Ok(ScalarValue::Time32Second(seconds)),
            Literal::Time32Millisecond { milliseconds } => {
                Ok(ScalarValue::Time32Millisecond(milliseconds))
            }
            Literal::Time64Microsecond { microseconds } => {
                Ok(ScalarValue::Time64Microsecond(microseconds))
            }
            Literal::Time64Nanosecond { nanoseconds } => {
                Ok(ScalarValue::Time64Nanosecond(nanoseconds))
            }
            Literal::DurationSecond { seconds } => Ok(ScalarValue::DurationSecond(seconds)),
            Literal::DurationMillisecond { milliseconds } => {
                Ok(ScalarValue::DurationMillisecond(milliseconds))
            }
            Literal::DurationMicrosecond { microseconds } => {
                Ok(ScalarValue::DurationMicrosecond(microseconds))
            }
            Literal::DurationNanosecond { nanoseconds } => {
                Ok(ScalarValue::DurationNanosecond(nanoseconds))
            }
            Literal::IntervalYearMonth { months, .. } => Ok(ScalarValue::IntervalYearMonth(months)),
            Literal::IntervalDayTimeMicrosecond { microseconds, .. } => {
                Ok(ScalarValue::DurationMicrosecond(microseconds))
            }
            Literal::IntervalDayTime { value } => {
                if let Some(value) = value {
                    Ok(ScalarValue::IntervalDayTime(Some(
                        adt::IntervalDayTime::new(value.days, value.milliseconds),
                    )))
                } else {
                    Ok(ScalarValue::IntervalDayTime(None))
                }
            }
            Literal::IntervalMonthDayNano { value } => {
                if let Some(value) = value {
                    Ok(ScalarValue::IntervalMonthDayNano(Some(
                        adt::IntervalMonthDayNanoType::make_value(
                            value.months,
                            value.days,
                            value.nanoseconds,
                        ),
                    )))
                } else {
                    Ok(ScalarValue::IntervalMonthDayNano(None))
                }
            }
            Literal::Binary { value } => Ok(ScalarValue::Binary(value)),
            Literal::FixedSizeBinary { size, value } => {
                Ok(ScalarValue::FixedSizeBinary(size, value))
            }
            Literal::LargeBinary { value } => Ok(ScalarValue::LargeBinary(value)),
            Literal::BinaryView { value } => Ok(ScalarValue::BinaryView(value)),
            Literal::Utf8 { value } => Ok(ScalarValue::Utf8(value)),
            Literal::LargeUtf8 { value } => Ok(ScalarValue::LargeUtf8(value)),
            Literal::Utf8View { value } => Ok(ScalarValue::Utf8View(value)),
            Literal::List {
                data_type,
                nullable,
                values,
            } => {
                let data_type = self.resolve_data_type(&data_type)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal))
                        .collect::<PlanResult<Vec<_>>>()?;
                    Ok(ScalarValue::List(ScalarValue::new_list_from_iter(
                        scalars.into_iter(),
                        &data_type,
                        nullable,
                    )))
                } else {
                    Ok(ScalarValue::new_null_list(data_type, true, 0))
                }
            }
            Literal::FixedSizeList {
                length,
                data_type,
                nullable,
                values,
            } => {
                let data_type = self.resolve_data_type(&data_type)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let scalars = if scalars.is_empty() {
                        new_empty_array(&data_type)
                    } else {
                        ScalarValue::iter_to_array(scalars).map_err(|e| {
                            PlanError::internal(format!(
                                "Resolve Literal: Error creating large list array: {e}"
                            ))
                        })?
                    };
                    Ok(ScalarValue::FixedSizeList(Arc::new(
                        SingleRowListArrayBuilder::new(scalars)
                            .build_fixed_size_list_array(length as usize),
                    )))
                } else {
                    let data_type = adt::DataType::FixedSizeList(
                        adt::Field::new_list_field(data_type, nullable).into(),
                        length,
                    );
                    Ok(ScalarValue::FixedSizeList(Arc::new(
                        FixedSizeListArray::from(ArrayData::new_null(&data_type, 0)),
                    )))
                }
            }
            Literal::LargeList {
                data_type,
                nullable,
                values,
            } => {
                let data_type = self.resolve_data_type(&data_type)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let scalars = if scalars.is_empty() {
                        new_empty_array(&data_type)
                    } else {
                        ScalarValue::iter_to_array(scalars).map_err(|e| {
                            PlanError::internal(format!(
                                "Resolve Literal: Error creating large list array: {e}"
                            ))
                        })?
                    };
                    Ok(ScalarValue::LargeList(Arc::new(
                        SingleRowListArrayBuilder::new(scalars).build_large_list_array(),
                    )))
                } else {
                    let data_type = adt::DataType::LargeList(
                        adt::Field::new_list_field(data_type, nullable).into(),
                    );
                    Ok(ScalarValue::LargeList(Arc::new(LargeListArray::from(
                        ArrayData::new_null(&data_type, 0),
                    ))))
                }
            }
            Literal::Struct { data_type, values } => {
                let data_type = self.resolve_data_type(&data_type)?;
                let fields = match &data_type {
                    datafusion::arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                    _ => return Err(PlanError::invalid("expected struct type")),
                };

                if let Some(values) = values {
                    let mut builder: ScalarStructBuilder = ScalarStructBuilder::new();
                    for (literal, field) in values.into_iter().zip(&fields) {
                        let scalar = self.resolve_literal(literal)?;
                        builder = builder.with_scalar(field, scalar);
                    }
                    Ok(builder.build()?)
                } else {
                    Ok(ScalarStructBuilder::new_null(fields))
                }
            }
            Literal::Union {
                union_fields,
                union_mode,
                value,
            } => {
                let (type_ids, fields): (Vec<_>, Vec<_>) = union_fields
                    .iter()
                    .map(|(index, field)| Ok((index, self.resolve_field(field)?)))
                    .collect::<PlanResult<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                let union_fields = adt::UnionFields::try_new(type_ids, fields)?;
                let union_mode = match union_mode {
                    spec::UnionMode::Sparse => adt::UnionMode::Sparse,
                    spec::UnionMode::Dense => adt::UnionMode::Dense,
                };
                let value = if let Some((index, literal)) = value {
                    let scalar = self.resolve_literal(*literal)?;
                    Some((index, Box::new(scalar)))
                } else {
                    None
                };
                Ok(ScalarValue::Union(value, union_fields, union_mode))
            }
            Literal::Dictionary {
                key_type,
                value_type: _,
                value,
            } => {
                let key_type = self.resolve_data_type(&key_type)?;
                if let Some(value) = value {
                    let value = self.resolve_literal(*value)?;
                    Ok(ScalarValue::Dictionary(Box::new(key_type), Box::new(value)))
                } else {
                    Ok(ScalarValue::Dictionary(
                        Box::new(key_type),
                        Box::new(ScalarValue::Null),
                    ))
                }
            }
            Literal::Decimal128 {
                precision,
                scale,
                value,
            } => Ok(ScalarValue::Decimal128(value, precision, scale)),
            Literal::Decimal256 {
                precision,
                scale,
                value,
            } => Ok(ScalarValue::Decimal256(value, precision, scale)),
            Literal::Map {
                key_type,
                value_type,
                value_type_nullable,
                keys,
                values,
            } => {
                let fields = spec::Fields::from(vec![
                    spec::Field {
                        name: SAIL_MAP_KEY_FIELD_NAME.to_string(),
                        data_type: key_type,
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: SAIL_MAP_VALUE_FIELD_NAME.to_string(),
                        data_type: value_type,
                        nullable: value_type_nullable,
                        metadata: vec![],
                    },
                ]);
                let key_value_fields = self.resolve_fields(&fields)?;
                let field = Arc::new(adt::Field::new(
                    SAIL_MAP_FIELD_NAME,
                    adt::DataType::Struct(key_value_fields.clone()),
                    false,
                ));
                if let (Some(keys), Some(values)) = (keys, values) {
                    if keys.len() != values.len() {
                        return Err(PlanError::invalid(
                            "Map keys and values must have the same length",
                        ));
                    }
                    let keys: Vec<ScalarValue> = keys
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let values: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let keys = ScalarValue::iter_to_array(keys).map_err(|e| {
                        PlanError::internal(format!(
                            "Resolve Literal: Error creating large list array: {e}"
                        ))
                    })?;
                    let values = ScalarValue::iter_to_array(values).map_err(|e| {
                        PlanError::internal(format!(
                            "Resolve Literal: Error creating large list array: {e}"
                        ))
                    })?;
                    let keys_len = keys.len();
                    let struct_array =
                        StructArray::try_new(key_value_fields, vec![keys, values], None)?;
                    let offsets = OffsetBuffer::new(vec![0, keys_len as i32].into());
                    let map_array = Arc::new(MapArray::try_new(
                        field,
                        offsets,
                        struct_array,
                        None,
                        false,
                    )?);
                    Ok(ScalarValue::Map(map_array))
                } else {
                    Ok(ScalarValue::Map(
                        new_null_array(&adt::DataType::Map(field, false), 0)
                            .as_map()
                            .to_owned()
                            .into(),
                    ))
                }
            }
        }
    }
}

impl DataTypeResolver<'_> {
    pub(super) fn date_literal(&self, value: &str) -> PlanResult<spec::Literal> {
        let date = parse_date(value)?;
        let literal = spec::Literal::Date32 {
            days: Some(Date32Type::from_naive_date(date.try_into()?)),
        };
        Ok(literal)
    }

    pub(super) fn timestamp_literal(
        &self,
        value: &str,
        timestamp_type: spec::TimestampType,
    ) -> PlanResult<spec::Literal> {
        let (datetime, timezone) = parse_timestamp(value).and_then(|x| x.into_naive())?;
        let timezone = if timezone.is_empty() {
            None
        } else {
            Some(timezone.parse::<Tz>()?)
        };
        let (datetime, timestamp_type) = match (timestamp_type, timezone, self.config.default_timestamp_type) {
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampLtz)
            | (spec::TimestampType::WithLocalTimeZone, None, _) => {
                let tz = Tz::from_str(&self.config.session_timezone)?;
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, Some(tz), _)
            | (spec::TimestampType::WithLocalTimeZone, Some(tz), _) => {
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampNtz)
            // If the timestamp type is explicitly `TIMESTAMP_NTZ`, the time zone in the literal
            // is simply ignored.
            | (spec::TimestampType::WithoutTimeZone, _, _) => {
                (datetime.and_utc(), spec::TimestampType::WithoutTimeZone)
            }
        };
        // Convert to microseconds.
        // The parser accepts up to 9 digits (nanoseconds) for compatibility with Spark SQL syntax,
        // but Spark stores timestamps with microsecond precision (6 digits).
        // Nanoseconds beyond microsecond precision are truncated to match Spark behavior.
        let literal = spec::Literal::TimestampMicrosecond {
            microseconds: Some(datetime.timestamp_micros()),
            timestamp_type,
        };
        Ok(literal)
    }

    pub(super) fn time_literal(&self, value: &str) -> PlanResult<spec::Literal> {
        let time = parse_time(value)?;

        // Convert to NaiveTime which validates hour (0-23), minute/second (0-59)
        let naive_time: NaiveTime = time.try_into()?;

        // Convert to microseconds since midnight.
        // The parser accepts up to 9 digits (nanoseconds) for compatibility with Spark SQL syntax,
        // but Spark stores timestamps with microsecond precision (6 digits).
        // Nanoseconds beyond microsecond precision are truncated to match Spark behavior.
        let seconds_from_midnight = naive_time.num_seconds_from_midnight() as i64;
        let nanoseconds = naive_time.nanosecond() as i64;
        let microseconds = seconds_from_midnight * 1_000_000 + nanoseconds / 1_000;

        let literal = spec::Literal::Time64Microsecond {
            microseconds: Some(microseconds),
        };
        Ok(literal)
    }
}
