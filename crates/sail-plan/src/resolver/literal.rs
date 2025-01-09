use std::ops::Add;
use std::sync::Arc;

use chrono::{Offset, TimeDelta, TimeZone, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    new_empty_array, new_null_array, ArrayData, AsArray, FixedSizeListArray, LargeListArray,
    MapArray, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes as adt;
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::ScalarValue;
use sail_common::spec::{self, Literal};

use crate::config::TimestampType;
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) fn resolve_literal(
        &self,
        literal: Literal,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
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
                timezone_info,
            } => {
                let timezone = Self::resolve_timezone(
                    &timezone_info,
                    self.config.system_timezone.as_str(),
                    &self.config.timestamp_type,
                )?;
                if let Some(seconds) = seconds {
                    let datetime = Utc.timestamp_opt(seconds, 0).earliest().ok_or_else(|| {
                        PlanError::invalid(format!("Invalid Literal TimestampSecond: {seconds}"))
                    })?;
                    let utc_datetime = Self::local_datetime_to_utc_datetime(
                        datetime,
                        &timezone_info,
                        &self.config.timestamp_type,
                        &self.config.system_timezone,
                    )?;
                    let adjusted_seconds = utc_datetime.timestamp();
                    let adjusted_timezone = self.get_adjusted_timezone(timezone, &timezone_info);
                    Ok(ScalarValue::TimestampSecond(
                        Some(adjusted_seconds),
                        adjusted_timezone,
                    ))
                } else {
                    Ok(ScalarValue::TimestampSecond(seconds, timezone))
                }
            }
            Literal::TimestampMillisecond {
                milliseconds,
                timezone_info,
            } => {
                let timezone = Self::resolve_timezone(
                    &timezone_info,
                    self.config.system_timezone.as_str(),
                    &self.config.timestamp_type,
                )?;
                if let Some(milliseconds) = milliseconds {
                    let datetime = Utc
                        .timestamp_millis_opt(milliseconds)
                        .earliest()
                        .ok_or_else(|| {
                            PlanError::invalid(format!(
                                "Literal to string TimestampMillisecond: {milliseconds}"
                            ))
                        })?;
                    let utc_datetime = Self::local_datetime_to_utc_datetime(
                        datetime,
                        &timezone_info,
                        &self.config.timestamp_type,
                        &self.config.system_timezone,
                    )?;
                    let adjusted_milliseconds = utc_datetime.timestamp_millis();
                    let adjusted_timezone = self.get_adjusted_timezone(timezone, &timezone_info);
                    Ok(ScalarValue::TimestampMillisecond(
                        Some(adjusted_milliseconds),
                        adjusted_timezone,
                    ))
                } else {
                    Ok(ScalarValue::TimestampMillisecond(milliseconds, timezone))
                }
            }
            Literal::TimestampMicrosecond {
                microseconds,
                timezone_info,
            } => {
                let timezone = Self::resolve_timezone(
                    &timezone_info,
                    self.config.system_timezone.as_str(),
                    &self.config.timestamp_type,
                )?;
                if let Some(microseconds) = microseconds {
                    let datetime =
                        Utc.timestamp_micros(microseconds)
                            .earliest()
                            .ok_or_else(|| {
                                PlanError::invalid(format!(
                                    "Literal to string TimestampMicrosecond: {microseconds}"
                                ))
                            })?;
                    let utc_datetime = Self::local_datetime_to_utc_datetime(
                        datetime,
                        &timezone_info,
                        &self.config.timestamp_type,
                        &self.config.system_timezone,
                    )?;
                    let adjusted_microseconds = utc_datetime.timestamp_micros();
                    let adjusted_timezone = self.get_adjusted_timezone(timezone, &timezone_info);
                    Ok(ScalarValue::TimestampMicrosecond(
                        Some(adjusted_microseconds),
                        adjusted_timezone,
                    ))
                } else {
                    Ok(ScalarValue::TimestampMicrosecond(microseconds, timezone))
                }
            }
            Literal::TimestampNanosecond {
                nanoseconds,
                timezone_info,
            } => {
                let timezone = Self::resolve_timezone(
                    &timezone_info,
                    self.config.system_timezone.as_str(),
                    &self.config.timestamp_type,
                )?;
                if let Some(nanoseconds) = nanoseconds {
                    let datetime = Utc.timestamp_nanos(nanoseconds);
                    let utc_datetime = Self::local_datetime_to_utc_datetime(
                        datetime,
                        &timezone_info,
                        &self.config.timestamp_type,
                        &self.config.system_timezone,
                    )?;
                    let adjusted_nanoseconds =
                        utc_datetime.timestamp_nanos_opt().ok_or_else(|| {
                            PlanError::invalid(format!(
                                "Invalid Literal TimestampNanosecond: {nanoseconds}"
                            ))
                        })?;
                    let adjusted_timezone = self.get_adjusted_timezone(timezone, &timezone_info);
                    Ok(ScalarValue::TimestampNanosecond(
                        Some(adjusted_nanoseconds),
                        adjusted_timezone,
                    ))
                } else {
                    Ok(ScalarValue::TimestampNanosecond(nanoseconds, timezone))
                }
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
            Literal::IntervalYearMonth { months } => Ok(ScalarValue::IntervalYearMonth(months)),
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
            Literal::List { data_type, values } => {
                let data_type = self.resolve_data_type(&data_type, state)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal, state))
                        .collect::<PlanResult<Vec<_>>>()?;
                    Ok(ScalarValue::List(ScalarValue::new_list_from_iter(
                        scalars.into_iter(),
                        &data_type,
                        true,
                    )))
                } else {
                    Ok(ScalarValue::new_null_list(data_type, true, 0))
                }
            }
            Literal::FixedSizeList {
                length,
                data_type,
                values,
            } => {
                let data_type = self.resolve_data_type(&data_type, state)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal, state))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let scalars = if scalars.is_empty() {
                        new_empty_array(&data_type)
                    } else {
                        ScalarValue::iter_to_array(scalars.into_iter()).map_err(|e| {
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
                        adt::Field::new_list_field(data_type, true).into(),
                        length,
                    );
                    Ok(ScalarValue::FixedSizeList(Arc::new(
                        FixedSizeListArray::from(ArrayData::new_null(&data_type, 0)),
                    )))
                }
            }
            Literal::LargeList { data_type, values } => {
                let data_type = self.resolve_data_type(&data_type, state)?;
                if let Some(values) = values {
                    let scalars: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal, state))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let scalars = if scalars.is_empty() {
                        new_empty_array(&data_type)
                    } else {
                        ScalarValue::iter_to_array(scalars.into_iter()).map_err(|e| {
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
                        adt::Field::new_list_field(data_type, true).into(),
                    );
                    Ok(ScalarValue::LargeList(Arc::new(LargeListArray::from(
                        ArrayData::new_null(&data_type, 0),
                    ))))
                }
            }
            Literal::Struct { data_type, values } => {
                let data_type = self.resolve_data_type(&data_type, state)?;
                let fields = match &data_type {
                    datafusion::arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                    _ => return Err(PlanError::invalid("expected struct type")),
                };

                if let Some(values) = values {
                    let mut builder: ScalarStructBuilder = ScalarStructBuilder::new();
                    for (literal, field) in values.into_iter().zip(fields.into_iter()) {
                        let scalar = self.resolve_literal(literal, state)?;
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
                    .map(|(index, field)| Ok((index, self.resolve_field(field, state)?)))
                    .collect::<PlanResult<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                let union_fields = adt::UnionFields::new(type_ids, fields);
                let union_mode = match union_mode {
                    spec::UnionMode::Sparse => adt::UnionMode::Sparse,
                    spec::UnionMode::Dense => adt::UnionMode::Dense,
                };
                let value = if let Some((index, literal)) = value {
                    let scalar = self.resolve_literal(*literal, state)?;
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
                let key_type = self.resolve_data_type(&key_type, state)?;
                if let Some(value) = value {
                    let value = self.resolve_literal(*value, state)?;
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
                keys,
                values,
            } => {
                let fields = spec::Fields::from(vec![
                    spec::Field {
                        name: "key".to_string(),
                        data_type: key_type,
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: "value".to_string(),
                        data_type: value_type,
                        nullable: true,
                        metadata: vec![],
                    },
                ]);
                let key_value_fields = self.resolve_fields(&fields, state)?;
                let field = Arc::new(adt::Field::new(
                    "entries",
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
                        .map(|literal| self.resolve_literal(literal, state))
                        .collect::<PlanResult<Vec<_>>>()?;
                    let values: Vec<ScalarValue> = values
                        .into_iter()
                        .map(|literal| self.resolve_literal(literal, state))
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

    pub fn local_datetime_to_utc_datetime(
        datetime: chrono::DateTime<Utc>,
        timezone_info: &spec::TimeZoneInfo,
        config_timestamp_type: &TimestampType,
        system_timezone: &str,
    ) -> PlanResult<chrono::DateTime<Utc>> {
        // FIXME: See FIXME in `PlanResolver::resolve_timezone` for more details.
        let should_rebase = match timezone_info {
            // PySpark client (via Spark Connect) applies the local timezone to timestamp literals
            // before sending them when TimeZoneInfo::LocalTimeZone is specified.
            //
            // Example: datetime(2022, 12, 22, 17, 0, 0) is passed up as Timestamp(1671757200000000)
            //   - Timestamp(1671757200000000): 2022-12-22 17:00:00 America/Los_Angeles
            //   - Timestamp(1671728400000000): 2022-12-22 17:00:00 UTC
            //
            // Rebasing to UTC is only needed for TimeZoneInfo::LocalTimeZone.
            // For TimeZoneInfo::SQLConfigured, we don't rebase because we parse the timestamp as UTC.
            spec::TimeZoneInfo::SQLConfigured => match config_timestamp_type {
                TimestampType::TimestampLtz => false,
                TimestampType::TimestampNtz => false,
            },
            spec::TimeZoneInfo::LocalTimeZone => true,
            spec::TimeZoneInfo::NoTimeZone => false,
            spec::TimeZoneInfo::TimeZone {
                timezone: _timezone,
            } => false,
        };
        if should_rebase {
            let system_timezone: Tz = system_timezone.parse()?;
            let offset_seconds: i64 = system_timezone
                .offset_from_utc_datetime(&datetime.naive_utc())
                .fix()
                .local_minus_utc() as i64;
            let result = datetime.add(TimeDelta::try_seconds(offset_seconds).ok_or_else(|| {
                PlanError::invalid(format!(
                    "Invalid offset seconds when converting from local to UTC: {offset_seconds}"
                ))
            })?);
            Ok(result)
        } else {
            Ok(datetime)
        }
    }

    fn get_adjusted_timezone(
        &self,
        timezone: Option<Arc<str>>,
        timezone_info: &spec::TimeZoneInfo,
    ) -> Option<Arc<str>> {
        // FIXME: See FIXME in `PlanResolver::resolve_timezone` for more details.
        match timezone_info {
            spec::TimeZoneInfo::LocalTimeZone => None,
            _ => timezone,
        }
    }
}
