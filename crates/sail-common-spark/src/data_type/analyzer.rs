use sail_common::error::{CommonError, CommonResult};
use sail_common::spec;

use super::common::{JsonDataType, JsonStructField};

impl TryFrom<JsonStructField> for spec::Field {
    type Error = CommonError;

    fn try_from(field: JsonStructField) -> CommonResult<Self> {
        let metadata = field
            .metadata
            .map(|metadata| {
                serde_json::to_string(&metadata)
                    .map(|metadata| vec![(spec::SPARK_METADATA_JSON_KEY.to_string(), metadata)])
                    .map_err(|e| CommonError::invalid(format!("invalid Spark metadata: {e}")))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            name: field.name,
            data_type: field.r#type.try_into()?,
            nullable: field.nullable,
            metadata,
        })
    }
}

impl TryFrom<JsonDataType> for spec::DataType {
    type Error = CommonError;

    fn try_from(data_type: JsonDataType) -> CommonResult<Self> {
        Ok(match data_type {
            JsonDataType::Type { r#type } => return (*r#type).try_into(),
            JsonDataType::Null => Self::Null,
            JsonDataType::String => Self::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Configured,
            },
            JsonDataType::Binary => Self::ConfiguredBinary,
            JsonDataType::Boolean => Self::Boolean,
            JsonDataType::Byte => Self::Int8,
            JsonDataType::Short => Self::Int16,
            JsonDataType::Integer => Self::Int32,
            JsonDataType::Long => Self::Int64,
            JsonDataType::Float => Self::Float32,
            JsonDataType::Double => Self::Float64,
            JsonDataType::Decimal => Self::Decimal128 {
                precision: 10,
                scale: 0,
            },
            JsonDataType::FixedDecimal { precision, scale } => {
                let precision = u8::try_from(precision)
                    .map_err(|_| CommonError::invalid("decimal precision"))?;
                let scale =
                    i8::try_from(scale).map_err(|_| CommonError::invalid("decimal scale"))?;
                if precision > spec::ARROW_DECIMAL128_MAX_PRECISION {
                    Self::Decimal256 { precision, scale }
                } else {
                    Self::Decimal128 { precision, scale }
                }
            }
            JsonDataType::Char { length } => Self::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Char {
                    length: length
                        .try_into()
                        .map_err(|_| CommonError::invalid("char length"))?,
                },
            },
            JsonDataType::Varchar { length } => Self::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::VarChar {
                    length: length
                        .try_into()
                        .map_err(|_| CommonError::invalid("varchar length"))?,
                },
            },
            JsonDataType::Date => Self::Date32,
            JsonDataType::Timestamp => Self::Timestamp {
                time_unit: spec::TimeUnit::Microsecond,
                timestamp_type: spec::TimestampType::WithLocalTimeZone,
            },
            JsonDataType::TimestampNtz => Self::Timestamp {
                time_unit: spec::TimeUnit::Microsecond,
                timestamp_type: spec::TimestampType::WithoutTimeZone,
            },
            JsonDataType::TimestampLtzNanos | JsonDataType::TimestampNtzNanos => {
                return Err(CommonError::unsupported("nanosecond timestamp type"));
            }
            JsonDataType::Time { precision } => match precision.unwrap_or(6) {
                0 => Self::Time32 {
                    time_unit: spec::TimeUnit::Second,
                },
                3 => Self::Time32 {
                    time_unit: spec::TimeUnit::Millisecond,
                },
                6 => Self::Time64 {
                    time_unit: spec::TimeUnit::Microsecond,
                },
                9 => Self::Time64 {
                    time_unit: spec::TimeUnit::Nanosecond,
                },
                precision => {
                    return Err(CommonError::invalid(format!(
                        "unsupported TIME precision: {precision}. Only 0, 3, 6, and 9 are supported"
                    )));
                }
            },
            JsonDataType::CalendarInterval => Self::Interval {
                interval_unit: spec::IntervalUnit::MonthDayNano,
                start_field: None,
                end_field: None,
            },
            JsonDataType::YearMonthInterval { start, end } => Self::Interval {
                interval_unit: spec::IntervalUnit::YearMonth,
                start_field: Some(
                    start
                        .map(|field| spec::YearMonthIntervalField::try_from(field as i32))
                        .transpose()?
                        .map(Into::into)
                        .unwrap_or(spec::IntervalFieldType::Year),
                ),
                end_field: Some(
                    end.map(|field| spec::YearMonthIntervalField::try_from(field as i32))
                        .transpose()?
                        .map(Into::into)
                        .unwrap_or(spec::IntervalFieldType::Month),
                ),
            },
            JsonDataType::DayTimeInterval { start, end } => Self::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field: Some(
                    start
                        .map(|field| spec::DayTimeIntervalField::try_from(field as i32))
                        .transpose()?
                        .map(Into::into)
                        .unwrap_or(spec::IntervalFieldType::Day),
                ),
                end_field: Some(
                    end.map(|field| spec::DayTimeIntervalField::try_from(field as i32))
                        .transpose()?
                        .map(Into::into)
                        .unwrap_or(spec::IntervalFieldType::Second),
                ),
            },
            JsonDataType::Variant => Self::Variant,
            JsonDataType::Geometry { srid } => Self::Geometry { srid },
            JsonDataType::Geography { srid, algorithm } => {
                if algorithm != "spherical" {
                    return Err(CommonError::unsupported(format!(
                        "geography algorithm: {algorithm}"
                    )));
                }
                Self::Geography {
                    srid,
                    algorithm: spec::EdgeInterpolationAlgorithm::Spherical,
                }
            }
            JsonDataType::Array {
                element_type,
                contains_null,
                ..
            } => Self::List {
                data_type: Box::new((*element_type).try_into()?),
                nullable: contains_null,
            },
            JsonDataType::Map {
                key_type,
                value_type,
                value_contains_null,
                ..
            } => Self::Map {
                key_type: Box::new((*key_type).try_into()?),
                value_type: Box::new((*value_type).try_into()?),
                value_type_nullable: value_contains_null,
                keys_sorted: false,
            },
            JsonDataType::Struct { fields, .. } => Self::Struct {
                fields: spec::Fields::from(
                    fields
                        .into_iter()
                        .map(spec::Field::try_from)
                        .collect::<CommonResult<Vec<_>>>()?,
                ),
            },
            JsonDataType::Udt {
                class,
                py_class,
                serialized_class,
                sql_type,
                ..
            } => Self::UserDefined {
                jvm_class: class,
                python_class: py_class,
                serialized_python_class: serialized_class,
                sql_type: Box::new(
                    (*sql_type.ok_or_else(|| CommonError::missing("UDT sql type"))?).try_into()?,
                ),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn nested_schema_preserves_nullability_and_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let data_type: JsonDataType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{
                "name": "Values",
                "type": {"type": "array", "elementType": "decimal(12,2)", "containsNull": false},
                "nullable": false,
                "metadata": {"count": 3, "enabled": true, "labels": ["a", "b"]}
            }]
        }))?;
        let spec::DataType::Struct { fields } = data_type.try_into()? else {
            return Err("expected struct".into());
        };
        assert_eq!(fields[0].name, "Values");
        assert!(!fields[0].nullable);
        assert_eq!(
            fields[0].data_type,
            spec::DataType::List {
                data_type: Box::new(spec::DataType::Decimal128 {
                    precision: 12,
                    scale: 2
                }),
                nullable: false,
            }
        );
        let metadata: serde_json::Value = serde_json::from_str(&fields[0].metadata[0].1)?;
        assert_eq!(
            metadata,
            json!({"count": 3, "enabled": true, "labels": ["a", "b"]})
        );
        Ok(())
    }

    #[test]
    fn nullable_defaults_and_simple_type_objects() -> Result<(), Box<dyn std::error::Error>> {
        let data_type: JsonDataType = serde_json::from_value(json!({
            "type": "array", "elementType": {"type": "integer"}
        }))?;
        assert_eq!(
            spec::DataType::try_from(data_type)?,
            spec::DataType::List {
                data_type: Box::new(spec::DataType::Int32),
                nullable: true,
            }
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_type_parameters() -> Result<(), Box<dyn std::error::Error>> {
        for schema in [
            r#""time(4)""#,
            r#""decimal(300,2)""#,
            r#""decimal(10,300)""#,
            r#""geography(4326, invalid)""#,
            r#"{"type":"udt"}"#,
        ] {
            let data_type: JsonDataType = serde_json::from_str(schema)?;
            assert!(spec::DataType::try_from(data_type).is_err(), "{schema}");
        }
        Ok(())
    }
}
