use std::sync::Arc;

use sail_common::spec;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::{DataType, data_type as sdt};

pub(crate) const DEFAULT_FIELD_NAME: &str = "value";

#[expect(dead_code)]
pub(crate) const SPARK_DECIMAL_MAX_PRECISION: u8 = 38;
#[expect(dead_code)]
pub(crate) const SPARK_DECIMAL_MAX_SCALE: i8 = 38;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_PRECISION: u8 = 10;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_SCALE: i8 = 0;
#[expect(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_PRECISION: u8 = 38;
#[expect(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_SCALE: i8 = 18;

pub(crate) fn parse_spark_data_type(schema: &str) -> SparkResult<spec::DataType> {
    sail_sql_analyzer::data_type::parse_spark_data_type(schema).map_err(|e| match e {
        sail_sql_analyzer::error::SqlError::InvalidArgument(message)
            if message == "empty data type" =>
        {
            SparkError::invalid(message)
        }
        e => SparkError::ParseError(e.to_string()),
    })
}

impl TryFrom<sdt::StructField> for spec::FieldRef {
    type Error = SparkError;

    fn try_from(field: sdt::StructField) -> SparkResult<spec::FieldRef> {
        let sdt::StructField {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let data_type = data_type.required("data type")?;
        let data_type = spec::DataType::try_from(data_type)?;
        let metadata = metadata
            .filter(|m| !m.trim().is_empty())
            .map(|m| vec![(spec::SPARK_METADATA_JSON_KEY.to_string(), m)])
            .unwrap_or_default();
        Ok(Arc::new(spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        }))
    }
}

/// Convert protocol types through the shared Spark type model.
impl TryFrom<DataType> for spec::DataType {
    type Error = SparkError;

    fn try_from(data_type: DataType) -> SparkResult<spec::DataType> {
        use sail_common_spark::data_type::JsonDataType;

        use crate::spark::connect::data_type::Kind;

        let kind = data_type.kind.required("data type kind")?;
        let data_type = match kind {
            Kind::Null(_) => JsonDataType::Null,
            Kind::Binary(_) => JsonDataType::Binary,
            Kind::Boolean(_) => JsonDataType::Boolean,
            Kind::Byte(_) => JsonDataType::Byte,
            Kind::Short(_) => JsonDataType::Short,
            Kind::Integer(_) => JsonDataType::Integer,
            Kind::Long(_) => JsonDataType::Long,
            Kind::Float(_) => JsonDataType::Float,
            Kind::Double(_) => JsonDataType::Double,
            Kind::Decimal(decimal) => JsonDataType::FixedDecimal {
                precision: decimal
                    .precision
                    .unwrap_or(i32::from(SPARK_DECIMAL_USER_DEFAULT_PRECISION)),
                scale: decimal
                    .scale
                    .unwrap_or(i32::from(SPARK_DECIMAL_USER_DEFAULT_SCALE)),
            },
            Kind::String(_) => JsonDataType::String,
            Kind::Char(char_type) => JsonDataType::Char {
                length: char_type.length,
            },
            Kind::VarChar(varchar_type) => JsonDataType::Varchar {
                length: varchar_type.length,
            },
            Kind::Date(_) => JsonDataType::Date,
            Kind::Timestamp(_) => JsonDataType::Timestamp,
            Kind::TimestampNtz(_) => JsonDataType::TimestampNtz,
            Kind::TimestampLtzNanos(_) => JsonDataType::TimestampLtzNanos,
            Kind::TimestampNtzNanos(_) => JsonDataType::TimestampNtzNanos,
            Kind::Time(time) => JsonDataType::Time {
                precision: time.precision,
            },
            Kind::CalendarInterval(_) => JsonDataType::CalendarInterval,
            Kind::Variant(_) => JsonDataType::Variant,
            Kind::Geometry(geometry) => JsonDataType::Geometry {
                srid: geometry.srid,
            },
            Kind::Geography(geography) => JsonDataType::Geography {
                srid: geography.srid,
                algorithm: "spherical".to_string(),
            },
            Kind::YearMonthInterval(sdt::YearMonthInterval {
                start_field,
                end_field,
                type_variation_reference: _,
            }) => {
                let start_field = start_field
                    .map(spec::YearMonthIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::from);
                let end_field = end_field
                    .map(spec::YearMonthIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::from);
                let start_field = Some(start_field.unwrap_or(spec::IntervalFieldType::Year));
                let end_field = Some(end_field.unwrap_or(spec::IntervalFieldType::Month));
                return Ok(spec::DataType::Interval {
                    interval_unit: spec::IntervalUnit::YearMonth,
                    start_field,
                    end_field,
                });
            }
            Kind::DayTimeInterval(sdt::DayTimeInterval {
                start_field,
                end_field,
                type_variation_reference: _,
            }) => {
                let start_field = start_field
                    .map(spec::DayTimeIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::from);
                let end_field = end_field
                    .map(spec::DayTimeIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::from);
                let start_field = Some(start_field.unwrap_or(spec::IntervalFieldType::Day));
                let end_field = Some(end_field.unwrap_or(spec::IntervalFieldType::Second));
                return Ok(spec::DataType::Interval {
                    interval_unit: spec::IntervalUnit::DayTime,
                    start_field,
                    end_field,
                });
            }
            Kind::Array(array) => {
                let sdt::Array {
                    element_type,
                    contains_null,
                    type_variation_reference: _,
                } = *array;
                let element_type = element_type.required("array element type")?;
                return Ok(spec::DataType::List {
                    data_type: Box::new(spec::DataType::try_from(*element_type)?),
                    nullable: contains_null,
                });
            }
            Kind::Struct(sdt::Struct {
                fields,
                type_variation_reference: _,
            }) => {
                let fields: Vec<spec::FieldRef> = fields
                    .into_iter()
                    .map(spec::FieldRef::try_from)
                    .collect::<SparkResult<_>>()?;
                return Ok(spec::DataType::Struct {
                    fields: spec::Fields::from(fields),
                });
            }
            Kind::Map(map) => {
                let sdt::Map {
                    key_type,
                    value_type,
                    value_contains_null,
                    type_variation_reference: _,
                } = *map;
                let key_type = key_type.required("map key type")?;
                let value_type = value_type.required("map value type")?;
                return Ok(spec::DataType::Map {
                    key_type: Box::new(spec::DataType::try_from(*key_type)?),
                    value_type: Box::new(spec::DataType::try_from(*value_type)?),
                    value_type_nullable: value_contains_null,
                    keys_sorted: false,
                });
            }
            Kind::Udt(udt) => {
                let sdt::Udt {
                    r#type: _,
                    jvm_class,
                    python_class,
                    serialized_python_class,
                    sql_type,
                } = *udt;
                let sql_type = sql_type.required("UDT sql type")?;
                return Ok(spec::DataType::UserDefined {
                    jvm_class,
                    python_class,
                    serialized_python_class,
                    sql_type: Box::new(spec::DataType::try_from(*sql_type)?),
                });
            }
            Kind::Unparsed(sdt::Unparsed { data_type_string }) => {
                return parse_spark_data_type(&data_type_string);
            }
        };
        Ok(data_type.try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use sail_common::spec;
    use sail_common::tests::test_gold_set;

    use super::{DEFAULT_FIELD_NAME, parse_spark_data_type};
    use crate::error::{SparkError, SparkResult};

    #[test]
    fn test_parse_spark_data_type_gold_set() -> SparkResult<()> {
        test_gold_set(
            "tests/gold_data/data_type.json",
            |s: String| parse_spark_data_type(&s),
            |e: String| SparkError::internal(e),
        )
    }

    #[test]
    fn test_parse_spark_table_schema_gold_set() -> SparkResult<()> {
        test_gold_set(
            "tests/gold_data/table_schema.json",
            |s: String| Ok(parse_spark_data_type(&s)?.into_schema(DEFAULT_FIELD_NAME, true)),
            |e: String| SparkError::internal(e),
        )
    }

    #[test]
    fn test_geometry_proto_conversion() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Geometry, Kind};

        let proto_type = crate::spark::connect::DataType {
            kind: Some(Kind::Geometry(Geometry {
                srid: 4326,
                type_variation_reference: 0,
            })),
        };

        let spec_type = spec::DataType::try_from(proto_type)?;
        assert_eq!(spec_type, spec::DataType::Geometry { srid: 4326 });

        Ok(())
    }

    #[test]
    fn test_geography_proto_conversion() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Geography, Kind};

        let proto_type = crate::spark::connect::DataType {
            kind: Some(Kind::Geography(Geography {
                srid: 4326,
                type_variation_reference: 0,
            })),
        };

        let spec_type = spec::DataType::try_from(proto_type)?;
        assert_eq!(
            spec_type,
            spec::DataType::Geography {
                srid: 4326,
                algorithm: spec::EdgeInterpolationAlgorithm::Spherical,
            }
        );

        Ok(())
    }

    #[test]
    fn test_geometry_default_srid() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Geometry, Kind};

        let proto_type = crate::spark::connect::DataType {
            kind: Some(Kind::Geometry(Geometry {
                srid: 0,
                type_variation_reference: 0,
            })),
        };

        let spec_type = spec::DataType::try_from(proto_type)?;
        assert_eq!(spec_type, spec::DataType::Geometry { srid: 0 });

        Ok(())
    }

    #[test]
    fn test_geography_default_srid() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Geography, Kind};

        let proto_type = crate::spark::connect::DataType {
            kind: Some(Kind::Geography(Geography {
                srid: 4326,
                type_variation_reference: 0,
            })),
        };

        let spec_type = spec::DataType::try_from(proto_type)?;
        assert_eq!(
            spec_type,
            spec::DataType::Geography {
                srid: 4326,
                algorithm: spec::EdgeInterpolationAlgorithm::Spherical,
            }
        );

        Ok(())
    }

    #[test]
    fn test_geometry_mixed_srid() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Geometry, Kind};

        let proto_type = crate::spark::connect::DataType {
            kind: Some(Kind::Geometry(Geometry {
                srid: -1,
                type_variation_reference: 0,
            })),
        };

        let spec_type = spec::DataType::try_from(proto_type)?;
        assert_eq!(spec_type, spec::DataType::Geometry { srid: -1 });

        Ok(())
    }

    #[test]
    fn test_time_proto_to_spec() -> SparkResult<()> {
        use crate::spark::connect::data_type::{Kind, Time};

        // TIME(0) -> Time32 Second
        let proto = crate::spark::connect::DataType {
            kind: Some(Kind::Time(Time {
                precision: Some(0),
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            spec::DataType::try_from(proto)?,
            spec::DataType::Time32 {
                time_unit: spec::TimeUnit::Second
            }
        );

        // TIME(3) -> Time32 Millisecond
        let proto = crate::spark::connect::DataType {
            kind: Some(Kind::Time(Time {
                precision: Some(3),
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            spec::DataType::try_from(proto)?,
            spec::DataType::Time32 {
                time_unit: spec::TimeUnit::Millisecond
            }
        );

        // TIME(6) -> Time64 Microsecond
        let proto = crate::spark::connect::DataType {
            kind: Some(Kind::Time(Time {
                precision: Some(6),
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            spec::DataType::try_from(proto)?,
            spec::DataType::Time64 {
                time_unit: spec::TimeUnit::Microsecond
            }
        );

        // TIME(9) -> Time64 Nanosecond
        let proto = crate::spark::connect::DataType {
            kind: Some(Kind::Time(Time {
                precision: Some(9),
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            spec::DataType::try_from(proto)?,
            spec::DataType::Time64 {
                time_unit: spec::TimeUnit::Nanosecond
            }
        );

        // TIME (no precision) defaults to TIME(6)
        let proto = crate::spark::connect::DataType {
            kind: Some(Kind::Time(Time {
                precision: None,
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            spec::DataType::try_from(proto)?,
            spec::DataType::Time64 {
                time_unit: spec::TimeUnit::Microsecond
            }
        );

        Ok(())
    }

    #[test]
    fn test_time_proto_invalid_precision() {
        use crate::spark::connect::data_type::{Kind, Time};

        // Invalid precisions: 1, 2, 4, 5, 7, 8, 10+
        for precision in [1, 2, 4, 5, 7, 8, 10] {
            let proto = crate::spark::connect::DataType {
                kind: Some(Kind::Time(Time {
                    precision: Some(precision),
                    type_variation_reference: 0,
                })),
            };
            assert!(
                spec::DataType::try_from(proto).is_err(),
                "Precision {} should be invalid",
                precision
            );
        }
    }
}
