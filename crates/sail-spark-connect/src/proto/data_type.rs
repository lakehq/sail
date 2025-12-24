use std::collections::HashMap;
use std::sync::Arc;

use sail_common::spec;
use sail_common::spec::ARROW_DECIMAL128_MAX_PRECISION;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser::parse_data_type;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::proto::data_type_json::parse_spark_json_data_type;
use crate::spark::connect::{data_type as sdt, DataType};

pub(crate) const DEFAULT_FIELD_NAME: &str = "value";

#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_MAX_PRECISION: u8 = 38;
#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_MAX_SCALE: i8 = 38;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_PRECISION: u8 = 10;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_SCALE: i8 = 0;
#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_PRECISION: u8 = 38;
#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_SCALE: i8 = 18;

/// Parse a Spark data type string of various forms.
/// Reference: org.apache.spark.sql.connect.planner.SparkConnectPlanner#parseDatatypeString
pub(crate) fn parse_spark_data_type(schema: &str) -> SparkResult<spec::DataType> {
    if let Ok(dt) = parse_data_type(schema).and_then(from_ast_data_type) {
        Ok(dt)
    } else if let Ok(dt) =
        parse_data_type(format!("struct<{schema}>").as_str()).and_then(from_ast_data_type)
    {
        match dt {
            spec::DataType::Struct { fields } if fields.is_empty() => {
                Err(SparkError::invalid("empty data type"))
            }
            // The SQL parser supports both `struct<name: type, ...>` and `struct<name type, ...>` syntax.
            // Therefore, by wrapping the input with `struct<...>`, we do not need separate logic
            // to parse table schema input (`name type, ...`).
            _ => Ok(dt),
        }
    } else {
        parse_spark_json_data_type(schema)?.try_into()
    }
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
        let metadata: HashMap<String, String> = metadata
            .map(|m| -> SparkResult<_> { Ok(serde_json::from_str(m.as_str())?) })
            .transpose()?
            .unwrap_or_default();
        Ok(Arc::new(spec::Field {
            name,
            data_type,
            nullable,
            // TODO: preserve metadata order in serde
            metadata: metadata.into_iter().collect(),
        }))
    }
}

/// Reference: https://github.com/apache/spark/blob/bb17665955ad536d8c81605da9a59fb94b6e0162/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
impl TryFrom<DataType> for spec::DataType {
    type Error = SparkError;

    fn try_from(data_type: DataType) -> SparkResult<spec::DataType> {
        use crate::spark::connect::data_type::Kind;

        let DataType { kind } = data_type;
        let kind = kind.required("data type kind")?;
        match kind {
            Kind::Null(_) => Ok(spec::DataType::Null),
            Kind::Binary(_) => Ok(spec::DataType::ConfiguredBinary),
            Kind::Boolean(_) => Ok(spec::DataType::Boolean),
            Kind::Byte(_) => Ok(spec::DataType::Int8),
            Kind::Short(_) => Ok(spec::DataType::Int16),
            Kind::Integer(_) => Ok(spec::DataType::Int32),
            Kind::Long(_) => Ok(spec::DataType::Int64),
            Kind::Float(_) => Ok(spec::DataType::Float32),
            Kind::Double(_) => Ok(spec::DataType::Float64),
            Kind::Decimal(sdt::Decimal {
                scale,
                precision,
                type_variation_reference: _,
            }) => {
                let scale = scale
                    .map(i8::try_from)
                    .transpose()
                    .map_err(|_| SparkError::invalid("decimal scale"))?
                    .unwrap_or(SPARK_DECIMAL_USER_DEFAULT_SCALE);
                let precision = precision
                    .map(u8::try_from)
                    .transpose()
                    .map_err(|_| SparkError::invalid("decimal precision"))?
                    .unwrap_or(SPARK_DECIMAL_USER_DEFAULT_PRECISION);
                if precision > ARROW_DECIMAL128_MAX_PRECISION {
                    Ok(spec::DataType::Decimal256 { precision, scale })
                } else {
                    Ok(spec::DataType::Decimal128 { precision, scale })
                }
            }
            Kind::String(_) => Ok(spec::DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Configured,
            }),
            Kind::Char(sdt::Char {
                length,
                type_variation_reference: _,
            }) => {
                let length = length
                    .try_into()
                    .map_err(|_| SparkError::invalid("char length"))?;
                Ok(spec::DataType::ConfiguredUtf8 {
                    utf8_type: spec::Utf8Type::Char { length },
                })
            }
            Kind::VarChar(sdt::VarChar {
                length,
                type_variation_reference: _,
            }) => {
                let length = length
                    .try_into()
                    .map_err(|_| SparkError::invalid("varchar length"))?;
                Ok(spec::DataType::ConfiguredUtf8 {
                    utf8_type: spec::Utf8Type::VarChar { length },
                })
            }
            Kind::Date(_) => Ok(spec::DataType::Date32),
            Kind::Timestamp(_) => Ok(spec::DataType::Timestamp {
                time_unit: spec::TimeUnit::Microsecond,
                timestamp_type: spec::TimestampType::WithLocalTimeZone,
            }),
            Kind::TimestampNtz(_) => Ok(spec::DataType::Timestamp {
                time_unit: spec::TimeUnit::Microsecond,
                timestamp_type: spec::TimestampType::WithoutTimeZone,
            }),
            Kind::CalendarInterval(_) => Ok(spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::MonthDayNano,
                start_field: None,
                end_field: None,
            }),
            Kind::YearMonthInterval(sdt::YearMonthInterval {
                start_field,
                end_field,
                type_variation_reference: _,
            }) => {
                let start_field = start_field
                    .map(spec::YearMonthIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::try_from)
                    .transpose()?;
                let end_field = end_field
                    .map(spec::YearMonthIntervalField::try_from)
                    .transpose()?
                    .map(spec::IntervalFieldType::try_from)
                    .transpose()?;
                let start_field = Some(start_field.unwrap_or(spec::IntervalFieldType::Year));
                let end_field = Some(end_field.unwrap_or(spec::IntervalFieldType::Month));
                Ok(spec::DataType::Interval {
                    interval_unit: spec::IntervalUnit::YearMonth,
                    start_field,
                    end_field,
                })
            }
            Kind::DayTimeInterval(sdt::DayTimeInterval {
                // FIXME: Currently `start_field` and `end_field` are lost in translation.
                //  This does not impact computation accuracy.
                //  This may affect the display string in the `data_type_to_simple_string` function.
                start_field: _,
                end_field: _,
                type_variation_reference: _,
            }) => {
                // Spark's DayTimeInterval has microsecond precision.
                // Arrow's IntervalUnit::DayTime has millisecond precision.
                Ok(spec::DataType::Duration {
                    time_unit: spec::TimeUnit::Microsecond,
                })
            }
            Kind::Array(array) => {
                let sdt::Array {
                    element_type,
                    contains_null,
                    type_variation_reference: _,
                } = *array;
                let element_type = element_type.required("array element type")?;
                Ok(spec::DataType::List {
                    data_type: Box::new(spec::DataType::try_from(*element_type)?),
                    nullable: contains_null,
                })
            }
            Kind::Struct(sdt::Struct {
                fields,
                type_variation_reference: _,
            }) => {
                let fields: Vec<spec::FieldRef> = fields
                    .into_iter()
                    .map(spec::FieldRef::try_from)
                    .collect::<SparkResult<_>>()?;
                Ok(spec::DataType::Struct {
                    fields: spec::Fields::from(fields),
                })
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
                Ok(spec::DataType::Map {
                    key_type: Box::new(spec::DataType::try_from(*key_type)?),
                    value_type: Box::new(spec::DataType::try_from(*value_type)?),
                    value_type_nullable: value_contains_null,
                    keys_sorted: false,
                })
            }
            Kind::Variant(_) => Err(SparkError::todo("variant data type")),
            Kind::Udt(udt) => {
                let sdt::Udt {
                    r#type: _,
                    jvm_class,
                    python_class,
                    serialized_python_class,
                    sql_type,
                } = *udt;
                let sql_type = sql_type.required("UDT sql type")?;
                Ok(spec::DataType::UserDefined {
                    jvm_class,
                    python_class,
                    serialized_python_class,
                    sql_type: Box::new(spec::DataType::try_from(*sql_type)?),
                })
            }
            Kind::Geometry(_) => Err(SparkError::todo("geometry data type")),
            Kind::Geography(_) => Err(SparkError::todo("geography data type")),
            Kind::Unparsed(sdt::Unparsed { data_type_string }) => {
                Ok(parse_spark_data_type(data_type_string.as_str())?)
            }
            Kind::Time(_) => Err(SparkError::todo("time data type")),
        }
    }
}

#[cfg(test)]
mod tests {
    use sail_common::tests::test_gold_set;

    use super::{parse_spark_data_type, DEFAULT_FIELD_NAME};
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
}
