use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DECIMAL128_MAX_PRECISION as ARROW_DECIMAL128_MAX_PRECISION;
use sail_common::spec;
use sail_sql::data_type::parse_data_type;

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
    if let Ok(dt) = parse_data_type(schema) {
        Ok(dt)
    } else if let Ok(dt) = parse_data_type(format!("struct<{schema}>").as_str()) {
        // The SQL parser supports both `struct<name: type, ...>` and `struct<name type, ...>` syntax.
        // Therefore, by wrapping the input with `struct<...>`, we do not need separate logic
        // to parse table schema input (`name type, ...`).
        Ok(dt)
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
                    Ok(spec::DataType::Decimal256(precision, scale))
                } else {
                    Ok(spec::DataType::Decimal128(precision, scale))
                }
            }
            Kind::String(_) => Ok(spec::DataType::ConfiguredUtf8(None, None)),
            Kind::Char(sdt::Char {
                length,
                type_variation_reference: _,
            }) => {
                let length = length
                    .try_into()
                    .map_err(|_| SparkError::invalid("char length"))?;
                Ok(spec::DataType::ConfiguredUtf8(
                    Some(length),
                    Some(spec::ConfiguredUtf8Type::Char),
                ))
            }
            Kind::VarChar(sdt::VarChar {
                length,
                type_variation_reference: _,
            }) => {
                let length = length
                    .try_into()
                    .map_err(|_| SparkError::invalid("varchar length"))?;
                Ok(spec::DataType::ConfiguredUtf8(
                    Some(length),
                    Some(spec::ConfiguredUtf8Type::VarChar),
                ))
            }
            Kind::Date(_) => Ok(spec::DataType::Date32),
            Kind::Timestamp(_) => {
                // FIXME: TIMESTAMP in Spark is a user-specified alias associated with one of the
                //  TIMESTAMP_LTZ and TIMESTAMP_NTZ variations.
                //  Users can set the default timestamp type as TIMESTAMP_LTZ or TIMESTAMP_NTZ
                //  via the configuration `spark.sql.timestampType`.
                //  Reference: org.apache.spark.sql.connect.planner.SparkConnectPlanner#parseDatatypeString
                Ok(spec::DataType::Timestamp(
                    spec::TimeUnit::Microsecond,
                    Some(Arc::<str>::from("ltz")),
                ))
            }
            Kind::TimestampNtz(_) => {
                Ok(spec::DataType::Timestamp(spec::TimeUnit::Microsecond, None))
            }
            Kind::CalendarInterval(_) => Ok(spec::DataType::Interval(
                spec::IntervalUnit::MonthDayNano,
                None,
                None,
            )),
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
                Ok(spec::DataType::Interval(
                    spec::IntervalUnit::YearMonth,
                    start_field,
                    end_field,
                ))
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
                Ok(spec::DataType::Duration(spec::TimeUnit::Microsecond))
            }
            Kind::Array(array) => {
                let sdt::Array {
                    element_type,
                    contains_null,
                    type_variation_reference: _,
                } = *array;
                let element_type = element_type.required("array element type")?;
                let field = spec::Field {
                    name: "item".to_string(),
                    data_type: spec::DataType::try_from(*element_type)?,
                    nullable: contains_null,
                    metadata: vec![],
                };
                Ok(spec::DataType::List(Arc::new(field)))
            }
            Kind::Struct(sdt::Struct {
                fields,
                type_variation_reference: _,
            }) => {
                let fields: Vec<spec::FieldRef> = fields
                    .into_iter()
                    .map(spec::FieldRef::try_from)
                    .collect::<SparkResult<_>>()?;
                Ok(spec::DataType::Struct(spec::Fields::from(fields)))
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
                let fields = spec::Fields::from(vec![
                    spec::Field {
                        name: "key".to_string(),
                        data_type: spec::DataType::try_from(*key_type)?,
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: "value".to_string(),
                        data_type: spec::DataType::try_from(*value_type)?,
                        nullable: value_contains_null,
                        metadata: vec![],
                    },
                ]);
                let keys_are_sorted = false;
                Ok(spec::DataType::Map(
                    Arc::new(spec::Field {
                        name: "entries".to_string(),
                        data_type: spec::DataType::Struct(fields),
                        nullable: false,
                        metadata: vec![],
                    }),
                    keys_are_sorted,
                ))
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
                Ok(spec::DataType::UserDefined {
                    jvm_class,
                    python_class,
                    serialized_python_class,
                    sql_type: Box::new(spec::DataType::try_from(*sql_type)?),
                })
            }
            Kind::Unparsed(sdt::Unparsed { data_type_string }) => {
                Ok(parse_spark_data_type(data_type_string.as_str())?)
            }
        }
    }
}

impl TryFrom<spec::Field> for sdt::StructField {
    type Error = SparkError;

    fn try_from(field: spec::Field) -> SparkResult<sdt::StructField> {
        let spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let data_type = data_type.try_into()?;
        let metadata: HashMap<_, _> = metadata.into_iter().collect();
        let metadata = serde_json::to_string(&metadata)?;
        Ok(sdt::StructField {
            name,
            data_type: Some(data_type),
            nullable,
            metadata: Some(metadata),
        })
    }
}

/// Reference: https://github.com/apache/spark/blob/bb17665955ad536d8c81605da9a59fb94b6e0162/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
impl TryFrom<spec::DataType> for DataType {
    type Error = SparkError;

    fn try_from(data_type: spec::DataType) -> SparkResult<DataType> {
        use crate::spark::connect::data_type::Kind;

        let kind = match data_type {
            spec::DataType::Null => Ok(Kind::Null(sdt::Null::default())),
            spec::DataType::Binary
            | spec::DataType::FixedSizeBinary(_)
            | spec::DataType::LargeBinary
            | spec::DataType::BinaryView
            | spec::DataType::ConfiguredBinary => Ok(Kind::Binary(sdt::Binary::default())),
            spec::DataType::Boolean => Ok(Kind::Boolean(sdt::Boolean::default())),
            spec::DataType::Int8 => Ok(Kind::Byte(sdt::Byte::default())),
            spec::DataType::UInt8 | spec::DataType::Int16 => Ok(Kind::Short(sdt::Short::default())),
            spec::DataType::UInt16 | spec::DataType::Int32 => {
                Ok(Kind::Integer(sdt::Integer::default()))
            }
            // FIXME: `spec::DataType::UInt64` to `Kind::Long` will overflow.
            spec::DataType::UInt32 | spec::DataType::UInt64 | spec::DataType::Int64 => {
                Ok(Kind::Long(sdt::Long::default()))
            }
            spec::DataType::Float16 => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Float16 to Spark Kind",
            )),
            spec::DataType::Float32 => Ok(Kind::Float(sdt::Float::default())),
            spec::DataType::Float64 => Ok(Kind::Double(sdt::Double::default())),
            spec::DataType::Decimal128(scale, precision)
            | spec::DataType::Decimal256(scale, precision) => Ok(Kind::Decimal(sdt::Decimal {
                scale: Some(scale as i32),
                precision: Some(precision as i32),
                type_variation_reference: 0,
            })),
            // FIXME: This mapping might not always be correct due to converting to Arrow data types and back.
            //  For example, this originally may have been a `Kind::Char` or `Kind::VarChar` in Spark.
            //  We retain the original type information in `ConfiguredUtf8`, which is currently lost when converting to Arrow.
            spec::DataType::Utf8 | spec::DataType::LargeUtf8 | spec::DataType::Utf8View => {
                Ok(Kind::String(sdt::String::default()))
            }
            spec::DataType::ConfiguredUtf8(Some(length), utf8_type) => match utf8_type {
                Some(spec::ConfiguredUtf8Type::Char) => Ok(Kind::Char(sdt::Char {
                    length: length
                        .try_into()
                        .map_err(|_| SparkError::invalid("char length"))?,
                    type_variation_reference: 0,
                })),
                Some(spec::ConfiguredUtf8Type::VarChar) => Ok(Kind::VarChar(sdt::VarChar {
                    length: length
                        .try_into()
                        .map_err(|_| SparkError::invalid("varchar length"))?,
                    type_variation_reference: 0,
                })),
                None => Ok(Kind::String(sdt::String::default())),
            },
            spec::DataType::ConfiguredUtf8(None, _utf8_type) => Err(SparkError::invalid(
                "TryFrom spec::DataType::ConfiguredUtf8(None, _) to Spark Kind",
            )),
            spec::DataType::Date32 => Ok(Kind::Date(sdt::Date::default())),
            spec::DataType::Date64 => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Date64 to Spark Kind",
            )),
            spec::DataType::Time32(_) => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Time32 to Spark Kind",
            )),
            spec::DataType::Time64(_) => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Time64 to Spark Kind",
            )),
            spec::DataType::Timestamp(spec::TimeUnit::Microsecond, None) => {
                Ok(Kind::TimestampNtz(sdt::TimestampNtz::default()))
            }
            spec::DataType::Timestamp(spec::TimeUnit::Microsecond, Some(_timezone)) => {
                Ok(Kind::Timestamp(sdt::Timestamp::default()))
            }
            spec::DataType::Timestamp(spec::TimeUnit::Second, _)
            | spec::DataType::Timestamp(spec::TimeUnit::Millisecond, _)
            | spec::DataType::Timestamp(spec::TimeUnit::Nanosecond, _) => {
                // This error theoretically should never be reached.
                Err(SparkError::unsupported(
                    "TryFrom spec::DataType::Timestamp(Second | Millisecond | Nanosecond) to Spark Kind",
                ))
            }
            spec::DataType::Interval(spec::IntervalUnit::MonthDayNano, _, _) => {
                Ok(Kind::CalendarInterval(sdt::CalendarInterval::default()))
            }
            spec::DataType::Interval(spec::IntervalUnit::YearMonth, start_field, end_field) => {
                Ok(Kind::YearMonthInterval(sdt::YearMonthInterval {
                    start_field: start_field.map(|f| f as i32),
                    end_field: end_field.map(|f| f as i32),
                    type_variation_reference: 0,
                }))
            }
            spec::DataType::Interval(spec::IntervalUnit::DayTime, _, _) => {
                // This error theoretically should never be reached.
                Err(SparkError::unsupported(
                    "TryFrom spec::DataType::Interval(DayTime) to Spark Kind",
                ))
            }
            spec::DataType::Duration(spec::TimeUnit::Microsecond) => {
                // TODO: [CHECK HERE] BEFORE MERGING IN MAKE SURE IT DOESN'T BREAK TPC-H
                Ok(Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                }))
            }
            spec::DataType::Duration(spec::TimeUnit::Second)
            | spec::DataType::Duration(spec::TimeUnit::Millisecond)
            | spec::DataType::Duration(spec::TimeUnit::Nanosecond) => {
                // This error theoretically should never be reached.
                Err(SparkError::unsupported(
                    "TryFrom spec::DataType::Duration(Second | Millisecond | Nanosecond) to Spark Kind",
                ))
            }
            spec::DataType::List(field)
            | spec::DataType::FixedSizeList(field, _)
            | spec::DataType::LargeList(field) => {
                let spec::Field {
                    name: _,
                    data_type,
                    nullable,
                    metadata: _,
                } = field.as_ref();
                Ok(Kind::Array(Box::new(sdt::Array {
                    element_type: Some(Box::new(data_type.clone().try_into()?)),
                    contains_null: *nullable,
                    type_variation_reference: 0,
                })))
            }
            spec::DataType::Struct(fields) => Ok(Kind::Struct(sdt::Struct {
                fields: fields
                    .into_iter()
                    .map(|f| f.as_ref().clone().try_into())
                    .collect::<SparkResult<Vec<sdt::StructField>>>()?,
                type_variation_reference: 0,
            })),
            spec::DataType::Map(field, _keys_are_sorted) => {
                let spec::Field {
                    name: _,
                    data_type,
                    nullable: _,
                    metadata: _,
                } = field.as_ref();
                let fields = match data_type {
                    spec::DataType::Struct(fields) => fields,
                    _ => {
                        return Err(SparkError::invalid(
                            "TryFrom spec::DataType::Map to Kind::Map: Invalid Map data type.",
                        ))
                    }
                };
                if fields.len() != 2 {
                    return Err(SparkError::invalid(
                        "TryFrom spec::DataType::Map to Kind::Map: Map data type must have key and value fields",
                    ));
                }
                let key_type = fields[0].data_type.clone();
                let value_type = fields[1].data_type.clone();
                Ok(Kind::Map(Box::new(sdt::Map {
                    key_type: Some(Box::new(key_type.try_into()?)),
                    value_type: Some(Box::new(value_type.try_into()?)),
                    value_contains_null: fields[1].nullable,
                    type_variation_reference: 0,
                })))
            }
            spec::DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => Ok(Kind::Udt(Box::new(sdt::Udt {
                r#type: "udt".to_string(),
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type: Some(Box::new((*sql_type).try_into()?)),
            }))),
            spec::DataType::Union(_, _) => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Union to Spark Kind",
            )),
            spec::DataType::Dictionary(_, _) => Err(SparkError::unsupported(
                "TryFrom spec::DataType::Dictionary to Spark Kind",
            )),
        };
        Ok(DataType { kind: Some(kind?) })
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
