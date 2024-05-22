use std::fmt;

use arrow_cast::cast;
use datafusion::arrow::datatypes as adt;
use datafusion::arrow::record_batch::RecordBatch;
use framework_common::spec;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::data_type as sdt;

pub(crate) fn from_spark_built_in_data_type(
    data_type: &sc::DataType,
) -> SparkResult<adt::DataType> {
    let data_type: spec::DataType = data_type.clone().try_into()?;
    Ok(data_type.try_into()?)
}

pub(crate) fn to_spark_schema(schema: adt::SchemaRef) -> SparkResult<sc::DataType> {
    let fields: spec::Fields = schema.fields().clone().try_into()?;
    Ok(spec::DataType::Struct { fields }.try_into()?)
}

pub(crate) fn to_spark_data_type(data_type: &adt::DataType) -> SparkResult<sc::DataType> {
    let data_type: spec::DataType = data_type.clone().try_into()?;
    Ok(data_type.try_into()?)
}

pub(crate) fn cast_record_batch(
    batch: RecordBatch,
    schema: adt::SchemaRef,
) -> SparkResult<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let data_type = field.data_type();
            let column = cast(column, data_type)?;
            Ok(column)
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

impl fmt::Display for sc::DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for sdt::Kind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl sc::DataType {
    pub fn to_simple_string(&self) -> SparkResult<String> {
        match &self.kind {
            Some(kind) => kind.to_simple_string(),
            None => Ok("none".to_string()),
        }
    }
}

impl sdt::Kind {
    pub fn to_simple_string(&self) -> SparkResult<String> {
        match self {
            sdt::Kind::Null(_) => Ok("void".to_string()),
            sdt::Kind::Binary(_) => Ok("binary".to_string()),
            sdt::Kind::Boolean(_) => Ok("boolean".to_string()),
            sdt::Kind::Byte(_) => Ok("tinyint".to_string()),
            sdt::Kind::Short(_) => Ok("smallint".to_string()),
            sdt::Kind::Integer(_) => Ok("int".to_string()),
            sdt::Kind::Long(_) => Ok("bigint".to_string()),
            sdt::Kind::Float(_) => Ok("float".to_string()),
            sdt::Kind::Double(_) => Ok("double".to_string()),
            sdt::Kind::Decimal(decimal) => Ok(format!(
                "decimal({},{})",
                decimal.precision.unwrap_or(10),
                decimal.scale.unwrap_or(0)
            )),
            sdt::Kind::String(_) => Ok("string".to_string()),
            sdt::Kind::Char(char) => Ok(format!("char({})", char.length)),
            sdt::Kind::VarChar(car_char) => Ok(format!("varchar({})", car_char.length)),
            sdt::Kind::Date(_) => Ok("date".to_string()),
            sdt::Kind::Timestamp(_) => Ok("timestamp".to_string()),
            sdt::Kind::TimestampNtz(_) => Ok("timestamp_ntz".to_string()),
            sdt::Kind::CalendarInterval(_) => Ok("interval".to_string()),
            sdt::Kind::YearMonthInterval(interval) => {
                let (start_field, end_field) = match (interval.start_field, interval.end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(end)) => (end, end), // Should not occur, but let's handle it
                    (None, None) => (0, 1),          // Default to YEAR and MONTH
                };

                let start_field_name = match start_field {
                    0 => "year",
                    1 => "month",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid year-month start_field value: {}",
                            start_field
                        )))
                    }
                };
                let end_field_name = match end_field {
                    0 => "year",
                    1 => "month",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid year-month end_field value: {}",
                            end_field
                        )))
                    }
                };

                if start_field_name == end_field_name {
                    Ok(format!("interval {}", start_field_name))
                } else if start_field < end_field {
                    Ok(format!(
                        "interval {} to {}",
                        start_field_name, end_field_name
                    ))
                } else {
                    Err(SparkError::invalid(format!(
                        "Invalid year-month interval: start_field={}, end_field={}",
                        start_field, end_field
                    )))
                }
            }
            sdt::Kind::DayTimeInterval(interval) => {
                let (start_field, end_field) = match (interval.start_field, interval.end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(end)) => (end, end), // Should not occur, but let's handle it
                    (None, None) => (0, 3),          // Default to DAY and SECOND
                };

                let start_field_name = match start_field {
                    0 => "day",
                    1 => "hour",
                    2 => "minute",
                    3 => "second",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid day-time start_field value: {}",
                            start_field
                        )))
                    }
                };
                let end_field_name = match end_field {
                    0 => "day",
                    1 => "hour",
                    2 => "minute",
                    3 => "second",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid day-time end_field value: {}",
                            end_field
                        )))
                    }
                };

                if start_field_name == end_field_name {
                    Ok(format!("interval {}", start_field_name))
                } else if start_field < end_field {
                    Ok(format!(
                        "interval {} to {}",
                        start_field_name, end_field_name
                    ))
                } else {
                    Err(SparkError::invalid(format!(
                        "Invalid day-time interval: start_field={}, end_field={}",
                        start_field, end_field
                    )))
                }
            }
            sdt::Kind::Array(array) => {
                let element_type = array.element_type.as_ref().required("array element type")?;
                Ok(format!("array<{}>", element_type.to_simple_string()?))
            }
            sdt::Kind::Struct(_) => Ok("struct".to_string()),
            sdt::Kind::Map(map) => {
                let key_type = map.key_type.as_ref().required("map key type")?;
                let value_type = map.value_type.as_ref().required("map value type")?;
                Ok(format!(
                    "map<{},{}>",
                    key_type.to_simple_string()?,
                    value_type.to_simple_string()?
                ))
            }
            sdt::Kind::Udt(udt) => {
                let sql_type = udt.sql_type.as_ref().required("UDT SQL type")?;
                Ok(sql_type.to_simple_string()?)
            }
            sdt::Kind::Unparsed(_) => Err(SparkError::invalid("to_simple_string for unparsed")),
        }
    }
}
