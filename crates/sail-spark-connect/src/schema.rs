use std::fmt;
use std::fmt::Display;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use sail_common::string::escape_meta_characters;

use crate::error::SparkResult;
use crate::spark::connect as sc;
use crate::spark::connect::data_type::Kind;

pub(crate) fn to_spark_schema(schema: SchemaRef) -> SparkResult<sc::DataType> {
    DataType::Struct(schema.fields().clone()).try_into()
}

// Since we cannot construct formatter errors when the data type is invalid,
// we write a "?" in the formatter output.
// It is unlikely to encounter invalid data types at runtime, so it should be acceptable
// to handle the error in this way.

fn format_year_month_interval_field(f: &mut fmt::Formatter, field: i32) -> fmt::Result {
    match field {
        0 => write!(f, "year"),
        1 => write!(f, "month"),
        _ => write!(f, "?"),
    }
}

fn format_day_time_interval_field(f: &mut fmt::Formatter, field: i32) -> fmt::Result {
    match field {
        0 => write!(f, "day")?,
        1 => write!(f, "hour")?,
        2 => write!(f, "minute")?,
        3 => write!(f, "second")?,
        _ => write!(f, "?")?,
    }
    Ok(())
}

fn format_type_name(f: &mut fmt::Formatter, data_type: Option<&sc::DataType>) -> fmt::Result {
    let kind = match data_type {
        Some(sc::DataType { kind: Some(x) }) => x,
        _ => return write!(f, "?"),
    };
    match kind {
        Kind::Null(_) => write!(f, "null"),
        Kind::Binary(_) => write!(f, "binary"),
        Kind::Boolean(_) => write!(f, "boolean"),
        Kind::Byte(_) => write!(f, "byte"),
        Kind::Short(_) => write!(f, "short"),
        Kind::Integer(_) => write!(f, "integer"),
        Kind::Long(_) => write!(f, "long"),
        Kind::Float(_) => write!(f, "float"),
        Kind::Double(_) => write!(f, "double"),
        Kind::Decimal(decimal) => {
            write!(f, "decimal(")?;
            if let Some(precision) = decimal.precision {
                write!(f, "{precision}")?;
            } else {
                write!(f, "?")?;
            }
            write!(f, ",")?;
            if let Some(scale) = decimal.scale {
                write!(f, "{scale}")?;
            } else {
                write!(f, "?")?;
            }
            write!(f, ")")
        }
        Kind::String(_) => write!(f, "string"),
        Kind::Char(char) => write!(f, "char({})", char.length),
        Kind::VarChar(varchar) => write!(f, "varchar({})", varchar.length),
        Kind::Date(_) => write!(f, "date"),
        Kind::Timestamp(_) => write!(f, "timestamp"),
        Kind::TimestampNtz(_) => write!(f, "timestamp_ntz"),
        Kind::CalendarInterval(_) => write!(f, "interval"),
        Kind::YearMonthInterval(interval) => match (interval.start_field, interval.end_field) {
            (Some(start), Some(end)) => {
                write!(f, "interval ")?;
                format_year_month_interval_field(f, start)?;
                write!(f, " to ")?;
                format_year_month_interval_field(f, end)
            }
            (Some(start), None) => {
                write!(f, "interval ")?;
                format_year_month_interval_field(f, start)
            }
            (None, Some(end)) => {
                write!(f, "interval ? to ")?;
                format_year_month_interval_field(f, end)
            }
            (None, None) => write!(f, "interval"),
        },
        Kind::DayTimeInterval(interval) => match (interval.start_field, interval.end_field) {
            (Some(start), Some(end)) => {
                write!(f, "interval ")?;
                format_day_time_interval_field(f, start)?;
                write!(f, " to ")?;
                format_day_time_interval_field(f, end)
            }
            (Some(start), None) => {
                write!(f, "interval ")?;
                format_day_time_interval_field(f, start)
            }
            (None, Some(end)) => {
                write!(f, "interval ? to ")?;
                format_day_time_interval_field(f, end)
            }
            (None, None) => write!(f, "interval"),
        },
        Kind::Array(_) => write!(f, "array"),
        Kind::Struct(_) => write!(f, "struct"),
        Kind::Map(_) => write!(f, "map"),
        Kind::Variant(_) => write!(f, "variant"),
        Kind::Udt(udt) => {
            if udt.jvm_class.is_none() && udt.python_class.is_some() {
                write!(f, "pythonuserdefined")
            } else {
                write!(f, "userdefined")
            }
        }
        Kind::Geometry(_) => write!(f, "geometry"),
        Kind::Geography(_) => write!(f, "geography"),
        Kind::Unparsed(_) => write!(f, "unparsed"),
        Kind::Time(_) => write!(f, "time"),
    }
}

fn format_prefix(f: &mut fmt::Formatter, level: i32) -> fmt::Result {
    for _ in 0..(level - 1) {
        write!(f, " |   ")?;
    }
    write!(f, " |-- ")
}

fn format_bool(f: &mut fmt::Formatter, value: bool) -> fmt::Result {
    if value {
        write!(f, "true")
    } else {
        write!(f, "false")
    }
}

fn format_tree_string(
    f: &mut fmt::Formatter,
    data_type: Option<&sc::DataType>,
    max_level: Option<i32>,
    level: i32,
) -> fmt::Result {
    if max_level.is_some_and(|m| m > 0 && level > m) {
        return Ok(());
    }
    let kind = match data_type {
        Some(sc::DataType { kind: Some(x) }) => x,
        _ => return Ok(()),
    };
    match kind {
        Kind::Array(array) => {
            format_prefix(f, level)?;
            write!(f, "element: ")?;
            format_type_name(f, array.element_type.as_deref())?;
            write!(f, " (containsNull = ",)?;
            format_bool(f, array.contains_null)?;
            writeln!(f, ")")?;
            format_tree_string(f, array.element_type.as_deref(), max_level, level + 1)
        }
        Kind::Struct(r#struct) => {
            for field in r#struct.fields.iter() {
                format_prefix(f, level)?;
                write!(f, "{}: ", escape_meta_characters(&field.name))?;
                format_type_name(f, field.data_type.as_ref())?;
                write!(f, " (nullable = ",)?;
                format_bool(f, field.nullable)?;
                writeln!(f, ")")?;
                format_tree_string(f, field.data_type.as_ref(), max_level, level + 1)?;
            }
            Ok(())
        }
        Kind::Map(map) => {
            format_prefix(f, level)?;
            write!(f, "key: ")?;
            format_type_name(f, map.key_type.as_deref())?;
            writeln!(f)?;
            format_tree_string(f, map.key_type.as_deref(), max_level, level + 1)?;

            format_prefix(f, level)?;
            write!(f, "value: ")?;
            format_type_name(f, map.value_type.as_deref())?;
            write!(f, " (valueContainsNull = ",)?;
            format_bool(f, map.value_contains_null)?;
            writeln!(f, ")")?;
            format_tree_string(f, map.value_type.as_deref(), max_level, level + 1)
        }
        _ => Ok(()),
    }
}

struct TreeString<'a> {
    data_type: Option<&'a sc::DataType>,
    max_level: Option<i32>,
}

impl Display for TreeString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "root")?;
        format_tree_string(f, self.data_type, self.max_level, 1)
    }
}

pub(crate) fn to_tree_string(data_type: &sc::DataType, max_level: Option<i32>) -> String {
    TreeString {
        data_type: Some(data_type),
        max_level,
    }
    .to_string()
}
