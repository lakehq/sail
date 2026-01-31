use std::fmt;
use std::fmt::Display;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use sail_common::string::escape_meta_characters;

use crate::error::{SparkError, SparkResult};
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

fn format_year_month_interval_field_name(field: i32) -> &'static str {
    match field {
        0 => "YEAR",
        1 => "MONTH",
        _ => "?",
    }
}

fn format_day_time_interval_field_name(field: i32) -> &'static str {
    match field {
        0 => "DAY",
        1 => "HOUR",
        2 => "MINUTE",
        3 => "SECOND",
        _ => "?",
    }
}

fn format_ddl_type(buf: &mut String, data_type: Option<&sc::DataType>) -> SparkResult<()> {
    let kind = match data_type {
        Some(sc::DataType { kind: Some(x) }) => x,
        _ => return Err(SparkError::invalid("missing data type")),
    };
    match kind {
        Kind::Null(_) => buf.push_str("NULL"),
        Kind::Binary(_) => buf.push_str("BINARY"),
        Kind::Boolean(_) => buf.push_str("BOOLEAN"),
        Kind::Byte(_) => buf.push_str("TINYINT"),
        Kind::Short(_) => buf.push_str("SHORT"),
        Kind::Integer(_) => buf.push_str("INT"),
        Kind::Long(_) => buf.push_str("BIGINT"),
        Kind::Float(_) => buf.push_str("FLOAT"),
        Kind::Double(_) => buf.push_str("DOUBLE"),
        Kind::Decimal(decimal) => {
            buf.push_str("DECIMAL(");
            if let Some(precision) = decimal.precision {
                buf.push_str(&precision.to_string());
            } else {
                buf.push_str("?");
            }
            buf.push_str(",");
            if let Some(scale) = decimal.scale {
                buf.push_str(&scale.to_string());
            } else {
                buf.push_str("?");
            }
            buf.push_str(")");
        }
        Kind::String(_) => buf.push_str("STRING"),
        Kind::Char(char) => {
            buf.push_str("CHAR(");
            buf.push_str(&char.length.to_string());
            buf.push_str(")");
        }
        Kind::VarChar(var_char) => {
            buf.push_str("VARCHAR(");
            buf.push_str(&var_char.length.to_string());
            buf.push_str(")");
        }
        Kind::Date(_) => buf.push_str("DATE"),
        Kind::Timestamp(_) => buf.push_str("TIMESTAMP"),
        Kind::TimestampNtz(_) => buf.push_str("TIMESTAMP_NTZ"),
        Kind::CalendarInterval(_) => buf.push_str("INTERVAL"),
        Kind::YearMonthInterval(interval) => match ( interval.start_field, interval.end_field,) {
            (Some(start), Some(end)) => {
                buf.push_str("INTERVAL ");
                buf.push_str(format_year_month_interval_field_name(start));
                buf.push_str(" TO ");
                buf.push_str(format_year_month_interval_field_name(end));
            }
            (Some(start), None) => {
                buf.push_str("INTERVAL ");
                buf.push_str(format_year_month_interval_field_name(start));
            }
            (None, Some(end)) => {
                buf.push_str("INTERVAL ? TO ");
                buf.push_str(format_year_month_interval_field_name(end));
            }
            (None, None) => {
                buf.push_str("INTERVAL");
            }
        }
        Kind::DayTimeInterval(interval) => match (interval.start_field, interval.end_field) {
            (Some(start), Some(end)) => {
                buf.push_str("INTERVAL ");
                buf.push_str(format_day_time_interval_field_name(start));
                buf.push_str(" TO ");
                buf.push_str(format_day_time_interval_field_name(end));
            }
            (Some(start), None) => {
                buf.push_str("INTERVAL ");
                buf.push_str(format_day_time_interval_field_name(start));
            }
            (None, Some(end)) => {
                buf.push_str("INTERVAL ? TO ");
                buf.push_str(format_day_time_interval_field_name(end));
            }
            (None, None) => {
                buf.push_str("INTERVAL");
            }
        }
        Kind::Array(array) => {
            buf.push_str("ARRAY<");
            format_ddl_type(buf, array.element_type.as_deref())?;
            buf.push('>');
        }
        Kind::Struct(r#struct) => {
            buf.push_str("STRUCT<");
            for (i, field) in r#struct.fields.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&field.name);
                buf.push_str(": ");
                format_ddl_type(buf, field.data_type.as_ref())?;
                if !field.nullable {
                    buf.push_str(" NOT NULL");
                }
            }
            buf.push('>');
        }
        Kind::Map(map) => {
            buf.push_str("MAP<");
            format_ddl_type(buf, map.key_type.as_deref())?;
            buf.push_str(", ");
            format_ddl_type(buf, map.value_type.as_deref())?;
            buf.push('>');
        }
        Kind::Variant(_) => buf.push_str("VARIANT"),
        Kind::Udt(udt) => {
            if udt.jvm_class.is_none() && udt.python_class.is_some() {
                buf.push_str("PYTHONUSERDEFINED");
            } else {
                buf.push_str("USERDEFINED");
            }
        }
        Kind::Geometry(_) => buf.push_str("GEOMETRY"),
        Kind::Geography(_) => buf.push_str("GEOGRAPHY"),
        Kind::Unparsed(unparsed) => {
            // For unparsed types, output the original string
            buf.push_str(&unparsed.data_type_string);
        }
        Kind::Time(time) => match time.precision {
            Some(p) => {
                buf.push_str("TIME(");
                buf.push_str(&p.to_string());
                buf.push_str(")");
            }
            None => buf.push_str("TIME"),
        },
    }
    Ok(())
}

pub(crate) fn to_ddl_string(data_type: &sc::DataType) -> SparkResult<String> {
    let mut buf = String::new();
    let kind = match data_type {
        sc::DataType { kind: Some(x) } => x,
        _ => return Ok(buf),
    };
    match kind {
        Kind::Struct(r#struct) => {
            for (i, field) in r#struct.fields.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&field.name);
                buf.push(' ');
                format_ddl_type(&mut buf, field.data_type.as_ref())?;
                if !field.nullable {
                    buf.push_str(" NOT NULL");
                }
            }
        }
        _ => format_ddl_type(&mut buf, Some(data_type))?,
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use crate::proto::data_type_json::parse_spark_json_data_type;

    use super::*;

    fn make_schema(type_json: &str) -> sc::DataType {
        let json = format!(
            r#"{{
                "fields": [
                    {{
                        "metadata": {{}},
                        "name": "col",
                        "nullable": true,
                        "type": {type_json}
                    }}
                ],
                "type": "struct"
            }}"#
        );
        parse_spark_json_data_type(&json).expect("failed to parse schema")
    }

    #[test]
    fn test_to_ddl_null() {
        let schema = make_schema(r#""null""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col NULL");
    }

    #[test]
    fn test_to_ddl_binary() {
        let schema = make_schema(r#""binary""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col BINARY");
    }

    #[test]
    fn test_to_ddl_boolean() {
        let schema = make_schema(r#""boolean""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col BOOLEAN");
    }

    #[test]
    fn test_to_ddl_byte() {
        let schema = make_schema(r#""byte""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col TINYINT");
    }

    #[test]
    fn test_to_ddl_short() {
        let schema = make_schema(r#""short""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col SHORT");
    }

    #[test]
    fn test_to_ddl_integer() {
        let schema = make_schema(r#""integer""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col INT");
    }

    #[test]
    fn test_to_ddl_long() {
        let schema = make_schema(r#""long""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col BIGINT");
    }

    #[test]
    fn test_to_ddl_float() {
        let schema = make_schema(r#""float""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col FLOAT");
    }

    #[test]
    fn test_to_ddl_double() {
        let schema = make_schema(r#""double""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col DOUBLE");
    }

    #[test]
    fn test_to_ddl_decimal() {
        let schema = make_schema(r#""decimal(10, 2)""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col DECIMAL(10,2)");
    }

    #[test]
    fn test_to_ddl_string() {
        let schema = make_schema(r#""string""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col STRING");
    }

    #[test]
    fn test_to_ddl_char() {
        let schema = make_schema(r#""char(10)""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col CHAR(10)");
    }

    #[test]
    fn test_to_ddl_varchar() {
        let schema = make_schema(r#""varchar(20)""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col VARCHAR(20)");
    }

    #[test]
    fn test_to_ddl_date() {
        let schema = make_schema(r#""date""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col DATE");
    }

    #[test]
    fn test_to_ddl_timestamp() {
        let schema = make_schema(r#""timestamp""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col TIMESTAMP");
    }

    #[test]
    fn test_to_ddl_timestamp_ntz() {
        let schema = make_schema(r#""timestamp_ntz""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col TIMESTAMP_NTZ");
    }

    #[test]
    fn test_to_ddl_calendar_interval() {
        let schema = make_schema(r#""interval""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col INTERVAL");
    }

    #[test]
    fn test_to_ddl_year_month_interval() {
        let schema = make_schema(r#""interval year to month""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col INTERVAL YEAR TO MONTH");
    }

    #[test]
    fn test_to_ddl_day_time_interval() {
        let schema = make_schema(r#""interval day to second""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col INTERVAL DAY TO SECOND");
    }

    #[test]
    fn test_to_ddl_time() {
        let schema = make_schema(r#""time""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col TIME");
    }

    #[test]
    fn test_to_ddl_time_with_precision() {
        let schema = make_schema(r#""time(3)""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col TIME(3)");
    }

    #[test]
    fn test_to_ddl_variant() {
        let schema = make_schema(r#""variant""#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col VARIANT");
    }

    #[test]
    fn test_to_ddl_array() {
        let schema = make_schema(r#"{"type": "array", "elementType": "string", "containsNull": true}"#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col ARRAY<STRING>");
    }

    #[test]
    fn test_to_ddl_map() {
        let schema = make_schema(r#"{"type": "map", "keyType": "string", "valueType": "integer", "valueContainsNull": true}"#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col MAP<STRING, INT>");
    }

    #[test]
    fn test_to_ddl_struct() {
        let schema = make_schema(r#"{"type": "struct", "fields": [{"name": "f1", "type": "string", "nullable": true}]}"#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col STRUCT<f1: STRING>");
    }

    #[test]
    fn test_to_ddl_udt_jvm() {
        let schema = make_schema(r#"{"type": "udt", "class": "SomeClass", "sqlType": "integer"}"#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col USERDEFINED");
    }

    #[test]
    fn test_to_ddl_udt_python() {
        let schema = make_schema(r#"{"type": "udt", "pyClass": "app.udf", "sqlType": "integer"}"#);
        assert_eq!(to_ddl_string(&schema).unwrap(), "col PYTHONUSERDEFINED");
    }

    #[test]
    fn test_to_ddl_not_null() {
        let json = r#"{
            "fields": [
                {"metadata": {}, "name": "col", "nullable": false, "type": "boolean"}
            ],
            "type": "struct"
        }"#;
        let schema = parse_spark_json_data_type(json).unwrap();
        assert_eq!(to_ddl_string(&schema).unwrap(), "col BOOLEAN NOT NULL");
    }
}