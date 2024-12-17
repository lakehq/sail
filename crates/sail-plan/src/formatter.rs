use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use sail_common::object::DynObject;
use sail_common::{impl_dyn_object_traits, spec};

use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;

/// Utilities to format various data structures in the plan specification.
pub trait PlanFormatter: DynObject + Debug + Send + Sync {
    /// Returns a human-readable simple string for the data type.
    fn data_type_to_simple_string(&self, data_type: &spec::DataType) -> PlanResult<String>;

    /// Returns a human-readable string for the literal.
    fn literal_to_string(&self, literal: &spec::Literal) -> PlanResult<String>;

    /// Returns a human-readable string for the function call.
    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> PlanResult<String>;
}

impl_dyn_object_traits!(PlanFormatter);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct DefaultPlanFormatter;

impl DefaultPlanFormatter {
    fn interval_field_type_to_simple_string(field: spec::IntervalFieldType) -> &'static str {
        match field {
            spec::IntervalFieldType::Year => "year",
            spec::IntervalFieldType::Month => "month",
            spec::IntervalFieldType::Day => "day",
            spec::IntervalFieldType::Hour => "hour",
            spec::IntervalFieldType::Minute => "minute",
            spec::IntervalFieldType::Second => "second",
        }
    }

    fn time_unit_to_simple_string(field: spec::TimeUnit) -> &'static str {
        match field {
            spec::TimeUnit::Second => "second",
            spec::TimeUnit::Millisecond => "millisecond",
            spec::TimeUnit::Microsecond => "microsecond",
            spec::TimeUnit::Nanosecond => "nanosecond",
        }
    }
}

impl PlanFormatter for DefaultPlanFormatter {
    fn data_type_to_simple_string(&self, data_type: &spec::DataType) -> PlanResult<String> {
        use spec::DataType;
        match data_type {
            DataType::Null => Ok("void".to_string()),
            DataType::Binary
            | DataType::FixedSizeBinary { size: _ }
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::ConfiguredBinary => Ok("binary".to_string()),
            DataType::Boolean => Ok("boolean".to_string()),
            DataType::Int8 => Ok("tinyint".to_string()),
            DataType::Int16 => Ok("smallint".to_string()),
            DataType::Int32 => Ok("int".to_string()),
            DataType::Int64 => Ok("bigint".to_string()),
            DataType::UInt8 => Ok("unsigned tinyint".to_string()),
            DataType::UInt16 => Ok("unsigned smallint".to_string()),
            DataType::UInt32 => Ok("unsigned int".to_string()),
            DataType::UInt64 => Ok("unsigned bigint".to_string()),
            DataType::Float16 => Ok("half_float".to_string()),
            DataType::Float32 => Ok("float".to_string()),
            DataType::Float64 => Ok("double".to_string()),
            DataType::Decimal128 { precision, scale }
            | DataType::Decimal256 { precision, scale } => {
                Ok(format!("decimal({precision},{scale})"))
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Configured,
            } => Ok("string".to_string()),
            DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::VarChar { length },
            } => Ok(format!("varchar({length})")),
            DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Char { length },
            } => Ok(format!("char({length})")),
            DataType::Date32 => Ok("date".to_string()),
            DataType::Date64 => Ok("date64".to_string()),
            DataType::Time32 { time_unit } => Ok(format!(
                "time32({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Time64 { time_unit } => Ok(format!(
                "time64({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Duration { time_unit } => Ok(format!(
                "duration({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Timestamp {
                time_unit: _,
                timezone_info: spec::TimeZoneInfo::Configured,
            }
            | DataType::Timestamp {
                time_unit: _,
                timezone_info: spec::TimeZoneInfo::LocalTimeZone,
            }
            | DataType::Timestamp {
                time_unit: _,
                timezone_info: spec::TimeZoneInfo::TimeZone { timezone: _ },
            } => Ok("timestamp".to_string()),
            DataType::Timestamp {
                time_unit: _,
                timezone_info: spec::TimeZoneInfo::NoTimeZone,
            } => Ok("timestamp_ntz".to_string()),
            DataType::Interval {
                interval_unit: spec::IntervalUnit::MonthDayNano,
                start_field: _,
                end_field: _,
            } => Ok("interval".to_string()),
            DataType::Interval {
                interval_unit: spec::IntervalUnit::YearMonth,
                start_field,
                end_field,
            } => {
                let (start_field, end_field) = match (*start_field, *end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(_)) => {
                        return Err(PlanError::invalid(
                            "year-month interval with end field and no start field",
                        ))
                    }
                    (None, None) => (
                        spec::IntervalFieldType::Year,
                        spec::IntervalFieldType::Month,
                    ),
                };

                match start_field.cmp(&end_field) {
                    Ordering::Less => Ok(format!(
                        "interval {} to {}",
                        Self::interval_field_type_to_simple_string(start_field),
                        Self::interval_field_type_to_simple_string(end_field),
                    )),
                    Ordering::Equal => Ok(format!(
                        "interval {}",
                        Self::interval_field_type_to_simple_string(start_field)
                    )),
                    Ordering::Greater => Err(PlanError::invalid(
                        "year-month interval with invalid start and end field order",
                    )),
                }
            }
            DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => {
                let (start_field, end_field) = match (*start_field, *end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(_)) => {
                        return Err(PlanError::invalid(
                            "day-time interval with end field and no start field",
                        ))
                    }
                    (None, None) => (
                        spec::IntervalFieldType::Day,
                        spec::IntervalFieldType::Second,
                    ),
                };

                match start_field.cmp(&end_field) {
                    Ordering::Less => Ok(format!(
                        "interval {} to {}",
                        Self::interval_field_type_to_simple_string(start_field),
                        Self::interval_field_type_to_simple_string(end_field),
                    )),
                    Ordering::Equal => Ok(format!(
                        "interval {}",
                        Self::interval_field_type_to_simple_string(start_field)
                    )),
                    Ordering::Greater => Err(PlanError::invalid(
                        "day-time interval with invalid start and end field order",
                    )),
                }
            }
            DataType::List {
                data_type,
                nullable: _,
            }
            | DataType::FixedSizeList {
                data_type,
                nullable: _,
                length: _,
            }
            | DataType::LargeList {
                data_type,
                nullable: _,
            } => Ok(format!(
                "array<{}>",
                self.data_type_to_simple_string(data_type)?
            )),
            DataType::Struct { fields } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(format!(
                            "{}:{}",
                            field.name,
                            self.data_type_to_simple_string(&field.data_type)?
                        ))
                    })
                    .collect::<PlanResult<Vec<String>>>()?;
                Ok(format!("struct<{}>", fields.join(",")))
            }
            DataType::Map {
                key_type,
                value_type,
                value_type_nullable: _,
                keys_sorted: _,
            } => Ok(format!(
                "map<{},{}>",
                self.data_type_to_simple_string(key_type.as_ref())?,
                self.data_type_to_simple_string(value_type.as_ref())?
            )),
            DataType::UserDefined { sql_type, .. } => {
                self.data_type_to_simple_string(sql_type.as_ref())
            }
            DataType::Union {
                union_fields: _,
                union_mode: _,
            } => {
                // TODO: Add union_fields and union_mode
                Ok("union".to_string())
            }
            DataType::Dictionary {
                key_type,
                value_type,
            } => Ok(format!(
                "dictionary<{},{}>",
                self.data_type_to_simple_string(key_type)?,
                self.data_type_to_simple_string(value_type)?
            )),
        }
    }

    fn literal_to_string(&self, literal: &spec::Literal) -> PlanResult<String> {
        use spec::Literal;

        let literal_list_to_string = |name: &str, values: &Vec<Literal>| -> PlanResult<String> {
            let values = values
                .iter()
                .map(|x| self.literal_to_string(x))
                .collect::<PlanResult<Vec<String>>>()?;
            Ok(format!("{name}({})", values.join(", ")))
        };

        match literal {
            Literal::Null => Ok("NULL".to_string()),
            Literal::Binary(x) => Ok(BinaryDisplay(x).to_string()),
            Literal::Boolean(x) => Ok(format!("{x}")),
            Literal::Byte(x) => Ok(format!("{x}")),
            Literal::Short(x) => Ok(format!("{x}")),
            Literal::Integer(x) => Ok(format!("{x}")),
            Literal::Long(x) => Ok(format!("{x}")),
            Literal::Float(x) => {
                let mut buffer = ryu::Buffer::new();
                Ok(buffer.format(*x).to_string())
            }
            Literal::Double(x) => {
                let mut buffer = ryu::Buffer::new();
                Ok(buffer.format(*x).to_string())
            }
            Literal::Decimal128(x) => Ok(Decimal128Display(x).to_string()),
            Literal::Decimal256(x) => Ok(Decimal256Display(x).to_string()),
            Literal::String(x) => Ok(x.clone()),
            Literal::Date { days } => {
                let date = chrono::NaiveDateTime::UNIX_EPOCH + chrono::Duration::days(*days as i64);
                Ok(format!("DATE '{}'", date.format("%Y-%m-%d")))
            }
            Literal::TimestampMicrosecond {
                microseconds,
                timezone: _timezone,
            } => {
                // TODO: Integrate timezone
                let date_time = chrono::NaiveDateTime::UNIX_EPOCH
                    + chrono::Duration::microseconds(*microseconds);
                Ok(format!(
                    "TIMESTAMP '{}'",
                    date_time.format("%Y-%m-%d %H:%M:%S.%6f")
                ))
            }
            Literal::TimestampNtz { microseconds } => {
                let date_time = chrono::NaiveDateTime::UNIX_EPOCH
                    + chrono::Duration::microseconds(*microseconds);
                Ok(format!(
                    "TIMESTAMP_NTZ '{}'",
                    date_time.format("%Y-%m-%d %H:%M:%S.%6f")
                ))
            }
            Literal::CalendarInterval {
                months,
                days,
                microseconds,
            } => {
                let years = *months / 12;
                let months = *months % 12;
                let days = *days;
                let hours = *microseconds / 3_600_000_000;
                let minutes = (*microseconds % 3_600_000_000) / 60_000_000;
                let seconds = (*microseconds % 60_000_000) / 1_000_000;
                let milliseconds = (*microseconds % 1_000_000) / 1_000;
                let microseconds = *microseconds % 1_000;
                Ok(format!(
                    "INTERVAL {years} YEAR {months} MONTH {days} DAY {hours} HOUR {minutes} MINUTE {seconds} SECOND {milliseconds} MILLISECOND {microseconds} MICROSECOND"
                ))
            }
            Literal::YearMonthInterval { months } => {
                if *months < 0 {
                    let years = *months / -12;
                    let months = -(*months % -12);
                    Ok(format!("INTERVAL '-{years}-{months}' YEAR TO MONTH"))
                } else {
                    let years = *months / 12;
                    let months = *months % 12;
                    Ok(format!("INTERVAL '{years}-{months}' YEAR TO MONTH"))
                }
            }
            Literal::DayTimeInterval { microseconds } => {
                if *microseconds < 0 {
                    let days = *microseconds / -86_400_000_000;
                    let hours = (*microseconds % -86_400_000_000) / -3_600_000_000;
                    let minutes = (*microseconds % -3_600_000_000) / -60_000_000;
                    let seconds = (*microseconds % -60_000_000) / -1_000_000;
                    let microseconds = -(*microseconds % -1_000_000);
                    Ok(format!(
                        "INTERVAL '-{days} {hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}' DAY TO SECOND"
                    ))
                } else {
                    let days = *microseconds / 86_400_000_000;
                    let hours = (*microseconds % 86_400_000_000) / 3_600_000_000;
                    let minutes = (*microseconds % 3_600_000_000) / 60_000_000;
                    let seconds = (*microseconds % 60_000_000) / 1_000_000;
                    let microseconds = *microseconds % 1_000_000;
                    Ok(format!(
                        "INTERVAL '{days} {hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}' DAY TO SECOND",
                    ))
                }
            }
            Literal::Array { elements, .. } => literal_list_to_string("array", elements),
            Literal::Map { keys, values, .. } => {
                let k = literal_list_to_string("array", keys)?;
                let v = literal_list_to_string("array", values)?;
                Ok(format!("map({k}, {v})"))
            }
            Literal::Struct {
                struct_type,
                elements,
            } => {
                let fields = match struct_type {
                    spec::DataType::Struct { fields } => fields,
                    _ => return Err(PlanError::invalid("struct type")),
                };
                let fields = fields
                    .iter()
                    .zip(elements.iter())
                    .map(|(field, value)| {
                        Ok(format!(
                            "{} AS {}",
                            self.literal_to_string(value)?,
                            field.name
                        ))
                    })
                    .collect::<PlanResult<Vec<String>>>()?;
                Ok(format!("struct({})", fields.join(", ")))
            }
        }
    }

    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> PlanResult<String> {
        match name.to_lowercase().as_str() {
            "!" | "~" => Ok(format!("({name} {})", arguments.one()?)),
            "+" | "-" => {
                if arguments.len() < 2 {
                    Ok(format!("({name} {})", arguments.one()?))
                } else {
                    let (left, right) = arguments.two()?;
                    Ok(format!("({left} {name} {right})"))
                }
            }
            "==" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} = {right})"))
            }
            "&" | "^" | "|" | "*" | "/" | "%" | "!=" | "<" | "<=" | "<=>" | "=" | ">" | ">=" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} {name} {right})"))
            }
            "and" | "or" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} {} {right})", name.to_uppercase()))
            }
            "not" => Ok(format!("(NOT {})", arguments.one()?)),
            "isnull" => Ok(format!("({} IS NULL)", arguments.one()?)),
            "isnotnull" => Ok(format!("({} IS NOT NULL)", arguments.one()?)),
            "in" => {
                let (value, list) = arguments.at_least_one()?;
                Ok(format!("({value} IN ({}))", list.join(", ")))
            }
            "case" | "when" => {
                let mut result = String::from("CASE");
                let mut i = 0;
                while i < arguments.len() {
                    if i + 1 < arguments.len() {
                        result.push_str(&format!(
                            " WHEN {} THEN {}",
                            arguments[i],
                            arguments[i + 1]
                        ));
                        i += 2;
                    } else {
                        result.push_str(&format!(" ELSE {}", arguments[i]));
                        break;
                    }
                }
                result.push_str(" END");
                Ok(result)
            }
            "dateadd" => {
                let arguments = arguments.join(", ");
                Ok(format!("date_add({arguments})"))
            }
            "sum" => {
                let mut args = arguments.join(", ");
                if is_distinct {
                    args = format!("DISTINCT {args}");
                }
                Ok(format!("{name}({args})"))
            }
            "first" | "last" => {
                let name = name.to_lowercase();
                let arg = arguments[0];
                Ok(format!("{name}({arg})"))
            }
            "any_value" | "first_value" | "last_value" => {
                let arg = arguments[0];
                Ok(format!("{name}({arg})"))
            }
            "substr" | "substring" => {
                let args = if arguments.len() == 2 {
                    let args = arguments.join(", ");
                    format!("{args}, 2147483647")
                } else {
                    arguments.join(", ")
                };
                Ok(format!("{name}({args})"))
            }
            // This case is only reached when both conditions are true:
            //   1. The `explode` operation is `ExplodeKind::ExplodeOuter`
            //   2. The data type being exploded is `ExplodeDataType::List`
            // In this specific scenario, we always use "col" as the column name.
            "explode_outer" => Ok("col".to_string()),
            "current_schema" => Ok("current_database()".to_string()),
            "acos" | "acosh" | "asin" | "asinh" | "atan" | "atan2" | "atanh" | "cbrt" | "ceil"
            | "exp" | "floor" | "log10" | "regexp" | "regexp_like" | "signum" | "sqrt" | "cos"
            | "cosh" | "cot" | "degrees" | "power" | "radians" | "sin" | "sinh" | "tan"
            | "tanh" | "pi" | "expm1" | "hypot" | "log1p" => {
                let name = name.to_uppercase();
                let arguments = arguments.join(", ");
                Ok(format!("{name}({arguments})"))
            }
            _ => {
                let arguments = arguments.join(", ");
                Ok(format!("{name}({arguments})"))
            }
        }
    }
}

struct BinaryDisplay<'a>(pub &'a Vec<u8>);

impl Display for BinaryDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "X'")?;
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        write!(f, "'")
    }
}

struct Decimal128Display<'a>(pub &'a spec::Decimal128);
impl Display for Decimal128Display<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format_decimal(&self.0.value, self.0.scale, f)
    }
}

struct Decimal256Display<'a>(pub &'a spec::Decimal256);
impl Display for Decimal256Display<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format_decimal(&self.0.value, self.0.scale, f)
    }
}

fn format_decimal<T: Display>(value: &T, scale: i8, f: &mut Formatter<'_>) -> std::fmt::Result {
    let s = format!("{value}");
    let start = if s.starts_with('-') {
        write!(f, "-")?;
        1
    } else {
        0
    };
    let scale = if scale > 0 { scale as usize } else { 0 };
    if scale == 0 {
        write!(f, "{}", &s[start..])
    } else if start + scale < s.len() {
        let d = s.len() - scale;
        write!(f, "{}.{}", &s[start..d], &s[d..])
    } else {
        write!(f, "0.{:0>width$}", &s[start..], width = scale)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::i256;
    use sail_common::spec::Literal;

    use super::*;

    #[test]
    fn test_literal_to_string() -> PlanResult<()> {
        let formatter = DefaultPlanFormatter;
        let to_string = |literal| formatter.literal_to_string(&literal);

        assert_eq!(to_string(Literal::Null)?, "NULL");
        assert_eq!(
            to_string(Literal::Binary(vec![16, 0x20, 0xff]))?,
            "X'1020FF'",
        );
        assert_eq!(to_string(Literal::Boolean(true))?, "true");
        assert_eq!(to_string(Literal::Byte(10))?, "10");
        assert_eq!(to_string(Literal::Short(-20))?, "-20");
        assert_eq!(to_string(Literal::Integer(30))?, "30");
        assert_eq!(to_string(Literal::Long(-40))?, "-40");
        assert_eq!(to_string(Literal::Float(1.0))?, "1.0");
        assert_eq!(to_string(Literal::Double(-0.1))?, "-0.1");
        assert_eq!(
            to_string(Literal::Decimal128(spec::Decimal128 {
                value: 123,
                precision: 3,
                scale: 0,
            }))?,
            "123",
        );
        assert_eq!(
            to_string(Literal::Decimal128(spec::Decimal128 {
                value: -123,
                precision: 3,
                scale: 0,
            }))?,
            "-123",
        );
        assert_eq!(
            to_string(Literal::Decimal128(spec::Decimal128 {
                value: 123,
                precision: 3,
                scale: 2,
            }))?,
            "1.23",
        );
        assert_eq!(
            to_string(Literal::Decimal128(spec::Decimal128 {
                value: 123,
                precision: 3,
                scale: 5,
            }))?,
            "0.00123",
        );
        assert_eq!(
            to_string(Literal::Decimal128(spec::Decimal128 {
                value: 12300,
                precision: 3,
                scale: -2,
            }))?,
            "12300",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from(123),
                precision: 3,
                scale: 0,
            }))?,
            "123",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from(-123),
                precision: 3,
                scale: 0,
            }))?,
            "-123",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from(123),
                precision: 3,
                scale: 2,
            }))?,
            "1.23",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from(123),
                precision: 3,
                scale: 5,
            }))?,
            "0.00123",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from(12300),
                precision: 3,
                scale: -2,
            }))?,
            "12300",
        );
        assert_eq!(
            to_string(Literal::Decimal256(spec::Decimal256 {
                value: i256::from_string("120000000000000000000000000000000000000000").unwrap(),
                precision: 42,
                scale: 5,
            }))?,
            "1200000000000000000000000000000000000.00000",
        );
        assert_eq!(to_string(Literal::String("abc".to_string()))?, "abc");
        assert_eq!(to_string(Literal::Date { days: 10 })?, "DATE '1970-01-11'");
        assert_eq!(to_string(Literal::Date { days: -5 })?, "DATE '1969-12-27'");
        assert_eq!(
            to_string(Literal::TimestampMicrosecond {
                microseconds: 123_000_000,
                timezone: None,
            })?,
            "TIMESTAMP '1970-01-01 00:02:03.000000'",
        );
        assert_eq!(
            to_string(Literal::TimestampNtz { microseconds: -1 })?,
            "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'",
        );
        assert_eq!(
            to_string(Literal::CalendarInterval {
                months: 15,
                days: -20,
                microseconds: 123_456_789,
            })?,
            "INTERVAL 1 YEAR 3 MONTH -20 DAY 0 HOUR 2 MINUTE 3 SECOND 456 MILLISECOND 789 MICROSECOND",
        );
        assert_eq!(
            to_string(Literal::CalendarInterval {
                months: -15,
                days: 10,
                microseconds: -1001,
            })?,
            "INTERVAL -1 YEAR -3 MONTH 10 DAY 0 HOUR 0 MINUTE 0 SECOND -1 MILLISECOND -1 MICROSECOND",
        );
        assert_eq!(
            to_string(Literal::YearMonthInterval { months: 15 })?,
            "INTERVAL '1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(Literal::YearMonthInterval { months: -15 })?,
            "INTERVAL '-1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(Literal::DayTimeInterval {
                microseconds: 123_456_789,
            })?,
            "INTERVAL '0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(Literal::DayTimeInterval {
                microseconds: -123_456_789,
            })?,
            "INTERVAL '-0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(Literal::Array {
                elements: vec![Literal::Integer(1), Literal::Integer(-2)],
                element_type: spec::DataType::Int32,
            })?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(Literal::Map {
                key_type: spec::DataType::Utf8,
                value_type: spec::DataType::Float64,
                keys: vec![
                    Literal::String("a".to_string()),
                    Literal::String("b".to_string()),
                ],
                values: vec![Literal::Double(1.0), Literal::Double(2.0)],
            })?,
            "map(array(a, b), array(1.0, 2.0))",
        );
        assert_eq!(
            to_string(Literal::Struct {
                struct_type: spec::DataType::Struct {
                    fields: spec::Fields::from(vec![
                        spec::Field {
                            name: "foo".to_string(),
                            data_type: spec::DataType::List {
                                data_type: Box::new(spec::DataType::Int64),
                                nullable: true,
                            },
                            nullable: false,
                            metadata: vec![],
                        },
                        spec::Field {
                            name: "bar".to_string(),
                            data_type: spec::DataType::Struct {
                                fields: spec::Fields::from(vec![spec::Field {
                                    name: "baz".to_string(),
                                    data_type: spec::DataType::Utf8,
                                    nullable: false,
                                    metadata: vec![],
                                }])
                            },
                            nullable: true,
                            metadata: vec![],
                        },
                    ])
                },
                elements: vec![
                    Literal::Array {
                        elements: vec![Literal::Long(1), Literal::Null],
                        element_type: spec::DataType::Int64,
                    },
                    Literal::Struct {
                        struct_type: spec::DataType::Struct {
                            fields: spec::Fields::from(vec![spec::Field {
                                name: "baz".to_string(),
                                data_type: spec::DataType::Utf8,
                                nullable: false,
                                metadata: vec![],
                            }])
                        },
                        elements: vec![Literal::String("hello".to_string())],
                    },
                ],
            })?,
            "struct(array(1, NULL) AS foo, struct(hello AS baz) AS bar)",
        );
        Ok(())
    }
}
