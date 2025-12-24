use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::{not_impl_err, plan_err, Result, ScalarValue};
use half::f16;
use sail_common_datafusion::formatter::{
    Date32Formatter, Date64Formatter, DurationMicrosecondFormatter, DurationMillisecondFormatter,
    DurationNanosecondFormatter, DurationSecondFormatter, IntervalDayTimeFormatter,
    IntervalMonthDayNanoFormatter, IntervalYearMonthFormatter, Time32MillisecondFormatter,
    Time32SecondFormatter, Time64MicrosecondFormatter, Time64NanosecondFormatter,
    TimestampMicrosecondFormatter, TimestampMillisecondFormatter, TimestampNanosecondFormatter,
    TimestampSecondFormatter,
};
use sail_common_datafusion::session::PlanFormatter;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SparkPlanFormatter;

impl SparkPlanFormatter {
    fn time_unit_to_simple_string(unit: &TimeUnit) -> &'static str {
        match unit {
            TimeUnit::Second => "second",
            TimeUnit::Millisecond => "millisecond",
            TimeUnit::Microsecond => "microsecond",
            TimeUnit::Nanosecond => "nanosecond",
        }
    }
}

impl PlanFormatter for SparkPlanFormatter {
    fn data_type_to_simple_string(&self, data_type: &DataType) -> Result<String> {
        match data_type {
            DataType::Null => Ok("void".to_string()),
            DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView => Ok("binary".to_string()),
            DataType::Boolean => Ok("boolean".to_string()),
            DataType::Int8 => Ok("tinyint".to_string()),
            DataType::Int16 => Ok("smallint".to_string()),
            DataType::Int32 => Ok("int".to_string()),
            DataType::Int64 => Ok("bigint".to_string()),
            DataType::UInt8 => Ok("unsigned tinyint".to_string()),
            DataType::UInt16 => Ok("unsigned smallint".to_string()),
            DataType::UInt32 => Ok("unsigned int".to_string()),
            DataType::UInt64 => Ok("unsigned bigint".to_string()),
            DataType::Float16 => Ok("half float".to_string()),
            DataType::Float32 => Ok("float".to_string()),
            DataType::Float64 => Ok("double".to_string()),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                Ok(format!("decimal({precision},{scale})"))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("string".to_string()),
            DataType::Date32 => Ok("date".to_string()),
            DataType::Date64 => Ok("date64".to_string()),
            DataType::Time32(time_unit) => Ok(format!(
                "time32({})",
                Self::time_unit_to_simple_string(time_unit)
            )),
            DataType::Time64(time_unit) => Ok(format!(
                "time64({})",
                Self::time_unit_to_simple_string(time_unit)
            )),
            DataType::Duration(TimeUnit::Microsecond) => Ok("interval day to second".to_string()),
            DataType::Duration(time_unit) => Ok(format!(
                "duration({})",
                Self::time_unit_to_simple_string(time_unit)
            )),
            DataType::Timestamp(_time_unit, Some(_)) => Ok("timestamp".to_string()),
            DataType::Timestamp(_time_unit, None) => Ok("timestamp_ntz".to_string()),
            DataType::Interval(IntervalUnit::MonthDayNano) => Ok("interval".to_string()),
            DataType::Interval(IntervalUnit::YearMonth) => Ok("interval year to month".to_string()),
            DataType::Interval(IntervalUnit::DayTime) => Ok("interval day to second".to_string()),
            DataType::List(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => Ok(format!(
                "array<{}>",
                self.data_type_to_simple_string(field.data_type())?
            )),
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(format!(
                            "{}:{}",
                            field.name(),
                            self.data_type_to_simple_string(field.data_type())?
                        ))
                    })
                    .collect::<Result<Vec<String>>>()?;
                Ok(format!("struct<{}>", fields.join(",")))
            }
            DataType::Map(field, _keys_sorted) => {
                let DataType::Struct(fields) = field.data_type() else {
                    return plan_err!("expected a struct field for map data type");
                };
                let [key_field, value_field] = fields.as_ref() else {
                    return plan_err!("expected a struct field with two fields for map data type");
                };
                Ok(format!(
                    "map<{},{}>",
                    self.data_type_to_simple_string(key_field.data_type())?,
                    self.data_type_to_simple_string(value_field.data_type())?
                ))
            }
            DataType::Union(_union_fields, _union_mode) => {
                // TODO: Add union_fields and union_mode
                Ok("union".to_string())
            }
            DataType::Dictionary(key_type, value_type) => Ok(format!(
                "dictionary<{},{}>",
                self.data_type_to_simple_string(key_type)?,
                self.data_type_to_simple_string(value_type)?
            )),
            DataType::RunEndEncoded(_, _)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _) => {
                not_impl_err!("data type: {data_type:?}")
            }
        }
    }

    fn literal_to_string(&self, literal: &ScalarValue, display_timezone: &str) -> Result<String> {
        let literal_list_to_string = |name: &str, values: Option<&dyn Array>| -> Result<String> {
            let Some(values) = values else {
                return Ok("NULL".to_string());
            };
            let values = (0..values.len())
                .map(|i| {
                    self.literal_to_string(
                        &ScalarValue::try_from_array(values, i)?,
                        display_timezone,
                    )
                })
                .collect::<Result<Vec<String>>>()?;
            Ok(format!("{name}({})", values.join(", ")))
        };

        match literal {
            ScalarValue::Null => Ok("NULL".to_string()),
            ScalarValue::Boolean(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int8(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int16(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int32(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int64(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt8(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt16(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt32(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt64(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float16(value) => match value {
                Some(value) => {
                    let value = f16::to_f32(*value);
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float32(value) => match value {
                Some(value) => {
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(*value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float64(value) => match value {
                Some(value) => {
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(*value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            // For timestamp values with no time zone, we use UTC as the time zone for formatting.
            ScalarValue::TimestampSecond(seconds, timezone) => match seconds {
                Some(seconds) => {
                    let (prefix, tz) = if timezone.is_some() {
                        ("TIMESTAMP", Some(display_timezone.parse::<Tz>()?))
                    } else {
                        ("TIMESTAMP_NTZ", None)
                    };
                    let timestamp = TimestampSecondFormatter(*seconds, tz.as_ref());
                    Ok(format!("{prefix} '{timestamp}'"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampMillisecond(milliseconds, timezone) => match milliseconds {
                Some(milliseconds) => {
                    let (prefix, tz) = if timezone.is_some() {
                        ("TIMESTAMP", Some(display_timezone.parse::<Tz>()?))
                    } else {
                        ("TIMESTAMP_NTZ", None)
                    };
                    let timestamp = TimestampMillisecondFormatter(*milliseconds, tz.as_ref());
                    Ok(format!("{prefix} '{timestamp}'"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampMicrosecond(microseconds, timezone) => match microseconds {
                Some(microseconds) => {
                    let (prefix, tz) = if timezone.is_some() {
                        ("TIMESTAMP", Some(display_timezone.parse::<Tz>()?))
                    } else {
                        ("TIMESTAMP_NTZ", None)
                    };
                    let timestamp = TimestampMicrosecondFormatter(*microseconds, tz.as_ref());
                    Ok(format!("{prefix} '{timestamp}'"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampNanosecond(nanoseconds, timezone) => match nanoseconds {
                Some(nanoseconds) => {
                    let (prefix, tz) = if timezone.is_some() {
                        ("TIMESTAMP", Some(display_timezone.parse::<Tz>()?))
                    } else {
                        ("TIMESTAMP_NTZ", None)
                    };
                    let timestamp = TimestampNanosecondFormatter(*nanoseconds, tz.as_ref());
                    Ok(format!("{prefix} '{timestamp}'"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Date32(days) => match days {
                Some(days) => Ok(format!("DATE '{}'", Date32Formatter(*days))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Date64(milliseconds) => match milliseconds {
                Some(milliseconds) => Ok(format!("DATE '{}'", Date64Formatter(*milliseconds))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time32Second(seconds) => match seconds {
                Some(seconds) => Ok(format!("TIME '{}'", Time32SecondFormatter(*seconds))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time32Millisecond(milliseconds) => match milliseconds {
                Some(milliseconds) => Ok(format!(
                    "TIME '{}'",
                    Time32MillisecondFormatter(*milliseconds)
                )),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time64Microsecond(microseconds) => match microseconds {
                Some(microseconds) => Ok(format!(
                    "TIME '{}'",
                    Time64MicrosecondFormatter(*microseconds)
                )),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time64Nanosecond(nanoseconds) => match nanoseconds {
                Some(nanoseconds) => Ok(format!(
                    "TIME '{}'",
                    Time64NanosecondFormatter(*nanoseconds)
                )),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationSecond(seconds) => match seconds {
                Some(seconds) => Ok(format!("{}", DurationSecondFormatter(*seconds))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationMillisecond(milliseconds) => match milliseconds {
                Some(milliseconds) => {
                    Ok(format!("{}", DurationMillisecondFormatter(*milliseconds)))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationMicrosecond(microseconds) => match microseconds {
                Some(microseconds) => {
                    Ok(format!("{}", DurationMicrosecondFormatter(*microseconds)))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationNanosecond(nanoseconds) => match nanoseconds {
                Some(nanoseconds) => Ok(format!("{}", DurationNanosecondFormatter(*nanoseconds))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalYearMonth(months) => match months {
                Some(months) => Ok(format!("{}", IntervalYearMonthFormatter(*months))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalDayTime(value) => match value {
                Some(value) => Ok(format!("{}", IntervalDayTimeFormatter(*value))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalMonthDayNano(value) => match value {
                Some(value) => Ok(format!("{}", IntervalMonthDayNanoFormatter(*value))),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Binary(value)
            | ScalarValue::FixedSizeBinary(_, value)
            | ScalarValue::LargeBinary(value)
            | ScalarValue::BinaryView(value) => match value {
                Some(value) => Ok(BinaryDisplay(value).to_string()),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Utf8(value)
            | ScalarValue::LargeUtf8(value)
            | ScalarValue::Utf8View(value) => match value {
                Some(value) => Ok(value.clone()),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::List(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::FixedSizeList(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::LargeList(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::Struct(values) => {
                let fields = values
                    .fields()
                    .iter()
                    .zip(values.columns())
                    .map(|(field, array)| {
                        if array.len() != 1 {
                            return plan_err!("expected struct literal with one value");
                        }
                        let value = ScalarValue::try_from_array(array, 0)?;
                        Ok(format!(
                            "{} AS {}",
                            self.literal_to_string(&value, display_timezone)?,
                            field.name()
                        ))
                    })
                    .collect::<Result<Vec<String>>>()?;
                Ok(format!("struct({})", fields.join(", ")))
            }
            ScalarValue::Union(value, _union_fields, _union_mode) => match value {
                Some((id, value)) => {
                    let value = self.literal_to_string(value, display_timezone)?;
                    Ok(format!("{id}:{value}"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Dictionary(_, value) => {
                let value = self.literal_to_string(value, display_timezone)?;
                Ok(format!("dictionary({value})"))
            }
            ScalarValue::Decimal32(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Decimal64(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Decimal128(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Decimal256(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Map(array) => match array.iter().collect::<Vec<_>>().one()? {
                Some(value) => {
                    let [keys, values] = value.columns() else {
                        return plan_err!("expected map literal with keys and values");
                    };
                    let k = literal_list_to_string("array", Some(keys))?;
                    let v = literal_list_to_string("array", Some(values))?;
                    Ok(format!("map({k}, {v})"))
                }
                None => Ok("NULL".to_string()),
            },
        }
    }

    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> Result<String> {
        match name.to_lowercase().as_str() {
            "!" | "not" => Ok(format!("(NOT {})", arguments.one()?)),
            "~" => Ok(format!("{name}{}", arguments.one()?)),
            "+" | "-" => {
                if arguments.len() < 2 {
                    Ok(format!("({name} {})", arguments.one()?))
                } else {
                    let (left, right) = arguments.two()?;
                    Ok(format!("({left} {name} {right})"))
                }
            }
            "positive" => Ok(format!("(+ {})", arguments.one()?)),
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
            "timestamp" | "date" => Ok(arguments.one()?.to_string()),
            "to_unix_timestamp" => {
                let mut argv = arguments.clone();
                if argv.len() == 1 {
                    argv.push("yyyy-MM-dd HH:mm:ss")
                }
                let args = argv.join(", ");
                Ok(format!("{name}({args})"))
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
                let (arg, _) = arguments.at_least_one()?;
                Ok(format!("{name}({arg})"))
            }
            "any_value" | "first_value" | "last_value" => {
                let (arg, _) = arguments.at_least_one()?;
                Ok(format!("{name}({arg})"))
            }
            "grouping" => {
                let name = name.to_lowercase();
                let arg = arguments.one()?;
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
            "ceil" | "floor" => {
                let name = if arguments.len() == 1 {
                    name.to_uppercase()
                } else {
                    name.to_string()
                };
                let args = arguments
                    .iter()
                    .map(|arg| {
                        if arg.starts_with("(- ") && arg.ends_with(")") {
                            format!("-{}", &arg[3..arg.len() - 1])
                        } else {
                            arg.to_string()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                Ok(format!("{name}({args})"))
            }
            "ceiling" | "typeof" => {
                let args = arguments
                    .iter()
                    .map(|arg| {
                        if arg.starts_with("(- ") && arg.ends_with(")") {
                            format!("-{}", &arg[3..arg.len() - 1])
                        } else {
                            arg.to_string()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                Ok(format!("{name}({args})"))
            }
            "cast" => {
                let (left, right) = arguments.two()?;
                Ok(format!(
                    "{}({left} AS {})",
                    name.to_uppercase(),
                    right.to_uppercase()
                ))
            }
            "listagg" | "string_agg" => {
                let (value, list) = arguments.at_least_one()?;
                let mut arg = value.to_string();
                if is_distinct {
                    arg = format!("DISTINCT {arg}");
                }
                let sep = *list.first().unwrap_or(&"NULL");
                Ok(format!("{name}({arg}, {sep})"))
            }
            "ltrim" | "rtrim" | "trim" => {
                let (value, list) = arguments.at_least_one()?;
                let what = if name == "ltrim" {
                    "LEADING"
                } else if name == "rtrim" {
                    "TRAILING"
                } else {
                    "BOTH"
                };
                let res_format = list
                    .first()
                    .map(|s| format!("TRIM({what} {value} FROM {s})"))
                    .unwrap_or(format!("{name}({value})"));

                Ok(res_format)
            }
            "round" | "bround" => {
                let (value, list) = arguments.at_least_one()?;
                let sep = *list.first().unwrap_or(&"0");
                Ok(format!("{name}({value}, {sep})"))
            }
            "startsWith" | "endsWith" => {
                let arguments = arguments.join(", ");
                Ok(format!("{}({arguments})", name.to_lowercase()))
            }
            "position" | "locate" => Ok(append_start_pos_if_arglen_eq(2, 1, name, arguments)),
            "regexp_instr" => Ok(append_start_pos_if_arglen_eq(2, 0, name, arguments)),
            "regexp_replace" => Ok(append_start_pos_if_arglen_eq(3, 1, name, arguments)),
            // When the data type being exploded is `ExplodeDataType::List`, use "col" as the column name.
            "explode" | "explode_outer" => Ok("col".to_string()),
            "stack" => Ok("col0".to_string()),
            "current_database" => Ok("current_schema()".to_string()),
            "acos" | "acosh" | "asin" | "asinh" | "atan" | "atan2" | "atanh" | "cbrt" | "exp"
            | "log" | "log10" | "log1p" | "log2" | "regexp" | "regexp_like" | "signum" | "sqrt"
            | "cos" | "cosh" | "cot" | "degrees" | "power" | "radians" | "sin" | "sinh" | "tan"
            | "tanh" | "pi" | "expm1" | "hypot" | "e" | "sec" | "csc" => {
                let name = name.to_uppercase();
                let arguments = arguments.join(", ");
                Ok(format!("{name}({arguments})"))
            }
            // FIXME: This is incorrect if the column name is `*`:
            //   ```
            //   SELECT count(`*`) FROM VALUES 1 AS t(`*`)
            //   ```
            "count" => {
                let arguments = arguments.join(", ");
                if is_distinct {
                    Ok(format!("{name}(DISTINCT {arguments})"))
                } else if arguments.as_str() == "*" {
                    Ok("count(1)".to_string())
                } else {
                    Ok(format!("{name}({arguments})"))
                }
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
            write!(f, "{b:02X}")?;
        }
        write!(f, "'")
    }
}

fn append_start_pos_if_arglen_eq(
    arglen: usize,
    start_pos: i8,
    name: &str,
    args: Vec<&str>,
) -> String {
    let start_pos_str = if args.len() == arglen {
        format!(", {start_pos}")
    } else {
        "".to_string()
    };
    let args = args.join(", ");
    format!("{name}({args}{start_pos_str})")
}

fn format_decimal(value: &str, scale: i8) -> String {
    let mut result = String::new();
    let start = if value.starts_with('-') {
        result.push('-');
        1
    } else {
        0
    };
    let scale = if scale > 0 { scale as usize } else { 0 };
    if scale == 0 {
        result.push_str(&value[start..]);
    } else if start + scale < value.len() {
        let d = value.len() - scale;
        result.push_str(&format!("{}.{}", &value[start..d], &value[d..]));
    } else {
        result.push_str(&format!("0.{:0>width$}", &value[start..], width = scale));
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        FixedSizeListArray, Float64Array, LargeListArray, ListArray, MapArray, StringArray,
        StructArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{
        i256, DataType, Field, Int32Type, IntervalDayTime, IntervalMonthDayNano,
    };
    use datafusion_common::arrow::array::ArrayRef;
    use sail_common::spec::SAIL_MAP_FIELD_NAME;

    use super::*;
    use crate::error::PlanResult;

    #[test]
    fn test_literal_to_string() -> PlanResult<()> {
        let formatter = SparkPlanFormatter;
        let to_string = |literal| formatter.literal_to_string(&literal, "UTC");

        assert_eq!(to_string(ScalarValue::Null)?, "NULL");
        assert_eq!(
            to_string(ScalarValue::Binary(Some(vec![16, 0x20, 0xff])))?,
            "X'1020FF'",
        );
        assert_eq!(to_string(ScalarValue::Boolean(Some(true)))?, "true");
        assert_eq!(to_string(ScalarValue::Int8(Some(10)))?, "10");
        assert_eq!(to_string(ScalarValue::Int16(Some(-20)))?, "-20");
        assert_eq!(to_string(ScalarValue::Int32(Some(30)))?, "30");
        assert_eq!(to_string(ScalarValue::Int64(Some(-40)))?, "-40");
        assert_eq!(to_string(ScalarValue::Float32(Some(1.0)))?, "1.0");
        assert_eq!(to_string(ScalarValue::Float64(Some(-0.1)))?, "-0.1");
        assert_eq!(to_string(ScalarValue::Decimal128(Some(123), 3, 0))?, "123",);
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(-123), 3, 0))?,
            "-123",
        );
        assert_eq!(to_string(ScalarValue::Decimal128(Some(123), 3, 2))?, "1.23",);
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(123), 3, 5))?,
            "0.00123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(12300), 3, -2))?,
            "12300",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 0))?,
            "123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(-123)), 3, 0))?,
            "-123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 2))?,
            "1.23",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 5))?,
            "0.00123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(12300)), 3, -2))?,
            "12300",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(
                i256::from_string("120000000000000000000000000000000000000000"),
                42,
                5,
            ))?,
            "1200000000000000000000000000000000000.00000",
        );
        assert_eq!(
            to_string(ScalarValue::Utf8(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::LargeUtf8(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::Utf8View(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::Date32(Some(10)))?,
            "DATE '1970-01-11'",
        );
        assert_eq!(
            to_string(ScalarValue::Date32(Some(-5)))?,
            "DATE '1969-12-27'",
        );
        assert_eq!(
            to_string(ScalarValue::TimestampMicrosecond(
                Some(123_000_000),
                Some(Arc::from("UTC")),
            ))?,
            "TIMESTAMP '1970-01-01 00:02:03'",
        );
        assert_eq!(
            to_string(ScalarValue::TimestampMicrosecond(Some(-1), None))?,
            "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano {
                    months: 15,
                    days: -20,
                    nanoseconds: 123_456_789_000,
                }
            )))?,
            "1 years 3 months -20 days 2 minutes 3.456789 seconds",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano {
                    months: -15,
                    days: 10,
                    nanoseconds: -1_001_000,
                }
            )))?,
            "-1 years -3 months 10 days -0.001001 seconds",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalYearMonth(Some(15)))?,
            "INTERVAL '1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalYearMonth(Some(-15)))?,
            "INTERVAL '-1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 123_456_000,
            })))?,
            "INTERVAL '1 10:17:36' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: -123_456_000,
            })))?,
            "INTERVAL '-1 10:17:36' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::DurationMicrosecond(Some(123_456_789)))?,
            "INTERVAL '0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::DurationMicrosecond(Some(-123_456_789)))?,
            "INTERVAL '-0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(-2)
                ])])
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::LargeList(Arc::new(
                LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(-2),
                ])])
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::FixedSizeList(Arc::new(
                FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![Some(vec![Some(1), Some(-2),])],
                    2
                )
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(Field::new(
                    SAIL_MAP_FIELD_NAME,
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Float64, true)),
                        ]
                        .into()
                    ),
                    false
                )),
                OffsetBuffer::new(vec![0, 2].into()),
                StructArray::try_from(vec![
                    (
                        "keys",
                        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef
                    ),
                    (
                        "values",
                        Arc::new(Float64Array::from(vec![Some(1.0), None])) as ArrayRef
                    )
                ])?,
                None,
                false
            ))))?,
            "map(array(a, b), array(1.0, NULL))",
        );
        assert_eq!(
            to_string(ScalarValue::Struct(Arc::new(StructArray::try_from(vec![
                (
                    "foo",
                    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                        Some(vec![Some(1), None])
                    ])) as ArrayRef
                ),
                (
                    "bar",
                    Arc::new(StructArray::try_from(vec![(
                        "baz",
                        Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef
                    )])?) as ArrayRef
                )
            ])?)))?,
            "struct(array(1, NULL) AS foo, struct(hello AS baz) AS bar)",
        );
        Ok(())
    }
}
