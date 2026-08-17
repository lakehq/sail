use std::borrow::Cow;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
// TODO: https://github.com/apache/spark/tree/master/common/utils/src/main/resources/error
use datafusion_common::{DataFusionError, exec_datafusion_err, internal_datafusion_err};

/// Backticks a struct field name when Spark's `quoteIfNeeded` would — anything
/// that does not start with a letter or `_`, or contains a character outside
/// `[a-zA-Z0-9_]` (Spark's `validIdentPattern = "^[a-zA-Z_][a-zA-Z0-9_]*"`) —
/// escaping any backtick by doubling it.
fn quote_field_name(name: &str) -> Cow<'_, str> {
    let mut bytes = name.bytes();
    let is_bare_identifier = match bytes.next() {
        Some(first) if first.is_ascii_alphabetic() || first == b'_' => {
            bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }
        _ => false,
    };
    if is_bare_identifier {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(format!("`{}`", name.replace('`', "``")))
    }
}

/// Renders the Spark SQL type name of an Arrow `DataType`, matching Spark's
/// `DataType.sql` / `toSQLType` (`Int32` → `INT`, `Utf8` → `STRING`,
/// `struct<a:int>` → `STRUCT<a: INT>`), for quoting types in analysis-time error
/// messages exactly as Spark does.
///
/// This is the uppercase `sql` counterpart of
/// `SparkPlanFormatter::data_type_to_simple_string` (Spark's lowercase
/// `simpleString`). The two tables should eventually derive from a single shared
/// mapping (they live in different crates today).
pub(crate) fn spark_sql_type_name(data_type: &DataType) -> Cow<'static, str> {
    match data_type {
        DataType::Null => Cow::Borrowed("VOID"),
        DataType::Boolean => Cow::Borrowed("BOOLEAN"),
        DataType::Int8 => Cow::Borrowed("TINYINT"),
        DataType::Int16 => Cow::Borrowed("SMALLINT"),
        DataType::Int32 => Cow::Borrowed("INT"),
        DataType::Int64 => Cow::Borrowed("BIGINT"),
        DataType::UInt8 => Cow::Borrowed("UNSIGNED TINYINT"),
        DataType::UInt16 => Cow::Borrowed("UNSIGNED SMALLINT"),
        DataType::UInt32 => Cow::Borrowed("UNSIGNED INT"),
        DataType::UInt64 => Cow::Borrowed("UNSIGNED BIGINT"),
        DataType::Float16 => Cow::Borrowed("HALF FLOAT"),
        DataType::Float32 => Cow::Borrowed("FLOAT"),
        DataType::Float64 => Cow::Borrowed("DOUBLE"),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Cow::Owned(format!("DECIMAL({precision},{scale})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Cow::Borrowed("STRING"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Cow::Borrowed("BINARY"),
        DataType::Date32 => Cow::Borrowed("DATE"),
        DataType::Date64 => Cow::Borrowed("DATE64"),
        DataType::Time32(TimeUnit::Second) => Cow::Borrowed("TIME(0)"),
        DataType::Time32(TimeUnit::Millisecond) => Cow::Borrowed("TIME(3)"),
        DataType::Time64(TimeUnit::Microsecond) => Cow::Borrowed("TIME(6)"),
        DataType::Time64(TimeUnit::Nanosecond) => Cow::Borrowed("TIME(9)"),
        DataType::Time32(_) | DataType::Time64(_) => Cow::Borrowed("TIME"),
        DataType::Timestamp(_, Some(_)) => Cow::Borrowed("TIMESTAMP"),
        DataType::Timestamp(_, None) => Cow::Borrowed("TIMESTAMP_NTZ"),
        DataType::Interval(IntervalUnit::YearMonth) => Cow::Borrowed("INTERVAL YEAR TO MONTH"),
        DataType::Interval(IntervalUnit::DayTime) | DataType::Duration(TimeUnit::Microsecond) => {
            Cow::Borrowed("INTERVAL DAY TO SECOND")
        }
        DataType::Duration(TimeUnit::Second) => Cow::Borrowed("DURATION(SECOND)"),
        DataType::Duration(TimeUnit::Millisecond) => Cow::Borrowed("DURATION(MILLISECOND)"),
        DataType::Duration(TimeUnit::Nanosecond) => Cow::Borrowed("DURATION(NANOSECOND)"),
        DataType::Interval(IntervalUnit::MonthDayNano) => Cow::Borrowed("INTERVAL"),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            Cow::Owned(format!("ARRAY<{}>", spark_sql_type_name(field.data_type())))
        }
        DataType::Struct(fields) => {
            // Spark's `StructField.sql` is `<quotedName>: <type>[ NOT NULL]`.
            let inner = fields
                .iter()
                .map(|field| {
                    let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
                    format!(
                        "{}: {}{nullability}",
                        quote_field_name(field.name()),
                        spark_sql_type_name(field.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            Cow::Owned(format!("STRUCT<{inner}>"))
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => Cow::Owned(format!(
                "MAP<{}, {}>",
                spark_sql_type_name(fields[0].data_type()),
                spark_sql_type_name(fields[1].data_type())
            )),
            _ => Cow::Borrowed("MAP"),
        },
        // Arrow-only encodings render as their logical value type.
        DataType::Dictionary(_, value_type) => spark_sql_type_name(value_type),
        DataType::RunEndEncoded(_, value_field) => spark_sql_type_name(value_field.data_type()),
        DataType::Union(..) => Cow::Borrowed("UNION"),
    }
}

pub fn invalid_arg_count_exec_err(
    function_name: &str,
    required_range: (i32, i32),
    provided: usize,
) -> DataFusionError {
    let (min_required, max_required) = required_range;
    let required = if min_required == max_required {
        format!(
            "{min_required} argument{}",
            if min_required == 1 { "" } else { "s" }
        )
    } else {
        format!("{min_required} to {max_required} arguments")
    };
    exec_datafusion_err!("Spark `{function_name}` function requires {required}, got {provided}")
}

pub fn unsupported_data_type_exec_err(
    function_name: &str,
    required: &str,
    provided: &DataType,
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {provided}"
    )
}

pub fn unsupported_data_types_exec_err(
    function_name: &str,
    required: &str,
    provided: &[DataType],
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {}",
        provided
            .iter()
            .map(|dt| format!("{dt}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn generic_exec_err(function_name: &str, message: &str) -> DataFusionError {
    exec_datafusion_err!("Spark `{function_name}` function: {message}")
}

pub fn generic_internal_err(function_name: &str, message: &str) -> DataFusionError {
    internal_datafusion_err!("Spark `{function_name}` function: {message}")
}
