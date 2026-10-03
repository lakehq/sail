//! Parameter substitution for parameterized SQL statements, such as `spark.sql(query, args)`.
//!
//! Like Spark 4.1 (SPARK-53573), parameter markers are substituted with the SQL text of their
//! argument values before the statement is analyzed, so that a marker can appear wherever the
//! grammar accepts a literal: in queries, in commands such as `INSERT` or `CREATE TABLE AS`,
//! and in positions that only accept a literal, such as `DECIMAL(:p, :s)` or `LOCATION :path`.
//!
//! The markers are located by parsing the statement with markers allowed in literal positions,
//! so that text such as `STRUCT<a:INT>` or `':key'` is never mistaken for a marker.
//! Each marker token is then replaced with the tokens of the rendered argument value, and the
//! result is parsed again. The replacement tokens carry the span of the marker they replace,
//! so errors from the second parse point at the marker in the original text.
//! A statement without parameter markers is parsed only once.
//!
//! Sail also accepts the markers `$name` and `$n` (1-based) in expressions, which Spark does not.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta};
use chumsky::Parser;
use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use sail_common::spec;
use sail_sql_parser::ast::identifier::Variable;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::create_parser;
use sail_sql_parser::span::TokenSpan;
use sail_sql_parser::token::Token;
use sail_sql_parser::tree::{TreeVisit, TreeVisitor};

use crate::error::{SqlError, SqlResult};
use crate::parser::{map_parser_input, tokenize};

/// The argument values of a parameterized SQL statement.
#[derive(Debug, Clone)]
pub enum SqlArguments {
    /// Values for named parameter markers (`:name`), keyed by the case-sensitive name.
    Named(Vec<(String, spec::Expr)>),
    /// Values for positional parameter markers (`?`), in order.
    Positional(Vec<spec::Expr>),
}

impl SqlArguments {
    /// No argument values. Any parameter marker in the statement is unbound.
    pub fn none() -> Self {
        SqlArguments::Named(vec![])
    }
}

/// Parses a SQL string containing exactly one statement, after substituting its parameter
/// markers with the SQL text of the argument values.
pub fn parse_one_statement_with_arguments(
    sql: &str,
    arguments: &SqlArguments,
) -> SqlResult<Statement> {
    let values = render_arguments(arguments)?;

    let locating = ParserOptions {
        allow_parameter_markers_in_literals: true,
        ..Default::default()
    };
    let tokens = tokenize(sql, &locating)?;
    let statement = parse_one(&tokens, sql.len(), &locating)?;
    let markers = ParameterMarkers::collect(&statement);
    if markers.is_empty() {
        return Ok(statement);
    }

    let replacements = markers.bind(sql, &values)?;
    let options = ParserOptions::default();
    let rendered = replacements
        .iter()
        .map(|(_, value)| tokenize(value, &options))
        .collect::<SqlResult<Vec<_>>>()?;
    let spliced = splice(&tokens, &replacements, &rendered);
    parse_one(&spliced, sql.len(), &options)
}

type Spanned<'a> = (Token<'a>, SimpleSpan);

fn parse_one(
    tokens: &[Spanned<'_>],
    length: usize,
    options: &ParserOptions,
) -> SqlResult<Statement> {
    let input = tokens.map((length..length).into(), map_parser_input);
    let parser = create_parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options);
    let mut statements = parser
        .parse(input)
        .into_result()
        .map_err(SqlError::parser)?;
    match (statements.pop(), statements.is_empty()) {
        (Some(statement), true) => Ok(statement),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

/// Replaces the tokens of each parameter marker with the tokens of its rendered value.
/// The replacement is surrounded by whitespace, so that it never joins an adjacent token.
fn splice<'a>(
    tokens: &[Spanned<'a>],
    replacements: &[(TokenSpan, String)],
    rendered: &[Vec<Spanned<'a>>],
) -> Vec<Spanned<'a>> {
    let mut output = Vec::with_capacity(tokens.len());
    let mut pending = replacements.iter().zip(rendered).peekable();
    let mut index = 0;
    while index < tokens.len() {
        let (token, span) = &tokens[index];
        match pending.peek() {
            Some(((marker, _), values)) if span.start == marker.start => {
                let start: SimpleSpan = (marker.start..marker.start).into();
                let end: SimpleSpan = (marker.end..marker.end).into();
                let replaced: SimpleSpan = (marker.start..marker.end).into();
                output.push((Token::Space { count: 1 }, start));
                output.extend(values.iter().map(|(t, _)| (t.clone(), replaced)));
                output.push((Token::Space { count: 1 }, end));
                while index < tokens.len() && tokens[index].1.start < marker.end {
                    index += 1;
                }
                pending.next();
            }
            _ => {
                output.push((token.clone(), *span));
                index += 1;
            }
        }
    }
    output
}

/// A parameter marker located in a statement.
#[derive(Debug, Clone)]
struct ParameterMarker {
    span: TokenSpan,
    kind: ParameterMarkerKind,
}

#[derive(Debug, Clone)]
enum ParameterMarkerKind {
    /// A named marker `:name`, or Sail's `$name`.
    Named(String),
    /// A positional marker `?`, bound to the next positional argument.
    Next,
    /// Sail's numbered marker `$n`, bound to the n-th positional argument (1-based).
    Numbered(usize),
}

impl ParameterMarkerKind {
    fn is_named(&self) -> bool {
        matches!(self, ParameterMarkerKind::Named(_))
    }
}

#[derive(Debug, Default)]
struct ParameterMarkers(Vec<ParameterMarker>);

impl TreeVisitor for ParameterMarkers {
    fn visit_variable(&mut self, variable: &Variable) {
        let kind = if variable.value == "?" {
            ParameterMarkerKind::Next
        } else if let Some(name) = variable.value.strip_prefix(':') {
            ParameterMarkerKind::Named(name.to_string())
        } else if let Some(name) = variable.value.strip_prefix('$') {
            // Sail also accepts `$name` and `$n` in expressions, which Spark does not.
            match name.parse::<usize>() {
                Ok(number) => ParameterMarkerKind::Numbered(number),
                Err(_) => ParameterMarkerKind::Named(name.to_string()),
            }
        } else {
            return;
        };
        self.0.push(ParameterMarker {
            span: variable.span,
            kind,
        });
    }
}

impl ParameterMarkers {
    fn collect(statement: &Statement) -> Self {
        let mut markers = ParameterMarkers::default();
        statement.visit(&mut markers);
        markers.0.sort_by_key(|marker| marker.span.start);
        markers.0.dedup_by_key(|marker| marker.span.start);
        markers
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the rendered value for each marker, in the order of the markers.
    fn bind(&self, sql: &str, values: &RenderedArguments) -> SqlResult<Vec<(TokenSpan, String)>> {
        let named = self.0.iter().any(|marker| marker.kind.is_named());
        let positional = self.0.iter().any(|marker| !marker.kind.is_named());
        if named && positional {
            return Err(SqlError::analysis(
                "[INVALID_QUERY_MIXED_QUERY_PARAMETERS] Parameterized query must either use positional, or named parameters, but not both.",
            ));
        }
        let mut position = 0;
        self.0
            .iter()
            .map(|marker| {
                let value = match (&marker.kind, values) {
                    (ParameterMarkerKind::Named(name), RenderedArguments::Named(values)) => {
                        values.get(name)
                    }
                    (ParameterMarkerKind::Next, RenderedArguments::Positional(values)) => {
                        position += 1;
                        values.get(position - 1)
                    }
                    (
                        ParameterMarkerKind::Numbered(number),
                        RenderedArguments::Positional(values),
                    ) => number.checked_sub(1).and_then(|index| values.get(index)),
                    _ => None,
                };
                match value {
                    Some(value) => Ok((marker.span, value.clone())),
                    None => Err(unbound_parameter(sql, marker)),
                }
            })
            .collect()
    }
}

fn unbound_parameter(sql: &str, marker: &ParameterMarker) -> SqlError {
    let name = match &marker.kind {
        ParameterMarkerKind::Named(name) => name.clone(),
        // Spark names an unbound positional marker after its character offset.
        ParameterMarkerKind::Next => format!(
            "_{}",
            sql.get(..marker.span.start).unwrap_or(sql).chars().count()
        ),
        ParameterMarkerKind::Numbered(number) => format!("${number}"),
    };
    SqlError::analysis(format!(
        "[UNBOUND_SQL_PARAMETER] Found the unbound parameter: {name}. Please, fix `args` and provide a mapping of the parameter to either a SQL literal or collection constructor functions such as `map()`, `array()`, `struct()`."
    ))
}

enum RenderedArguments {
    Named(HashMap<String, String>),
    Positional(Vec<String>),
}

/// Renders every argument, including the ones no marker uses, so that an invalid argument
/// is reported even when it is unused, as in Spark.
fn render_arguments(arguments: &SqlArguments) -> SqlResult<RenderedArguments> {
    match arguments {
        SqlArguments::Named(arguments) => Ok(RenderedArguments::Named(
            arguments
                .iter()
                .map(|(name, value)| Ok((name.clone(), render_argument(name, value)?)))
                .collect::<SqlResult<_>>()?,
        )),
        SqlArguments::Positional(arguments) => Ok(RenderedArguments::Positional(
            arguments
                .iter()
                .enumerate()
                .map(|(i, value)| render_argument(&format!("_{i}"), value))
                .collect::<SqlResult<_>>()?,
        )),
    }
}

/// The functions an argument may call, matching the collection constructors that Spark
/// accepts for `spark.sql()` arguments.
const ARGUMENT_FUNCTIONS: [&str; 6] = [
    "array",
    "map",
    "struct",
    "named_struct",
    "map_from_arrays",
    "map_from_entries",
];

fn render_argument(name: &str, value: &spec::Expr) -> SqlResult<String> {
    render_argument_expression(value).ok_or_else(|| {
        SqlError::analysis(format!(
            "[INVALID_SQL_ARG] The argument {name} of `sql()` is invalid. Consider to replace it either by a SQL literal or by collection constructor functions such as `map()`, `array()`, `struct()`."
        ))
    })?
}

/// Returns `None` if the expression is not a valid argument, or an error if it is a literal
/// whose type cannot be written in SQL.
fn render_argument_expression(value: &spec::Expr) -> Option<SqlResult<String>> {
    match value {
        spec::Expr::Literal(literal) => Some(render_literal(literal)),
        spec::Expr::Alias { expr, .. } => render_argument_expression(expr),
        spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments,
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: _,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        }) if named_arguments.is_empty() => {
            let [name] = function_name.parts() else {
                return None;
            };
            let name = name.as_ref().to_lowercase();
            if !ARGUMENT_FUNCTIONS.contains(&name.as_str()) {
                return None;
            }
            if name == "struct" {
                return render_struct_argument(arguments);
            }
            let arguments = arguments
                .iter()
                .map(render_argument_expression)
                .collect::<Option<SqlResult<Vec<_>>>>()?;
            Some(arguments.map(|arguments| format!("{name}({})", arguments.join(", "))))
        }
        _ => None,
    }
}

/// Writes `struct(...)` as `named_struct(...)`, so that each field keeps its name:
/// the alias of the argument, or `colN` for an argument without one, as in Spark.
fn render_struct_argument(arguments: &[spec::Expr]) -> Option<SqlResult<String>> {
    let fields = arguments
        .iter()
        .enumerate()
        .map(|(i, argument)| {
            let name = match argument {
                spec::Expr::Alias { name, .. } => match name.as_slice() {
                    [name] => name.as_ref().to_string(),
                    _ => return None,
                },
                _ => format!("col{}", i + 1),
            };
            let value = render_argument_expression(argument)?;
            Some(value.map(|value| format!("{}, {value}", render_string(&name))))
        })
        .collect::<Option<SqlResult<Vec<_>>>>()?;
    Some(fields.map(|fields| format!("named_struct({})", fields.join(", "))))
}

/// The number of days from 0001-01-01 to 1970-01-01.
const DAYS_FROM_CE_TO_UNIX_EPOCH: i32 = 719_163;
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_MINUTE: i64 = 60 * MICROSECONDS_PER_SECOND;
const MICROSECONDS_PER_HOUR: i64 = 60 * MICROSECONDS_PER_MINUTE;
const MICROSECONDS_PER_DAY: i64 = 24 * MICROSECONDS_PER_HOUR;

/// Writes a literal as SQL text that parses back to the same value,
/// following the rules of Spark's `Literal.sql`.
pub fn render_literal(literal: &spec::Literal) -> SqlResult<String> {
    use spec::Literal;

    match literal {
        Literal::Null => Ok("NULL".to_string()),
        Literal::Boolean { value } => render_or_null(value, literal, |v| {
            Ok(if *v { "TRUE" } else { "FALSE" }.to_string())
        }),
        Literal::Int8 { value } => render_or_null(value, literal, |v| {
            Ok(render_integer(*v, *v == i8::MIN, "Y", "TINYINT"))
        }),
        Literal::Int16 { value } => render_or_null(value, literal, |v| {
            Ok(render_integer(*v, *v == i16::MIN, "S", "SMALLINT"))
        }),
        Literal::Int32 { value } => render_or_null(value, literal, |v| {
            Ok(render_integer(*v, *v == i32::MIN, "", "INT"))
        }),
        Literal::Int64 { value } => render_or_null(value, literal, |v| {
            Ok(render_integer(*v, *v == i64::MIN, "L", "BIGINT"))
        }),
        Literal::Float32 { value } => render_or_null(value, literal, |v| {
            Ok(format!(
                "CAST('{}' AS FLOAT)",
                render_float(f64::from(*v), *v)
            ))
        }),
        Literal::Float64 { value } => render_or_null(value, literal, |v| {
            if v.is_finite() {
                Ok(format!("{v}D"))
            } else {
                Ok(format!("CAST('{}' AS DOUBLE)", render_float(*v, *v)))
            }
        }),
        Literal::Decimal128 {
            precision: _,
            scale,
            value,
        } => render_or_null(value, literal, |v| {
            Ok(format!("{}BD", render_decimal(&v.to_string(), *scale)))
        }),
        Literal::Decimal256 {
            precision: _,
            scale,
            value,
        } => render_or_null(value, literal, |v| {
            Ok(format!("{}BD", render_decimal(&v.to_string(), *scale)))
        }),
        Literal::Utf8 { value } | Literal::LargeUtf8 { value } | Literal::Utf8View { value } => {
            render_or_null(value, literal, |v| Ok(render_string(v)))
        }
        Literal::Binary { value }
        | Literal::LargeBinary { value }
        | Literal::BinaryView { value }
        | Literal::FixedSizeBinary { value, .. } => render_or_null(value, literal, |v| {
            Ok(format!(
                "X'{}'",
                v.iter().map(|b| format!("{b:02X}")).collect::<String>()
            ))
        }),
        Literal::Date32 { days } => render_or_null(days, literal, |v| {
            let date =
                NaiveDate::from_num_days_from_ce_opt(v.saturating_add(DAYS_FROM_CE_TO_UNIX_EPOCH))
                    .ok_or_else(|| SqlError::invalid(format!("date out of range: {v}")))?;
            Ok(format!("DATE '{}'", date.format("%Y-%m-%d")))
        }),
        Literal::TimestampSecond {
            seconds,
            timestamp_type,
        } => render_or_null(seconds, literal, |v| {
            render_timestamp(v.saturating_mul(MICROSECONDS_PER_SECOND), timestamp_type)
        }),
        Literal::TimestampMillisecond {
            milliseconds,
            timestamp_type,
        } => render_or_null(milliseconds, literal, |v| {
            render_timestamp(v.saturating_mul(1_000), timestamp_type)
        }),
        Literal::TimestampMicrosecond {
            microseconds,
            timestamp_type,
        } => render_or_null(microseconds, literal, |v| {
            render_timestamp(*v, timestamp_type)
        }),
        Literal::Time32Second { seconds } => render_or_null(seconds, literal, |v| {
            render_time(i64::from(*v) * MICROSECONDS_PER_SECOND)
        }),
        Literal::Time32Millisecond { milliseconds } => render_or_null(milliseconds, literal, |v| {
            render_time(i64::from(*v) * 1_000)
        }),
        Literal::Time64Microsecond { microseconds } => {
            render_or_null(microseconds, literal, |v| render_time(*v))
        }
        Literal::IntervalYearMonth {
            months,
            start_field,
            end_field,
        } => render_or_null(months, literal, |v| {
            render_year_month_interval(*v, *start_field, *end_field)
        }),
        Literal::IntervalDayTimeMicrosecond {
            microseconds,
            start_field,
            end_field,
        } => render_or_null(microseconds, literal, |v| {
            render_day_time_interval(*v, *start_field, *end_field)
        }),
        Literal::IntervalMonthDayNano { value } => render_or_null(value, literal, |v| {
            if v.nanoseconds % 1_000 != 0 {
                return Err(unsupported_literal(literal));
            }
            Ok(format!(
                "INTERVAL '{} months {} days {} microseconds'",
                v.months,
                v.days,
                v.nanoseconds / 1_000
            ))
        }),
        Literal::List {
            data_type, values, ..
        }
        | Literal::LargeList {
            data_type, values, ..
        }
        | Literal::FixedSizeList {
            data_type, values, ..
        } => render_or_null(values, literal, |v| {
            if v.is_empty() {
                return Ok(format!(
                    "CAST(ARRAY() AS ARRAY<{}>)",
                    render_data_type(data_type)?
                ));
            }
            let values = v
                .iter()
                .map(render_literal)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(format!("ARRAY({})", values.join(", ")))
        }),
        Literal::Map {
            key_type,
            value_type,
            keys,
            values,
            ..
        } => match (keys, values) {
            (Some(keys), Some(values)) if keys.len() == values.len() => {
                if keys.is_empty() {
                    return Ok(format!(
                        "CAST(MAP() AS MAP<{}, {}>)",
                        render_data_type(key_type)?,
                        render_data_type(value_type)?
                    ));
                }
                let entries = keys
                    .iter()
                    .zip(values)
                    .map(|(k, v)| Ok(format!("{}, {}", render_literal(k)?, render_literal(v)?)))
                    .collect::<SqlResult<Vec<_>>>()?;
                Ok(format!("MAP({})", entries.join(", ")))
            }
            (None, None) => render_null(literal),
            _ => Err(SqlError::invalid(
                "map literal with mismatched keys and values",
            )),
        },
        Literal::Struct { data_type, values } => render_or_null(values, literal, |v| {
            let spec::DataType::Struct { fields } = data_type else {
                return Err(unsupported_literal(literal));
            };
            if fields.len() != v.len() {
                return Err(SqlError::invalid("struct literal with mismatched fields"));
            }
            let entries = fields
                .iter()
                .zip(v)
                .map(|(field, value)| {
                    Ok(format!(
                        "{}, {}",
                        render_string(&field.name),
                        render_literal(value)?
                    ))
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(format!("NAMED_STRUCT({})", entries.join(", ")))
        }),
        Literal::UInt8 { .. }
        | Literal::UInt16 { .. }
        | Literal::UInt32 { .. }
        | Literal::UInt64 { .. }
        | Literal::Float16 { .. }
        | Literal::TimestampNanosecond { .. }
        | Literal::Date64 { .. }
        | Literal::Time64Nanosecond { .. }
        | Literal::DurationSecond { .. }
        | Literal::DurationMillisecond { .. }
        | Literal::DurationMicrosecond { .. }
        | Literal::DurationNanosecond { .. }
        | Literal::IntervalDayTime { .. }
        | Literal::Union { .. }
        | Literal::Dictionary { .. } => Err(unsupported_literal(literal)),
    }
}

fn render_or_null<T>(
    value: &Option<T>,
    literal: &spec::Literal,
    render: impl FnOnce(&T) -> SqlResult<String>,
) -> SqlResult<String> {
    match value {
        Some(value) => render(value),
        None => render_null(literal),
    }
}

/// Writes a typed null as `CAST(NULL AS <type>)`, so that it keeps its type.
fn render_null(literal: &spec::Literal) -> SqlResult<String> {
    Ok(format!(
        "CAST(NULL AS {})",
        render_data_type(&literal_data_type(literal)?)?
    ))
}

fn unsupported_literal(literal: &spec::Literal) -> SqlError {
    SqlError::unsupported(format!(
        "parameter value cannot be written in SQL: {literal:?}"
    ))
}

/// Returns the data type of a literal, which is only needed to write a typed null.
fn literal_data_type(literal: &spec::Literal) -> SqlResult<spec::DataType> {
    use spec::{DataType, IntervalUnit, Literal, TimeUnit};

    let data_type = match literal {
        Literal::Null => DataType::Null,
        Literal::Boolean { .. } => DataType::Boolean,
        Literal::Int8 { .. } => DataType::Int8,
        Literal::Int16 { .. } => DataType::Int16,
        Literal::Int32 { .. } => DataType::Int32,
        Literal::Int64 { .. } => DataType::Int64,
        Literal::Float32 { .. } => DataType::Float32,
        Literal::Float64 { .. } => DataType::Float64,
        Literal::Decimal128 {
            precision, scale, ..
        } => DataType::Decimal128 {
            precision: *precision,
            scale: *scale,
        },
        Literal::Decimal256 {
            precision, scale, ..
        } => DataType::Decimal256 {
            precision: *precision,
            scale: *scale,
        },
        Literal::Utf8 { .. } | Literal::LargeUtf8 { .. } | Literal::Utf8View { .. } => {
            DataType::Utf8
        }
        Literal::Binary { .. }
        | Literal::LargeBinary { .. }
        | Literal::BinaryView { .. }
        | Literal::FixedSizeBinary { .. } => DataType::Binary,
        Literal::Date32 { .. } => DataType::Date32,
        Literal::TimestampSecond { timestamp_type, .. }
        | Literal::TimestampMillisecond { timestamp_type, .. }
        | Literal::TimestampMicrosecond { timestamp_type, .. } => DataType::Timestamp {
            time_unit: TimeUnit::Microsecond,
            timestamp_type: timestamp_type.clone(),
        },
        Literal::Time32Second { .. }
        | Literal::Time32Millisecond { .. }
        | Literal::Time64Microsecond { .. } => DataType::Time64 {
            time_unit: TimeUnit::Microsecond,
        },
        Literal::IntervalYearMonth {
            start_field,
            end_field,
            ..
        } => DataType::Interval {
            interval_unit: IntervalUnit::YearMonth,
            start_field: *start_field,
            end_field: *end_field,
        },
        Literal::IntervalDayTimeMicrosecond {
            start_field,
            end_field,
            ..
        } => DataType::Interval {
            interval_unit: IntervalUnit::DayTime,
            start_field: *start_field,
            end_field: *end_field,
        },
        Literal::List {
            data_type,
            nullable,
            ..
        }
        | Literal::LargeList {
            data_type,
            nullable,
            ..
        }
        | Literal::FixedSizeList {
            data_type,
            nullable,
            ..
        } => DataType::List {
            data_type: Box::new(data_type.clone()),
            nullable: *nullable,
        },
        Literal::Map {
            key_type,
            value_type,
            value_type_nullable,
            ..
        } => DataType::Map {
            key_type: Box::new(key_type.clone()),
            value_type: Box::new(value_type.clone()),
            value_type_nullable: *value_type_nullable,
            keys_sorted: false,
        },
        Literal::Struct { data_type, .. } => data_type.clone(),
        _ => return Err(unsupported_literal(literal)),
    };
    Ok(data_type)
}

/// Writes a data type in the Spark SQL syntax for a column type.
pub fn render_data_type(data_type: &spec::DataType) -> SqlResult<String> {
    use spec::{DataType, IntervalUnit, TimestampType};

    let text = match data_type {
        DataType::Null => "VOID".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128 { precision, scale } | DataType::Decimal256 { precision, scale } => {
            format!("DECIMAL({precision}, {scale})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary { .. } => "BINARY".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp { timestamp_type, .. } => match timestamp_type {
            TimestampType::Configured => "TIMESTAMP",
            TimestampType::WithLocalTimeZone => "TIMESTAMP_LTZ",
            TimestampType::WithoutTimeZone => "TIMESTAMP_NTZ",
        }
        .to_string(),
        DataType::Time64 { .. } | DataType::Time32 { .. } => "TIME".to_string(),
        DataType::Interval {
            interval_unit: IntervalUnit::YearMonth,
            start_field,
            end_field,
        } => {
            let (start, end) = year_month_fields(*start_field, *end_field);
            format!("INTERVAL {}", render_interval_fields(start, end))
        }
        DataType::Interval {
            interval_unit: IntervalUnit::DayTime,
            start_field,
            end_field,
        } => {
            let (start, end) = day_time_fields(*start_field, *end_field);
            format!("INTERVAL {}", render_interval_fields(start, end))
        }
        DataType::List { data_type, .. }
        | DataType::LargeList { data_type, .. }
        | DataType::FixedSizeList { data_type, .. } => {
            format!("ARRAY<{}>", render_data_type(data_type)?)
        }
        DataType::Map {
            key_type,
            value_type,
            ..
        } => format!(
            "MAP<{}, {}>",
            render_data_type(key_type)?,
            render_data_type(value_type)?
        ),
        DataType::Struct { fields } => {
            let fields = fields
                .iter()
                .map(|field| {
                    Ok(format!(
                        "`{}`: {}",
                        field.name.replace('`', "``"),
                        render_data_type(&field.data_type)?
                    ))
                })
                .collect::<SqlResult<Vec<_>>>()?;
            format!("STRUCT<{}>", fields.join(", "))
        }
        DataType::UserDefined { sql_type, .. } => render_data_type(sql_type)?,
        _ => {
            return Err(SqlError::unsupported(format!(
                "parameter value type cannot be written in SQL: {data_type:?}"
            )));
        }
    };
    Ok(text)
}

/// Writes an integer with the suffix of its type. The minimum value is written as a cast,
/// since the SQL parser reads `-128Y` as the negation of `128Y`, which is out of range.
fn render_integer(
    value: impl std::fmt::Display,
    is_minimum: bool,
    suffix: &str,
    sql_type: &str,
) -> String {
    if is_minimum {
        format!("CAST('{value}' AS {sql_type})")
    } else {
        format!("{value}{suffix}")
    }
}

/// Writes a string literal, escaping the backslash and the quote as Spark does.
fn render_string(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 2);
    result.push('\'');
    for c in value.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\'' => result.push_str("\\'"),
            c => result.push(c),
        }
    }
    result.push('\'');
    result
}

/// Writes a floating-point value as the string a SQL cast reads back exactly.
fn render_float(value: f64, display: impl std::fmt::Display) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        display.to_string()
    }
}

/// Inserts the decimal point into the digits of an unscaled decimal value.
fn render_decimal(unscaled: &str, scale: i8) -> String {
    let (sign, digits) = match unscaled.strip_prefix('-') {
        Some(digits) => ("-", digits),
        None => ("", unscaled),
    };
    if scale <= 0 {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{digits}{zeros}");
    }
    let scale = usize::from(scale.unsigned_abs());
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn render_timestamp(microseconds: i64, timestamp_type: &spec::TimestampType) -> SqlResult<String> {
    let timestamp = DateTime::from_timestamp_micros(microseconds)
        .ok_or_else(|| SqlError::invalid(format!("timestamp out of range: {microseconds}")))?;
    let text = timestamp.format("%Y-%m-%d %H:%M:%S%.6f");
    // A timestamp with a time zone is an instant, so it is written with an explicit UTC offset
    // and does not depend on the session time zone.
    Ok(match timestamp_type {
        spec::TimestampType::WithoutTimeZone => format!("TIMESTAMP_NTZ '{text}'"),
        spec::TimestampType::WithLocalTimeZone => format!("TIMESTAMP_LTZ '{text}+00:00'"),
        spec::TimestampType::Configured => format!("TIMESTAMP '{text}+00:00'"),
    })
}

fn render_time(microseconds: i64) -> SqlResult<String> {
    if !(0..MICROSECONDS_PER_DAY).contains(&microseconds) {
        return Err(SqlError::invalid(format!(
            "time out of range: {microseconds}"
        )));
    }
    let time = NaiveTime::MIN
        .overflowing_add_signed(TimeDelta::microseconds(microseconds))
        .0;
    Ok(format!("TIME '{}'", time.format("%H:%M:%S%.6f")))
}

fn year_month_fields(
    start: Option<spec::IntervalFieldType>,
    end: Option<spec::IntervalFieldType>,
) -> (spec::IntervalFieldType, spec::IntervalFieldType) {
    let start = start.unwrap_or(spec::IntervalFieldType::Year);
    let end = end.unwrap_or(match start {
        spec::IntervalFieldType::Year => spec::IntervalFieldType::Month,
        _ => start,
    });
    (start, end)
}

fn day_time_fields(
    start: Option<spec::IntervalFieldType>,
    end: Option<spec::IntervalFieldType>,
) -> (spec::IntervalFieldType, spec::IntervalFieldType) {
    let start = start.unwrap_or(spec::IntervalFieldType::Day);
    let end = end.unwrap_or(match start {
        spec::IntervalFieldType::Day => spec::IntervalFieldType::Second,
        _ => start,
    });
    (start, end)
}

fn interval_field_name(field: spec::IntervalFieldType) -> &'static str {
    match field {
        spec::IntervalFieldType::Year => "YEAR",
        spec::IntervalFieldType::Month => "MONTH",
        spec::IntervalFieldType::Day => "DAY",
        spec::IntervalFieldType::Hour => "HOUR",
        spec::IntervalFieldType::Minute => "MINUTE",
        spec::IntervalFieldType::Second => "SECOND",
    }
}

fn render_interval_fields(start: spec::IntervalFieldType, end: spec::IntervalFieldType) -> String {
    if start == end {
        interval_field_name(start).to_string()
    } else {
        format!(
            "{} TO {}",
            interval_field_name(start),
            interval_field_name(end)
        )
    }
}

fn render_year_month_interval(
    months: i32,
    start: Option<spec::IntervalFieldType>,
    end: Option<spec::IntervalFieldType>,
) -> SqlResult<String> {
    use spec::IntervalFieldType::{Month, Year};

    let (start, end) = year_month_fields(start, end);
    let sign = if months < 0 { "-" } else { "" };
    let months = i64::from(months).abs();
    let value = match (start, end) {
        (Year, Year) => format!("{sign}{}", months / 12),
        (Year, Month) => format!("{sign}{}-{}", months / 12, months % 12),
        (Month, Month) => format!("{sign}{months}"),
        _ => return Err(SqlError::invalid("invalid year-month interval fields")),
    };
    Ok(format!(
        "INTERVAL '{value}' {}",
        render_interval_fields(start, end)
    ))
}

fn render_day_time_interval(
    microseconds: i64,
    start: Option<spec::IntervalFieldType>,
    end: Option<spec::IntervalFieldType>,
) -> SqlResult<String> {
    use spec::IntervalFieldType::{Day, Hour, Minute, Second};

    let (start, end) = day_time_fields(start, end);
    let sign = if microseconds < 0 { "-" } else { "" };
    let total = i128::from(microseconds).abs();
    let unit = |field: spec::IntervalFieldType| -> i128 {
        i128::from(match field {
            Day => MICROSECONDS_PER_DAY,
            Hour => MICROSECONDS_PER_HOUR,
            Minute => MICROSECONDS_PER_MINUTE,
            _ => MICROSECONDS_PER_SECOND,
        })
    };
    let fields = [Day, Hour, Minute, Second];
    let (Some(first), Some(last)) = (
        fields.iter().position(|f| *f == start),
        fields.iter().position(|f| *f == end),
    ) else {
        return Err(SqlError::invalid("invalid day-time interval fields"));
    };
    if first > last {
        return Err(SqlError::invalid("invalid day-time interval fields"));
    }
    let mut rest = total;
    let mut parts = vec![];
    for (i, field) in fields[first..=last].iter().enumerate() {
        let part = if *field == Second {
            let seconds = rest / unit(Second);
            let micros = rest % unit(Second);
            let width = if i == 0 { 1 } else { 2 };
            format!("{seconds:0width$}.{micros:06}")
        } else {
            let value = rest / unit(*field);
            rest %= unit(*field);
            if i == 0 {
                value.to_string()
            } else {
                format!("{value:02}")
            }
        };
        parts.push(part);
        if *field == Second {
            rest = 0;
        }
    }
    let mut value = String::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            value.push(if first == 0 && i == 1 { ' ' } else { ':' });
        }
        value.push_str(part);
    }
    Ok(format!(
        "INTERVAL '{sign}{value}' {}",
        render_interval_fields(start, end)
    ))
}

#[cfg(test)]
mod tests {
    use sail_common::spec;
    use sail_sql_parser::tree::TreeText;

    use super::*;
    use crate::parser::{parse_expression, parse_one_statement};

    fn literal(value: spec::Literal) -> spec::Expr {
        spec::Expr::Literal(value)
    }

    fn int(value: i32) -> spec::Expr {
        literal(spec::Literal::Int32 { value: Some(value) })
    }

    fn string(value: &str) -> spec::Expr {
        literal(spec::Literal::Utf8 {
            value: Some(value.to_string()),
        })
    }

    fn named(arguments: &[(&str, spec::Expr)]) -> SqlArguments {
        SqlArguments::Named(
            arguments
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    fn positional(arguments: &[spec::Expr]) -> SqlArguments {
        SqlArguments::Positional(arguments.to_vec())
    }

    /// Asserts that the statement parses to the same tree as the expected SQL text.
    fn assert_substituted(sql: &str, arguments: &SqlArguments, expected: &str) -> SqlResult<()> {
        let actual = parse_one_statement_with_arguments(sql, arguments)?;
        let expected = parse_one_statement(expected)?;
        assert_eq!(actual.text(), expected.text(), "{sql}");
        Ok(())
    }

    fn error(sql: &str, arguments: &SqlArguments) -> SqlResult<String> {
        match parse_one_statement_with_arguments(sql, arguments) {
            Ok(statement) => Err(SqlError::invalid(format!(
                "{sql}: expected an error, got {}",
                statement.text()
            ))),
            Err(e) => Ok(e.to_string()),
        }
    }

    #[test]
    fn test_statement_without_markers_is_unchanged() -> SqlResult<()> {
        for sql in [
            "SELECT 1",
            "SELECT CAST(NULL AS STRUCT<a:INT, b:STRING>)",
            "CREATE TABLE t (s STRUCT<a:INT>, m MAP<STRING,INT>) USING parquet",
            "SELECT a::INT, '12:00', ':x', '?' FROM t",
            "SELECT 1 -- :x ?\n",
            "SELECT /* :x ? */ 1",
            "SELECT `:x` FROM t",
        ] {
            assert_substituted(sql, &SqlArguments::none(), sql)?;
            assert_substituted(sql, &named(&[("x", int(1))]), sql)?;
        }
        Ok(())
    }

    #[test]
    fn test_markers_in_queries_and_commands() -> SqlResult<()> {
        let p = named(&[("p", int(1))]);
        for (sql, expected) in [
            ("SELECT :p", "SELECT 1"),
            ("SELECT :p + :p AS v", "SELECT 1 + 1 AS v"),
            ("INSERT INTO t SELECT :p", "INSERT INTO t SELECT 1"),
            ("INSERT INTO t VALUES (:p)", "INSERT INTO t VALUES (1)"),
            (
                "CREATE TABLE t2 USING parquet AS SELECT :p AS c",
                "CREATE TABLE t2 USING parquet AS SELECT 1 AS c",
            ),
            ("DELETE FROM t WHERE c = :p", "DELETE FROM t WHERE c = 1"),
            (
                "UPDATE t SET c = :p WHERE c > :p",
                "UPDATE t SET c = 1 WHERE c > 1",
            ),
            (
                "CREATE VIEW v AS SELECT :p AS c",
                "CREATE VIEW v AS SELECT 1 AS c",
            ),
            (
                "SELECT * FROM IDENTIFIER(:p)",
                "SELECT * FROM IDENTIFIER(1)",
            ),
        ] {
            assert_substituted(sql, &p, expected)?;
        }
        assert_substituted(
            "SELECT ?, ?",
            &positional(&[int(1), string("a")]),
            "SELECT 1, 'a'",
        )?;
        Ok(())
    }

    #[test]
    fn test_markers_in_literal_positions() -> SqlResult<()> {
        let arguments = named(&[
            ("p", int(10)),
            ("s", int(2)),
            ("v", string("x")),
            ("pattern", string("t*")),
        ]);
        for (sql, expected) in [
            (
                "SELECT CAST(1 AS DECIMAL(:p, :s))",
                "SELECT CAST(1 AS DECIMAL(10, 2))",
            ),
            (
                "CREATE TABLE t (c INT) USING parquet LOCATION :v",
                "CREATE TABLE t (c INT) USING parquet LOCATION 'x'",
            ),
            (
                "CREATE TABLE t (c INT) USING parquet COMMENT :v",
                "CREATE TABLE t (c INT) USING parquet COMMENT 'x'",
            ),
            (
                "CREATE TABLE t (c INT) USING parquet TBLPROPERTIES ('k' = :v, :v = 'k')",
                "CREATE TABLE t (c INT) USING parquet TBLPROPERTIES ('k' = 'x', 'x' = 'k')",
            ),
            ("SHOW TABLES LIKE :pattern", "SHOW TABLES LIKE 't*'"),
        ] {
            assert_substituted(sql, &arguments, expected)?;
        }
        Ok(())
    }

    #[test]
    fn test_view_text_has_the_values() -> SqlResult<()> {
        let statement = parse_one_statement_with_arguments(
            "CREATE VIEW v AS SELECT :p AS c",
            &named(&[("p", string("it's"))]),
        )?;
        assert!(
            statement.text().contains(r"'it\'s'"),
            "{}",
            statement.text()
        );
        Ok(())
    }

    #[test]
    fn test_unused_arguments_and_case_sensitive_names() -> SqlResult<()> {
        assert_substituted(
            "SELECT :a",
            &named(&[("a", int(1)), ("A", int(2)), ("unused", int(3))]),
            "SELECT 1",
        )?;
        assert_substituted("SELECT ?", &positional(&[int(1), int(2)]), "SELECT 1")?;
        Ok(())
    }

    #[test]
    fn test_unbound_markers() -> SqlResult<()> {
        let e = error("SELECT :a, :b", &named(&[("a", int(1))]))?;
        assert!(
            e.contains("[UNBOUND_SQL_PARAMETER] Found the unbound parameter: b."),
            "{e}"
        );
        let e = error("SELECT :a", &SqlArguments::none())?;
        assert!(e.contains("unbound parameter: a."), "{e}");
        // An unbound positional marker is named after its character offset.
        let e = error("SELECT 'é', ?, ?", &positional(&[int(1)]))?;
        assert!(e.contains("unbound parameter: _15."), "{e}");
        let e = error("SELECT ?", &named(&[("a", int(1))]))?;
        assert!(e.contains("unbound parameter: _7."), "{e}");
        let e = error("SELECT :a", &positional(&[int(1)]))?;
        assert!(e.contains("unbound parameter: a."), "{e}");
        Ok(())
    }

    #[test]
    fn test_dollar_markers() -> SqlResult<()> {
        assert_substituted(
            "SELECT $2, $1, $2",
            &positional(&[int(1), int(2)]),
            "SELECT 2, 1, 2",
        )?;
        assert_substituted("SELECT $foo", &named(&[("foo", int(1))]), "SELECT 1")?;
        let e = error("SELECT $3", &positional(&[int(1)]))?;
        assert!(e.contains("unbound parameter: $3."), "{e}");
        Ok(())
    }

    #[test]
    fn test_mixed_markers() -> SqlResult<()> {
        let e = error("SELECT :a, ?", &named(&[("a", int(1))]))?;
        assert!(e.contains("[INVALID_QUERY_MIXED_QUERY_PARAMETERS]"), "{e}");
        Ok(())
    }

    #[test]
    fn test_invalid_arguments() -> SqlResult<()> {
        let column = spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::bare("c"),
            plan_id: None,
            is_metadata_column: false,
        };
        // An invalid argument is reported even if no marker uses it.
        let e = error("SELECT 1", &named(&[("c", column.clone())]))?;
        assert!(
            e.contains("[INVALID_SQL_ARG] The argument c of `sql()` is invalid."),
            "{e}"
        );
        let e = error("SELECT ?", &positional(&[int(1), column.clone()]))?;
        assert!(e.contains("The argument _1 of `sql()`"), "{e}");
        let nested = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
            function_name: spec::ObjectName::bare("array"),
            arguments: vec![int(1), column],
            named_arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        });
        let e = error("SELECT :a", &named(&[("a", nested)]))?;
        assert!(e.contains("[INVALID_SQL_ARG]"), "{e}");
        Ok(())
    }

    #[test]
    fn test_collection_constructor_arguments() -> SqlResult<()> {
        let call = |name: &str, arguments: Vec<spec::Expr>| {
            spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(name),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            })
        };
        let arguments = named(&[
            ("a", call("array", vec![int(1), int(2)])),
            ("m", call("MAP", vec![string("k"), call("array", vec![])])),
            (
                "s",
                spec::Expr::Alias {
                    expr: Box::new(call("named_struct", vec![string("f"), int(1)])),
                    name: vec!["x".into()],
                    metadata: None,
                },
            ),
        ]);
        assert_substituted(
            "SELECT :a, :m, :s",
            &arguments,
            "SELECT array(1, 2), map('k', array()), named_struct('f', 1)",
        )?;
        let alias = |expr: spec::Expr, name: &str| spec::Expr::Alias {
            expr: Box::new(expr),
            name: vec![name.into()],
            metadata: None,
        };
        assert_substituted(
            "SELECT :s",
            &named(&[("s", call("struct", vec![alias(int(1), "x"), string("y")]))]),
            "SELECT named_struct('x', 1, 'col2', 'y')",
        )?;
        Ok(())
    }

    #[test]
    fn test_error_from_substituted_statement_points_at_marker() -> SqlResult<()> {
        let e = error(
            "SELECT CAST(1 AS DECIMAL(:p, 2))",
            &named(&[("p", string("x"))]),
        )?;
        assert!(e.contains("at 25:27"), "{e}");
        Ok(())
    }

    #[test]
    fn test_render_literal() -> SqlResult<()> {
        use spec::Literal;

        let cases: Vec<(Literal, &str)> = vec![
            (Literal::Null, "NULL"),
            (Literal::Boolean { value: Some(true) }, "TRUE"),
            (Literal::Int8 { value: Some(-127) }, "-127Y"),
            (
                Literal::Int8 {
                    value: Some(i8::MIN),
                },
                "CAST('-128' AS TINYINT)",
            ),
            (Literal::Int16 { value: Some(7) }, "7S"),
            (Literal::Int32 { value: Some(-7) }, "-7"),
            (
                Literal::Int64 {
                    value: Some(i64::MIN),
                },
                "CAST('-9223372036854775808' AS BIGINT)",
            ),
            (
                Literal::Float32 { value: Some(1.5) },
                "CAST('1.5' AS FLOAT)",
            ),
            (
                Literal::Float32 {
                    value: Some(f32::NAN),
                },
                "CAST('NaN' AS FLOAT)",
            ),
            (Literal::Float64 { value: Some(0.1) }, "0.1D"),
            (
                Literal::Float64 {
                    value: Some(f64::NEG_INFINITY),
                },
                "CAST('-Infinity' AS DOUBLE)",
            ),
            (
                Literal::Decimal128 {
                    precision: 10,
                    scale: 2,
                    value: Some(-5),
                },
                "-0.05BD",
            ),
            (
                Literal::Utf8 {
                    value: Some(r"a'b\c".to_string()),
                },
                r"'a\'b\\c'",
            ),
            (
                Literal::Binary {
                    value: Some(vec![0xab, 0x01]),
                },
                "X'AB01'",
            ),
            (Literal::Date32 { days: Some(19844) }, "DATE '2024-05-01'"),
            (
                Literal::TimestampMicrosecond {
                    microseconds: Some(1_714_564_800_123_456),
                    timestamp_type: spec::TimestampType::WithLocalTimeZone,
                },
                "TIMESTAMP_LTZ '2024-05-01 12:00:00.123456+00:00'",
            ),
            (
                Literal::TimestampMicrosecond {
                    microseconds: Some(1_714_564_800_000_000),
                    timestamp_type: spec::TimestampType::WithoutTimeZone,
                },
                "TIMESTAMP_NTZ '2024-05-01 12:00:00.000000'",
            ),
            (
                Literal::IntervalYearMonth {
                    months: Some(-14),
                    start_field: Some(spec::IntervalFieldType::Year),
                    end_field: Some(spec::IntervalFieldType::Month),
                },
                "INTERVAL '-1-2' YEAR TO MONTH",
            ),
            (
                Literal::IntervalDayTimeMicrosecond {
                    microseconds: Some(93_784_000_005),
                    start_field: Some(spec::IntervalFieldType::Day),
                    end_field: Some(spec::IntervalFieldType::Second),
                },
                "INTERVAL '1 02:03:04.000005' DAY TO SECOND",
            ),
            (
                Literal::IntervalDayTimeMicrosecond {
                    microseconds: Some(-5_400_000_000),
                    start_field: Some(spec::IntervalFieldType::Hour),
                    end_field: Some(spec::IntervalFieldType::Minute),
                },
                "INTERVAL '-1:30' HOUR TO MINUTE",
            ),
            (
                Literal::IntervalMonthDayNano {
                    value: Some(spec::IntervalMonthDayNano {
                        months: 1,
                        days: -2,
                        nanoseconds: 3_000,
                    }),
                },
                "INTERVAL '1 months -2 days 3 microseconds'",
            ),
            (
                Literal::Time64Microsecond {
                    microseconds: Some(45_045_000_001),
                },
                "TIME '12:30:45.000001'",
            ),
            (
                Literal::List {
                    data_type: spec::DataType::Int32,
                    nullable: true,
                    values: Some(vec![
                        Literal::Int32 { value: Some(1) },
                        Literal::Int32 { value: None },
                    ]),
                },
                "ARRAY(1, CAST(NULL AS INT))",
            ),
            (
                Literal::List {
                    data_type: spec::DataType::Utf8,
                    nullable: true,
                    values: Some(vec![]),
                },
                "CAST(ARRAY() AS ARRAY<STRING>)",
            ),
            (
                Literal::Map {
                    key_type: spec::DataType::Utf8,
                    value_type: spec::DataType::Int64,
                    value_type_nullable: true,
                    keys: Some(vec![Literal::Utf8 {
                        value: Some("k".to_string()),
                    }]),
                    values: Some(vec![Literal::Int64 { value: Some(1) }]),
                },
                "MAP('k', 1L)",
            ),
            (
                Literal::Struct {
                    data_type: spec::DataType::Struct {
                        fields: spec::Fields::from(vec![spec::Field {
                            name: "a`b".to_string(),
                            data_type: spec::DataType::Int32,
                            nullable: true,
                            metadata: vec![],
                        }]),
                    },
                    values: None,
                },
                "CAST(NULL AS STRUCT<`a``b`: INT>)",
            ),
        ];
        for (literal, expected) in cases {
            let rendered = render_literal(&literal)?;
            assert_eq!(rendered, expected, "{literal:?}");
            parse_expression(&rendered)?;
        }
        Ok(())
    }
}
