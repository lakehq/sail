use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder,
    LargeListArray, ListArray, MapArray, OffsetSizeTrait, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// Spark's ANSI `CAST(<string> AS <numeric | boolean>)`, applied to a string or to the strings
/// nested in arrays, structs and maps.
///
/// Arrow's string parsers differ from Spark's (`'on'` is a boolean in Arrow, the whitespace Spark
/// trims is wider, `'1d'` is a double in Java, a decimal is rounded and not truncated), and a
/// DataFusion `Cast` over a literal raises its error while the query is planned, even inside a
/// branch that is never taken. A scalar function keeps the error for execution time.
///
/// Error texts follow Spark 4.2.0 (`CAST_INVALID_INPUT`, `NUMERIC_VALUE_OUT_OF_RANGE`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkAnsiStringCast {
    signature: Signature,
    target_type: DataType,
}

impl SparkAnsiStringCast {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }

    /// Whether a value of `from` holds a string that `to` turns into a number or a boolean, at the
    /// top level or nested, so that this function (and not a plain cast) has to carry it.
    pub fn applies_to(from: &DataType, to: &DataType) -> bool {
        match (from, to) {
            (from, to) if is_string(from) => is_leaf_target(to),
            (
                DataType::List(from) | DataType::LargeList(from) | DataType::FixedSizeList(from, _),
                DataType::List(to) | DataType::LargeList(to) | DataType::FixedSizeList(to, _),
            ) => Self::applies_to(from.data_type(), to.data_type()),
            (DataType::Struct(from), DataType::Struct(to)) => {
                from.len() == to.len()
                    && from
                        .iter()
                        .zip(to.iter())
                        .any(|(from, to)| Self::applies_to(from.data_type(), to.data_type()))
            }
            (DataType::Map(from, _), DataType::Map(to, _)) => {
                Self::applies_to(from.data_type(), to.data_type())
            }
            _ => false,
        }
    }
}

impl ScalarUDFImpl for SparkAnsiStringCast {
    fn name(&self) -> &str {
        "spark_ansi_string_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    /// `Cast.nullable` is `child.nullable || forceNullable(from, to)`, and `forceNullable` holds
    /// only for a string that has to be parsed (`Cast.scala:427-448`, `:630`). An array, map or
    /// struct of strings is cast element by element, so it is nullable exactly when its input is.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| is_string(field.data_type()) || field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_ansi_string_cast",
                (1, 1),
                args.args.len(),
            ));
        };
        match arg {
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let converted = cast_array(&array, &self.target_type, None)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &converted, 0,
                )?))
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_array(
                array,
                &self.target_type,
                None,
            )?)),
        }
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_leaf_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64 | DataType::Float64 | DataType::Boolean | DataType::Decimal128(_, _)
    )
}

/// Converts `array` to `to`. `visible` says which rows a consumer can read: Arrow only defines the
/// validity of a nested array's slots, so the child slot under a NULL struct, list or map holds any
/// value, and Spark never looks at it. A string there is skipped instead of parsed.
fn cast_array(array: &ArrayRef, to: &DataType, visible: Option<&NullBuffer>) -> Result<ArrayRef> {
    match (array.data_type(), to) {
        (from, to) if is_string(from) && is_leaf_target(to) => parse_strings(array, to, visible),
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_list::<i32>();
            let rows = NullBuffer::union(visible, list.nulls());
            let seen = element_visibility(list.offsets(), rows.as_ref(), list.values().len());
            let values = cast_array(list.values(), field.data_type(), Some(&seen))?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            let list = array.as_list::<i64>();
            let rows = NullBuffer::union(visible, list.nulls());
            let seen = element_visibility(list.offsets(), rows.as_ref(), list.values().len());
            let values = cast_array(list.values(), field.data_type(), Some(&seen))?;
            Ok(Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        // Spark has one array type: bring the source to the flavor of the target first, so the
        // elements go through the same parsing whatever the Arrow storage is.
        (DataType::FixedSizeList(field, _) | DataType::LargeList(field), DataType::List(_)) => {
            let list = plain_cast(array, &DataType::List(Arc::clone(field)))?;
            cast_array(&list, to, visible)
        }
        (DataType::FixedSizeList(field, _) | DataType::List(field), DataType::LargeList(_)) => {
            let list = plain_cast(array, &DataType::LargeList(Arc::clone(field)))?;
            cast_array(&list, to, visible)
        }
        (DataType::FixedSizeList(field, size), DataType::FixedSizeList(target, other_size))
            if size == other_size =>
        {
            let list = plain_cast(array, &DataType::List(Arc::clone(field)))?;
            let converted = cast_array(&list, &DataType::List(Arc::clone(target)), visible)?;
            plain_cast(&converted, to)
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let source = array.as_struct();
            if source.num_columns() != fields.len() {
                return plain_cast(array, to);
            }
            let rows = NullBuffer::union(visible, source.nulls());
            let columns = source
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| cast_array(column, field.data_type(), rows.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(field, sorted)) => {
            let map = array.as_map();
            let rows = NullBuffer::union(visible, map.nulls());
            let seen = element_visibility(map.offsets(), rows.as_ref(), map.entries().len());
            let entries = cast_array(
                &(Arc::new(map.entries().clone()) as ArrayRef),
                field.data_type(),
                Some(&seen),
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => plain_cast(array, to),
    }
}

/// The elements of a list or map that belong to a visible row. An element outside every row, or
/// inside a hidden one, is not read.
fn element_visibility<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    rows: Option<&NullBuffer>,
    elements: usize,
) -> NullBuffer {
    let mut seen = vec![false; elements];
    for (row, bounds) in offsets.windows(2).enumerate() {
        if rows.is_some_and(|rows| rows.is_null(row)) {
            continue;
        }
        let (start, end) = (bounds[0].as_usize(), bounds[1].as_usize().min(elements));
        if let Some(range) = seen.get_mut(start..end) {
            range.fill(true);
        }
    }
    NullBuffer::from(seen)
}

fn plain_cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    Ok(cast_with_options(array, to, &options)?)
}

fn parse_strings(
    array: &ArrayRef,
    to: &DataType,
    visible: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    let strings: Vec<Option<&str>> = match array.data_type() {
        DataType::Utf8 => as_string_array(array)?.iter().collect(),
        DataType::LargeUtf8 => as_large_string_array(array)?.iter().collect(),
        DataType::Utf8View => as_string_view_array(array)?.iter().collect(),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_ansi_string_cast",
                "a string",
                other,
            ));
        }
    };
    let strings: Vec<Option<&str>> = strings
        .into_iter()
        .enumerate()
        .map(|(row, value)| value.filter(|_| visible.is_none_or(|visible| visible.is_valid(row))))
        .collect();
    match to {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(strings.len());
            for value in strings {
                match value {
                    Some(text) => builder
                        .append_value(parse_long(text).ok_or_else(|| invalid_input(text, to))?),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(strings.len());
            for value in strings {
                match value {
                    Some(text) => builder
                        .append_value(parse_double(text).ok_or_else(|| invalid_input(text, to))?),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(strings.len());
            for value in strings {
                match value {
                    Some(text) => builder
                        .append_value(parse_boolean(text).ok_or_else(|| invalid_input(text, to))?),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(strings.len())
                .with_precision_and_scale(*precision, *scale)?;
            for value in strings {
                match value {
                    Some(text) => match parse_decimal(text, *precision, *scale) {
                        Decimal::Value(unscaled) => builder.append_value(unscaled),
                        Decimal::Malformed => return Err(invalid_input(text, to)),
                        Decimal::TooManyDigits => {
                            return Err(DataFusionError::Execution(format!(
                                "[NUMERIC_OUT_OF_SUPPORTED_RANGE] The value {} cannot be \
                                 interpreted as a numeric since it has more than 38 digits.",
                                text.trim()
                            )));
                        }
                        Decimal::OutOfRange(number) => {
                            // The template of the parent class is empty, hence the two spaces.
                            return Err(DataFusionError::Execution(format!(
                                "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {number} cannot be \
                                 represented as Decimal({precision}, {scale}). If necessary set \
                                 \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and \
                                 return NULL instead."
                            )));
                        }
                    },
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(unsupported_data_type_exec_err(
            "spark_ansi_string_cast",
            "a numeric or boolean target",
            other,
        )),
    }
}

/// `CAST_INVALID_INPUT` as 4.2.0 words it (`error-conditions.json`), with the value escaped the way
/// `toSQLValue` does.
fn invalid_input(text: &str, to: &DataType) -> DataFusionError {
    let escaped = text.replace('\\', "\\\\").replace('\'', "\\'");
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{escaped}' of the type \"STRING\" cannot be cast to \"{}\" \
         because it is malformed. Correct the value as per the syntax, or change its target type. \
         Use `try_cast` to tolerate malformed input and return NULL instead.",
        sql_type_name(to)
    ))
}

fn sql_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => other.to_string().to_uppercase(),
    }
}

/// The most integral digits Spark reads from a decimal string (`DecimalType.MAX_PRECISION`).
const MAX_DECIMAL_DIGITS: u8 = 38;

/// `UTF8String.toLongExact` trims the single-byte whitespace and control characters, then reads an
/// optionally signed run of ASCII digits (`UTF8String.java`).
fn parse_long(text: &str) -> Option<i64> {
    let trimmed = text.trim_matches(|c: char| (c as u32) <= 0x20 || c == '\u{7F}');
    trimmed.parse::<i64>().ok()
}

/// `java.lang.Double.parseDouble` after Java's `String.trim`, plus the special literals Spark adds
/// (`inf`, `infinity`, `nan`, case-insensitive, `Cast.scala`). Hexadecimal floats are not read.
fn parse_double(text: &str) -> Option<f64> {
    let trimmed = text.trim_matches(|c: char| (c as u32) <= 0x20);
    // Rust reads `nan`, `inf` and `infinity` in any case with any sign; Spark takes Java's exact
    // spellings, then the lower-case literals of `processFloatingPointSpecialLiterals`.
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if unsigned.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return match trimmed {
            "NaN" | "+NaN" | "-NaN" => Some(f64::NAN),
            "Infinity" | "+Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => match trimmed.to_lowercase().as_str() {
                "nan" => Some(f64::NAN),
                "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
                "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
                _ => None,
            },
        };
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return Some(value);
    }
    let without_suffix = trimmed.strip_suffix(['d', 'D', 'f', 'F'])?;
    if without_suffix
        .chars()
        .last()
        .is_some_and(|c| c.is_ascii_digit() || c == '.')
    {
        return without_suffix.parse::<f64>().ok();
    }
    None
}

/// `StringUtils.isTrueString` / `isFalseString` after `trimAll` and lower-casing. `trimAll` goes
/// byte by byte (`UTF8String.java:1000-1016`), so it removes the single-byte whitespace and control
/// characters and no multi-byte space.
fn parse_boolean(text: &str) -> Option<bool> {
    let trimmed = text
        .trim_matches(|c: char| (c as u32) <= 0x20 || c == '\u{7F}')
        .to_lowercase();
    match trimmed.as_str() {
        "t" | "true" | "y" | "yes" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "0" => Some(false),
        _ => None,
    }
}

enum Decimal {
    Value(i128),
    Malformed,
    /// More than 38 integral digits, which Spark rejects before rounding or fitting the target
    /// (`Decimal.scala:660-661`).
    TooManyDigits,
    /// Does not fit the target; holds the number as `BigDecimal.toPlainString` writes it, which is
    /// what Spark reports.
    OutOfRange(String),
}

fn plain_decimal(negative: bool, digits: &[u8], fractional_digits: usize, exponent: i32) -> String {
    let start = digits
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(digits.len().saturating_sub(1));
    let unscaled: String = digits[start..]
        .iter()
        .map(|digit| char::from(b'0' + digit))
        .collect();
    let scale = i64::try_from(fractional_digits).unwrap_or(i64::MAX) - i64::from(exponent);
    let mut plain = if scale <= 0 {
        let zeros = usize::try_from(scale.unsigned_abs()).unwrap_or(0);
        format!("{unscaled}{}", "0".repeat(zeros))
    } else {
        let scale = usize::try_from(scale).unwrap_or(usize::MAX);
        if scale >= unscaled.len() {
            format!("0.{}{unscaled}", "0".repeat(scale - unscaled.len()))
        } else {
            let (integral, fractional) = unscaled.split_at(unscaled.len() - scale);
            format!("{integral}.{fractional}")
        }
    };
    if negative {
        plain.insert(0, '-');
    }
    plain
}

/// `Decimal.fromStringANSI`: `new BigDecimal(str.trim)` rounded HALF_UP to the target scale, and an
/// error when the result does not fit the precision.
fn parse_decimal(text: &str, precision: u8, scale: i8) -> Decimal {
    let trimmed = text.trim_matches(|c: char| (c as u32) <= 0x20);
    let (negative, rest) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(at) => match rest[at + 1..].parse::<i32>() {
            Ok(exponent) => (&rest[..at], exponent),
            Err(_) => return Decimal::Malformed,
        },
        None => (rest, 0),
    };
    let (integral, fractional) = match mantissa.split_once('.') {
        Some((integral, fractional)) => (integral, fractional),
        None => (mantissa, ""),
    };
    if integral.is_empty() && fractional.is_empty() {
        return Decimal::Malformed;
    }
    if !integral
        .chars()
        .chain(fractional.chars())
        .all(|c| c.is_ascii_digit())
    {
        return Decimal::Malformed;
    }
    let digits: Vec<u8> = integral
        .bytes()
        .chain(fractional.bytes())
        .map(|b| b - b'0')
        .collect();
    // `numDigitsInIntegralPart` is `precision - scale` of the `BigDecimal`: the digits from the
    // first non-zero one, minus the fractional digits, plus the exponent.
    let significant = digits
        .iter()
        .skip_while(|digit| **digit == 0)
        .count()
        .max(1) as i64;
    if significant + i64::from(exponent) - fractional.len() as i64 > i64::from(MAX_DECIMAL_DIGITS) {
        return Decimal::TooManyDigits;
    }
    // value = digits * 10^-(fractional.len() - exponent); we want it at 10^-scale.
    let shift = i64::from(scale) + i64::from(exponent) - fractional.len() as i64;
    let max = 10_i128
        .checked_pow(u32::from(precision))
        .unwrap_or(i128::MAX);
    let mut unscaled: i128 = 0;
    let mut round_up = false;
    let kept = if shift >= 0 {
        digits.len()
    } else {
        let drop = shift.unsigned_abs() as usize;
        if drop > digits.len() {
            0
        } else {
            digits.len() - drop
        }
    };
    let out_of_range =
        || Decimal::OutOfRange(plain_decimal(negative, &digits, fractional.len(), exponent));
    for &digit in &digits[..kept] {
        unscaled = match unscaled
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(digit)))
        {
            Some(value) => value,
            None => return out_of_range(),
        };
    }
    if shift < 0 {
        let drop = shift.unsigned_abs() as usize;
        if drop <= digits.len() {
            round_up = digits[kept] >= 5;
        }
    } else {
        for _ in 0..shift {
            unscaled = match unscaled.checked_mul(10) {
                Some(value) => value,
                None => return out_of_range(),
            };
        }
    }
    if round_up {
        unscaled = match unscaled.checked_add(1) {
            Some(value) => value,
            None => return out_of_range(),
        };
    }
    if unscaled >= max {
        return out_of_range();
    }
    Decimal::Value(if negative { -unscaled } else { unscaled })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longs_follow_the_spark_trim_and_digit_rules() {
        assert_eq!(parse_long(" 5 "), Some(5));
        assert_eq!(parse_long("1\u{B}"), Some(1));
        assert_eq!(parse_long("\u{1}7"), Some(7));
        assert_eq!(parse_long("+1"), Some(1));
        assert_eq!(parse_long("1.5"), None);
        assert_eq!(parse_long("1e3"), None);
        assert_eq!(parse_long("0x1"), None);
        assert_eq!(parse_long("1\u{A0}"), None);
        assert_eq!(parse_long("99999999999999999999"), None);
    }

    #[test]
    fn doubles_accept_the_java_suffixes() {
        assert_eq!(parse_double("1d"), Some(1.0));
        assert_eq!(parse_double("1.5F"), Some(1.5));
        assert_eq!(parse_double(" 1.5 "), Some(1.5));
        assert_eq!(parse_double("-inf"), Some(f64::NEG_INFINITY));
        assert!(parse_double("nan").is_some_and(f64::is_nan));
        assert_eq!(parse_double("infd"), None);
        assert_eq!(parse_double(""), None);
    }

    #[test]
    fn special_doubles_use_the_java_and_spark_spellings() {
        for text in ["NaN", "+NaN", "-NaN", "nan", "NAN"] {
            assert!(parse_double(text).is_some_and(f64::is_nan), "{text}");
        }
        assert_eq!(parse_double("Infinity"), Some(f64::INFINITY));
        assert_eq!(parse_double("+Infinity"), Some(f64::INFINITY));
        assert_eq!(parse_double("-Infinity"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_double("+inf"), Some(f64::INFINITY));
        assert_eq!(parse_double("INFINITY"), Some(f64::INFINITY));
        assert_eq!(parse_double("-iNf"), Some(f64::NEG_INFINITY));
        for text in ["+nan", "-nan", "+NAN", "nand", "Infinityd", "e5"] {
            assert_eq!(parse_double(text), None, "{text}");
        }
        assert_eq!(parse_double("1e999"), Some(f64::INFINITY));
    }

    #[test]
    fn booleans_use_the_ten_spark_tokens() {
        for token in ["t", "TRUE", " y ", "Yes", "1"] {
            assert_eq!(parse_boolean(token), Some(true), "{token}");
        }
        for token in ["f", "False", "n", "NO", "0"] {
            assert_eq!(parse_boolean(token), Some(false), "{token}");
        }
        for token in [
            "on",
            "tru",
            "of",
            "maybe",
            "",
            "true\u{A0}",
            "\u{2003}true",
            "true\u{1680}",
            "\u{85}true",
            "\u{3000}true",
        ] {
            assert_eq!(parse_boolean(token), None, "{token}");
        }
        assert_eq!(parse_boolean("\u{1}true"), Some(true));
    }

    #[test]
    fn decimals_round_half_up() {
        let value = |text: &str| match parse_decimal(text, 22, 2) {
            Decimal::Value(value) => Some(value),
            _ => None,
        };
        assert_eq!(value("1.555"), Some(156));
        assert_eq!(value("-1.555"), Some(-156));
        assert_eq!(value(" 1.5 "), Some(150));
        assert_eq!(value("1e2"), Some(10_000));
        assert_eq!(value("1.554"), Some(155));
        assert_eq!(value(".5"), Some(50));
        assert_eq!(value("abc"), None);
        assert!(matches!(
            parse_decimal("99999999999999999999999", 22, 2),
            Decimal::OutOfRange(_)
        ));
    }

    #[test]
    fn more_than_38_integral_digits_are_rejected_before_rounding() {
        let too_many = |text: &str, precision: u8, scale: i8| {
            matches!(
                parse_decimal(text, precision, scale),
                Decimal::TooManyDigits
            )
        };
        assert!(too_many("170141183460469231731687303715884105727.5", 38, 0));
        assert!(too_many(
            "-170141183460469231731687303715884105727.5",
            38,
            0
        ));
        assert!(too_many("170141183460469231731687303715884105727.5", 22, 2));
        assert!(too_many("100000000000000000000000000000000000000", 38, 0));
        assert!(too_many("1e39", 22, 2));
        assert!(!too_many("99999999999999999999999999999999999999", 38, 0));
        assert!(!too_many(
            "0.000000000000000000000000000000000000000001",
            22,
            2
        ));
        assert!(!too_many("0", 22, 2));
        // `BigDecimal` gives zero a precision of 1, so a large exponent counts for a zero too.
        assert!(too_many("0e100", 22, 2));
        assert!(too_many("0.0e100", 22, 2));
        assert!(too_many("0e2000000000", 22, 2));
        assert!(!too_many("0e-5", 22, 2));
        assert!(!too_many(
            "0000000000000000000000000000000000000000000",
            22,
            2
        ));
        assert!(matches!(
            parse_decimal("99999999999999999999999999999999999999", 22, 2),
            Decimal::OutOfRange(_)
        ));
    }

    fn long_list(field: &str) -> DataType {
        DataType::List(Arc::new(Field::new(field, DataType::Int64, true)))
    }

    #[test]
    fn every_list_flavor_goes_through_the_same_parsing() -> Result<()> {
        use datafusion::arrow::array::{FixedSizeListArray, StringArray};
        use datafusion::arrow::datatypes::Int64Type;

        // Two rows of two strings; the second row is NULL and holds a value that does not parse.
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "on", "x"]));
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let nulls = Some(NullBuffer::from(vec![true, false]));
        let fixed: ArrayRef = Arc::new(FixedSizeListArray::try_new(
            Arc::clone(&item),
            2,
            Arc::clone(&strings),
            nulls.clone(),
        )?);
        let large: ArrayRef = Arc::new(LargeListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0_i64, 2, 4].into()),
            Arc::clone(&strings),
            nulls,
        )?);
        let expected = vec![Some(vec![Some(1_i64), Some(2)]), None];
        for source in [&fixed, &large] {
            let converted = cast_array(source, &long_list("item"), None)?;
            let values = converted.as_list::<i32>();
            let rows: Vec<Option<Vec<Option<i64>>>> = (0..values.len())
                .map(|row| {
                    values.is_valid(row).then(|| {
                        values
                            .value(row)
                            .as_primitive::<Int64Type>()
                            .iter()
                            .collect()
                    })
                })
                .collect();
            assert_eq!(rows, expected);
        }
        let fixed_long =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2);
        let converted = cast_array(&fixed, &fixed_long, None)?;
        assert_eq!(converted.data_type(), &fixed_long);
        assert!(SparkAnsiStringCast::applies_to(
            fixed.data_type(),
            &fixed_long
        ));
        assert!(SparkAnsiStringCast::applies_to(
            large.data_type(),
            &long_list("item")
        ));
        Ok(())
    }

    #[test]
    fn a_visible_bad_element_of_a_fixed_size_list_raises() -> Result<()> {
        use datafusion::arrow::array::{FixedSizeListArray, StringArray};

        let strings: ArrayRef = Arc::new(StringArray::from(vec!["1", "on"]));
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let fixed: ArrayRef = Arc::new(FixedSizeListArray::try_new(item, 2, strings, None)?);
        let result = cast_array(&fixed, &long_list("item"), None);
        assert!(
            result.is_err_and(|error| error.to_string().contains("CAST_INVALID_INPUT")),
            "'on' is malformed"
        );
        Ok(())
    }

    #[test]
    fn an_out_of_range_decimal_is_reported_as_a_plain_number() {
        let reported = |text: &str, expected: &str| matches!(parse_decimal(text, 22, 2), Decimal::OutOfRange(number) if number == expected);
        assert!(reported("1e30", "1000000000000000000000000000000"));
        assert!(reported(
            "+99999999999999999999999",
            "99999999999999999999999"
        ));
        assert!(reported(
            "-99999999999999999999999",
            "-99999999999999999999999"
        ));
        assert!(reported(
            "123456789012345678901234.5",
            "123456789012345678901234.5"
        ));
        assert!(reported("5e23", "500000000000000000000000"));
        assert!(reported(
            "0.00000123456789012345678901234e30",
            "1234567890123456789012340"
        ));
    }

    #[test]
    fn rounding_up_the_largest_value_does_not_overflow() {
        // 38 integral digits and a rounding digit: the unscaled value stays within `i128`.
        assert!(matches!(
            parse_decimal("99999999999999999999999999999999999999.5", 38, 0),
            Decimal::OutOfRange(_)
        ));
    }
}
