use std::iter::Peekable;
use std::str::Chars;
use std::sync::Arc;

use datafusion::arrow::array::{
    downcast_array, Array, ArrayRef, MapArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfJson {
    signature: Signature,
}

impl Default for SparkSchemaOfJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfJson {
    pub const SCHEMA_OF_JSON_NAME: &'static str = "schema_of_json";

    pub fn new() -> Self {
        SparkSchemaOfJson {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn validate_args_len<T>(args: &[T]) -> Result<()> {
        if args.is_empty() || args.len() > 2 {
            return plan_err!(
                "function `{}` expected 1 to 2 args but got {}",
                Self::SCHEMA_OF_JSON_NAME,
                args.len()
            );
        };
        Ok(())
    }

    fn validate_args_are_literal(cols: &[ColumnarValue]) -> Result<()> {
        if let Some(ColumnarValue::Array(_)) = cols.first() {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the first arg of `{}`, instead got a column",
                Self::SCHEMA_OF_JSON_NAME,
            )));
        }
        if let Some(ColumnarValue::Array(_)) = cols.get(1) {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the second arg of `{}`, instead got a column",
                Self::SCHEMA_OF_JSON_NAME,
            )));
        }
        Ok(())
    }

    fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => Ok(()),
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(map_field, _)] => {
                match map_field.data_type() {
                    DataType::Struct(fields) => {
                        let key = fields[0].clone();
                        let value = fields[1].clone();
                        if !key.data_type().is_string() || !value.data_type().is_string() {
                            return Err(DataFusionError::Plan(format!(
                                "For function `{}`, the options map keys/values should both be type string. Instead got key: {}, value: {}",
                                Self::SCHEMA_OF_JSON_NAME,
                                key.data_type(),
                                value.data_type(),
                            )));
                        }
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
            _ => plan_err!(
                "For function `{:?}` found invalid arg types: {:?}",
                Self::SCHEMA_OF_JSON_NAME,
                arg_types
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    fn name(&self) -> &str {
        Self::SCHEMA_OF_JSON_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::validate_args_len(&args.args)?;
        Self::validate_args_are_literal(&args.args)?;
        let hints = vec![Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(schema_of_json_inner, hints)(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Self::validate_args_len(arg_types)?;
        Self::validate_arg_types(arg_types)?;
        let mut coerce_to = vec![DataType::Utf8];
        if arg_types.len() > 1 {
            // Force map<k,v> → map<Utf8, Utf8>
            coerce_to.push(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ));
        }
        // utf8, optional<map>
        Ok(coerce_to)
    }
}

fn schema_of_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    SparkSchemaOfJson::validate_args_len(args)?;
    let rows = downcast_arg!(&args[0], StringArray);
    let options = if let Some(arg) = args.get(1) {
        let map_array = downcast_arg!(arg, MapArray);
        SparkSchemaOfJsonOptions::default().map_to_options(map_array)?
    } else {
        SparkSchemaOfJsonOptions::default()
    };
    let type_ddl = if rows.is_empty() {
        return Err(DataFusionError::Execution(
            "No value passed into input".to_string(),
        ));
    } else if rows.value(0).is_empty() {
        "STRING".to_string()
    } else {
        infer_json_schema_type(rows.value(0), &options)?
    };
    Ok(Arc::new(StringArray::from(vec![type_ddl])))
}

fn infer_json_schema_type(json_string: &str, options: &SparkSchemaOfJsonOptions) -> Result<String> {
    if json_string.trim().is_empty() {
        return Ok("STRING".to_string());
    }
    let mut parser = JsonSchemaParser::new(json_string, options);
    // Content after the first JSON value is ignored, matching Spark.
    let inferred = parser.parse_value()?;
    // Mirror Spark's `SchemaOfJsonEvaluator.evaluate`: structs and arrays of
    // structs fall back to an empty struct when fully canonicalized away,
    // and everything else falls back to a string.
    let ddl = match inferred {
        InferredType::Struct(_) => {
            canonicalize_ddl(&inferred).unwrap_or_else(|| "STRUCT<>".to_string())
        }
        InferredType::Array(element) if matches!(*element, InferredType::Struct(_)) => {
            canonicalize_ddl(&element)
                .map(|e| format!("ARRAY<{e}>"))
                .unwrap_or_else(|| "ARRAY<STRUCT<>>".to_string())
        }
        other => canonicalize_ddl(&other).unwrap_or_else(|| "STRING".to_string()),
    };
    Ok(ddl)
}

/// The maximum precision of Spark's `DecimalType`.
const MAX_DECIMAL_PRECISION: usize = 38;

/// A JSON type inferred from a literal JSON string, mirroring the types that
/// Spark's `JsonInferSchema` can produce for `schema_of_json`.
#[derive(Debug, Clone, PartialEq)]
enum InferredType {
    Null,
    Boolean,
    Long,
    /// A number inferred as a decimal with the given precision and scale.
    Decimal(u8, u8),
    Double,
    String,
    Array(Box<InferredType>),
    /// Fields are sorted by name and duplicate names are preserved, matching
    /// Spark's `JsonInferSchema`.
    Struct(Vec<(String, InferredType)>),
}

/// A parser that infers the Spark type of a literal JSON string, mirroring
/// the Jackson lexing behavior that Spark relies on for `schema_of_json`,
/// including features that strict JSON parsers reject: single-quoted strings,
/// unquoted field names, and non-numeric numbers (`NaN` and `Infinity`).
struct JsonSchemaParser<'a> {
    chars: Peekable<Chars<'a>>,
    options: &'a SparkSchemaOfJsonOptions,
}

impl<'a> JsonSchemaParser<'a> {
    fn new(json: &'a str, options: &'a SparkSchemaOfJsonOptions) -> Self {
        JsonSchemaParser {
            chars: json.chars().peekable(),
            options,
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.chars.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.chars.next();
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        match self.chars.next() {
            Some(c) if c == expected => Ok(()),
            Some(c) => exec_err!("expected `{expected}` but found `{c}`"),
            None => exec_err!("expected `{expected}` but found end of input"),
        }
    }

    fn expect_literal(&mut self, literal: &str) -> Result<()> {
        for expected in literal.chars() {
            match self.chars.next() {
                Some(c) if c == expected => {}
                Some(c) => return exec_err!("invalid character `{c}` in literal `{literal}`"),
                None => return exec_err!("unexpected end of input in literal `{literal}`"),
            }
        }
        Ok(())
    }

    fn parse_value(&mut self) -> Result<InferredType> {
        self.skip_whitespace();
        match self.chars.peek().copied() {
            None => exec_err!("unexpected end of input when parsing a JSON value"),
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => {
                self.parse_string('"')?;
                Ok(InferredType::String)
            }
            Some('\'') if self.options.allow_single_quotes => {
                self.parse_string('\'')?;
                Ok(InferredType::String)
            }
            Some('t') => {
                self.expect_literal("true")?;
                Ok(InferredType::Boolean)
            }
            Some('f') => {
                self.expect_literal("false")?;
                Ok(InferredType::Boolean)
            }
            Some('n') => {
                self.expect_literal("null")?;
                Ok(InferredType::Null)
            }
            Some('N') if self.options.allow_non_numeric_numbers => {
                self.expect_literal("NaN")?;
                Ok(InferredType::Double)
            }
            Some('I') if self.options.allow_non_numeric_numbers => {
                self.expect_literal("Infinity")?;
                Ok(InferredType::Double)
            }
            Some(sign @ ('-' | '+')) => {
                self.chars.next();
                if self.chars.peek() == Some(&'I') && self.options.allow_non_numeric_numbers {
                    self.expect_literal("Infinity")?;
                    Ok(InferredType::Double)
                } else if sign == '-' {
                    self.parse_number(true)
                } else {
                    // A `+` sign is only valid before `Infinity`.
                    exec_err!("unexpected character `+` when parsing a JSON value")
                }
            }
            Some(c) if c.is_ascii_digit() => self.parse_number(false),
            Some(c) => exec_err!("unexpected character `{c}` when parsing a JSON value"),
        }
    }

    fn parse_object(&mut self) -> Result<InferredType> {
        self.expect_char('{')?;
        let mut fields: Vec<(String, InferredType)> = Vec::new();
        self.skip_whitespace();
        if self.chars.peek() == Some(&'}') {
            self.chars.next();
            return Ok(InferredType::Struct(fields));
        }
        loop {
            self.skip_whitespace();
            let name = match self.chars.peek().copied() {
                Some('"') => self.parse_string('"')?,
                Some('\'') if self.options.allow_single_quotes => self.parse_string('\'')?,
                Some(c) if self.options.allow_unquoted_field_names && is_unquoted_name_char(c) => {
                    self.parse_unquoted_name()
                }
                Some(c) => {
                    return exec_err!("unexpected character `{c}` when parsing a field name")
                }
                None => return exec_err!("unexpected end of input when parsing a field name"),
            };
            self.skip_whitespace();
            self.expect_char(':')?;
            let value = self.parse_value()?;
            fields.push((name, value));
            self.skip_whitespace();
            match self.chars.next() {
                Some(',') => {}
                Some('}') => break,
                Some(c) => return exec_err!("expected `,` or `}}` but found `{c}`"),
                None => return exec_err!("unexpected end of input when parsing an object"),
            }
        }
        // Spark sorts struct fields by name during inference and keeps
        // duplicate names.
        fields.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(InferredType::Struct(fields))
    }

    fn parse_array(&mut self) -> Result<InferredType> {
        self.expect_char('[')?;
        let mut element = InferredType::Null;
        self.skip_whitespace();
        if self.chars.peek() == Some(&']') {
            self.chars.next();
            return Ok(InferredType::Array(Box::new(element)));
        }
        loop {
            let value = self.parse_value()?;
            element = merge_types(element, value);
            self.skip_whitespace();
            match self.chars.next() {
                Some(',') => {}
                Some(']') => break,
                Some(c) => return exec_err!("expected `,` or `]` but found `{c}`"),
                None => return exec_err!("unexpected end of input when parsing an array"),
            }
        }
        Ok(InferredType::Array(Box::new(element)))
    }

    /// Parses a string enclosed in the given quote character and returns its
    /// decoded value. Single-quoted strings additionally allow the `\'`
    /// escape, matching Jackson.
    fn parse_string(&mut self, quote: char) -> Result<String> {
        self.expect_char(quote)?;
        let mut value = String::new();
        loop {
            match self.chars.next() {
                None => return exec_err!("unexpected end of input when parsing a string"),
                Some(c) if c == quote => return Ok(value),
                Some('\\') => value.push(self.parse_escape(quote)?),
                Some(c) if (c as u32) < 0x20 => {
                    return exec_err!("unescaped control character in a string")
                }
                Some(c) => value.push(c),
            }
        }
    }

    fn parse_escape(&mut self, quote: char) -> Result<char> {
        match self.chars.next() {
            Some('"') => Ok('"'),
            Some('\\') => Ok('\\'),
            Some('/') => Ok('/'),
            Some('b') => Ok('\u{0008}'),
            Some('f') => Ok('\u{000C}'),
            Some('n') => Ok('\n'),
            Some('r') => Ok('\r'),
            Some('t') => Ok('\t'),
            Some('\'') if quote == '\'' => Ok('\''),
            Some('u') => {
                let code = self.parse_unicode_escape()?;
                match code {
                    0xD800..=0xDBFF => {
                        // A high surrogate must be followed by a low surrogate.
                        if self.chars.next() != Some('\\') || self.chars.next() != Some('u') {
                            return exec_err!("unpaired surrogate in a unicode escape");
                        }
                        let low = self.parse_unicode_escape()?;
                        if !(0xDC00..=0xDFFF).contains(&low) {
                            return exec_err!("unpaired surrogate in a unicode escape");
                        }
                        let c = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                        char::from_u32(c).ok_or_else(|| {
                            DataFusionError::Execution("invalid unicode escape".to_string())
                        })
                    }
                    0xDC00..=0xDFFF => exec_err!("unpaired surrogate in a unicode escape"),
                    _ => char::from_u32(code).ok_or_else(|| {
                        DataFusionError::Execution("invalid unicode escape".to_string())
                    }),
                }
            }
            Some(c) => exec_err!("invalid escape character `{c}` in a string"),
            None => exec_err!("unexpected end of input in a string escape"),
        }
    }

    fn parse_unicode_escape(&mut self) -> Result<u32> {
        let mut code = 0;
        for _ in 0..4 {
            let digit = self
                .chars
                .next()
                .and_then(|c| c.to_digit(16))
                .ok_or_else(|| {
                    DataFusionError::Execution("invalid unicode escape in a string".to_string())
                })?;
            code = code * 16 + digit;
        }
        Ok(code)
    }

    fn parse_unquoted_name(&mut self) -> String {
        let mut name = String::new();
        while let Some(c) = self.chars.peek().copied() {
            if is_unquoted_name_char(c) {
                name.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        name
    }

    /// Parses a number; the leading `-` sign must already be consumed.
    /// Mirrors Spark's number type inference: integral values that fit in a
    /// long are `BIGINT`, wider integral values are `DECIMAL(p, 0)` up to the
    /// maximum precision, and everything else is `DOUBLE`.
    fn parse_number(&mut self, negative: bool) -> Result<InferredType> {
        let mut digits = String::new();
        while let Some(c) = self.chars.peek().copied() {
            if c.is_ascii_digit() {
                digits.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        if digits.is_empty() {
            return exec_err!("a number must contain at least one digit");
        }
        if digits.len() > 1 && digits.starts_with('0') && !self.options.allow_numeric_leading_zeros
        {
            return exec_err!("leading zeros are not allowed in numbers");
        }
        let mut fraction = String::new();
        let mut exponent: Option<i64> = None;
        if self.chars.peek() == Some(&'.') {
            self.chars.next();
            while matches!(self.chars.peek(), Some(c) if c.is_ascii_digit()) {
                fraction.push(self.chars.next().unwrap_or_default());
            }
            if fraction.is_empty() {
                return exec_err!("a number cannot end with a decimal point");
            }
        }
        if matches!(self.chars.peek(), Some('e' | 'E')) {
            self.chars.next();
            let mut text = String::new();
            if matches!(self.chars.peek(), Some('-' | '+')) {
                text.push(self.chars.next().unwrap_or_default());
            }
            let mut exponent_digits = 0;
            while matches!(self.chars.peek(), Some(c) if c.is_ascii_digit()) {
                text.push(self.chars.next().unwrap_or_default());
                exponent_digits += 1;
            }
            if exponent_digits == 0 {
                return exec_err!("an exponent must contain at least one digit");
            }
            // An exponent too large for `i64` cannot produce a valid decimal
            // scale anyway.
            exponent = Some(text.parse::<i64>().unwrap_or(i64::MAX));
        }
        if !fraction.is_empty() || exponent.is_some() {
            // Spark infers a decimal type for floating-point numbers when the
            // `prefersDecimal` option is set, using the precision and scale
            // of the value interpreted as a Java `BigDecimal`: the scale is
            // the number of fraction digits minus the exponent (a negative
            // scale is an error), and the precision counts the unscaled
            // digits without leading zeros, floored at the scale.
            if self.options.prefers_decimal {
                let mut significant = digits.clone();
                significant.push_str(&fraction);
                let significant = significant.trim_start_matches('0');
                let scale = (fraction.len() as i64).saturating_sub(exponent.unwrap_or(0));
                if scale < 0 {
                    return plan_err!("Negative scale is not allowed: '{scale}'");
                }
                let precision = (significant.len() as i64).max(1).max(scale);
                if precision <= MAX_DECIMAL_PRECISION as i64 {
                    return Ok(InferredType::Decimal(precision as u8, scale as u8));
                }
            }
            return Ok(InferredType::Double);
        }
        let value = if negative {
            format!("-{digits}")
        } else {
            digits.clone()
        };
        // Leading zeros do not count toward the decimal precision.
        let digit_count = digits.trim_start_matches('0').len().max(1);
        if value.parse::<i64>().is_ok() {
            Ok(InferredType::Long)
        } else if digit_count <= MAX_DECIMAL_PRECISION {
            Ok(InferredType::Decimal(digit_count as u8, 0))
        } else {
            Ok(InferredType::Double)
        }
    }
}

/// The characters that Jackson accepts in unquoted field names: ASCII
/// alphanumeric characters, `_$@#*+-`, and all non-ASCII characters.
fn is_unquoted_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(c, '_' | '$' | '@' | '#' | '*' | '+' | '-')
        || !c.is_ascii()
}

/// Returns the most specific type that both types can be promoted to,
/// mirroring Spark's `JsonInferSchema.compatibleType`.
fn merge_types(left: InferredType, right: InferredType) -> InferredType {
    use InferredType::*;
    match (left, right) {
        (Null, t) | (t, Null) => t,
        (l, r) if l == r => l,
        (Long, Double) | (Double, Long) => Double,
        // A long is at most `DECIMAL(20, 0)` when promoted to a decimal.
        (Long, Decimal(p, s)) | (Decimal(p, s), Long) => merge_decimals(p.max(20), s, 20, 0),
        (Double, Decimal(_, _)) | (Decimal(_, _), Double) => Double,
        (Decimal(p1, s1), Decimal(p2, s2)) => merge_decimals(p1, s1, p2, s2),
        (Array(l), Array(r)) => Array(Box::new(merge_types(*l, *r))),
        (Struct(l), Struct(r)) => Struct(merge_fields(l, r)),
        _ => String,
    }
}

/// Merges two decimal types by keeping enough integer digits and scale to
/// hold both, falling back to `DOUBLE` when the maximum precision is
/// exceeded, mirroring Spark's `JsonInferSchema.compatibleType`.
fn merge_decimals(p1: u8, s1: u8, p2: u8, s2: u8) -> InferredType {
    let integer_digits = (p1 - s1).max(p2 - s2);
    let scale = s1.max(s2);
    if (integer_digits + scale) as usize > MAX_DECIMAL_PRECISION {
        InferredType::Double
    } else {
        InferredType::Decimal(integer_digits + scale, scale)
    }
}

/// Merges the fields of two structs by name, mirroring Spark's behavior of
/// grouping fields by name and reducing each group with `compatibleType`.
fn merge_fields(
    left: Vec<(String, InferredType)>,
    right: Vec<(String, InferredType)>,
) -> Vec<(String, InferredType)> {
    let mut fields = left;
    fields.extend(right);
    fields.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut merged: Vec<(String, InferredType)> = Vec::new();
    for (name, t) in fields {
        match merged.last_mut() {
            Some((last_name, last_type)) if *last_name == name => {
                *last_type = merge_types(last_type.clone(), t);
            }
            _ => merged.push((name, t)),
        }
    }
    merged
}

/// Writes the type as a Spark DDL string, returning `None` for types that
/// Spark drops during canonicalization (empty structs, fields with empty
/// names, and arrays of dropped types). `NULL` types become `STRING`.
fn canonicalize_ddl(t: &InferredType) -> Option<String> {
    match t {
        InferredType::Null => Some("STRING".to_string()),
        InferredType::Boolean => Some("BOOLEAN".to_string()),
        InferredType::Long => Some("BIGINT".to_string()),
        InferredType::Decimal(p, s) => Some(format!("DECIMAL({p},{s})")),
        InferredType::Double => Some("DOUBLE".to_string()),
        InferredType::String => Some("STRING".to_string()),
        InferredType::Array(element) => canonicalize_ddl(element).map(|e| format!("ARRAY<{e}>")),
        InferredType::Struct(fields) => {
            let fields = fields
                .iter()
                .filter(|(name, _)| !name.is_empty())
                .filter_map(|(name, t)| {
                    canonicalize_ddl(t).map(|d| format!("{}: {d}", quote_if_needed(name)))
                })
                .collect::<Vec<_>>();
            if fields.is_empty() {
                None
            } else {
                Some(format!("STRUCT<{}>", fields.join(", ")))
            }
        }
    }
}

/// Quotes a field name with backticks unless it is a valid identifier,
/// mirroring Spark's `quoteIfNeeded`.
fn quote_if_needed(name: &str) -> String {
    let mut chars = name.chars();
    let valid = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if valid {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    Permissive,
    FailFast,
    DropMalformed,
}

impl ModeOptions {
    fn from_str(value: String) -> Result<Self, DataFusionError> {
        match value.as_str() {
            "PERMISSIVE" => Ok(ModeOptions::Permissive),
            "FAILFAST" => Ok(ModeOptions::FailFast),
            "DROPMALFORMED" => Ok(ModeOptions::DropMalformed),
            other => plan_err!("Invalid mode option: {other}"),
        }
    }
}

#[derive(Debug)]
struct SparkSchemaOfJsonOptions {
    mode: ModeOptions,
    allow_unquoted_field_names: bool,
    allow_single_quotes: bool,
    allow_non_numeric_numbers: bool,
    prefers_decimal: bool,
    allow_numeric_leading_zeros: bool,
}

impl Default for SparkSchemaOfJsonOptions {
    fn default() -> Self {
        SparkSchemaOfJsonOptions {
            mode: ModeOptions::default(),
            allow_unquoted_field_names: false,
            // Spark enables `allowSingleQuotes` and `allowNonNumericNumbers`
            // by default.
            allow_single_quotes: true,
            allow_non_numeric_numbers: true,
            prefers_decimal: false,
            allow_numeric_leading_zeros: false,
        }
    }
}

impl SparkSchemaOfJsonOptions {
    pub fn map_to_options(mut self, map_array: &MapArray) -> Result<Self> {
        let inner_struct = map_array.value(0);
        // validate map is of type map<string, string>
        let (keys, values) = Self::get_keys_values_from_map(inner_struct)?;
        // match each k v pair
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = Self::unwrap_or_key_value(key, value)?;
            // Spark reads JSON options through a case-insensitive map.
            match key.to_lowercase().as_str() {
                "mode" => self.mode = ModeOptions::from_str(value.to_string())?,
                "allowunquotedfieldnames" => {
                    self.allow_unquoted_field_names = Self::parse_boolean_option(key, value)?;
                }
                "allowsinglequotes" => {
                    self.allow_single_quotes = Self::parse_boolean_option(key, value)?;
                }
                "allownonnumericnumbers" => {
                    self.allow_non_numeric_numbers = Self::parse_boolean_option(key, value)?;
                }
                "prefersdecimal" => {
                    self.prefers_decimal = Self::parse_boolean_option(key, value)?;
                }
                "allownumericleadingzeros" => {
                    self.allow_numeric_leading_zeros = Self::parse_boolean_option(key, value)?;
                }
                // TODO: support the remaining Spark JSON options below
                //
                // These options change the inferred schema or the set of
                // accepted inputs when enabled, so fail instead of silently
                // producing a result that diverges from Spark. They all
                // default to false in Spark, so an explicit false is a no-op
                // and falls through to the ignored-options arm below.
                "primitivesasstring"
                | "allowcomments"
                | "allowbackslashescapinganycharacter"
                | "allowunquotedcontrolchars"
                | "dropfieldifallnull"
                | "infertimestamp"
                    if Self::parse_boolean_option(key, value)? =>
                {
                    return Err(DataFusionError::NotImplemented(format!(
                        "`{}` does not support the option `{key}`",
                        SparkSchemaOfJson::SCHEMA_OF_JSON_NAME,
                    )));
                }
                _ => {
                    // Unknown options are silently ignored, matching Spark:
                    // `JSONOptions` only does `parameters.get(...)` lookups on
                    // the keys it knows and never validates the rest.
                }
            }
        }
        Ok(self)
    }

    fn parse_boolean_option(key: &str, value: &str) -> Result<bool> {
        // Spark parses boolean options with Scala's `toBoolean`, which is
        // case-insensitive and rejects anything other than "true"/"false".
        match value.to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => plan_err!(
                "Invalid boolean value `{value}` for option `{key}` in `{}`",
                SparkSchemaOfJson::SCHEMA_OF_JSON_NAME
            ),
        }
    }

    fn get_keys_values_from_map(inner_struct: StructArray) -> Result<(StringArray, StringArray)> {
        let (keys, values) = match inner_struct.data_type() {
            DataType::Struct(fields) => {
                let key_type = fields[0].data_type();
                let value_type = fields[1].data_type();
                if key_type == &DataType::Utf8 && value_type == &DataType::Utf8 {
                    let keys = downcast_array::<StringArray>(inner_struct.column(0));
                    let values = downcast_array::<StringArray>(inner_struct.column(1));
                    (keys, values)
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Expected options to be type map<string, string> but found key type {:?} and value type {:?}",
                        key_type,
                        value_type
                    )))
                }
            },
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Should be unreachable: options should be a map with an inner struct but instead got {:?}",
                    other
                )))
            }
        };
        Ok((keys, values))
    }

    fn unwrap_or_key_value<'a>(
        key: Option<&'a str>,
        value: Option<&'a str>,
    ) -> Result<(&'a str, &'a str)> {
        match (key, value) {
            (Some(k), Some(v)) => Ok((k, v)),
            _ => Err(DataFusionError::Plan(format!(
                "Unexpected options key value pair: {:?}: {:?}",
                key, value
            ))),
        }
    }
}
