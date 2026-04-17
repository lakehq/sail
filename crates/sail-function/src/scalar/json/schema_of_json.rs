use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, Array, MapArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

use crate::error::invalid_arg_count_exec_err;

/// Spark DDL type name constants for JSON schema inference.
const DDL_BIGINT: &str = "BIGINT";
const DDL_DOUBLE: &str = "DOUBLE";
const DDL_STRING: &str = "STRING";
const DDL_BOOLEAN: &str = "BOOLEAN";
const DDL_NULL: &str = "NULL";

/// Infers the schema of a JSON string and returns it in DDL format.
///
/// Example: schema_of_json('[{"col":0}]') returns 'ARRAY<STRUCT<col: BIGINT>>'
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfJson {
    signature: Signature,
}

impl Default for SparkSchemaOfJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn validate_args_are_literal(cols: &[ColumnarValue]) -> Result<()> {
        if let Some(ColumnarValue::Array(_)) = cols.first() {
            return exec_err!(
                "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"schema_of_json()\" \
                 due to data type mismatch: the input `json` should be a foldable \"STRING\" \
                 expression; however, got a column reference."
            );
        }
        Ok(())
    }

    fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => Ok(()),
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(map_field, _)] => {
                if let DataType::Struct(fields) = map_field.data_type() {
                    let key = &fields[0];
                    let value = &fields[1];
                    if !key.data_type().is_string() || !value.data_type().is_string() {
                        return exec_err!(
                            "schema_of_json options map keys/values must both be string type, got key: {}, value: {}",
                            key.data_type(),
                            value.data_type()
                        );
                    }
                    Ok(())
                } else {
                    exec_err!("schema_of_json: invalid arg types: {:?}", arg_types)
                }
            }
            [DataType::Null] => exec_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_NULL] The json must not be null."
            ),
            _ => exec_err!("schema_of_json: invalid arg types: {:?}", arg_types),
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "schema_of_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "schema_of_json",
                (1, 2),
                arg_types.len(),
            ));
        }
        Self::validate_arg_types(arg_types)?;
        let mut coerced = vec![DataType::Utf8];
        if arg_types.len() > 1 {
            coerced.push(DataType::Map(
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
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        Self::validate_args_are_literal(&args)?;

        // Parse and validate options from the optional second argument.
        // TODO: apply mode option to inference behavior.
        if args.len() > 1 {
            if let ColumnarValue::Scalar(ScalarValue::Map(map_arr)) = &args[1] {
                let _options =
                    SparkSchemaOfJsonOptions::default().map_to_options(map_arr.as_ref())?;
            }
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let json_str = match scalar {
                    ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
                        s.as_deref()
                    }
                    ScalarValue::Null => {
                        return exec_err!(
                            "[DATATYPE_MISMATCH.UNEXPECTED_NULL] The json must not be null."
                        );
                    }
                    _ => return exec_err!("schema_of_json first argument must be a string"),
                };

                let json_str = json_str.ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "[DATATYPE_MISMATCH.UNEXPECTED_NULL] The json must not be null."
                            .to_string(),
                    )
                })?;

                let schema = infer_json_schema(json_str)?;

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(schema))))
            }
            ColumnarValue::Array(_) => {
                exec_err!(
                    "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"schema_of_json()\" \
                     due to data type mismatch: the input `json` should be a foldable \"STRING\" \
                     expression; however, got a column reference."
                )
            }
        }
    }
}

/// In Spark's JSON schema inference, a bare `null` resolves to STRING.
fn null_as_string(t: String) -> String {
    if t == DDL_NULL {
        DDL_STRING.to_string()
    } else {
        t
    }
}

/// Infer the Spark SQL DDL schema from a JSON string.
fn infer_json_schema(json: &str) -> Result<String> {
    if json.is_empty() {
        return Ok(DDL_STRING.to_string());
    }
    let mut jiter = Jiter::new(json.as_bytes());
    let result = match jiter.peek() {
        Ok(peek) => infer_type_from_peek(&mut jiter, peek),
        Err(e) => {
            return exec_err!("Failed to parse JSON: {e}");
        }
    };
    Ok(null_as_string(result))
}

fn infer_type_from_peek(jiter: &mut Jiter, peek: Peek) -> String {
    match peek {
        Peek::Null => {
            let _ = jiter.known_null();
            DDL_NULL.to_string()
        }
        Peek::True | Peek::False => {
            let _ = jiter.known_bool(peek);
            DDL_BOOLEAN.to_string()
        }
        Peek::String => {
            let _ = jiter.known_str();
            DDL_STRING.to_string()
        }
        Peek::Minus => infer_number_type(jiter),
        Peek::Infinity | Peek::NaN => {
            let _ = jiter.known_float(peek);
            DDL_DOUBLE.to_string()
        }
        Peek::Array => infer_array_type(jiter),
        Peek::Object => infer_struct_type(jiter),
        _ => infer_number_type(jiter),
    }
}

fn infer_number_type(jiter: &mut Jiter) -> String {
    let start = jiter.current_index();
    if jiter.next_skip().is_err() {
        return DDL_BIGINT.to_string();
    }
    let slice = jiter.slice_to_current(start);
    let num_str = std::str::from_utf8(slice).unwrap_or("");

    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        DDL_DOUBLE.to_string()
    } else {
        // Check if integer fits in BIGINT range; if not, use DECIMAL(N,0)
        let digits = num_str.trim_start_matches('-');
        if digits.len() > 18 {
            // More than 18 digits might overflow BIGINT
            if num_str.parse::<i64>().is_err() {
                return format!("DECIMAL({},0)", digits.len());
            }
        }
        DDL_BIGINT.to_string()
    }
}

fn infer_array_type(jiter: &mut Jiter) -> String {
    let Ok(first_peek) = jiter.known_array() else {
        return format!("ARRAY<{DDL_STRING}>");
    };

    let Some(element_peek) = first_peek else {
        return format!("ARRAY<{DDL_STRING}>");
    };

    let mut element_type = infer_type_from_peek(jiter, element_peek);

    while let Ok(Some(peek)) = jiter.array_step() {
        let next_type = infer_type_from_peek(jiter, peek);
        element_type = common_supertype(&element_type, &next_type);
    }

    format!("ARRAY<{}>", null_as_string(element_type))
}

/// Returns the common supertype of two Spark DDL type strings.
/// Follows Spark's type promotion rules for JSON schema inference.
fn common_supertype(a: &str, b: &str) -> String {
    if a == b {
        return a.to_string();
    }
    match (a, b) {
        (DDL_NULL, other) | (other, DDL_NULL) => other.to_string(),
        (DDL_BIGINT, DDL_DOUBLE) | (DDL_DOUBLE, DDL_BIGINT) => DDL_DOUBLE.to_string(),
        (a, b) if a.starts_with("ARRAY<") && b.starts_with("ARRAY<") => {
            let inner_a = &a[6..a.len() - 1];
            let inner_b = &b[6..b.len() - 1];
            format!("ARRAY<{}>", common_supertype(inner_a, inner_b))
        }
        (a, b) if a.starts_with("STRUCT<") && b.starts_with("STRUCT<") => merge_struct_types(a, b),
        _ => DDL_STRING.to_string(),
    }
}

/// Merges two STRUCT DDL types: union of fields, common supertype for shared fields.
fn merge_struct_types(a: &str, b: &str) -> String {
    let fields_a = parse_struct_fields(a);
    let fields_b = parse_struct_fields(b);

    let mut merged: Vec<(String, String)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (name, type_a) in &fields_a {
        let merged_type = if let Some((_, type_b)) = fields_b.iter().find(|(n, _)| n == name) {
            common_supertype(type_a, type_b)
        } else {
            type_a.clone()
        };
        merged.push((name.clone(), merged_type));
        seen.insert(name.clone());
    }

    for (name, type_b) in &fields_b {
        if !seen.contains(name) {
            merged.push((name.clone(), type_b.clone()));
        }
    }

    merged.sort_by(|(a, _), (b, _)| a.cmp(b));
    let fields_str: Vec<String> = merged
        .into_iter()
        .map(|(name, typ)| format!("{}: {typ}", escape_field_name(&name)))
        .collect();
    format!("STRUCT<{}>", fields_str.join(", "))
}

/// Parses `STRUCT<name1: type1, name2: type2>` into vec of (name, type) pairs.
/// Handles nested angle brackets correctly.
fn parse_struct_fields(s: &str) -> Vec<(String, String)> {
    let inner = &s[7..s.len() - 1]; // strip "STRUCT<" and ">"
    if inner.is_empty() {
        return Vec::new();
    }

    let mut fields = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in inner.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                let field = inner[start..i].trim();
                if let Some((name, typ)) = field.split_once(": ") {
                    fields.push((name.to_string(), typ.to_string()));
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    let field = inner[start..].trim();
    if let Some((name, typ)) = field.split_once(": ") {
        fields.push((name.to_string(), typ.to_string()));
    }

    fields
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    Permissive,
    FailFast,
    DropMalformed,
}

impl ModeOptions {
    fn from_str(value: String) -> Result<Self> {
        match value.as_str() {
            "PERMISSIVE" => Ok(ModeOptions::Permissive),
            "FAILFAST" => Ok(ModeOptions::FailFast),
            "DROPMALFORMED" => Ok(ModeOptions::DropMalformed),
            other => exec_err!("Invalid mode option: {other}"),
        }
    }
}

#[derive(Debug, Default)]
struct SparkSchemaOfJsonOptions {
    mode: ModeOptions,
    _allow_numeric_leading_zeros: bool,
}

impl SparkSchemaOfJsonOptions {
    pub fn map_to_options(mut self, map_array: &MapArray) -> Result<Self> {
        let inner_struct = map_array.value(0);
        let (keys, values) = Self::get_keys_values_from_map(inner_struct)?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = Self::unwrap_or_key_value(key, value)?;
            match key {
                "mode" => self.mode = ModeOptions::from_str(value.to_string())?,
                "allowNumericLeadingZeros" => {
                    return exec_err!(
                        "schema_of_json currently doesn't support option allowNumericLeadingZeros"
                    );
                }
                other => {
                    return exec_err!(
                        "Found unsupported option type when parsing options: {other}"
                    );
                }
            }
        }
        Ok(self)
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
                    return exec_err!(
                        "Expected options to be type map<string, string> but found key type {:?} and value type {:?}",
                        key_type,
                        value_type
                    );
                }
            }
            other => {
                return exec_err!(
                    "options should be a map with an inner struct but instead got {:?}",
                    other
                );
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
            _ => exec_err!("Unexpected options key value pair: {:?}: {:?}", key, value),
        }
    }
}

/// Escape a field name with backticks if it contains special characters.
/// Spark backtick-escapes names with dots, spaces, and other non-alphanumeric chars.
fn escape_field_name(name: &str) -> String {
    let needs_escape = name
        .chars()
        .any(|c| !c.is_alphanumeric() && c != '_');
    if needs_escape {
        format!("`{name}`")
    } else {
        name.to_string()
    }
}

fn infer_struct_type(jiter: &mut Jiter) -> String {
    let Ok(first_key) = jiter.known_object() else {
        return "STRUCT<>".to_string();
    };

    let Some(mut current_key) = first_key else {
        return "STRUCT<>".to_string();
    };

    let mut fields = Vec::new();

    loop {
        let field_name = current_key.to_string();
        let field_type = match jiter.peek() {
            Ok(peek) => null_as_string(infer_type_from_peek(jiter, peek)),
            Err(_) => DDL_STRING.to_string(),
        };

        let escaped_name = escape_field_name(&field_name);
        fields.push(format!("{escaped_name}: {field_type}"));

        match jiter.next_key() {
            Ok(Some(key)) => current_key = key,
            _ => break,
        }
    }

    if fields.is_empty() {
        "STRUCT<>".to_string()
    } else {
        fields.sort();
        format!("STRUCT<{}>", fields.join(", "))
    }
}
