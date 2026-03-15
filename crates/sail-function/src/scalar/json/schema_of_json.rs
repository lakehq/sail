use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
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
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                Volatility::Immutable,
            ),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() || args.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "schema_of_json",
                (1, 2),
                args.len(),
            ));
        }

        // First argument is the JSON string.
        // Spark requires the input to be a foldable (constant/literal) expression.
        // Second argument (optional) is a map of options - we ignore options for now.
        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let json_str = match scalar {
                    ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
                        s.as_deref()
                    }
                    ScalarValue::Null => None,
                    _ => return exec_err!("schema_of_json first argument must be a string"),
                };

                let schema = json_str
                    .map(infer_json_schema)
                    .unwrap_or_else(|| DDL_STRING.to_string());

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(schema))))
            }
            ColumnarValue::Array(_) => {
                exec_err!(
                    "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"schema_of_json()\" due to data type mismatch: the input `json` should be a foldable \"STRING\" expression; however, got a column reference."
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
fn infer_json_schema(json: &str) -> String {
    let mut jiter = Jiter::new(json.as_bytes());
    let result = match jiter.peek() {
        Ok(peek) => infer_type_from_peek(&mut jiter, peek),
        Err(_) => DDL_STRING.to_string(),
    };
    null_as_string(result)
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
        .map(|(name, typ)| format!("{name}: {typ}"))
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
            '<' => depth += 1,
            '>' => depth -= 1,
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

        fields.push(format!("{field_name}: {field_type}"));

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
