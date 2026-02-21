use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

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
            signature: Signature::variadic_any(Volatility::Immutable),
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

        if args.is_empty() {
            return exec_err!("schema_of_json requires at least 1 argument");
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
                    .unwrap_or_else(|| "STRING".to_string());

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

/// Infer the Spark SQL DDL schema from a JSON string.
fn infer_json_schema(json: &str) -> String {
    let mut jiter = Jiter::new(json.as_bytes());
    match jiter.peek() {
        Ok(peek) => infer_type_from_peek(&mut jiter, peek),
        Err(_) => "STRING".to_string(),
    }
}

fn infer_type_from_peek(jiter: &mut Jiter, peek: Peek) -> String {
    match peek {
        Peek::Null => {
            let _ = jiter.known_null();
            "STRING".to_string()
        }
        Peek::True | Peek::False => {
            let _ = jiter.known_bool(peek);
            "BOOLEAN".to_string()
        }
        Peek::String => {
            let _ = jiter.known_str();
            "STRING".to_string()
        }
        Peek::Minus => {
            // Could be negative number
            infer_number_type(jiter)
        }
        Peek::Infinity | Peek::NaN => {
            let _ = jiter.known_float(peek);
            "DOUBLE".to_string()
        }
        Peek::Array => infer_array_type(jiter),
        Peek::Object => infer_struct_type(jiter),
        _ => {
            // Likely a number starting with a digit
            infer_number_type(jiter)
        }
    }
}

fn infer_number_type(jiter: &mut Jiter) -> String {
    // Try to determine if it's an integer or float by looking at the raw value
    let start = jiter.current_index();
    if jiter.next_skip().is_err() {
        return "BIGINT".to_string();
    }
    let slice = jiter.slice_to_current(start);
    let num_str = std::str::from_utf8(slice).unwrap_or("");

    // If it contains a decimal point or exponent, it's a double
    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        "DOUBLE".to_string()
    } else {
        "BIGINT".to_string()
    }
}

fn infer_array_type(jiter: &mut Jiter) -> String {
    let Ok(first_peek) = jiter.known_array() else {
        return "ARRAY<STRING>".to_string();
    };

    let Some(element_peek) = first_peek else {
        // Empty array
        return "ARRAY<STRING>".to_string();
    };

    let element_type = infer_type_from_peek(jiter, element_peek);

    // Skip remaining elements
    while let Ok(Some(peek)) = jiter.array_step() {
        let _ = jiter.known_skip(peek);
    }

    format!("ARRAY<{element_type}>")
}

fn infer_struct_type(jiter: &mut Jiter) -> String {
    let Ok(first_key) = jiter.known_object() else {
        return "STRUCT<>".to_string();
    };

    let Some(mut current_key) = first_key else {
        // Empty object
        return "STRUCT<>".to_string();
    };

    let mut fields = Vec::new();

    loop {
        let field_name = current_key.to_string();
        let field_type = match jiter.peek() {
            Ok(peek) => infer_type_from_peek(jiter, peek),
            Err(_) => "STRING".to_string(),
        };

        // Skip the value we just peeked
        let _ = jiter.next_skip();

        fields.push(format!("{field_name}: {field_type}"));

        match jiter.next_key() {
            Ok(Some(key)) => current_key = key,
            _ => break,
        }
    }

    if fields.is_empty() {
        "STRUCT<>".to_string()
    } else {
        // Sort fields alphabetically by field name (Spark behavior)
        fields.sort();
        format!("STRUCT<{}>", fields.join(", "))
    }
}
