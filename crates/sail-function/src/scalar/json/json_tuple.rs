use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

/// Extracts multiple values from a JSON string given a list of keys.
/// Returns a struct with fields named c0, c1, c2, etc.
///
/// Example: json_tuple('{"a":1, "b":2}', 'a', 'b') returns struct(c0: "1", c1: "2")
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonTuple {
    signature: Signature,
    num_keys: usize,
}

impl JsonTuple {
    pub fn new(num_keys: usize) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            num_keys,
        }
    }
}

impl ScalarUDFImpl for JsonTuple {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_tuple"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        let fields: Vec<Field> = (0..self.num_keys)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect();
        Ok(DataType::Struct(Fields::from(fields)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() < 2 {
            return exec_err!(
                "json_tuple requires at least 2 arguments: json string and at least one key"
            );
        }

        // Extract keys from arguments (all args after the first one)
        let keys: Vec<String> = args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Ok(s.clone()),
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
                | ColumnarValue::Scalar(ScalarValue::Null) => Ok(String::new()),
                _ => exec_err!("json_tuple keys must be string literals"),
            })
            .collect::<Result<Vec<_>>>()?;

        // Handle the JSON input
        match &args[0] {
            ColumnarValue::Array(json_array) => {
                json_tuple_array(json_array, &keys).map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(scalar) => {
                let json_str = match scalar {
                    ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
                        s.as_deref()
                    }
                    ScalarValue::Null => None,
                    _ => return exec_err!("json_tuple first argument must be a string"),
                };

                let values = extract_json_values(json_str, &keys);
                let arrays: Vec<ArrayRef> = values
                    .into_iter()
                    .map(|v| Arc::new(StringArray::from(vec![v])) as ArrayRef)
                    .collect();

                let fields: Vec<(Arc<Field>, ArrayRef)> = arrays
                    .into_iter()
                    .enumerate()
                    .map(|(i, arr)| {
                        (
                            Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true)),
                            arr,
                        )
                    })
                    .collect();

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                    StructArray::from(fields),
                ))))
            }
        }
    }
}

fn json_tuple_array(json_array: &ArrayRef, keys: &[String]) -> Result<ArrayRef> {
    let json_strings: Vec<Option<&str>> = match json_array.data_type() {
        DataType::Utf8 => {
            let arr = json_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to StringArray".to_string(),
                    )
                })?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        }
        DataType::LargeUtf8 => {
            let arr = json_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to LargeStringArray".to_string(),
                    )
                })?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        }
        DataType::Utf8View => {
            let arr = json_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to StringViewArray".to_string(),
                    )
                })?;
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        }
        other => {
            return exec_err!("json_tuple expects string input, got {:?}", other);
        }
    };

    let num_rows = json_strings.len();
    let num_keys = keys.len();

    // Create builders for each output column
    let mut builders: Vec<StringBuilder> = (0..num_keys)
        .map(|_| StringBuilder::with_capacity(num_rows, 0))
        .collect();

    // Process each row
    for json_str in &json_strings {
        let values = extract_json_values(*json_str, keys);
        for (i, value) in values.into_iter().enumerate() {
            builders[i].append_option(value);
        }
    }

    // Build the struct array
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();

    let fields: Vec<(Arc<Field>, ArrayRef)> = arrays
        .into_iter()
        .enumerate()
        .map(|(i, arr)| {
            (
                Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true)),
                arr,
            )
        })
        .collect();

    Ok(Arc::new(StructArray::from(fields)))
}

/// Extract values for the given keys from a JSON string.
/// Returns a vector of Option<String> with one element per key.
fn extract_json_values(json_str: Option<&str>, keys: &[String]) -> Vec<Option<String>> {
    let Some(json) = json_str else {
        return vec![None; keys.len()];
    };

    // Parse the JSON once and extract all keys
    let mut results = vec![None; keys.len()];

    let Ok(mut jiter) = Jiter::new(json.as_bytes())
        .peek()
        .map(|peek| (Jiter::new(json.as_bytes()), peek))
    else {
        return results;
    };

    let (ref mut jiter, peek) = jiter;

    if peek != Peek::Object {
        return results;
    }

    // Iterate through the object keys
    let Ok(first_key) = jiter.known_object() else {
        return results;
    };

    let Some(mut current_key) = first_key else {
        return results;
    };

    loop {
        // Check if this key matches any of our target keys
        for (i, target_key) in keys.iter().enumerate() {
            if current_key == target_key.as_str() && results[i].is_none() {
                // Extract the value
                if let Ok(peek) = jiter.peek() {
                    results[i] = extract_value_as_string(jiter, peek);
                }
                break;
            }
        }

        // Skip the value if we didn't extract it
        if jiter.next_skip().is_err() {
            break;
        }

        // Move to next key
        match jiter.next_key() {
            Ok(Some(key)) => current_key = key,
            _ => break,
        }
    }

    results
}

fn extract_value_as_string(jiter: &mut Jiter, peek: Peek) -> Option<String> {
    match peek {
        Peek::Null => {
            let _ = jiter.known_null();
            None
        }
        Peek::String => jiter.known_str().ok().map(|s| s.to_owned()),
        Peek::True | Peek::False => jiter.known_bool(peek).ok().map(|b| b.to_string()),
        Peek::Minus | Peek::Infinity | Peek::NaN => {
            let start = jiter.current_index();
            if jiter.known_skip(peek).is_ok() {
                let slice = jiter.slice_to_current(start);
                std::str::from_utf8(slice).ok().map(|s| s.to_owned())
            } else {
                None
            }
        }
        _ => {
            // For numbers and other values, get the raw string representation
            let start = jiter.current_index();
            if jiter.known_skip(peek).is_ok() {
                let slice = jiter.slice_to_current(start);
                std::str::from_utf8(slice).ok().map(|s| s.to_owned())
            } else {
                None
            }
        }
    }
}
