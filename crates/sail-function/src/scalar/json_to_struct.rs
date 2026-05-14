use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// A scalar UDF that takes a JSON string column and a fixed list of field names
/// (baked in at planning time) and returns a `Struct<c0: Utf8, c1: Utf8, ...>`
///
/// Every field in the returned struct is `Utf8` and nullable, matching Spark's
/// `json_tuple` semantics: all extracted values are strings, missing or null
/// keys produce SQL NULL
///
/// This UDF is internal to the `json_tuple` generator implementation.
/// It is wrapped with `Explode(ExplodeKind::Inline)` by the planner so that
/// the struct fields are expanded into individual output columns (`c0`, `c1`, …)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonToStruct {
    /// The JSON keys to extract, in order.  These become the struct field names
    /// (`c0`, `c1`, …) after `inline` expansion
    field_names: Vec<String>,
    /// Pre-computed return type so we only build it once
    return_type: DataType,
}

impl Hash for JsonToStruct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.field_names.hash(state);
        // return_type is derived from field_names, so we don't need to hash it
    }
}

impl JsonToStruct {
    pub fn new(field_names: Vec<String>) -> Self {
        let fields: Fields = field_names
            .iter()
            .enumerate()
            .map(|(i, _)| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>()
            .into();
        let return_type = DataType::Struct(fields);
        Self {
            field_names,
            return_type,
        }
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }
}

impl ScalarUDFImpl for JsonToStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_to_struct"
    }

    fn signature(&self) -> &Signature {
        // Store the signature as a static or in the struct
        use std::sync::OnceLock;
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(1, Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        // Materialise the JSON column into a StringArray.
        let json_array: ArrayRef = match &args[0] {
            ColumnarValue::Array(arr) => Arc::clone(arr),
            ColumnarValue::Scalar(sv) => sv.to_array_of_size(number_rows)?,
        };

        let json_strings = json_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_to_struct: first argument must be a Utf8 array".to_string(),
                )
            })?;

        let n_rows = json_strings.len();
        let n_fields = self.field_names.len();

        // One String builder per output field
        let mut builders: Vec<Vec<Option<String>>> =
            (0..n_fields).map(|_| Vec::with_capacity(n_rows)).collect();

        for i in 0..n_rows {
            if json_strings.is_null(i) {
                // NULL input → all fields NULL
                for col in &mut builders {
                    col.push(None);
                }
                continue;
            }

            let raw = json_strings.value(i);
            match serde_json::from_str::<serde_json::Value>(raw) {
                Ok(serde_json::Value::Object(map)) => {
                    for (j, key) in self.field_names.iter().enumerate() {
                        let extracted = map.get(key.as_str()).and_then(|v| match v {
                            serde_json::Value::Null => None,
                            serde_json::Value::String(s) => Some(s.clone()),
                            other => Some(other.to_string()),
                        });
                        builders[j].push(extracted);
                    }
                }
                _ => {
                    // Non-object JSON (array, scalar) or parse failure all NULL
                    for col in &mut builders {
                        col.push(None);
                    }
                }
            }
        }

        // Build one StringArray per field
        let field_arrays: Vec<(Arc<Field>, ArrayRef)> = self
            .field_names
            .iter()
            .enumerate()
            .map(|(i, _name)| {
                let field = Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true));
                let array: ArrayRef = Arc::new(StringArray::from(builders[i].clone()));
                (field, array)
            })
            .collect();

        if field_arrays.is_empty() {
            // Edge case: zero field names return an empty struct array
            let struct_array = StructArray::new_empty_fields(n_rows, None);
            return Ok(ColumnarValue::Array(Arc::new(struct_array)));
        }

        let struct_array = StructArray::from(field_arrays);
        Ok(ColumnarValue::Array(Arc::new(struct_array)))
    }
}
