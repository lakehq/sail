use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringViewArray, StructArray};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::exec_datafusion_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion::scalar::ScalarValue;
use datafusion_expr_common::signature::Volatility;
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use parquet_variant_json::JsonToVariant as JsonToVariantExt;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::variant::spark_json_to_variant::convert_binaryview_to_binary;
use crate::scalar::variant::utils::helper::{try_field_as_string, try_parse_string_scalar};

/// Returns a Variant from a JSON string, or NULL if the JSON is invalid.
/// This is the safe version of `parse_json`.
///
/// Like Spark, trailing content after valid JSON is ignored (the valid prefix is parsed).
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#try_parse_json>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryParseJsonUdf {
    signature: Signature,
}

impl SparkTryParseJsonUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SparkTryParseJsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

/// Try to parse a JSON string leniently. If `serde_json::from_str` fails
/// (e.g. trailing garbage), try streaming parsing which reads the first
/// complete JSON value (matching Spark's behavior).
fn try_parse_json_lenient(json_str: &str) -> Option<serde_json::Value> {
    // First try strict parsing
    if let Ok(value) = serde_json::from_str(json_str) {
        return Some(value);
    }
    // Fallback: streaming parse — reads the first complete JSON token.
    // StreamDeserializer stops after the first complete value, ignoring trailing content.
    let mut stream = serde_json::Deserializer::from_str(json_str).into_iter::<serde_json::Value>();
    stream.next().and_then(|r| r.ok())
}

/// Try to append a JSON string to the builder. Returns true if successful.
fn try_append_json(builder: &mut VariantArrayBuilder, json_str: &str) -> bool {
    match try_parse_json_lenient(json_str) {
        Some(value) => {
            let json_str = value.to_string();
            builder.append_json(json_str.as_str()).is_ok()
        }
        None => false,
    }
}

impl ScalarUDFImpl for SparkTryParseJsonUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "try_parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ])))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = self.return_type(
            args.arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        Ok(Arc::new(
            Field::new(self.name(), data_type, true).with_extension_type(VariantType),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        try_field_as_string(arg_field.as_ref())?;

        let arg = args
            .args
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        let out = match arg {
            ColumnarValue::Scalar(scalar_value) => {
                let json_str = try_parse_string_scalar(scalar_value)?;

                let mut builder = VariantArrayBuilder::new(1);

                match json_str {
                    Some(json_str) => {
                        if !try_append_json(&mut builder, json_str.as_str()) {
                            builder.append_null();
                        }
                    }
                    None => builder.append_null(),
                }

                let struct_array: StructArray = builder.build().into();
                let struct_array = convert_binaryview_to_binary(struct_array)?;
                ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(struct_array)))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Utf8View => ColumnarValue::Array(try_from_utf8view_arr(arr)?),
                DataType::Null => {
                    let mut builder = VariantArrayBuilder::new(arr.len());
                    for _ in 0..arr.len() {
                        builder.append_null();
                    }
                    let struct_array: StructArray = builder.build().into();
                    let struct_array = convert_binaryview_to_binary(struct_array)?;
                    ColumnarValue::Array(Arc::new(struct_array) as ArrayRef)
                }
                _ => {
                    return Err(unsupported_data_type_exec_err(
                        "try_parse_json",
                        "string",
                        arr.data_type(),
                    ));
                }
            },
        };

        Ok(out)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "try_parse_json",
                (1, 1),
                arg_types.len(),
            ));
        }

        let coerced_type = match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8View,
            DataType::Null => DataType::Null,
            other => {
                return Err(unsupported_data_type_exec_err(
                    "try_parse_json",
                    "string",
                    other,
                ));
            }
        };

        Ok(vec![coerced_type])
    }
}

fn try_from_utf8view_arr(arr: &ArrayRef) -> Result<ArrayRef> {
    let typed_arr = arr
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "Unable to downcast array of type {} to StringViewArray",
                arr.data_type()
            )
        })?;

    let mut builder = VariantArrayBuilder::new(typed_arr.len());

    for v in typed_arr {
        match v {
            Some(json_str) => {
                if !try_append_json(&mut builder, json_str) {
                    builder.append_null();
                }
            }
            None => builder.append_null(),
        }
    }

    let variant_array: StructArray = builder.build().into();
    let variant_array = convert_binaryview_to_binary(variant_array)?;
    Ok(Arc::new(variant_array) as ArrayRef)
}
