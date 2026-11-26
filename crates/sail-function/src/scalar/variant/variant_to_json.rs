// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/variant_to_json.rs

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant_compute::VariantArray;
use sail_common_datafusion::utils::datetime::SparkZone;

use super::variant_get::write_variant_json;
use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::utils::helper::try_field_as_variant_array;

/// Converts a variant ColumnarValue to a Utf8View JSON string representation.
/// This is the shared logic used by both `variant_to_json` and `to_json` (for variant inputs).
pub fn variant_to_json_columnar(
    arg: &ColumnarValue,
    timezone: &SparkZone,
) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None))),
            ScalarValue::Struct(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                if variant_array.is_empty() {
                    return exec_err!(
                        "Cannot convert empty VariantArray to JSON: the array must contain at least one element"
                    );
                }
                if variant_array.is_null(0) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)))
                } else {
                    let v = variant_array.value(0);
                    let mut json = Vec::new();
                    write_variant_json(v, timezone, &mut json)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                        String::from_utf8(json).map_err(|e| exec_datafusion_err!("{e}"))?,
                    ))))
                }
            }
            _ => exec_err!("Unsupported data type: {}", scalar.data_type()),
        },
        ColumnarValue::Array(arr) => match arr.data_type() {
            DataType::Struct(_) => {
                let variant_array = VariantArray::try_new(arr.as_ref())?;
                let mut builder =
                    arrow::array::StringViewBuilder::with_capacity(variant_array.len());
                let mut json = Vec::new();
                for variant in variant_array.iter() {
                    match variant {
                        Some(v) => {
                            json.clear();
                            write_variant_json(v, timezone, &mut json)?;
                            builder.append_value(
                                std::str::from_utf8(&json)
                                    .map_err(|e| exec_datafusion_err!("{e}"))?,
                            );
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            unsupported => exec_err!("Invalid data type: {unsupported}"),
        },
    }
}

/// Returns a JSON string from a VariantArray
///
/// ## Arguments
/// - expr: a DataType::Struct expression that represents a VariantArray
/// - options: an optional MAP (not yet implemented — currently rejected at runtime)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VariantToJsonUdf {
    timezone: Arc<str>,
    signature: Signature,
}

impl VariantToJsonUdf {
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            timezone,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for VariantToJsonUdf {
    fn default() -> Self {
        Self::new("UTC".into())
    }
}

impl ScalarUDFImpl for VariantToJsonUdf {
    fn name(&self) -> &str {
        "variant_to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Spark: "If expr is a VARIANT, the options are ignored."
        // https://docs.databricks.com/en/sql/language-manual/functions/to_json.html

        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("missing argument field metadata"))?;

        try_field_as_variant_array(field.as_ref())?;

        variant_to_json_columnar(&args.args[0], &SparkZone::parse(&self.timezone)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "variant_to_json",
                (1, 2),
                arg_types.len(),
            ));
        }

        // Accept the variant type as-is (it's a Struct with extension type)
        // If there's a second argument (options MAP), accept it as-is too
        Ok(arg_types.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use parquet_variant_compute::VariantArrayBuilder;
    use serde_json::Value;

    use super::*;
    fn build_variant_array_from_json(value: &Value) -> Result<VariantArray> {
        use parquet_variant_json::JsonToVariant;

        let json_str = value.to_string();
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json_str.as_str())?;

        Ok(builder.build())
    }

    #[test]
    fn test_scalar_primitive() -> Result<()> {
        let expected_json = serde_json::json!("norm");
        let input = build_variant_array_from_json(&expected_json)?;

        let arg_field = Arc::new(input.field("input"));
        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = VariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            return exec_err!("expected valid json string");
        };

        assert_eq!(j.as_str(), r#""norm""#);
        Ok(())
    }

    #[test]
    fn test_variant_to_json_udf_scalar_complex() -> Result<()> {
        let expected_json = serde_json::json!({
            "name": "norm",
            "age": 50,
            "list": [false, true, ()]
        });

        let input = build_variant_array_from_json(&expected_json)?;

        let arg_field = Arc::new(input.field("input"));
        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = VariantToJsonUdf::default();

        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            return exec_err!("expected valid json string");
        };

        let json: Value = serde_json::from_str(j.as_str())
            .map_err(|e| exec_datafusion_err!("failed to parse json: {}", e))?;
        assert_eq!(json, expected_json);
        Ok(())
    }

    #[test]
    fn test_scalar_null_returns_null() -> Result<()> {
        let udf = VariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            VariantArrayBuilder::new(0)
                .build()
                .field("input")
                .with_nullable(true),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Null)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Scalar(ScalarValue::Utf8View(None)) = result else {
            return exec_err!("expected NULL Utf8View");
        };
        Ok(())
    }

    #[test]
    fn test_columnar_with_nulls() -> Result<()> {
        use arrow::array::{Array, ArrayRef, StringViewArray, StructArray};
        use parquet_variant_json::JsonToVariant;

        let mut builder = VariantArrayBuilder::new(3);
        builder.append_json(r#"{"a":1}"#)?;
        builder.append_null();
        builder.append_json(r#""hello""#)?;
        let input = builder.build();
        let arg_field = Arc::new(input.field("input"));
        let arr: StructArray = input.into();

        let udf = VariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr) as ArrayRef)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Array(arr) = result else {
            return exec_err!("expected Array");
        };
        let str_arr = arr
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| exec_datafusion_err!("expected StringViewArray"))?;

        assert_eq!(str_arr.len(), 3);
        assert_eq!(str_arr.value(0), r#"{"a":1}"#);
        assert!(str_arr.is_null(1));
        assert_eq!(str_arr.value(2), r#""hello""#);
        Ok(())
    }
}
