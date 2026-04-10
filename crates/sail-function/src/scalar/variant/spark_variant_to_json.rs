use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/variant_to_json.rs>
use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::utils::helper::try_field_as_variant_array;

/// Converts a variant ColumnarValue to a Utf8View JSON string representation.
/// This is the shared logic used by both `variant_to_json` and `to_json` (for variant inputs).
pub fn variant_to_json_columnar(arg: &ColumnarValue) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None))),
            ScalarValue::Struct(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                if variant_array.is_empty() {
                    return exec_err!("Cannot convert empty VariantArray to JSON: the array must contain at least one element");
                }
                if variant_array.is_null(0) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)))
                } else {
                    let v = variant_array.value(0);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                        v.to_json_string()?,
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
                for variant in variant_array.iter() {
                    match variant {
                        Some(v) => builder.append_value(v.to_json_string()?),
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
pub struct SparkVariantToJsonUdf {
    signature: Signature,
}

impl SparkVariantToJsonUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SparkVariantToJsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkVariantToJsonUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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

        variant_to_json_columnar(&args.args[0])
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
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::{VariantArrayBuilder, VariantType};
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

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

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

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkVariantToJsonUdf::default();

        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

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
        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
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
        let arr: StructArray = builder.build().into();

        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

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
