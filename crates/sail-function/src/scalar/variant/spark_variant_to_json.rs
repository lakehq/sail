use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/variant_to_json.rs>
use arrow::array::StringViewArray;
use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;

use crate::scalar::variant::spark_is_variant_null::try_field_as_variant_array;

/// Returns a JSON string from a VariantArray
///
/// ## Arguments
/// - expr: a DataType::Struct expression that represents a VariantArray
/// - options: an optional MAP (note, it seems arrow-rs' parquet-variant is pretty restrictive about the options)
#[derive(Debug, Hash, PartialEq, Eq)]
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
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                Volatility::Immutable,
            ),
        }
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
        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        try_field_as_variant_array(field.as_ref())?;

        let arg = args
            .args
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        let out = match arg {
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::Struct(variant_array) = scalar else {
                    return exec_err!("Unsupported data type: {}", scalar.data_type());
                };

                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let v = variant_array.value(0);

                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v.to_json_string()?)))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Struct(_) => {
                    let variant_array = VariantArray::try_new(arr.as_ref())?;

                    let out: StringViewArray = variant_array
                        .iter()
                        .map(|v| v.map(|v| v.to_json_string()).transpose())
                        .collect::<Result<Vec<_>, _>>()?
                        .into();

                    ColumnarValue::Array(Arc::new(out))
                }
                unsupported => return exec_err!("Invalid data type: {unsupported}"),
            },
        };

        Ok(out)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return exec_err!(
                "variant_to_json expects 1 or 2 arguments, got {}",
                arg_types.len()
            );
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
    use parquet_variant_json::JsonToVariant;
    use serde_json::Value;

    use super::*;
    fn build_variant_array_from_json(value: &Value) -> Result<VariantArray> {
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
}
