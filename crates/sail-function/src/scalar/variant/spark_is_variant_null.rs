use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/is_variant_null.rs>
use arrow::array::{Array, ArrayRef};
use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::utils::helper::{try_field_as_variant_array, try_parse_variant_scalar};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIsVariantNullUdf {
    signature: Signature,
}

impl Default for SparkIsVariantNullUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIsVariantNullUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkIsVariantNullUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "is_variant_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected 1 argument field type"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        let [variant_arg] = args.args.as_slice() else {
            return exec_err!("expected 1 argument");
        };

        let out = match variant_arg {
            ColumnarValue::Scalar(scalar_variant) => match scalar_variant {
                // SQL NULL is not a Variant null, so return false
                ScalarValue::Null => ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
                ScalarValue::Struct(arc_struct) if arc_struct.is_null(0) => {
                    // Struct marked as NULL (from parse_json(null)) is also not a Variant null
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))
                }
                ScalarValue::Struct(_) => {
                    let variant_array = try_parse_variant_scalar(scalar_variant)?;
                    let variant = variant_array.value(0);
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(variant == Variant::Null)))
                }
                unsupported => {
                    return exec_err!(
                        "expected variant scalar value, got data type: {}",
                        unsupported.data_type()
                    );
                }
            },
            ColumnarValue::Array(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;

                let mut builder = arrow::array::BooleanBuilder::with_capacity(variant_array.len());
                for variant_option in variant_array.iter() {
                    match variant_option {
                        None => builder.append_value(false),
                        Some(variant) => builder.append_value(variant == Variant::Null),
                    }
                }
                let boolean_array = builder.finish();

                ColumnarValue::Array(Arc::new(boolean_array) as ArrayRef)
            }
        };

        Ok(out)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "is_variant_null",
                (1, 1),
                arg_types.len(),
            ));
        }

        // Accept the variant type as-is (it's a Struct with extension type)
        Ok(vec![arg_types[0].clone()])
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, StructArray};
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::{VariantArrayBuilder, VariantType};

    use super::*;
    fn build_variant_array_from_json(value: &serde_json::Value) -> Result<VariantArray> {
        use parquet_variant_json::JsonToVariant;

        let json_str = value.to_string();
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json_str.as_str())?;

        Ok(builder.build())
    }
    fn build_variant_array_from_json_array(
        jsons: &[Option<serde_json::Value>],
    ) -> Result<VariantArray> {
        use parquet_variant_json::JsonToVariant;

        let mut builder = VariantArrayBuilder::new(jsons.len());

        for json_value in jsons.iter() {
            match json_value.as_ref() {
                Some(json) => builder.append_json(json.to_string().as_str())?,
                None => builder.append_null(),
            };
        }

        Ok(builder.build())
    }

    #[test]
    fn test_build_variant_array_from_json_error() {
        use parquet_variant_compute::VariantArrayBuilder;
        use parquet_variant_json::JsonToVariant;

        let mut builder = VariantArrayBuilder::new(1);
        let result = builder.append_json("{invalid: json}");
        assert!(result.is_err());
    }

    #[test]
    fn test_scalar() -> Result<()> {
        let json_value = serde_json::json!(null);
        let input = build_variant_array_from_json(&json_value)?;

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkIsVariantNullUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
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

        let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result else {
            return exec_err!("expected Scalar Boolean, got different variant");
        };

        assert!(b);
        Ok(())
    }

    #[test]
    fn test_scalar_non_null_value() -> Result<()> {
        let json_value = serde_json::json!({"name": "test"});
        let input = build_variant_array_from_json(&json_value)?;

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkIsVariantNullUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
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

        let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result else {
            return exec_err!("expected Scalar Boolean, got different variant");
        };

        assert!(!b);
        Ok(())
    }

    #[test]
    fn test_columnar() -> Result<()> {
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!(null)),
            Some(serde_json::json!(null)),
            Some(serde_json::json!("null")), // this is a ShortString('null')
            Some(serde_json::json!({
                "name": "norm"
            })),
        ])?;

        let input: StructArray = input.into();

        let variant_input = Arc::new(input) as ArrayRef;

        let udf = SparkIsVariantNullUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Array(boolean_array) = result else {
            return exec_err!("expected Array variant, got Scalar");
        };

        let Some(bool_array) = boolean_array.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!("expected BooleanArray");
        };

        let bool_array = bool_array.into_iter().collect::<Vec<_>>();

        assert_eq!(
            bool_array,
            vec![Some(true), Some(true), Some(false), Some(false),]
        );
        Ok(())
    }
}
