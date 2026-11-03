use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/variant_to_json.rs>
use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;
use sail_common_datafusion::display::spark_f64_to_string;

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
                    return exec_err!(
                        "Cannot convert empty VariantArray to JSON: the array must contain at least one element"
                    );
                }
                if variant_array.is_null(0) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)))
                } else {
                    let v = variant_array.value(0);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                        variant_to_json_string(&v)?,
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
                        Some(v) => builder.append_value(variant_to_json_string(&v)?),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            unsupported => exec_err!("Invalid data type: {unsupported}"),
        },
    }
}

fn variant_to_json_string(variant: &Variant<'_, '_>) -> Result<String> {
    let mut buffer = Vec::new();
    write_variant_json(variant, &mut buffer)?;
    String::from_utf8(buffer).map_err(|e| exec_datafusion_err!("JSON encoding error: {e}"))
}

/// Override DOUBLE formatting recursively, leaving other Variant encodings and
/// string escaping to parquet-variant-json. Rewriting serialized JSON would also
/// change numeric-looking strings and object keys.
fn write_variant_json(variant: &Variant<'_, '_>, buffer: &mut Vec<u8>) -> Result<()> {
    match variant {
        Variant::Double(value) => {
            if !value.is_finite() {
                buffer.push(b'"');
            }
            buffer.extend_from_slice(spark_f64_to_string(*value).as_bytes());
            if !value.is_finite() {
                buffer.push(b'"');
            }
        }
        Variant::List(list) => {
            buffer.push(b'[');
            for (index, value) in list.iter().enumerate() {
                if index > 0 {
                    buffer.push(b',');
                }
                write_variant_json(&value, buffer)?;
            }
            buffer.push(b']');
        }
        Variant::Object(object) => {
            buffer.push(b'{');
            for (index, (key, value)) in object.iter().enumerate() {
                if index > 0 {
                    buffer.push(b',');
                }
                serde_json::to_writer(&mut *buffer, key)
                    .map_err(|e| exec_datafusion_err!("JSON encoding error: {e}"))?;
                buffer.push(b':');
                write_variant_json(&value, buffer)?;
            }
            buffer.push(b'}');
        }
        _ => variant.to_json(buffer)?,
    }
    Ok(())
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
    fn test_double_json_formatting() -> Result<()> {
        for (value, expected) in [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (0.15, "0.15"),
            (1e-3, "0.001"),
            (1e-4, "1.0E-4"),
            (1e6, "1000000.0"),
            (1e7, "1.0E7"),
            (1e-18, "1.0E-18"),
            (f64::from_bits(1), "4.9E-324"),
            (f64::NAN, "\"NaN\""),
            (f64::INFINITY, "\"Infinity\""),
            (f64::NEG_INFINITY, "\"-Infinity\""),
        ] {
            assert_eq!(variant_to_json_string(&Variant::Double(value))?, expected);
        }
        Ok(())
    }

    #[test]
    fn test_nested_double_formatting_preserves_strings_and_other_types() -> Result<()> {
        use parquet_variant::{VariantBuilder, VariantDecimal4};

        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("a\"b", -0.0_f64);
        object.insert("text", "\\1e10\n-0.0\"");
        let mut list = object.new_list("values");
        list.append_value(1e10_f64);
        list.append_value(f64::INFINITY);
        list.append_value(VariantDecimal4::try_new(12300, 4)?);
        list.append_value(Variant::Binary(b"abc"));
        list.append_value(Variant::Null);
        list.finish();
        object.finish();
        let (metadata, value) = builder.finish();
        assert_eq!(
            variant_to_json_string(&Variant::try_new(&metadata, &value)?)?,
            r#"{"a\"b":-0.0,"text":"\\1e10\n-0.0\"","values":[1.0E10,"Infinity",1.23,"YWJj",null]}"#,
        );
        // The numeric override must not alter the remaining leaf encodings.
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .ok_or_else(|| exec_datafusion_err!("invalid test date"))?;
        for value in [
            Variant::Float(1.5),
            Variant::Date(date),
            Variant::TimestampNtzMicros(
                date.and_hms_opt(3, 4, 5)
                    .ok_or_else(|| exec_datafusion_err!("invalid test time"))?,
            ),
            Variant::BooleanTrue,
            Variant::from("é 1e10 -0.0"),
        ] {
            assert_eq!(variant_to_json_string(&value)?, value.to_json_string()?);
        }
        Ok(())
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
