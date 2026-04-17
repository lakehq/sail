use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use datafusion_common::exec_err;
use parquet_variant_compute::{cast_to_variant, VariantType};

use super::spark_json_to_variant::convert_binaryview_to_binary;

/// Implements `CAST(expr AS VARIANT)` for Spark-compatible variant conversion.
///
/// Converts any supported Arrow type to a Variant using `parquet-variant-compute`.
/// Map and Struct types are rejected (matching Spark behavior).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastToVariant {
    signature: Signature,
}

impl SparkCastToVariant {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for SparkCastToVariant {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkCastToVariant {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_cast_to_variant"
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
        let input_type = args
            .arg_fields
            .first()
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);

        // Reject Map and Struct (Spark doesn't support CAST to VARIANT for these)
        if matches!(input_type, DataType::Map(_, _)) {
            return exec_err!("cannot cast MAP to VARIANT");
        }
        if matches!(input_type, DataType::Struct(_)) {
            return exec_err!("cannot cast STRUCT to VARIANT");
        }

        let arg = &args.args[0];
        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    // SQL NULL → NULL Variant struct (must match promised return type)
                    let fields = Fields::from(vec![
                        Field::new("metadata", DataType::Binary, false),
                        Field::new("value", DataType::Binary, false),
                    ]);
                    let null_struct = StructArray::new_null(fields, 1);
                    return Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                        null_struct,
                    ))));
                }
                let arr = scalar.to_array()?;
                let variant_array = cast_to_variant(&arr)?;
                let struct_array: StructArray = variant_array.into();
                let struct_array = convert_binaryview_to_binary(struct_array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                    struct_array,
                ))))
            }
            ColumnarValue::Array(arr) => {
                let variant_array = cast_to_variant(arr.as_ref())?;
                let struct_array: StructArray = variant_array.into();
                let struct_array = convert_binaryview_to_binary(struct_array)?;
                Ok(ColumnarValue::Array(Arc::new(struct_array) as ArrayRef))
            }
        }
    }
}
