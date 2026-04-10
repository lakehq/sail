use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant_compute::{cast_to_variant, VariantType};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::variant::spark_json_to_variant::convert_binaryview_to_binary;

/// Recursively checks if a DataType contains Null (VOID) anywhere.
fn contains_void_type(dt: &DataType) -> bool {
    match dt {
        DataType::Null => true,
        DataType::List(f) | DataType::LargeList(f) => contains_void_type(f.data_type()),
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                fields
                    .iter()
                    .any(|field| contains_void_type(field.data_type()))
            } else {
                false
            }
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_void_type(field.data_type())),
        _ => false,
    }
}

/// Converts a complex type (struct, array, or map) into a Variant.
///
/// `to_variant_object(named_struct('a', 1, 'b', 'hello'))` → variant containing `{"a":1,"b":"hello"}`
/// `to_variant_object(array(1, 2, 3))` → variant containing `[1,2,3]`
/// `to_variant_object(map('x', 1))` → variant containing `{"x":1}`
///
/// Rejects primitive types (int, string, etc.) — only complex/container types are accepted.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#to_variant_object>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToVariantObjectUdf {
    signature: Signature,
}

impl SparkToVariantObjectUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for SparkToVariantObjectUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkToVariantObjectUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "to_variant_object"
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

        // Must be a complex/container type (struct, array, map) — reject primitives
        match &input_type {
            DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _)
            | DataType::Null => {}
            _ => {
                return exec_err!(
                    "to_variant_object: cannot cast \"{}\" to \"VARIANT\"",
                    input_type
                );
            }
        }
        // Reject types containing VOID (e.g. ARRAY<VOID>, MAP<VOID, VOID>)
        if contains_void_type(&input_type) {
            return exec_err!(
                "to_variant_object: cannot cast \"{}\" to \"VARIANT\"",
                input_type
            );
        }

        let arg = &args.args[0];
        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
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
                let variant_array = cast_to_variant(&arr).map_err(|e| {
                    exec_datafusion_err!("to_variant_object: failed to convert to VARIANT: {e}")
                })?;
                let struct_array: StructArray = variant_array.into();
                let struct_array = convert_binaryview_to_binary(struct_array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                    struct_array,
                ))))
            }
            ColumnarValue::Array(arr) => {
                let variant_array = cast_to_variant(arr.as_ref()).map_err(|e| {
                    exec_datafusion_err!("to_variant_object: failed to convert to VARIANT: {e}")
                })?;
                let struct_array: StructArray = variant_array.into();
                let struct_array = convert_binaryview_to_binary(struct_array)?;
                Ok(ColumnarValue::Array(Arc::new(struct_array) as ArrayRef))
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "to_variant_object",
                (1, 1),
                arg_types.len(),
            ));
        }
        // Reject types containing Null/VOID (e.g. ARRAY<VOID>, MAP<VOID, VOID>)
        // Spark cannot cast these to VARIANT
        if contains_void_type(&arg_types[0]) {
            return Err(unsupported_data_type_exec_err(
                "to_variant_object",
                "non-void complex type",
                &arg_types[0],
            ));
        }
        Ok(vec![arg_types[0].clone()])
    }
}
