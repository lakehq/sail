use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Builder, ListArray, StringBuilder, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::exec_datafusion_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::{VariantArray, VariantArrayBuilder, VariantType};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::spark_parse_json::convert_binaryview_to_binary;
use crate::scalar::variant::utils::helper::try_field_as_variant_array;

fn variant_explode_value_field() -> Field {
    Field::new(
        "value",
        DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ])),
        true,
    )
    .with_extension_type(VariantType)
}

/// Returns the output struct fields for variant_explode: `{pos: int, key: string, value: variant}`.
fn variant_explode_fields() -> Fields {
    Fields::from(vec![
        Field::new("pos", DataType::Int32, false),
        Field::new("key", DataType::Utf8, true),
        variant_explode_value_field(),
    ])
}

fn variant_explode_item_field() -> Arc<Field> {
    Arc::new(Field::new_struct("item", variant_explode_fields(), true))
}

/// Returns the full output type: `List<Struct<pos, key, value>>`.
fn variant_explode_return_type() -> DataType {
    DataType::List(variant_explode_item_field())
}

/// Converts a single variant value into a list of `(pos, key, value)` tuples.
///
/// - For objects: iterates fields, key = field name, value = field value.
/// - For arrays: iterates elements, key = NULL, value = element.
/// - Otherwise: returns empty list.
fn explode_variant(
    variant: Variant<'_, '_>,
    pos_builder: &mut Int32Builder,
    key_builder: &mut StringBuilder,
    value_builder: &mut VariantArrayBuilder,
) -> usize {
    match variant {
        Variant::Object(obj) => {
            let len = obj.len();
            for (i, (name, val)) in obj.iter().enumerate() {
                pos_builder.append_value(i as i32);
                key_builder.append_value(name);
                value_builder.append_variant(val);
            }
            len
        }
        Variant::List(list) => {
            let len = list.len();
            for (i, val) in list.iter().enumerate() {
                pos_builder.append_value(i as i32);
                key_builder.append_null();
                value_builder.append_variant(val);
            }
            len
        }
        _ => 0,
    }
}

/// Scalar UDF that converts a variant value into `List<Struct<pos: Int32, key: Utf8, value: Variant>>`.
///
/// This is a helper function used by the explode rewriter. It is not directly exposed to users.
/// The ExplodeRewriter wraps the input with this UDF and then unnests the result inline-style.
///
/// - For variant objects: each field becomes a `(pos, key, value)` tuple.
/// - For variant arrays: each element becomes a `(pos, NULL, value)` tuple.
/// - For other variant types (null, scalar, SQL NULL): returns an empty list.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVariantExplodeUdf {
    signature: Signature,
}

impl SparkVariantExplodeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for SparkVariantExplodeUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkVariantExplodeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "spark_variant_explode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(variant_explode_return_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(Arc::new(Field::new(
            self.name(),
            variant_explode_return_type(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                arg_types.len(),
            ));
        }
        Ok(vec![arg_types[0].clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected 1 argument"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        let arg = &args.args[0];
        match arg {
            ColumnarValue::Scalar(scalar) => self.invoke_scalar(scalar),
            ColumnarValue::Array(arr) => self.invoke_array(arr),
        }
    }
}

impl SparkVariantExplodeUdf {
    fn invoke_scalar(&self, scalar: &ScalarValue) -> Result<ColumnarValue> {
        let mut pos_builder = Int32Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = VariantArrayBuilder::new(0);

        let len = if scalar.is_null() {
            0
        } else {
            let arr = scalar.to_array()?;
            let variant_array = VariantArray::try_new(arr.as_ref())?;
            match variant_array.iter().next().flatten() {
                Some(variant @ (Variant::Object(_) | Variant::List(_))) => explode_variant(
                    variant,
                    &mut pos_builder,
                    &mut key_builder,
                    &mut value_builder,
                ),
                _ => 0,
            }
        };

        let list_arr = build_list_array(pos_builder, key_builder, value_builder, &[len])?;
        let scalar = ScalarValue::try_from_array(&list_arr, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    }

    fn invoke_array(&self, arr: &ArrayRef) -> Result<ColumnarValue> {
        let variant_array = VariantArray::try_new(arr.as_ref())?;
        let num_rows = variant_array.len();

        let mut pos_builder = Int32Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = VariantArrayBuilder::new(0);
        let mut lengths = Vec::with_capacity(num_rows);

        for v in variant_array.iter() {
            match v {
                Some(variant @ (Variant::Object(_) | Variant::List(_))) => {
                    let len = explode_variant(
                        variant,
                        &mut pos_builder,
                        &mut key_builder,
                        &mut value_builder,
                    );
                    lengths.push(len);
                }
                _ => lengths.push(0),
            }
        }

        let list_arr = build_list_array(pos_builder, key_builder, value_builder, &lengths)?;
        Ok(ColumnarValue::Array(Arc::new(list_arr) as ArrayRef))
    }
}

/// Build a `ListArray` of `Struct<pos, key, value>` from the builders.
fn build_list_array(
    mut pos_builder: Int32Builder,
    mut key_builder: StringBuilder,
    value_builder: VariantArrayBuilder,
    lengths: &[usize],
) -> Result<ListArray> {
    let pos_arr: ArrayRef = Arc::new(pos_builder.finish());
    let key_arr: ArrayRef = Arc::new(key_builder.finish());

    let value_struct: StructArray = value_builder.build().into();
    let value_struct = convert_binaryview_to_binary(value_struct)?;
    let value_arr: ArrayRef = Arc::new(value_struct);

    let struct_arr = StructArray::try_new(
        variant_explode_fields(),
        vec![pos_arr, key_arr, value_arr],
        None,
    )?;

    // Build offsets from lengths.
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut running = 0i32;
    for &len in lengths {
        running += len as i32;
        offsets.push(running);
    }

    Ok(ListArray::new(
        variant_explode_item_field(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_arr),
        None,
    ))
}
