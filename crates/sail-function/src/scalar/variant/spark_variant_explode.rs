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
use crate::scalar::variant::spark_json_to_variant::convert_binaryview_to_binary;
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

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, BinaryArray, Int32Array, StringArray};
    use arrow_schema::Fields;
    use datafusion::common::exec_datafusion_err;
    use datafusion::logical_expr::ReturnFieldArgs;
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;

    use super::*;

    fn build_variant_scalar(json: &str) -> Result<ScalarValue> {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json)?;
        let variant_array = builder.build();
        let struct_arr: StructArray = variant_array.into();
        Ok(ScalarValue::Struct(Arc::new(struct_arr)))
    }

    fn build_variant_array_from_jsons(jsons: &[Option<&str>]) -> Result<ArrayRef> {
        let mut builder = VariantArrayBuilder::new(jsons.len());
        for json in jsons {
            match json {
                Some(j) => builder.append_json(j)?,
                None => builder.append_null(),
            }
        }
        let variant_array = builder.build();
        let struct_arr: StructArray = variant_array.into();
        Ok(Arc::new(struct_arr) as ArrayRef)
    }

    fn variant_arg_field() -> Arc<Field> {
        Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        )
    }

    fn build_args(
        udf: &SparkVariantExplodeUdf,
        arg: ColumnarValue,
        arg_fields: Vec<Arc<Field>>,
        number_rows: usize,
    ) -> Result<ScalarFunctionArgs> {
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[],
        })?;
        Ok(ScalarFunctionArgs {
            args: vec![arg],
            return_field,
            arg_fields,
            number_rows,
            config_options: Default::default(),
        })
    }

    fn invoke_scalar(json: &str) -> Result<ColumnarValue> {
        let udf = SparkVariantExplodeUdf::new();
        let scalar = build_variant_scalar(json)?;
        let arg_field = variant_arg_field();
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(scalar)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: 1,
            config_options: Default::default(),
        };
        udf.invoke_with_args(args)
    }

    fn invoke_array(jsons: &[Option<&str>]) -> Result<ColumnarValue> {
        let udf = SparkVariantExplodeUdf::new();
        let arr = build_variant_array_from_jsons(jsons)?;
        let arg_field = variant_arg_field();
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(arr)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: jsons.len(),
            config_options: Default::default(),
        };
        udf.invoke_with_args(args)
    }

    fn get_list_lengths(result: &ColumnarValue) -> Result<Vec<Option<usize>>> {
        match result {
            ColumnarValue::Scalar(s) => {
                let arr = s.to_array()?;
                let list = arr.as_list::<i32>();
                Ok(vec![Some(list.value(0).len())])
            }
            ColumnarValue::Array(arr) => {
                let list = arr.as_list::<i32>();
                Ok((0..list.len()).map(|i| Some(list.value(i).len())).collect())
            }
        }
    }

    fn extract_scalar_rows(result: &ColumnarValue) -> Result<Vec<(i32, Option<String>, String)>> {
        let ColumnarValue::Scalar(scalar) = result else {
            return Err(exec_datafusion_err!("expected scalar result"));
        };
        let arr = scalar.to_array()?;
        let list = arr.as_list::<i32>();
        let values = list.value(0);
        let rows = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| exec_datafusion_err!("expected StructArray rows"))?;
        let pos_arr = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| exec_datafusion_err!("expected Int32Array for pos"))?;
        let key_arr = rows
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_datafusion_err!("expected StringArray for key"))?;
        let value_arr = rows
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| exec_datafusion_err!("expected StructArray for value"))?;
        let metadata_arr = value_arr
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("expected BinaryArray for metadata"))?;
        let value_arr = value_arr
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("expected BinaryArray for value bytes"))?;

        let mut output = Vec::with_capacity(rows.len());
        for index in 0..rows.len() {
            let variant = Variant::try_new(metadata_arr.value(index), value_arr.value(index))?;
            let key = (!key_arr.is_null(index)).then(|| key_arr.value(index).to_string());
            output.push((
                pos_arr.value(index),
                key,
                parquet_variant_json::VariantToJson::to_json_string(&variant)?,
            ));
        }
        Ok(output)
    }

    fn assert_variant_value_field(field: &Field) -> Result<()> {
        let item = match field.data_type() {
            DataType::List(item) => item,
            data_type => {
                return Err(exec_datafusion_err!(
                    "expected list field, got {data_type:?}"
                ));
            }
        };
        let fields = match item.data_type() {
            DataType::Struct(fields) => fields,
            data_type => {
                return Err(exec_datafusion_err!(
                    "expected list item struct, got {data_type:?}"
                ));
            }
        };
        let Some((_, value_field)) = fields.find("value") else {
            return Err(exec_datafusion_err!("missing value field"));
        };
        assert_eq!(
            value_field.extension_type_name(),
            variant_explode_value_field().extension_type_name()
        );
        Ok(())
    }

    fn explode_variant_len(json: &str) -> Result<usize> {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json)?;
        let variant_array = builder.build();
        let variant = variant_array
            .iter()
            .next()
            .flatten()
            .ok_or_else(|| exec_datafusion_err!("expected a variant value"))?;

        let mut pos = Int32Builder::new();
        let mut key = StringBuilder::new();
        let mut val = VariantArrayBuilder::new(0);

        Ok(explode_variant(variant, &mut pos, &mut key, &mut val))
    }

    #[test]
    fn test_explode_variant_array() -> Result<()> {
        assert_eq!(explode_variant_len("[1, 2, 3]")?, 3);
        Ok(())
    }

    #[test]
    fn test_explode_variant_object() -> Result<()> {
        assert_eq!(explode_variant_len(r#"{"a": 1, "b": 2}"#)?, 2);
        Ok(())
    }

    #[test]
    fn test_explode_variant_non_containers() -> Result<()> {
        assert_eq!(explode_variant_len("42")?, 0);
        assert_eq!(explode_variant_len("null")?, 0);
        assert_eq!(explode_variant_len("[]")?, 0);
        assert_eq!(explode_variant_len("{}")?, 0);
        Ok(())
    }

    #[test]
    fn test_scalar_array_input() -> Result<()> {
        let result = invoke_scalar("[1, 2, 3]")?;
        let lengths = get_list_lengths(&result)?;
        assert_eq!(lengths, vec![Some(3)]);
        Ok(())
    }

    #[test]
    fn test_scalar_object_input() -> Result<()> {
        let result = invoke_scalar(r#"{"a": 1, "b": 2}"#)?;
        let lengths = get_list_lengths(&result)?;
        assert_eq!(lengths, vec![Some(2)]);
        Ok(())
    }

    #[test]
    fn test_scalar_rows_preserve_positions_and_keys() -> Result<()> {
        let result = invoke_scalar(r#"{"answer": 42}"#)?;
        let rows = extract_scalar_rows(&result)?;
        assert_eq!(
            rows,
            vec![(0, Some("answer".to_string()), "42".to_string())]
        );
        Ok(())
    }

    #[test]
    fn test_scalar_non_containers_return_empty_lists() -> Result<()> {
        for json in ["[]", "{}", "null", "42", "\"hello\"", "true"] {
            let result = invoke_scalar(json)?;
            let lengths = get_list_lengths(&result)?;
            assert_eq!(lengths, vec![Some(0)]);
        }
        Ok(())
    }

    #[test]
    fn test_scalar_sql_null() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let args = build_args(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Null),
            vec![variant_arg_field()],
            1,
        )?;
        let result = udf.invoke_with_args(args)?;
        let lengths = get_list_lengths(&result)?;
        assert_eq!(lengths, vec![Some(0)]);
        Ok(())
    }

    #[test]
    fn test_array_mixed_inputs() -> Result<()> {
        let result = invoke_array(&[
            Some("[1, 2]"),
            Some(r#"{"a": 1}"#),
            Some("42"),
            None,
            Some("[]"),
            Some("{}"),
            Some("null"),
        ])?;
        let lengths = get_list_lengths(&result)?;
        assert_eq!(
            lengths,
            vec![
                Some(2),
                Some(1),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_udf_name() {
        let udf = SparkVariantExplodeUdf::new();
        assert_eq!(udf.name(), "spark_variant_explode");
    }

    #[test]
    fn test_udf_return_type() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let result = udf.return_type(&[DataType::Struct(Fields::empty())])?;
        let field = Field::new("result", result.clone(), true);
        assert_variant_value_field(&field)?;
        assert_eq!(result, variant_explode_return_type());
        Ok(())
    }

    #[test]
    fn test_udf_return_field_metadata() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[],
            scalar_arguments: &[],
        })?;
        assert_eq!(field.name(), "spark_variant_explode");
        assert_variant_value_field(field.as_ref())?;
        Ok(())
    }

    #[test]
    fn test_udf_coerce_types() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let result = udf.coerce_types(&[DataType::Struct(Fields::empty())])?;
        assert_eq!(result, vec![DataType::Struct(Fields::empty())]);
        assert!(udf
            .coerce_types(&[DataType::Int32, DataType::Int32])
            .is_err());
        Ok(())
    }

    #[test]
    fn test_udf_invoke_with_args_requires_arg_field() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let args = build_args(&udf, ColumnarValue::Scalar(ScalarValue::Null), vec![], 1)?;
        let error = match udf.invoke_with_args(args) {
            Ok(_) => return Err(exec_datafusion_err!("expected invoke_with_args to fail")),
            Err(error) => error,
        };
        assert!(error.to_string().contains("expected 1 argument"));
        Ok(())
    }

    #[test]
    fn test_udf_invoke_with_args_rejects_non_variant_field() -> Result<()> {
        let udf = SparkVariantExplodeUdf::new();
        let args = build_args(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            vec![Arc::new(Field::new("input", DataType::Int32, true))],
            1,
        )?;
        let error = match udf.invoke_with_args(args) {
            Ok(_) => return Err(exec_datafusion_err!("expected invoke_with_args to fail")),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("field does not have extension type VariantType"));
        Ok(())
    }

    #[test]
    fn test_udf_default() {
        let udf = SparkVariantExplodeUdf::default();
        assert_eq!(udf.name(), "spark_variant_explode");
    }
}
