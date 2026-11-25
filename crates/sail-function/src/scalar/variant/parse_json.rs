// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/json_to_variant.rs

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringViewArray, StructArray, new_null_array};
use arrow::compute::cast;
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::exec_datafusion_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use datafusion_functions::utils::make_scalar_function;
use jiter::{Jiter, Peek};
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal4, VariantDecimal8,
    VariantDecimal16,
};
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use sail_common_datafusion::variant::{
    VARIANT_METADATA_FIELD_NAME, VARIANT_VALUE_FIELD_NAME, variant_metadata_field,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// Returns a Variant from a JSON string.
///
/// Drives both `parse_json` (strict, errors on invalid JSON) and
/// `try_parse_json` (safe, returns NULL on invalid JSON; also tolerates
/// trailing garbage by parsing the first valid JSON value).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseJson {
    signature: Signature,
    safe: bool,
}

impl ParseJson {
    pub fn new(safe: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            safe,
        }
    }

    pub fn safe(&self) -> bool {
        self.safe
    }
}

impl Default for ParseJson {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Spark chooses the variant type from the JSON token before rounding to a double.
fn variant_number(token: &str) -> Option<Variant<'static, 'static>> {
    if let Ok(i) = token.parse::<i64>() {
        return Some(if let Ok(n) = i8::try_from(i) {
            n.into()
        } else if let Ok(n) = i16::try_from(i) {
            n.into()
        } else if let Ok(n) = i32::try_from(i) {
            n.into()
        } else {
            i.into()
        });
    }
    if !token.contains(['e', 'E']) {
        let fraction = token.split_once('.').map_or("", |(_, fraction)| fraction);
        if fraction.len() <= 38
            && let Ok(n) = token.replace('.', "").parse::<i128>()
            && let Ok(scale) = u8::try_from(fraction.len())
        {
            if let Ok(i) = i32::try_from(n)
                && let Ok(d) = VariantDecimal4::try_new(i, scale)
            {
                return Some(d.into());
            }
            if let Ok(i) = i64::try_from(n)
                && let Ok(d) = VariantDecimal8::try_new(i, scale)
            {
                return Some(d.into());
            }
            if let Ok(d) = VariantDecimal16::try_new(n, scale) {
                return Some(d.into());
            }
        }
    }
    token.parse::<f64>().ok().map(Into::into)
}

fn primitive_token(parser: &mut Jiter<'_>, peek: Peek) -> Option<Variant<'static, 'static>> {
    match peek {
        Peek::Null => {
            parser.known_null().ok()?;
            Some(Variant::Null)
        }
        Peek::True | Peek::False => Some(parser.known_bool(peek).ok()?.into()),
        _ => variant_number(std::str::from_utf8(parser.known_number_bytes(peek).ok()?).ok()?),
    }
}

fn append_json_value(
    parser: &mut Jiter<'_>,
    peek: Peek,
    builder: &mut impl VariantBuilderExt,
    depth: usize,
) -> Option<()> {
    if depth >= 128 {
        return None;
    }
    match peek {
        Peek::String => builder.append_value(parser.known_str().ok()?),
        Peek::Array => {
            let mut list = builder.try_new_list().ok()?;
            let mut next = parser.known_array().ok()?;
            while let Some(peek) = next {
                append_json_value(parser, peek, &mut list, depth + 1)?;
                next = parser.array_step().ok()?;
            }
            list.finish();
        }
        Peek::Object => {
            let mut object = builder.try_new_object().ok()?;
            let mut names = HashSet::new();
            let mut next = parser.known_object().ok()?;
            while let Some(name) = next {
                let name = name.to_owned();
                if names.contains(&name) {
                    return None;
                }
                let mut field = ObjectFieldBuilder::new(&name, &mut object);
                let peek = parser.peek().ok()?;
                append_json_value(parser, peek, &mut field, depth + 1)?;
                names.insert(name);
                next = parser.next_key().ok()?;
            }
            object.finish();
        }
        _ => builder.append_value(primitive_token(parser, peek)?),
    }
    Some(())
}

/// Parse one complete value. Spark accepts trailing content after a valid prefix.
fn try_append_json(builder: &mut VariantArrayBuilder, json: &str) -> Option<()> {
    let mut parser = Jiter::new(json.as_bytes());
    let peek = parser.peek().ok()?;
    if matches!(peek, Peek::Array | Peek::Object | Peek::String) {
        return append_json_value(&mut parser, peek, builder, 0);
    }
    // Primitive tokens must end at a delimiter before any trailing content.
    let value = primitive_token(&mut parser, peek)?;
    if let Some(&next) = json.as_bytes().get(parser.current_index())
        && !matches!(
            next,
            b' ' | b'\n' | b'\t' | b'\r' | b'"' | b'[' | b']' | b'{' | b'}' | b',' | b':'
        )
    {
        return None;
    }
    builder.append_value(value);
    Some(())
}

/// Wrap a JSON-parse failure with Spark's canonical error code so feature
/// tests and user-facing errors match `[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]`.
fn malformed_record_err(record: &str) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: {record}."
    )
}

impl ScalarUDFImpl for ParseJson {
    fn name(&self) -> &str {
        if self.safe {
            "try_parse_json"
        } else {
            "parse_json"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Use Binary instead of BinaryView for PySpark compatibility.
        // parquet-variant uses BinaryView internally (zero-copy, more efficient),
        // but PySpark doesn't support BinaryView in Arrow-to-Python conversion,
        // failing at gRPC serialization. The ideal approach would be BinaryView
        // internally and convert to Binary only at the Spark Connect serialization
        // layer, but that requires a broader refactor of the serialization path.
        Ok(DataType::Struct(Fields::from(vec![
            Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
            variant_metadata_field(DataType::Binary, false),
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
        // Fast path: all-null input column propagates to all-null Variant output
        // without parsing any rows. Placed after coerce_types has validated the
        // string arg type; the JSON parse itself is per-row, so there is no
        // batch-level validation that this short-circuit could silence.
        if let Some(ColumnarValue::Array(arr)) = args.args.first()
            && !arr.is_empty()
            && arr.null_count() == arr.len()
        {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_field.data_type(),
                arr.len(),
            )));
        }

        let safe = self.safe;
        let name = self.name().to_string();
        make_scalar_function(
            move |arrays: &[ArrayRef]| parse_json_kernel(arrays, safe, &name),
            vec![],
        )(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                arg_types.len(),
            ));
        }

        // Coerce all string types to Utf8View for consistency
        let coerced_type = match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8View,
            DataType::Null => DataType::Null,
            other => {
                return datafusion_common::plan_err!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{}\" due to data type mismatch: The first parameter requires the \"STRING\" type, however the input has the type \"{}\".",
                    self.name(),
                    other
                );
            }
        };

        Ok(vec![coerced_type])
    }
}

fn parse_json_kernel(args: &[ArrayRef], safe: bool, name: &str) -> Result<ArrayRef> {
    let arr = &args[0];
    match arr.data_type() {
        DataType::Utf8View => from_utf8view_arr(arr, safe),
        DataType::Utf8 | DataType::LargeUtf8 => {
            let view = cast(arr, &DataType::Utf8View)
                .map_err(|e| exec_datafusion_err!("cast to Utf8View failed: {e}"))?;
            from_utf8view_arr(&view, safe)
        }
        DataType::Null => {
            let mut builder = VariantArrayBuilder::new(arr.len());
            for _ in 0..arr.len() {
                builder.append_null();
            }
            let struct_array: StructArray = builder.build().into();
            let struct_array = convert_variant_binaryview_to_binary(struct_array)?;
            Ok(Arc::new(struct_array) as ArrayRef)
        }
        other => Err(unsupported_data_type_exec_err(name, "string", other)),
    }
}

pub(crate) fn from_utf8view_arr(arr: &ArrayRef, safe: bool) -> Result<ArrayRef> {
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
            Some(json_str) if try_append_json(&mut builder, json_str).is_some() => {}
            Some(json_str) if !safe => return Err(malformed_record_err(json_str)),
            Some(_) | None => builder.append_null(),
        }
    }

    let variant_array: StructArray = builder.build().into();
    let variant_array = convert_variant_binaryview_to_binary(variant_array)?;
    Ok(Arc::new(variant_array) as ArrayRef)
}

/// Converts a StructArray with BinaryView fields to Binary fields for PySpark compatibility
pub(crate) fn convert_binaryview_to_binary(struct_array: StructArray) -> Result<StructArray> {
    let fields: Vec<Arc<Field>> = struct_array
        .fields()
        .iter()
        .map(|f| {
            if matches!(f.data_type(), DataType::Binary) {
                return f.clone();
            }
            Arc::new(
                Field::new(f.name(), DataType::Binary, f.is_nullable())
                    .with_metadata(f.metadata().clone()),
            )
        })
        .collect();

    let columns: Result<Vec<ArrayRef>> = struct_array
        .columns()
        .iter()
        .map(|col| {
            if matches!(col.data_type(), DataType::Binary) {
                return Ok(col.clone());
            }
            cast(col, &DataType::Binary)
                .map_err(|e| exec_datafusion_err!("Failed to cast BinaryView to Binary: {e}"))
        })
        .collect();

    Ok(StructArray::new(
        Fields::from(fields),
        columns?,
        struct_array.nulls().cloned(),
    ))
}

pub(crate) fn convert_variant_binaryview_to_binary(
    struct_array: StructArray,
) -> Result<StructArray> {
    let struct_array = convert_binaryview_to_binary(struct_array)?;
    let (value_index, value_field) = struct_array
        .fields()
        .find(VARIANT_VALUE_FIELD_NAME)
        .ok_or_else(|| exec_datafusion_err!("missing variant field: {VARIANT_VALUE_FIELD_NAME}"))?;
    let (metadata_index, metadata_field) = struct_array
        .fields()
        .find(VARIANT_METADATA_FIELD_NAME)
        .ok_or_else(|| {
            exec_datafusion_err!("missing variant field: {VARIANT_METADATA_FIELD_NAME}")
        })?;
    let value_column = struct_array.column(value_index);
    let metadata_column = struct_array.column(metadata_index);

    let field = variant_metadata_field(DataType::Binary, metadata_field.is_nullable());
    let mut metadata = metadata_field.metadata().clone();
    metadata.extend(field.metadata().clone());

    Ok(StructArray::new(
        Fields::from(vec![
            value_field.clone(),
            Arc::new(field.with_metadata(metadata)),
        ]),
        vec![value_column.clone(), metadata_column.clone()],
        struct_array.nulls().cloned(),
    ))
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{ReturnFieldArgs, ScalarFunctionArgs};
    use datafusion_common::{ScalarValue, exec_err};
    use parquet_variant::{Variant, VariantBuilder};
    use parquet_variant_compute::VariantArray;
    use sail_common_datafusion::variant::is_variant_metadata_field;

    use super::*;

    #[test]
    fn test_variant_builder_output_is_marked_by_variant_binary_conversion() -> Result<()> {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::from("x"));
        let struct_array: StructArray = builder.build().into();

        let names = struct_array
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![VARIANT_METADATA_FIELD_NAME, VARIANT_VALUE_FIELD_NAME]
        );
        assert!(!is_variant_metadata_field(
            struct_array.fields()[0].as_ref()
        ));

        let generic_array = convert_binaryview_to_binary(struct_array.clone())?;
        let names = generic_array
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![VARIANT_METADATA_FIELD_NAME, VARIANT_VALUE_FIELD_NAME]
        );
        assert!(!is_variant_metadata_field(
            generic_array.fields()[0].as_ref()
        ));

        let struct_array = convert_variant_binaryview_to_binary(struct_array)?;
        let names = struct_array
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![VARIANT_VALUE_FIELD_NAME, VARIANT_METADATA_FIELD_NAME]
        );
        assert!(is_variant_metadata_field(struct_array.fields()[1].as_ref()));

        Ok(())
    }

    #[test]
    fn test_json_to_variant_udf_scalar_none() -> Result<()> {
        let json_input = ScalarValue::Utf8(None);

        let udf = ParseJson::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));

        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(sv)) => {
                // parse_json(null) should return SQL NULL
                assert!(sv.is_null(0), "expected SQL NULL for parse_json(null)");
            }
            _ => return exec_err!("Expected Variant struct result"),
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_udf_scalar_null() -> Result<()> {
        let json_input = ScalarValue::Utf8(Some("null".into()));

        let udf = ParseJson::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref())?;
                let variant = variant_array.value(0);
                assert_eq!(variant, Variant::from(()));
            }
            _ => return exec_err!("Expected scalar BinaryView result"),
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_udf_scalar_complex() -> Result<()> {
        let json_input =
            ScalarValue::Utf8(Some(r#"{"key": 123, "data": [4, 5, "str"]}"#.to_string()));

        let udf = ParseJson::default();

        let (expected_m, expected_v) = {
            let mut variant_builder = VariantBuilder::new();
            let mut object_builder = variant_builder.new_object();

            object_builder.insert("key", 123_u8);

            let mut inner_array_builder = object_builder.new_list("data");

            inner_array_builder.append_value(4u8);
            inner_array_builder.append_value(5u8);
            inner_array_builder.append_value("str");

            inner_array_builder.finish();

            object_builder.finish();

            variant_builder.finish()
        };

        let expected_variant = Variant::try_new(&expected_m, &expected_v)?;

        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref())?;
                let variant = variant_array.value(0);
                assert_eq!(variant, expected_variant);
            }
            _ => return exec_err!("Expected scalar BinaryView result"),
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_udf_scalar_primitive() -> Result<()> {
        let json_input = ScalarValue::Utf8(Some("123".to_string()));

        let udf = ParseJson::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&arg_field),
            scalar_arguments: &[],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref())?;
                let variant = variant_array.value(0);
                assert_eq!(variant, Variant::from(123_u8));
            }
            _ => return exec_err!("Expected scalar BinaryView result"),
        }
        Ok(())
    }

    #[test]
    fn test_parse_json_scientific_notation_is_numeric() -> Result<()> {
        assert_eq!(variant_number("1.5e-1"), Some(Variant::Double(0.15)));
        Ok(())
    }
}
