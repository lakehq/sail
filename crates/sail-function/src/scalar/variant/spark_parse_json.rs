use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/json_to_variant.rs>
use arrow::array::{Array, ArrayRef, StringViewArray, StructArray, new_null_array};
use arrow::compute::cast;
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::exec_datafusion_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use jiter::{Jiter, Peek};
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilder, VariantBuilderExt, VariantDecimal4,
    VariantDecimal8, VariantDecimal16,
};
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use regex::Regex;
use sail_common_datafusion::variant::{
    VARIANT_METADATA_FIELD_NAME, VARIANT_VALUE_FIELD_NAME, variant_metadata_field,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

/// Returns a Variant from a JSON string.
///
/// Drives both `parse_json` (strict, errors on invalid JSON) and
/// `try_parse_json` (safe, returns NULL on invalid JSON; also tolerates
/// trailing garbage by parsing the first valid JSON value).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkParseJson {
    signature: Signature,
    safe: bool,
}

impl SparkParseJson {
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

impl Default for SparkParseJson {
    fn default() -> Self {
        Self::new(false)
    }
}

// Spark uses Jackson's default StreamReadConstraints.
const MAX_JSON_DEPTH: usize = 1000;
const MAX_JSON_NUMBER_DIGITS: usize = 1000;

#[expect(
    clippy::expect_used,
    reason = "the hard-coded Java identifier-part regex is valid"
)]
static JAVA_IDENTIFIER_PART: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\p{L}\p{Sc}\p{Pc}\p{Nd}\p{Nl}\p{Mc}\p{Mn}\p{Cf}]$")
        .expect("valid Java identifier-part regex")
});

fn json_parse_err(error: impl std::fmt::Display) -> datafusion_common::DataFusionError {
    exec_datafusion_err!("failed to parse JSON: {error}")
}

fn append_json_number(builder: &mut impl VariantBuilderExt, number: &str) -> Result<()> {
    // Jackson counts the integer, fraction, and exponent digits, excluding the
    // sign, decimal point, exponent marker, and exponent sign.
    if number.bytes().filter(|byte| byte.is_ascii_digit()).count() > MAX_JSON_NUMBER_DIGITS {
        return Err(json_parse_err("number length exceeds the maximum"));
    }

    if !number.contains(['.', 'e', 'E'])
        && let Ok(value) = number.parse::<i64>()
    {
        if let Ok(value) = i8::try_from(value) {
            builder.append_value(value);
        } else if let Ok(value) = i16::try_from(value) {
            builder.append_value(value);
        } else if let Ok(value) = i32::try_from(value) {
            builder.append_value(value);
        } else {
            builder.append_value(value);
        }
        return Ok(());
    }

    // Spark first tries non-scientific numbers as exact BigDecimals, selecting
    // the narrowest Variant decimal width that accommodates their scale and
    // precision. Only values outside Decimal16 fall back to f64.
    if !number.contains(['e', 'E']) {
        let unsigned = number.strip_prefix('-').unwrap_or(number);
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let scale = fraction.len();
        let precision = whole
            .bytes()
            .chain(fraction.bytes())
            .skip_while(|digit| *digit == b'0')
            .count()
            .max(1);

        if scale <= 38 && precision <= 38 {
            let mut digits = String::with_capacity(number.len());
            if number.starts_with('-') {
                digits.push('-');
            }
            digits.push_str(whole);
            digits.push_str(fraction);
            let unscaled = digits.parse::<i128>().map_err(json_parse_err)?;
            let scale = scale as u8;

            if scale <= 9 && precision <= 9 {
                builder.append_value(VariantDecimal4::try_new(unscaled as i32, scale)?);
            } else if scale <= 18 && precision <= 18 {
                builder.append_value(VariantDecimal8::try_new(unscaled as i64, scale)?);
            } else {
                builder.append_value(VariantDecimal16::try_new(unscaled, scale)?);
            }
            return Ok(());
        }
    }

    builder.append_value(number.parse::<f64>().map_err(json_parse_err)?);
    Ok(())
}

#[recursive::recursive]
fn append_jiter_value(
    parser: &mut Jiter<'_>,
    peek: Peek,
    builder: &mut impl VariantBuilderExt,
    depth: usize,
) -> Result<()> {
    // Jackson counts every entered array/object, including an empty innermost
    // container. The root container is depth one in that accounting.
    if matches!(peek, Peek::Array | Peek::Object) && depth >= MAX_JSON_DEPTH {
        return Err(json_parse_err("recursion limit exceeded"));
    }

    match peek {
        Peek::Null => {
            parser.known_null().map_err(json_parse_err)?;
            builder.append_value(Variant::Null);
        }
        Peek::True | Peek::False => {
            builder.append_value(parser.known_bool(peek).map_err(json_parse_err)?);
        }
        Peek::String => {
            builder.append_value(parser.known_str().map_err(json_parse_err)?);
        }
        Peek::Array => {
            let mut item = parser.known_array().map_err(json_parse_err)?;
            let mut array = builder.try_new_list()?;
            while let Some(peek) = item {
                append_jiter_value(parser, peek, &mut array, depth + 1)?;
                item = parser.array_step().map_err(json_parse_err)?;
            }
            array.finish();
        }
        Peek::Object => {
            let mut key = parser
                .known_object()
                .map_err(json_parse_err)?
                .map(str::to_owned);
            let mut keys = HashSet::new();
            let mut object = builder.try_new_object()?;
            while let Some(current_key) = key {
                if !keys.insert(current_key.clone()) {
                    return Err(json_parse_err(format!("duplicate key '{current_key}'")));
                }
                let peek = parser.peek().map_err(json_parse_err)?;
                let mut field = ObjectFieldBuilder::new(&current_key, &mut object);
                append_jiter_value(parser, peek, &mut field, depth + 1)?;
                key = parser
                    .next_key()
                    .map_err(json_parse_err)?
                    .map(str::to_owned);
            }
            object.finish();
        }
        _ if peek.is_num() => {
            let number = parser.known_number_bytes(peek).map_err(json_parse_err)?;
            let number = std::str::from_utf8(number).map_err(json_parse_err)?;
            append_json_number(builder, number)?;
        }
        _ => return Err(json_parse_err("unexpected JSON token")),
    }
    Ok(())
}

fn is_java_identifier_part(ch: char) -> bool {
    // Character.isJavaIdentifierPart additionally accepts these ignorable C0
    // and C1 controls. Other accepted Unicode categories are covered above.
    matches!(ch, '\u{0000}'..='\u{0008}' | '\u{000e}'..='\u{001b}' | '\u{007f}'..='\u{009f}')
        || JAVA_IDENTIFIER_PART.is_match(ch.encode_utf8(&mut [0; 4]))
}

fn is_invalid_root_literal_terminator(ch: char) -> bool {
    // ReaderBasedJsonParser._matchToken only checks a terminator at or above
    // '0' (except ']' and '}'), and passes one UTF-16 code unit to
    // Character.isJavaIdentifierPart(char). A supplementary scalar starts with
    // a surrogate, which is not itself an identifier part.
    let mut utf16 = [0; 2];
    let first = ch.encode_utf16(&mut utf16)[0];
    if first < u16::from(b'0') || first == u16::from(b']') || first == u16::from(b'}') {
        return false;
    }
    char::from_u32(u32::from(first)).is_some_and(is_java_identifier_part)
}

/// Parse the first complete JSON value, matching Spark's acceptance of trailing
/// content, and append it transactionally so invalid safe parses become SQL NULL.
fn append_parsed_json(builder: &mut VariantArrayBuilder, json_str: &str) -> Result<()> {
    let mut parser = Jiter::new(json_str.as_bytes());
    let peek = parser.peek().map_err(json_parse_err)?;

    // Build containers directly into the array builder. Its nested builders
    // roll back on parse failure, and this avoids recursively validating and
    // copying an already-valid 1000-level container through an intermediate
    // Variant after parsing it.
    if matches!(peek, Peek::Array | Peek::Object) {
        return append_jiter_value(&mut parser, peek, builder, 0);
    }

    // Keep root primitives transactional until their terminating character is
    // validated below; unlike containers, a primitive append commits at once.
    let mut value_builder = VariantBuilder::new();
    append_jiter_value(&mut parser, peek, &mut value_builder, 0)?;

    // Jackson validates the character terminating a root primitive while
    // scanning that token. Numbers require one of its four JSON whitespace
    // bytes, while literals reject only Java identifier-part characters. Once
    // the token is terminated, Spark intentionally ignores remaining content.
    let trailing = json_str.get(parser.current_index()..).unwrap_or_default();
    if peek.is_num() {
        if let Some(next) = trailing.as_bytes().first()
            && !matches!(next, b' ' | b'\t' | b'\r' | b'\n')
        {
            return Err(json_parse_err("invalid character after root number"));
        }
    } else if matches!(peek, Peek::Null | Peek::True | Peek::False)
        && trailing
            .chars()
            .next()
            .is_some_and(is_invalid_root_literal_terminator)
    {
        return Err(json_parse_err("invalid character after root literal"));
    }

    let (metadata, value) = value_builder.finish();
    let variant = Variant::try_new(&metadata, &value)?;
    builder.append_variant(variant);
    Ok(())
}

/// Try to append a JSON string to the builder leniently. Returns true if successful.
fn try_append_json(builder: &mut VariantArrayBuilder, json_str: &str) -> bool {
    append_parsed_json(builder, json_str).is_ok()
}

/// Wrap a JSON-parse failure with Spark's canonical error code so feature
/// tests and user-facing errors match `[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]`.
fn malformed_record_err(record: &str) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: {record}."
    )
}

/// Strict-path append: accept trailing garbage (first valid prefix), error
/// with `MALFORMED_RECORD_IN_PARSING` on unparseable input.
fn append_json_strict(builder: &mut VariantArrayBuilder, json_str: &str) -> Result<()> {
    append_parsed_json(builder, json_str).map_err(|_| malformed_record_err(json_str))
}

impl ScalarUDFImpl for SparkParseJson {
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
            Some(json_str) => {
                if safe {
                    if !try_append_json(&mut builder, json_str) {
                        builder.append_null();
                    }
                } else {
                    append_json_strict(&mut builder, json_str)?;
                }
            }
            None => builder.append_null(),
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

    fn json_parses(json: &str) -> bool {
        append_parsed_json(&mut VariantArrayBuilder::new(1), json).is_ok()
    }

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

        let udf = SparkParseJson::default();
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

        let udf = SparkParseJson::default();
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

        let udf = SparkParseJson::default();

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

        let udf = SparkParseJson::default();
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
    fn test_json_to_variant_preserves_wide_decimal_precision() -> Result<()> {
        let mut builder = VariantArrayBuilder::new(1);
        append_json_strict(
            &mut builder,
            r#"{"dec16":467440737095.51617,"-dec16":-67.849438003827263}"#,
        )?;
        let array: StructArray = builder.build().into();
        let array = VariantArray::try_new(&array)?;
        let variant = array.value(0);

        assert_eq!(
            variant.get_object_field("dec16"),
            Some(Variant::from(VariantDecimal8::try_new(
                46_744_073_709_551_617,
                5,
            )?))
        );
        assert_eq!(
            variant.get_object_field("-dec16"),
            Some(Variant::from(VariantDecimal8::try_new(
                -67_849_438_003_827_263,
                15,
            )?))
        );
        Ok(())
    }

    #[test]
    fn test_json_root_primitive_termination_matches_spark() {
        for json in [
            "1 ignored",
            "1\tignored",
            "1\rignored",
            "1\nignored",
            "true,garbage",
            "null-garbage",
            "null$",
            "true\u{0001}garbage",
            "false\u{000b}garbage",
            "true\u{10400}",
        ] {
            assert!(json_parses(json), "expected valid JSON prefix: {json:?}");
        }

        for json in [
            "1a",
            "1,garbage",
            "1\u{000b}garbage",
            "1\u{000c}garbage",
            "truex",
            "true0",
            "false\u{0301}",
        ] {
            assert!(!json_parses(json), "expected invalid JSON prefix: {json:?}");
        }
    }

    #[test]
    fn test_json_stream_read_constraints_match_spark() {
        assert!(json_parses(&"9".repeat(MAX_JSON_NUMBER_DIGITS)));
        assert!(!json_parses(&"9".repeat(MAX_JSON_NUMBER_DIGITS + 1)));
        assert!(json_parses(&format!(
            "1e{}",
            "9".repeat(MAX_JSON_NUMBER_DIGITS - 1)
        )));
        assert!(!json_parses(&format!(
            "1e{}",
            "9".repeat(MAX_JSON_NUMBER_DIGITS)
        )));

        let at_depth_limit = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH),
            "]".repeat(MAX_JSON_DEPTH)
        );
        assert!(json_parses(&at_depth_limit));

        let beyond_depth_limit = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH + 1),
            "]".repeat(MAX_JSON_DEPTH + 1)
        );
        assert!(!json_parses(&beyond_depth_limit));
    }

    #[test]
    fn test_json_negative_zero_storage_matches_spark() -> Result<()> {
        let mut builder = VariantArrayBuilder::new(2);
        append_json_strict(&mut builder, "-0.0")?;
        append_json_strict(&mut builder, "-0e0")?;
        let array: StructArray = builder.build().into();
        let array = VariantArray::try_new(&array)?;

        assert_eq!(
            array.value(0),
            Variant::from(VariantDecimal4::try_new(0, 1)?)
        );
        let Variant::Double(exponent_zero) = array.value(1) else {
            return exec_err!("expected exponent form to be stored as double");
        };
        assert!(exponent_zero.is_sign_negative());
        Ok(())
    }
}
