use std::collections::BTreeMap;
use std::sync::Arc;

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
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal4, VariantDecimal8,
    VariantDecimal16,
};
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use sail_common_datafusion::variant::{
    VARIANT_METADATA_FIELD_NAME, VARIANT_VALUE_FIELD_NAME, variant_metadata_field,
};
use serde::de::{DeserializeSeed, Error as _};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

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

/// Validate the entire value before appending a row, including nested duplicate keys.
/// Numbers are classified from their original JSON tokens, not serde's f64 values.
#[derive(Debug)]
enum StrictValue {
    Scalar(Variant<'static, 'static>),
    String(String),
    Array(Vec<StrictValue>),
    Object(BTreeMap<String, StrictValue>),
}

#[derive(Clone, Copy)]
struct StrictValueSeed {
    remaining_depth: usize,
}

impl<'de> DeserializeSeed<'de> for StrictValueSeed {
    type Value = StrictValue;

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        let raw = <&RawValue>::deserialize(deserializer)?;
        let token = raw.get();
        if matches!(token.as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
            return variant_from_number(token)
                .map(StrictValue::Scalar)
                .map_err(D::Error::custom);
        }
        // Each RawValue gets its own deserializer, so carry the original serde
        // nesting limit explicitly rather than resetting it for every child.
        let remaining_depth = if token.starts_with(['[', '{']) {
            if self.remaining_depth <= 1 {
                return Err(D::Error::custom("recursion limit exceeded"));
            }
            self.remaining_depth - 1
        } else {
            self.remaining_depth
        };
        serde_json::Deserializer::from_str(token)
            .deserialize_any(StrictValueSeed { remaining_depth })
            .map_err(D::Error::custom)
    }
}

impl<'de> serde::de::Visitor<'de> for StrictValueSeed {
    type Value = StrictValue;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any valid JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue::Scalar(v.into()))
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue::String(v.to_owned()))
    }
    fn visit_string<E>(self, v: String) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue::String(v))
    }
    fn visit_unit<E>(self) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue::Scalar(Variant::Null))
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<StrictValue, A::Error> {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element_seed(self)? {
            values.push(value);
        }
        Ok(StrictValue::Array(values))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<StrictValue, A::Error> {
        let mut object = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(A::Error::custom(format!("duplicate key '{key}'")));
            }
            object.insert(key, map.next_value_seed(self)?);
        }
        Ok(StrictValue::Object(object))
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        StrictValueSeed {
            remaining_depth: 128,
        }
        .deserialize(deserializer)
    }
}

/// Spark VariantBuilder tries a signed integer, then a decimal without an
/// exponent, then a double. Decimal capacity uses the ORIGINAL precision and
/// scale: stripping trailing zeros first would incorrectly admit wider tokens.
fn variant_from_number(token: &str) -> Result<Variant<'static, 'static>> {
    if !token.contains(['.', 'e', 'E'])
        && let Ok(value) = token.parse::<i64>()
    {
        return Ok(if let Ok(value) = i8::try_from(value) {
            value.into()
        } else if let Ok(value) = i16::try_from(value) {
            value.into()
        } else if let Ok(value) = i32::try_from(value) {
            value.into()
        } else {
            value.into()
        });
    }
    if !token.contains(['e', 'E']) {
        let scale = token
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len());
        let unscaled = token.replace('.', "");
        let precision = unscaled
            .trim_start_matches('-')
            .trim_start_matches('0')
            .len()
            .max(1);
        let width = precision.max(scale);
        if width <= 38 {
            let integer = unscaled
                .parse::<i128>()
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let scale = scale as u8;
            return Ok(if width <= 9 {
                VariantDecimal4::try_new(integer as i32, scale)?.into()
            } else if width <= 18 {
                VariantDecimal8::try_new(integer as i64, scale)?.into()
            } else {
                VariantDecimal16::try_new(integer, scale)?.into()
            });
        }
    }
    // Keep IEEE negative zero (including underflow) and infinity on overflow.
    token
        .parse::<f64>()
        .map(Variant::Double)
        .map_err(|e| exec_datafusion_err!("{e}"))
}

fn append_json(value: &StrictValue, builder: &mut impl VariantBuilderExt) -> Result<()> {
    match value {
        StrictValue::Scalar(value) => builder.append_value(value.clone()),
        StrictValue::String(value) => builder.append_value(value.as_str()),
        StrictValue::Array(values) => {
            let mut list = builder.try_new_list()?;
            for value in values {
                append_json(value, &mut list)?;
            }
            list.finish();
        }
        StrictValue::Object(values) => {
            let mut object = builder.try_new_object()?;
            for (key, value) in values {
                append_json(value, &mut ObjectFieldBuilder::new(key, &mut object))?;
            }
            object.finish();
        }
    }
    Ok(())
}

/// Bound container nesting before RawValue's iterative scanner can allocate an
/// unchecked nesting stack. Only inspect the first value: trailing content is
/// intentionally accepted. Serde still validates the full JSON grammar; here we
/// only track matching delimiters outside strings (including escaped quotes).
/// The error offset identifies the first rejected byte, without reading a
/// potentially large payload after it.
// FIXME: Add allocation-budget regression coverage for excessive nesting and large
// payloads beyond the depth limit. Rejection must not allocate a stack per subtree;
// strict-mode error diagnostics may still allocate proportionally to input length.
fn check_json_depth(json: &str) -> std::result::Result<(), usize> {
    // Serde's budget is 128, with rejection on entering container 128.
    let mut containers = [0u8; 127];
    let mut depth = 0;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, byte) in json.bytes().enumerate() {
        if depth == 0 {
            match byte {
                b' ' | b'\t' | b'\r' | b'\n' => continue,
                b'[' | b'{' => {}
                // A scalar RawValue cannot contain any nested containers.
                _ => return Ok(()),
            }
        }
        if in_string {
            if escaped {
                escaped = false;
            } else {
                match byte {
                    b'\\' => escaped = true,
                    b'"' => in_string = false,
                    _ => {}
                }
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                if depth == containers.len() {
                    return Err(offset);
                }
                containers[depth] = byte;
                depth += 1;
            }
            b']' | b'}' => {
                let expected = if byte == b']' { b'[' } else { b'{' };
                if containers[depth - 1] != expected {
                    return Err(offset);
                }
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Reject duplicates but accept trailing content after a complete first value,
/// matching Spark's parse_json and try_parse_json.
fn try_parse_json_lenient(json_str: &str) -> Option<StrictValue> {
    check_json_depth(json_str).ok()?;
    if let Ok(value) = serde_json::from_str::<StrictValue>(json_str) {
        return Some(value);
    }
    let mut stream = serde_json::Deserializer::from_str(json_str).into_iter::<StrictValue>();
    stream.next().and_then(|r| r.ok())
}

/// Try to append a JSON string to the builder leniently. Returns true if successful.
fn try_append_json(builder: &mut VariantArrayBuilder, json_str: &str) -> bool {
    match try_parse_json_lenient(json_str) {
        Some(value) => append_json(&value, builder).is_ok(),
        None => false,
    }
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
    match try_parse_json_lenient(json_str) {
        Some(value) => append_json(&value, builder).map_err(|_| malformed_record_err(json_str)),
        None => Err(malformed_record_err(json_str)),
    }
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
    fn test_numeric_encoding_preserves_original_precision_and_scale() -> Result<()> {
        let cases = [
            ("-0", Variant::Int8(0)),
            ("128", Variant::Int16(128)),
            ("32768", Variant::Int32(32768)),
            ("2147483648", Variant::Int64(2147483648)),
            ("-9223372036854775808", Variant::Int64(i64::MIN)),
            ("9223372036854775807", Variant::Int64(i64::MAX)),
            ("-0.0", VariantDecimal4::try_new(0, 1)?.into()),
            ("10.1000", VariantDecimal4::try_new(101000, 4)?.into()),
            (
                "999999999.0",
                VariantDecimal8::try_new(9999999990, 1)?.into(),
            ),
            ("0.000000001", VariantDecimal4::try_new(1, 9)?.into()),
            ("0.0000000001", VariantDecimal8::try_new(1, 10)?.into()),
            (
                "0.000000000000000001",
                VariantDecimal8::try_new(1, 18)?.into(),
            ),
            (
                "0.0000000000000000001",
                VariantDecimal16::try_new(1, 19)?.into(),
            ),
            (
                "9223372036854775808",
                VariantDecimal16::try_new(9223372036854775808, 0)?.into(),
            ),
            (
                "-9223372036854775809",
                VariantDecimal16::try_new(-9223372036854775809, 0)?.into(),
            ),
        ];
        let mut builder = VariantArrayBuilder::new(cases.len());
        for (token, _) in &cases {
            append_json_strict(&mut builder, token)?;
        }
        let array = builder.build();
        for (index, (token, expected)) in cases.into_iter().enumerate() {
            assert_eq!(array.value(index), expected, "{token}");
        }
        for (token, expected) in [
            (
                "9".repeat(38),
                VariantDecimal16::try_new(10_i128.pow(38) - 1, 0)?.into(),
            ),
            (
                format!("1.{}", "0".repeat(37)),
                VariantDecimal16::try_new(10_i128.pow(37), 37)?.into(),
            ),
            (
                format!("0.{}1", "0".repeat(37)),
                VariantDecimal16::try_new(1, 38)?.into(),
            ),
            (
                format!("-0.{}", "0".repeat(38)),
                VariantDecimal16::try_new(0, 38)?.into(),
            ),
        ] {
            assert_eq!(variant_from_number(&token)?, expected, "{token}");
        }
        Ok(())
    }

    #[test]
    fn test_double_tokens_keep_ieee_sign_and_capacity_fallback() -> Result<()> {
        let cases = [
            ("1.5e-1".to_string(), 0.15_f64),
            ("-0e0".to_string(), -0.0),
            ("-1e-400".to_string(), -0.0),
            ("1e-400".to_string(), 0.0),
            ("5e-324".to_string(), f64::from_bits(1)),
            ("1e309".to_string(), f64::INFINITY),
            ("-1e309".to_string(), f64::NEG_INFINITY),
            ("9".repeat(39), 1e39),
            (format!("1.{}", "0".repeat(38)), 1.0),
            (format!("-0.{}", "0".repeat(39)), -0.0),
            (format!("0.{}10", "0".repeat(37)), 1e-38),
        ];
        let mut builder = VariantArrayBuilder::new(cases.len());
        for (token, _) in &cases {
            append_json_strict(&mut builder, token)?;
        }
        let array = builder.build();
        for (index, (token, expected)) in cases.into_iter().enumerate() {
            let Variant::Double(actual) = array.value(index) else {
                return exec_err!("expected DOUBLE for {token}");
            };
            assert_eq!(actual.to_bits(), expected.to_bits(), "{token}");
        }
        Ok(())
    }

    #[test]
    fn test_nested_values_preserve_tokens_and_reject_duplicates_atomically() -> Result<()> {
        let invalid = [
            r#"{"a":[-0.0],"a":1e0}"#,
            r#"[{"x":1,"x":2}]"#,
            r#"{"a":1,"\u0061":2}"#,
            r#"{"a":1,"a":2} trailing"#,
            "[1e0,]",
            "1e",
            "1.",
            "01",
            "0xff",
            "NaN",
            "Infinity",
        ];
        let mut builder = VariantArrayBuilder::new(invalid.len() + 2);
        append_json_strict(&mut builder, "1.2300 trailing")?;
        for token in invalid {
            assert!(!try_append_json(&mut builder, token), "{token}");
            builder.append_null();
            assert!(matches!(
                append_json_strict(&mut VariantArrayBuilder::new(1), token),
                Err(error) if error.to_string().contains("MALFORMED_RECORD_IN_PARSING")
            ));
        }
        append_json_strict(&mut builder, r#"{"n":[-0,-0.0,-0e0],"s":"-0e0"} trailing"#)?;
        let array = builder.build();
        assert_eq!(array.len(), invalid.len() + 2);
        assert_eq!(array.value(0), VariantDecimal4::try_new(12300, 4)?.into());
        for index in 1..=invalid.len() {
            assert!(array.is_null(index));
        }
        let value = array.value(invalid.len() + 1);
        let object = value
            .as_object()
            .ok_or_else(|| exec_datafusion_err!("expected object"))?;
        assert_eq!(object.get("s"), Some(Variant::from("-0e0")));
        let list = object
            .get("n")
            .ok_or_else(|| exec_datafusion_err!("expected n"))?;
        let list = list
            .as_list()
            .ok_or_else(|| exec_datafusion_err!("expected list"))?;
        assert_eq!(list.get(0), Some(Variant::Int8(0)));
        assert_eq!(list.get(1), Some(VariantDecimal4::try_new(0, 1)?.into()));
        let Some(Variant::Double(value)) = list.get(2) else {
            return exec_err!("expected DOUBLE");
        };
        assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
        Ok(())
    }

    #[test]
    fn test_raw_values_retain_the_json_recursion_limit() {
        for depth in [127, 128, 256] {
            for (open, close) in [("[", "]"), (r#"{"a":"#, "}")] {
                let input = format!("{}0{}", open.repeat(depth), close.repeat(depth));
                assert_eq!(try_parse_json_lenient(&input).is_some(), depth == 127);
            }
        }
    }

    #[test]
    fn test_depth_scan_stops_before_deep_subtrees_and_payloads() {
        for input in [
            format!("{}0{}", "[".repeat(1_000_000), "]".repeat(1_000_000)),
            format!(
                "{}\"{}\"{}",
                "[".repeat(128),
                "x".repeat(2_000_000),
                "]".repeat(128)
            ),
        ] {
            assert_eq!(check_json_depth(&input), Err(127));
            assert!(try_parse_json_lenient(&input).is_none());
        }
        let mixed = format!("{}[]{}", r#"{"a":["#.repeat(64), "]}".repeat(64));
        assert_eq!(check_json_depth(&mixed), Err(383));
        assert!(try_parse_json_lenient(&mixed).is_none());
    }

    #[test]
    fn test_depth_scan_preserves_strings_escapes_and_valid_prefixes() -> serde_json::Result<()> {
        let deep_suffix = "[".repeat(256);
        let quoted_brackets = serde_json::to_string(&format!("{}\\\"{{}}", "[".repeat(256)))?;
        for prefix in [
            "1e0".to_string(),
            "1.00".to_string(),
            "true".to_string(),
            "null".to_string(),
            quoted_brackets.clone(),
            format!("[{quoted_brackets}]"),
            format!(r#"{{{quoted_brackets}:[1.1,1e0]}}"#),
            r#"["\\", "\"", "\u005b", "\u0022", "]}"]"#.to_string(),
        ] {
            let json = format!(" \t\r\n{prefix} {deep_suffix}");
            assert!(check_json_depth(&json).is_ok());
            assert!(try_parse_json_lenient(&json).is_some(), "{prefix}");
        }
        for invalid in [
            "[}",
            "{]",
            r#"["\q"]"#,
            r#"["unterminated]"#,
            "[1e]",
            "[01]",
        ] {
            assert!(try_parse_json_lenient(invalid).is_none(), "{invalid}");
        }
        Ok(())
    }
}
