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
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use parquet_variant_json::append_json;
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

/// A `serde_json::Value` wrapper that rejects objects with duplicate keys
/// during deserialization. Spark's `parse_json` treats duplicate keys in
/// any nested object as a parse failure (MALFORMED_RECORD_IN_PARSING).
struct StrictValue(serde_json::Value);

struct StrictValueVisitor;

impl<'de> serde::de::Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any valid JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }
    fn visit_i64<E>(self, v: i64) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }
    fn visit_u64<E>(self, v: u64) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }
    fn visit_f64<E>(self, v: f64) -> std::result::Result<StrictValue, E> {
        // Spark normalizes negative zero to positive zero in variants, e.g.
        // `parse_json('-0')` / `parse_json('-0.0')` render as `0`.
        let v = if v == 0.0 { 0.0 } else { v };
        Ok(StrictValue(v.into()))
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(v.to_owned().into()))
    }
    fn visit_string<E>(self, v: String) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }
    fn visit_none<E>(self) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(serde_json::Value::Null))
    }
    fn visit_unit<E>(self) -> std::result::Result<StrictValue, E> {
        Ok(StrictValue(serde_json::Value::Null))
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<StrictValue, A::Error> {
        let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(StrictValue(v)) = seq.next_element()? {
            vec.push(v);
        }
        Ok(StrictValue(serde_json::Value::Array(vec)))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<StrictValue, A::Error> {
        let mut obj = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let StrictValue(v) = map.next_value()?;
            if obj.contains_key(&key) {
                return Err(serde::de::Error::custom(format!("duplicate key '{key}'")));
            }
            obj.insert(key, v);
        }
        Ok(StrictValue(serde_json::Value::Object(obj)))
    }
}

impl<'de> serde::Deserialize<'de> for StrictValue {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

/// Try to parse a JSON string leniently, rejecting objects with duplicate
/// keys. If `serde_json::from_str` fails because of trailing garbage, use
/// streaming parsing to read the first complete JSON value (matching
/// Spark's `parse_json` / `try_parse_json`, which both accept trailing
/// content after a valid prefix).
fn try_parse_json_lenient(json_str: &str) -> Option<serde_json::Value> {
    if let Ok(StrictValue(value)) = serde_json::from_str::<StrictValue>(json_str) {
        return Some(value);
    }
    let mut stream = serde_json::Deserializer::from_str(json_str).into_iter::<StrictValue>();
    stream.next().and_then(|r| r.ok()).map(|sv| sv.0)
}

/// Try to append a JSON string to the builder leniently. Returns true if successful.
fn try_append_json(builder: &mut VariantArrayBuilder, json_str: &str) -> bool {
    append_exact_json(builder, json_str).is_ok()
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
    append_exact_json(builder, json_str).map_err(|_| malformed_record_err(json_str))
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
        // ParseJson's StaticInvoke declares returnNullable = !failOnError
        // (variantExpressions.scala:58-68); SQL NULL input still propagates.
        let nullable = self.safe || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(
            Field::new(self.name(), data_type, nullable).with_extension_type(VariantType),
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

/// Validate duplicate keys as before, but append numbers from their original
/// JSON tokens. Going through serde_json::Number/f64 loses decimal precision
/// before CAST can perform its checked conversion.
fn append_exact_json(builder: &mut VariantArrayBuilder, json: &str) -> Result<()> {
    if try_parse_json_lenient(json).is_none() {
        return Err(malformed_record_err(json));
    }
    let mut stream =
        serde_json::Deserializer::from_str(json).into_iter::<Box<serde_json::value::RawValue>>();
    let raw = stream
        .next()
        .transpose()
        .map_err(|_| malformed_record_err(json))?
        .ok_or_else(|| malformed_record_err(json))?;
    append_raw_json(&raw, builder)
}

fn append_raw_json(
    raw: &serde_json::value::RawValue,
    builder: &mut impl parquet_variant::VariantBuilderExt,
) -> Result<()> {
    use parquet_variant::{ObjectFieldBuilder, VariantDecimal4, VariantDecimal8, VariantDecimal16};

    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'[') => {
            let values: Vec<Box<serde_json::value::RawValue>> =
                serde_json::from_str(text).map_err(|_| malformed_record_err(text))?;
            let mut list = builder.try_new_list()?;
            for value in values {
                append_raw_json(&value, &mut list)?;
            }
            list.finish();
        }
        Some(b'{') => {
            let values: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
                serde_json::from_str(text).map_err(|_| malformed_record_err(text))?;
            let mut object = builder.try_new_object()?;
            for (key, value) in values {
                append_raw_json(&value, &mut ObjectFieldBuilder::new(&key, &mut object))?;
            }
            object.finish();
        }
        Some(b'-' | b'0'..=b'9') => {
            // Spark VariantBuilder tries integers, then decimal tokens up to
            // precision/scale 38, and only then falls back to double. Exponent
            // notation always goes to double (VariantBuilder.java:590-644).
            if let Ok(integer) = text.parse::<i64>() {
                // Spark VariantBuilder.appendLong chooses the smallest signed
                // width. Preserve compact integers while retaining exact decimals.
                if let Ok(value) = i8::try_from(integer) {
                    builder.append_value(value);
                } else if let Ok(value) = i16::try_from(integer) {
                    builder.append_value(value);
                } else if let Ok(value) = i32::try_from(integer) {
                    builder.append_value(value);
                } else {
                    builder.append_value(integer);
                }
                return Ok(());
            }
            if !text.contains(['e', 'E']) {
                let scale = text
                    .split_once('.')
                    .map_or(0, |(_, fraction)| fraction.len());
                if scale <= 38
                    && let Ok(mut integer) = text.replace('.', "").parse::<i128>()
                    && integer.unsigned_abs() < 10_u128.pow(38)
                {
                    let mut scale = scale as u8;
                    // Spark's Variant.getDecimal strips trailing zeros.
                    while scale > 0 && integer % 10 == 0 {
                        integer /= 10;
                        scale -= 1;
                    }
                    if let Some(decimal) = i32::try_from(integer)
                        .ok()
                        .and_then(|value| VariantDecimal4::try_new(value, scale).ok())
                    {
                        builder.append_value(decimal);
                    } else if let Some(decimal) = i64::try_from(integer)
                        .ok()
                        .and_then(|value| VariantDecimal8::try_new(value, scale).ok())
                    {
                        builder.append_value(decimal);
                    } else {
                        builder.append_value(VariantDecimal16::try_new(integer, scale)?);
                    }
                    return Ok(());
                }
            }
            let value = text
                .parse::<f64>()
                .map_err(|_| malformed_record_err(text))?;
            // This is reached only for exponent-notation input (the plain-decimal
            // path above already returned). Spark keeps a scientific negative
            // zero as `-0.0` -- only the non-exponent forms (`-0`, `-0.0`)
            // normalize to `0` -- so the sign of the parsed value must survive
            // as-is, unlike a decimal zero.
            builder.append_value(value);
        }
        _ => append_json(
            &serde_json::from_str(text).map_err(|_| malformed_record_err(text))?,
            builder,
        )?,
    }
    Ok(())
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
    fn test_parse_json_scientific_notation_is_numeric() -> Result<()> {
        let Some(serde_json::Value::Number(number)) = try_parse_json_lenient("1.5e-1") else {
            return exec_err!("expected a JSON number");
        };
        assert_eq!(number.as_f64(), Some(0.15));
        Ok(())
    }
}
