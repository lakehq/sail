use std::sync::Arc;

use arrow::array::ArrayRef;
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

/// Returns the schema (type string) of a variant value using Spark type names.
///
/// Examples:
/// - `schema_of_variant(parse_json('42'))` → `"BIGINT"`
/// - `schema_of_variant(parse_json('{"a":1}'))` → `"OBJECT<a: BIGINT>"`
/// - `schema_of_variant(parse_json('[1,2]'))` → `"ARRAY<BIGINT>"`
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#schema_of_variant>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfVariantUdf {
    signature: Signature,
}

impl SparkSchemaOfVariantUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SparkSchemaOfVariantUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkSchemaOfVariantUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "schema_of_variant"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
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
            ColumnarValue::Scalar(scalar_variant) => {
                if scalar_variant.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                let variant_array = try_parse_variant_scalar(scalar_variant)?;
                if variant_array.is_null(0) {
                    ColumnarValue::Scalar(ScalarValue::Utf8(None))
                } else {
                    let variant = variant_array.value(0);
                    let schema = variant_to_spark_type(&variant);
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(schema)))
                }
            }
            ColumnarValue::Array(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let mut builder = arrow::array::StringBuilder::with_capacity(
                    variant_array.len(),
                    variant_array.len() * 10,
                );
                for v in variant_array.iter() {
                    match v {
                        Some(variant) => {
                            let schema = variant_to_spark_type(&variant);
                            builder.append_value(schema);
                        }
                        None => builder.append_null(),
                    }
                }
                let result = builder.finish();
                ColumnarValue::Array(Arc::new(result) as ArrayRef)
            }
        };

        Ok(out)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "schema_of_variant",
                (1, 1),
                arg_types.len(),
            ));
        }
        Ok(vec![arg_types[0].clone()])
    }
}

/// Convert a Variant value to its Spark type string representation.
pub(crate) fn variant_to_spark_type(variant: &Variant) -> String {
    match variant {
        Variant::Null => "VOID".to_string(),
        Variant::BooleanTrue | Variant::BooleanFalse => "BOOLEAN".to_string(),
        Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) => {
            "BIGINT".to_string()
        }
        Variant::Float(_) => "FLOAT".to_string(),
        Variant::Double(_) => "DOUBLE".to_string(),
        Variant::Decimal4(d) => decimal_type_string(d.integer() as i128, d.scale()),
        Variant::Decimal8(d) => decimal_type_string(d.integer() as i128, d.scale()),
        Variant::Decimal16(d) => decimal_type_string(d.integer(), d.scale()),
        Variant::String(_) | Variant::ShortString(_) => "STRING".to_string(),
        Variant::Binary(_) => "BINARY".to_string(),
        Variant::Date(_) => "DATE".to_string(),
        Variant::TimestampMicros(_) | Variant::TimestampNanos(_) => "TIMESTAMP".to_string(),
        Variant::TimestampNtzMicros(_) | Variant::TimestampNtzNanos(_) => {
            "TIMESTAMP_NTZ".to_string()
        }
        Variant::Time(_) => "STRING".to_string(),
        Variant::Uuid(_) => "STRING".to_string(),
        Variant::Object(obj) => {
            if obj.is_empty() {
                return "OBJECT<>".to_string();
            }
            // Collect fields, sorted by field name (Spark sorts alphabetically)
            let mut fields: Vec<(String, String)> = Vec::with_capacity(obj.len());
            for (name, value) in obj.iter() {
                fields.push((name.to_string(), variant_to_spark_type(&value)));
            }
            fields.sort_by(|a, b| a.0.cmp(&b.0));
            let fields_str: Vec<String> = fields
                .iter()
                .map(|(name, ty)| format!("{name}: {ty}"))
                .collect();
            format!("OBJECT<{}>", fields_str.join(", "))
        }
        Variant::List(list) => {
            if list.is_empty() {
                return "ARRAY<VOID>".to_string();
            }
            let element_types: Vec<String> =
                list.iter().map(|e| variant_to_spark_type(&e)).collect();
            let merged = merge_types(&element_types);
            format!("ARRAY<{merged}>")
        }
    }
}

/// Merge a list of Spark type strings into a single type.
///
/// Rules (matching Spark behavior):
/// - VOID is absorbed by any non-VOID type: `[VOID, BIGINT]` → `BIGINT`
/// - OBJECT types with different fields are merged (field union): `[OBJECT<a: X>, OBJECT<b: Y>]` → `OBJECT<a: X, b: Y>`
/// - All other mismatches → `VARIANT`
fn merge_types(types: &[String]) -> String {
    // Filter out VOIDs
    let non_void: Vec<&String> = types.iter().filter(|t| t.as_str() != "VOID").collect();

    if non_void.is_empty() {
        return "VOID".to_string();
    }

    // Check if all non-void types are identical
    if non_void.iter().all(|t| *t == non_void[0]) {
        return non_void[0].clone();
    }

    // Try merging OBJECTs: if all non-void types are OBJECT<...>, union their fields
    if non_void.iter().all(|t| t.starts_with("OBJECT<")) {
        return merge_object_types(&non_void);
    }

    "VARIANT".to_string()
}

/// Merge multiple OBJECT<...> type strings by unioning their fields.
fn merge_object_types(types: &[&String]) -> String {
    let mut merged_fields: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();

    for ty in types {
        // Strip "OBJECT<" prefix and ">" suffix
        let inner = ty
            .strip_prefix("OBJECT<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or("");

        if inner.is_empty() {
            continue;
        }

        // Parse fields: "a: BIGINT, b: STRING" → [("a", "BIGINT"), ("b", "STRING")]
        // Handle nested types by tracking angle bracket depth
        for field_str in split_fields(inner) {
            if let Some((name, field_type)) = field_str.split_once(':') {
                let name = name.trim().to_string();
                let field_type = field_type.trim().to_string();
                merged_fields.entry(name).or_insert(field_type);
            }
        }
    }

    if merged_fields.is_empty() {
        return "OBJECT<>".to_string();
    }

    let fields_str: Vec<String> = merged_fields
        .iter()
        .map(|(name, ty)| format!("{name}: {ty}"))
        .collect();
    format!("OBJECT<{}>", fields_str.join(", "))
}

/// Split a comma-separated field list respecting nested angle brackets.
/// e.g. "a: OBJECT<x: BIGINT>, b: ARRAY<STRING>" → ["a: OBJECT<x: BIGINT>", "b: ARRAY<STRING>"]
fn split_fields(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                result.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        result.push(last);
    }
    result
}

/// Compute the decimal precision from the integer value and scale.
fn decimal_type_string(integer: i128, scale: u8) -> String {
    let abs = integer.unsigned_abs();
    let int_digits = if abs == 0 {
        1u8
    } else {
        // Number of decimal digits in the integer part
        let mut d = 0u8;
        let mut n = abs;
        while n > 0 {
            d += 1;
            n /= 10;
        }
        d
    };
    let precision = int_digits.max(scale);
    format!("DECIMAL({precision},{scale})")
}
