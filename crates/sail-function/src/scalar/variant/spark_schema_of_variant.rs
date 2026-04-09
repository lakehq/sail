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
            // Check if all elements have the same type
            let mut element_types: Vec<String> = Vec::new();
            for elem in list.iter() {
                let ty = variant_to_spark_type(&elem);
                if !element_types.contains(&ty) {
                    element_types.push(ty);
                }
            }
            if element_types.len() == 1 {
                format!("ARRAY<{}>", element_types[0])
            } else {
                // Mixed types → VARIANT
                "ARRAY<VARIANT>".to_string()
            }
        }
    }
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
