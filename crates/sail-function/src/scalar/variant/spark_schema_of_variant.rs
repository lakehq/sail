use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::utils::helper::{try_field_as_variant_array, try_parse_variant_scalar};
use crate::schema_inference::{InferredType, TypeMerger};

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
                let mut builder =
                    arrow::array::StringBuilder::with_capacity(variant_array.len(), 0);
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
    variant_type_to_spark_type(&variant_to_inferred_type(variant))
}

pub(crate) fn variant_to_inferred_type(variant: &Variant) -> InferredType {
    match variant {
        Variant::Null => InferredType::Null,
        Variant::BooleanTrue | Variant::BooleanFalse => InferredType::Boolean,
        Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) => {
            InferredType::Long
        }
        Variant::Float(_) => InferredType::Float,
        Variant::Double(_) => InferredType::Double,
        Variant::Decimal4(d) => decimal_type(d.integer() as i128, d.scale()),
        Variant::Decimal8(d) => decimal_type(d.integer() as i128, d.scale()),
        Variant::Decimal16(d) => decimal_type(d.integer(), d.scale()),
        Variant::String(_) | Variant::ShortString(_) => InferredType::String,
        Variant::Binary(_) => InferredType::Binary,
        Variant::Date(_) => InferredType::Date,
        Variant::TimestampMicros(_) | Variant::TimestampNanos(_) => InferredType::Timestamp,
        Variant::TimestampNtzMicros(_) | Variant::TimestampNtzNanos(_) => {
            InferredType::TimestampNtz
        }
        Variant::Time(_) | Variant::Uuid(_) => InferredType::String,
        Variant::Object(obj) => {
            let mut fields = Vec::with_capacity(obj.len());
            for (name, value) in obj.iter() {
                fields.push((name.to_string(), variant_to_inferred_type(&value)));
            }
            fields.sort_by(|a, b| a.0.cmp(&b.0));
            InferredType::Struct(fields)
        }
        Variant::List(list) => {
            let mut element_type = InferredType::Null;
            for element in list.iter() {
                element_type =
                    merge_variant_types(element_type, variant_to_inferred_type(&element));
            }
            InferredType::Array(Box::new(element_type))
        }
    }
}

pub(crate) fn merge_variant_types(left: InferredType, right: InferredType) -> InferredType {
    left.merge_with(right, &VariantTypeMerger)
}

struct VariantTypeMerger;

impl TypeMerger for VariantTypeMerger {
    fn merge_atomic(&self, left: InferredType, right: InferredType) -> InferredType {
        use InferredType::*;

        // Spark SchemaOfVariant uses JsonInferSchema.compatibleType, not the
        // string-promoting policy used by ordinary format schema inference.
        match (left, right) {
            (Long, Float) | (Float, Long) => Float,
            (Double, Long | Float | Decimal(_, _))
            | (Long | Float | Decimal(_, _), Double)
            | (Float, Decimal(_, _))
            | (Decimal(_, _), Float) => Double,
            // Every Variant integer is inferred as BIGINT. Spark reserves 20
            // integral digits when combining BIGINT with a decimal schema.
            (Long, decimal @ Decimal(_, _)) | (decimal @ Decimal(_, _), Long) => {
                self.merge_atomic(Decimal(20, 0), decimal)
            }
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                let scale = s1.max(s2);
                let precision = (p1 - s1).max(p2 - s2) + scale;
                if precision > 38 {
                    Double
                } else {
                    Decimal(precision, scale)
                }
            }
            _ => Variant,
        }
    }
}

pub(crate) fn variant_type_to_spark_type(inferred: &InferredType) -> String {
    match inferred {
        InferredType::Null => "VOID".to_string(),
        InferredType::Boolean => "BOOLEAN".to_string(),
        InferredType::Long => "BIGINT".to_string(),
        InferredType::Float => "FLOAT".to_string(),
        InferredType::Decimal(precision, scale) => format!("DECIMAL({precision},{scale})"),
        InferredType::Double => "DOUBLE".to_string(),
        InferredType::String => "STRING".to_string(),
        InferredType::Binary => "BINARY".to_string(),
        InferredType::Date => "DATE".to_string(),
        InferredType::Timestamp => "TIMESTAMP".to_string(),
        InferredType::TimestampNtz => "TIMESTAMP_NTZ".to_string(),
        InferredType::Array(element) => {
            format!("ARRAY<{}>", variant_type_to_spark_type(element))
        }
        InferredType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, ty)| format!("{name}: {}", variant_type_to_spark_type(ty)))
                .collect::<Vec<_>>();
            format!("OBJECT<{}>", fields.join(", "))
        }
        InferredType::Variant => "VARIANT".to_string(),
    }
}

pub(crate) fn variant_type_from_spark_type(s: &str) -> Result<InferredType> {
    match s {
        "VOID" => return Ok(InferredType::Null),
        "BOOLEAN" => return Ok(InferredType::Boolean),
        "BIGINT" => return Ok(InferredType::Long),
        "FLOAT" => return Ok(InferredType::Float),
        "DOUBLE" => return Ok(InferredType::Double),
        "STRING" => return Ok(InferredType::String),
        "BINARY" => return Ok(InferredType::Binary),
        "DATE" => return Ok(InferredType::Date),
        "TIMESTAMP" => return Ok(InferredType::Timestamp),
        "TIMESTAMP_NTZ" => return Ok(InferredType::TimestampNtz),
        "VARIANT" => return Ok(InferredType::Variant),
        _ => {}
    }
    if let Some(inner) = s.strip_circumfix("DECIMAL(", ')') {
        let (precision, scale) = inner.split_once(',').ok_or_else(|| {
            DataFusionError::Execution(format!("invalid inferred decimal type '{s}'"))
        })?;
        let precision = precision.parse::<u8>().map_err(|_| {
            DataFusionError::Execution(format!("invalid inferred decimal precision '{precision}'"))
        })?;
        let scale = scale.parse::<u8>().map_err(|_| {
            DataFusionError::Execution(format!("invalid inferred decimal scale '{scale}'"))
        })?;
        return Ok(InferredType::Decimal(precision, scale));
    }
    if let Some(inner) = s.strip_circumfix("ARRAY<", '>') {
        return Ok(InferredType::Array(Box::new(variant_type_from_spark_type(
            inner,
        )?)));
    }
    if let Some(inner) = s.strip_circumfix("OBJECT<", '>') {
        let mut fields = Vec::new();
        for field in split_top_level(inner) {
            let (name, ty) = field.split_once(": ").ok_or_else(|| {
                DataFusionError::Execution(format!("invalid inferred object field '{field}'"))
            })?;
            fields.push((
                name.trim().to_string(),
                variant_type_from_spark_type(ty.trim())?,
            ));
        }
        return Ok(InferredType::Struct(fields));
    }
    Err(DataFusionError::Execution(format!(
        "invalid inferred variant type '{s}'"
    )))
}

fn split_top_level(s: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ',' if angle_depth == 0 && paren_depth == 0 => {
                fields.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        fields.push(s[start..].trim());
    }
    fields
}

/// Spark infers decimals from their value after removing fractional trailing
/// zeros, even when their Variant encoding retains the original scale.
fn decimal_type(mut integer: i128, mut scale: u8) -> InferredType {
    while scale > 0 && integer % 10 == 0 {
        integer /= 10;
        scale -= 1;
    }
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
    InferredType::Decimal(precision, scale)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Decimal128Array, ListArray, StringViewArray};
    use arrow::buffer::OffsetBuffer;
    use arrow_schema::Field;
    use parquet_variant::{VariantDecimal4, VariantDecimal8, VariantDecimal16};
    use parquet_variant_compute::cast_to_variant;

    use super::*;
    use crate::scalar::variant::spark_parse_json::from_utf8view_arr;

    #[test]
    fn test_variant_numeric_common_types() {
        use InferredType::*;

        // Spark JsonInferSchema.compatibleType, including DecimalType.LongDecimal.
        for (left, right, expected) in [
            (Long, Float, Float),
            (Long, Double, Double),
            (Float, Double, Double),
            (Decimal(2, 1), Double, Double),
            (Decimal(2, 1), Float, Double),
            (Decimal(3, 2), Decimal(1, 0), Decimal(3, 2)),
            (Decimal(18, 18), Decimal(3, 1), Decimal(20, 18)),
            (Long, Decimal(1, 0), Decimal(20, 0)),
            (Long, Decimal(38, 0), Decimal(38, 0)),
            (Long, Decimal(18, 18), Decimal(38, 18)),
            (Long, Decimal(19, 19), Double),
            (Decimal(38, 0), Decimal(2, 1), Double),
            (Decimal(38, 38), Decimal(1, 0), Double),
            (Decimal(38, 38), Decimal(37, 37), Decimal(38, 38)),
            (Null, Decimal(3, 2), Decimal(3, 2)),
            (Long, String, Variant),
            (Boolean, Decimal(2, 1), Variant),
            (Variant, Double, Variant),
        ] {
            assert_eq!(merge_variant_types(left.clone(), right.clone()), expected);
            assert_eq!(merge_variant_types(right, left), expected);
        }
    }

    #[test]
    fn test_parsed_numeric_schemas_merge_recursively() -> Result<()> {
        let cases = [
            ("[1.1,1e0]", "ARRAY<DOUBLE>"),
            ("[1.01,1.00]", "ARRAY<DECIMAL(3,2)>"),
            ("[null,1,1.0]", "ARRAY<DECIMAL(20,0)>"),
            (
                "[0.000000000000000001,10.1000,-0.0]",
                "ARRAY<DECIMAL(20,18)>",
            ),
            (
                "[1.1,11111111111111111111111111111111111111]",
                "ARRAY<DOUBLE>",
            ),
            (
                r#"[{"a":[1.01],"b":null},{"a":[1.00],"b":1e0}]"#,
                "ARRAY<OBJECT<a: ARRAY<DECIMAL(3,2)>, b: DOUBLE>>",
            ),
            ("[1.1,true]", "ARRAY<VARIANT>"),
        ];
        let input: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            cases.iter().map(|(json, _)| *json),
        ));
        for safe in [false, true] {
            let output = from_utf8view_arr(&input, safe)?;
            let variants = VariantArray::try_new(output.as_ref())?;
            for (index, (json, expected)) in cases.iter().enumerate() {
                assert_eq!(
                    variant_to_spark_type(&variants.value(index)),
                    *expected,
                    "{json}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_decimal_array_cast_schema_merges_normalized_elements() -> Result<()> {
        let decimals = Decimal128Array::from(vec![101, 100]).with_precision_and_scale(3, 2)?;
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Decimal128(3, 2), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(decimals),
            None,
        );
        let variants = cast_to_variant(&list)?;
        assert_eq!(
            variant_to_spark_type(&variants.value(0)),
            "ARRAY<DECIMAL(3,2)>"
        );
        Ok(())
    }

    #[test]
    fn test_decimal_schema_normalizes_all_storage_widths() -> Result<()> {
        for (value, expected) in [
            (VariantDecimal4::try_new(0, 9)?.into(), "DECIMAL(1,0)"),
            (VariantDecimal4::try_new(101000, 4)?.into(), "DECIMAL(3,1)"),
            (
                VariantDecimal8::try_new(-10000000, 4)?.into(),
                "DECIMAL(4,0)",
            ),
            (VariantDecimal8::try_new(1, 18)?.into(), "DECIMAL(18,18)"),
            (VariantDecimal16::try_new(0, 38)?.into(), "DECIMAL(1,0)"),
            (
                VariantDecimal16::try_new(10_i128.pow(37), 37)?.into(),
                "DECIMAL(1,0)",
            ),
            (
                VariantDecimal16::try_new(10_i128.pow(37), 0)?.into(),
                "DECIMAL(38,0)",
            ),
            (VariantDecimal16::try_new(1, 38)?.into(), "DECIMAL(38,38)"),
        ] {
            assert_eq!(variant_to_spark_type(&value), expected);
        }
        Ok(())
    }
}
