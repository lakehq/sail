use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use parquet_variant_compute::VariantArray;

use crate::scalar::variant::spark_schema_of_variant::{
    merge_variant_types, variant_to_inferred_type, variant_type_from_spark_type,
    variant_type_to_spark_type,
};
use crate::schema_inference::InferredType;

/// Aggregate function that merges variant schemas across rows.
///
/// Returns the merged schema as a Spark type string, widening compatible numeric
/// types and merging object fields and array elements. Incompatible types fall
/// back to VARIANT.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#schema_of_variant_agg>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SchemaOfVariantAggFunction {
    signature: Signature,
}

impl SchemaOfVariantAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for SchemaOfVariantAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for SchemaOfVariantAggFunction {
    fn name(&self) -> &str {
        "schema_of_variant_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // We store the merged schema as a single UTF-8 string in the state.
        // This is a serialized representation of the merged type.
        Ok(vec![Arc::new(Field::new(
            "merged_schema",
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SchemaOfVariantAggAccumulator {
            merged_schema: None,
        }))
    }
}

#[derive(Debug)]
struct SchemaOfVariantAggAccumulator {
    merged_schema: Option<InferredType>,
}

impl Accumulator for SchemaOfVariantAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = &values[0];
        let variant_array = VariantArray::try_new(arr.as_ref())?;

        for variant in variant_array.iter().flatten() {
            let current_type = variant_to_inferred_type(&variant);
            self.merged_schema = Some(match self.merged_schema.take() {
                Some(existing) => merge_variant_types(existing, current_type),
                None => current_type,
            });
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let schema_str = self
            .merged_schema
            .as_ref()
            .map_or_else(|| "VOID".to_string(), variant_type_to_spark_type);
        Ok(vec![ScalarValue::Utf8(Some(schema_str))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let schema_arr = states[0]
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "schema_of_variant_agg: expected StringArray state".to_string(),
                )
            })?;

        for i in 0..schema_arr.len() {
            if schema_arr.is_null(i) {
                continue;
            }
            let other_schema = variant_type_from_spark_type(schema_arr.value(i))?;
            self.merged_schema = Some(match self.merged_schema.take() {
                Some(existing) => merge_variant_types(existing, other_schema),
                None => other_schema,
            });
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // When no non-null values were seen, return "VOID" (matching Spark behavior)
        let result = self
            .merged_schema
            .as_ref()
            .map_or_else(|| "VOID".to_string(), variant_type_to_spark_type);
        Ok(ScalarValue::Utf8(Some(result)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .merged_schema
                .as_ref()
                .map_or(0, |s| s.estimated_size())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Decimal128Array, StringArray, StringViewArray};
    use parquet_variant_compute::cast_to_variant;

    use super::*;
    use crate::scalar::variant::spark_parse_json::from_utf8view_arr;

    fn assert_update_and_merge(input: ArrayRef, expected: &str) -> Result<()> {
        let expected = ScalarValue::Utf8(Some(expected.to_string()));
        let mut direct = SchemaOfVariantAggAccumulator {
            merged_schema: None,
        };
        direct.update_batch(std::slice::from_ref(&input))?;
        assert_eq!(direct.evaluate()?, expected);

        // Exercise serialized partial states, multi-row merge batches, an empty
        // partial, SQL NULL state, and a second level of final aggregation.
        for chunk_size in [1, 2, 3] {
            let mut partials = vec![ScalarValue::Utf8(None)];
            let mut empty = SchemaOfVariantAggAccumulator {
                merged_schema: None,
            };
            partials.extend(empty.state()?);
            for offset in (0..input.len()).step_by(chunk_size) {
                let mut partial = SchemaOfVariantAggAccumulator {
                    merged_schema: None,
                };
                partial
                    .update_batch(&[input.slice(offset, chunk_size.min(input.len() - offset))])?;
                partials.extend(partial.state()?);
            }
            for reverse in [false, true] {
                let mut states = partials.clone();
                if reverse {
                    states.reverse();
                }
                let mut merged = SchemaOfVariantAggAccumulator {
                    merged_schema: None,
                };
                merged.merge_batch(&[ScalarValue::iter_to_array(states)?])?;
                assert_eq!(merged.evaluate()?, expected);
                let mut final_agg = SchemaOfVariantAggAccumulator {
                    merged_schema: None,
                };
                final_agg.merge_batch(&[ScalarValue::iter_to_array(merged.state()?)?])?;
                final_agg.merge_batch(&[Arc::new(StringArray::from(vec![None, Some("VOID")]))])?;
                assert_eq!(final_agg.evaluate()?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn test_numeric_schema_update_and_partial_final_merge() -> Result<()> {
        for (json, expected) in [
            (vec![Some("1.1"), Some("1e0"), None, Some("null")], "DOUBLE"),
            (vec![Some("1.01"), Some("1.00"), None], "DECIMAL(3,2)"),
            (
                vec![Some("null"), Some("1"), Some("1.0"), None],
                "DECIMAL(20,0)",
            ),
            (
                vec![Some("0.000000000000000001"), Some("10.1000"), Some("-0.0")],
                "DECIMAL(20,18)",
            ),
            (
                vec![Some("1.1"), Some("11111111111111111111111111111111111111")],
                "DOUBLE",
            ),
            (
                vec![
                    Some(r#"{"a":[1.01],"b":null}"#),
                    Some(r#"{"a":[1.00],"b":1e0}"#),
                ],
                "OBJECT<a: ARRAY<DECIMAL(3,2)>, b: DOUBLE>",
            ),
            (vec![Some("1.1"), Some("true")], "VARIANT"),
            (vec![Some("null"), None], "VOID"),
        ] {
            let input: ArrayRef = Arc::new(StringViewArray::from(json));
            assert_update_and_merge(from_utf8view_arr(&input, false)?, expected)?;
        }
        Ok(())
    }

    #[test]
    fn test_decimal_cast_schema_update_and_partial_final_merge() -> Result<()> {
        let decimals = Decimal128Array::from(vec![Some(101), Some(100), None])
            .with_precision_and_scale(3, 2)?;
        let variants: arrow::array::StructArray = cast_to_variant(&decimals)?.into();
        assert_update_and_merge(Arc::new(variants), "DECIMAL(3,2)")
    }
}
