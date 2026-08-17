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
/// Returns the merged schema as a Spark type string. When all rows have the same
/// type, returns that type. When types differ, returns VARIANT. For objects,
/// merges fields from all rows.
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
