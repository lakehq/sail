use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;

use crate::scalar::variant::spark_schema_of_variant::variant_to_spark_type;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

/// Internal representation of a merged variant schema.
#[derive(Debug, Clone, PartialEq)]
enum MergedType {
    /// A concrete primitive type (e.g., "BIGINT", "STRING", "BOOLEAN")
    Primitive(String),
    /// VOID (from null values)
    Void,
    /// An object with merged fields (field_name → merged_type)
    Object(BTreeMap<String, MergedType>),
    /// An array with a merged element type
    Array(Box<MergedType>),
    /// Mixed types that cannot be unified → VARIANT
    Variant,
}

impl MergedType {
    /// Parse a Spark type string back into a MergedType.
    fn from_spark_type(s: &str) -> Result<Self> {
        if s == "VOID" {
            return Ok(MergedType::Void);
        }
        if s == "VARIANT" {
            return Ok(MergedType::Variant);
        }
        if let Some(inner) = s.strip_prefix("OBJECT<").and_then(|s| s.strip_suffix('>')) {
            if inner.is_empty() {
                return Ok(MergedType::Object(BTreeMap::new()));
            }
            let mut fields = BTreeMap::new();
            for field_str in split_top_level(inner) {
                let (name, type_str) = field_str.split_once(": ").ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "schema_of_variant_agg: invalid object field entry '{field_str}'"
                    ))
                })?;
                fields.insert(
                    name.trim().to_string(),
                    MergedType::from_spark_type(type_str.trim())?,
                );
            }
            return Ok(MergedType::Object(fields));
        }
        if let Some(inner) = s.strip_prefix("ARRAY<").and_then(|s| s.strip_suffix('>')) {
            return Ok(MergedType::Array(Box::new(MergedType::from_spark_type(
                inner,
            )?)));
        }
        Ok(MergedType::Primitive(s.to_string()))
    }

    /// Convert to Spark type string.
    fn to_spark_type(&self) -> String {
        match self {
            MergedType::Void => "VOID".to_string(),
            MergedType::Primitive(s) => s.clone(),
            MergedType::Variant => "VARIANT".to_string(),
            MergedType::Object(fields) => {
                if fields.is_empty() {
                    return "OBJECT<>".to_string();
                }
                let fields_str: Vec<String> = fields
                    .iter()
                    .map(|(name, ty)| format!("{name}: {}", ty.to_spark_type()))
                    .collect();
                format!("OBJECT<{}>", fields_str.join(", "))
            }
            MergedType::Array(elem) => {
                format!("ARRAY<{}>", elem.to_spark_type())
            }
        }
    }

    /// Estimate heap memory usage recursively.
    fn estimated_size(&self) -> usize {
        match self {
            MergedType::Void | MergedType::Variant => 0,
            MergedType::Primitive(s) => s.len(),
            MergedType::Object(fields) => fields
                .iter()
                .map(|(k, v)| {
                    k.len() + v.estimated_size() + std::mem::size_of::<(String, MergedType)>()
                })
                .sum(),
            MergedType::Array(elem) => std::mem::size_of::<MergedType>() + elem.estimated_size(),
        }
    }

    /// Merge two types together.
    fn merge(self, other: MergedType) -> MergedType {
        match (self, other) {
            // VOID merges with anything → the other type
            (MergedType::Void, other) | (other, MergedType::Void) => other,
            // VARIANT absorbs everything
            (MergedType::Variant, _) | (_, MergedType::Variant) => MergedType::Variant,
            // Same primitive → keep
            (MergedType::Primitive(a), MergedType::Primitive(b)) if a == b => {
                MergedType::Primitive(a)
            }
            // Different primitives → VARIANT
            (MergedType::Primitive(_), MergedType::Primitive(_)) => MergedType::Variant,
            // Object + Object → merge fields
            (MergedType::Object(mut a), MergedType::Object(b)) => {
                for (key, val) in b {
                    match a.entry(key) {
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            let old = std::mem::replace(entry.get_mut(), MergedType::Void);
                            *entry.get_mut() = old.merge(val);
                        }
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(val);
                        }
                    }
                }
                MergedType::Object(a)
            }
            // Array + Array → merge element types
            (MergedType::Array(a), MergedType::Array(b)) => {
                MergedType::Array(Box::new(a.merge(*b)))
            }
            // Incompatible types → VARIANT
            _ => MergedType::Variant,
        }
    }
}

/// Split a string by top-level commas (respecting nested angle brackets and parentheses).
/// Handles types like `DECIMAL(10,2)` inside `OBJECT<...>` fields.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut angle_depth: usize = 0;
    let mut paren_depth: usize = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ',' if angle_depth == 0 && paren_depth == 0 => {
                results.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(s[start..].trim());
    }
    results
}

/// Convert a Variant to a MergedType.
fn variant_to_merged_type(variant: &Variant) -> MergedType {
    match variant {
        Variant::Null => MergedType::Void,
        Variant::Object(obj) => {
            let mut fields = BTreeMap::new();
            for (name, value) in obj.iter() {
                fields.insert(name.to_string(), variant_to_merged_type(&value));
            }
            MergedType::Object(fields)
        }
        Variant::List(list) => {
            let mut elem_type = MergedType::Void;
            for elem in list.iter() {
                elem_type = elem_type.merge(variant_to_merged_type(&elem));
            }
            MergedType::Array(Box::new(elem_type))
        }
        _ => {
            // Use the same type string as schema_of_variant for primitives
            MergedType::Primitive(variant_to_spark_type(variant))
        }
    }
}

#[derive(Debug)]
struct SchemaOfVariantAggAccumulator {
    merged_schema: Option<MergedType>,
}

impl Accumulator for SchemaOfVariantAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = &values[0];
        let variant_array = VariantArray::try_new(arr.as_ref())?;

        for variant in variant_array.iter().flatten() {
            let current_type = variant_to_merged_type(&variant);
            self.merged_schema = Some(match self.merged_schema.take() {
                Some(existing) => existing.merge(current_type),
                None => current_type,
            });
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let schema_str = self
            .merged_schema
            .as_ref()
            .map_or_else(|| "VOID".to_string(), |t| t.to_spark_type());
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
            let other_schema = MergedType::from_spark_type(schema_arr.value(i))?;
            self.merged_schema = Some(match self.merged_schema.take() {
                Some(existing) => existing.merge(other_schema),
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
            .map_or_else(|| "VOID".to_string(), |t| t.to_spark_type());
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
