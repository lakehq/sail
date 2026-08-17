use std::fmt::Debug;

use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};

use super::utils::get_scalar_value;
use crate::hll_sketch::{
    DEFAULT_LG_CONFIG_K, empty_hll_union_bytes, new_hll_sketch, normalize_hll_sketch_bytes,
    union_hll_sketches, update_hll_sketch_from_array, validate_lg_config_k,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllSketchAggFunction {
    signature: Signature,
}

impl Default for HllSketchAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketchAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for HllSketchAggFunction {
    fn name(&self) -> &str {
        "hll_sketch_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_hll_sketch_agg_types("hll_sketch_agg", arg_types)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let lg_config_k = resolve_lg_config_k(&acc_args, 1, "hll_sketch_agg")?;
        Ok(Box::new(HllSketchAggAccumulator::new(lg_config_k)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "hll_sketch"),
                DataType::Binary,
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllUnionAggFunction {
    signature: Signature,
}

impl Default for HllUnionAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllUnionAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for HllUnionAggFunction {
    fn name(&self) -> &str {
        "hll_union_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_hll_binary_agg_types("hll_union_agg", arg_types)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let allow_different_lg_config_k =
            resolve_allow_different_lg_config_k(&acc_args, 1, "hll_union_agg")?;
        Ok(Box::new(HllUnionAggAccumulator::new(
            allow_different_lg_config_k,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "hll_union"),
                DataType::Binary,
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct HllSketchAggAccumulator {
    sketch: datasketches::hll::HllSketch,
    merged: Option<Vec<u8>>,
}

impl HllSketchAggAccumulator {
    fn new(lg_config_k: u8) -> Self {
        Self {
            sketch: new_hll_sketch(lg_config_k),
            merged: None,
        }
    }

    fn update_bytes(&self) -> Vec<u8> {
        self.sketch.serialize()
    }

    fn evaluate_bytes(&self) -> Result<Vec<u8>> {
        let update_bytes = self.update_bytes();
        if let Some(merged) = &self.merged {
            union_hll_sketches(
                [merged.as_slice(), update_bytes.as_slice()],
                true,
                "hll_sketch_agg",
            )
        } else {
            Ok(update_bytes)
        }
    }

    fn merge_one(&mut self, bytes: &[u8]) -> Result<()> {
        self.merged = Some(if let Some(merged) = &self.merged {
            union_hll_sketches([merged.as_slice(), bytes], true, "hll_sketch_agg")?
        } else {
            normalize_hll_sketch_bytes(bytes, "hll_sketch_agg")?
        });
        Ok(())
    }
}

impl Accumulator for HllSketchAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        update_hll_sketch_from_array(&mut self.sketch, &values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.evaluate_bytes()?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .merged
                .as_ref()
                .map(|bytes| bytes.capacity())
                .unwrap_or_default()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.evaluate_bytes()?))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "hll_sketch_agg state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                self.merge_one(states.value(row))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct HllUnionAggAccumulator {
    sketch: Option<Vec<u8>>,
    allow_different_lg_config_k: bool,
}

impl HllUnionAggAccumulator {
    fn new(allow_different_lg_config_k: bool) -> Self {
        Self {
            sketch: None,
            allow_different_lg_config_k,
        }
    }

    fn merge_one(&mut self, bytes: &[u8]) -> Result<()> {
        self.sketch = Some(if let Some(sketch) = &self.sketch {
            union_hll_sketches(
                [sketch.as_slice(), bytes],
                self.allow_different_lg_config_k,
                "hll_union_agg",
            )?
        } else {
            normalize_hll_sketch_bytes(bytes, "hll_union_agg")?
        });
        Ok(())
    }

    fn evaluate_bytes(&self) -> Vec<u8> {
        self.sketch.clone().unwrap_or_else(empty_hll_union_bytes)
    }
}

impl Accumulator for HllUnionAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = as_binary_array(&values[0], "hll_union_agg")? else {
            return Ok(());
        };
        for row in 0..values.len() {
            if !values.is_null(row) {
                self.merge_one(values.value(row))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.evaluate_bytes())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .sketch
                .as_ref()
                .map(|bytes| bytes.capacity())
                .unwrap_or_default()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(self.sketch.clone())])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "hll_union_agg state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                self.merge_one(states.value(row))?;
            }
        }
        Ok(())
    }
}

fn resolve_lg_config_k(args: &AccumulatorArgs, index: usize, function_name: &str) -> Result<u8> {
    let Some(expr) = args.exprs.get(index) else {
        return validate_lg_config_k(DEFAULT_LG_CONFIG_K, function_name);
    };
    let value = get_scalar_value(expr).map_err(|_| {
        DataFusionError::Plan(format!(
            "{function_name} requires lgConfigK to be a constant integer"
        ))
    })?;
    let value = match value {
        ScalarValue::Int32(Some(value)) => value,
        ScalarValue::Int64(Some(value)) => i32::try_from(value).map_err(|_| {
            DataFusionError::Plan(format!(
                "{function_name} requires lgConfigK between 4 and 21, got {value}"
            ))
        })?,
        value => {
            return exec_err!(
                "{function_name} requires lgConfigK to be a non-null integer literal, got {}",
                value.data_type()
            );
        }
    };
    validate_lg_config_k(value, function_name)
}

fn resolve_allow_different_lg_config_k(
    args: &AccumulatorArgs,
    index: usize,
    function_name: &str,
) -> Result<bool> {
    let Some(expr) = args.exprs.get(index) else {
        return Ok(false);
    };
    let value = get_scalar_value(expr).map_err(|_| {
        DataFusionError::Plan(format!(
            "{function_name} requires allowDifferentLgConfigK to be a constant boolean"
        ))
    })?;
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(value),
        value => exec_err!(
            "{function_name} requires allowDifferentLgConfigK to be a non-null boolean literal, got {}",
            value.data_type()
        ),
    }
}

fn validate_hll_sketch_agg_types(function_name: &str, arg_types: &[DataType]) -> Result<()> {
    if !(1..=2).contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} requires 1 or 2 arguments, got {}",
            arg_types.len()
        );
    }
    match &arg_types[0] {
        DataType::Int32
        | DataType::Int64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        data_type => {
            return exec_err!("{function_name} does not support input type {data_type}");
        }
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "{function_name} requires an integer lgConfigK argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}

fn validate_hll_binary_agg_types(function_name: &str, arg_types: &[DataType]) -> Result<()> {
    if !(1..=2).contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} requires 1 or 2 arguments, got {}",
            arg_types.len()
        );
    }
    if !matches!(arg_types[0], DataType::Binary | DataType::Null) {
        return exec_err!(
            "{function_name} requires a binary sketch argument, got {}",
            arg_types[0]
        );
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Boolean) {
        return exec_err!(
            "{function_name} requires a boolean allowDifferentLgConfigK argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}

fn as_binary_array<'a>(array: &'a ArrayRef, context: &str) -> Result<Option<&'a BinaryArray>> {
    if matches!(array.data_type(), DataType::Null) {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(Some)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{context} expected BinaryArray, got {}",
                array.data_type()
            ))
        })
}
