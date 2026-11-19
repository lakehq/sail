// SPDX-License-Identifier: Apache-2.0

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
use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::utils::count_min_sketch::SparkCountMinSketch;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CountMinSketchFunction {
    signature: Signature,
}

impl Default for CountMinSketchFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl CountMinSketchFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(4)], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountMinSketchFunction {
    fn name(&self) -> &str {
        "count_min_sketch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_count_min_sketch_types(arg_types)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let eps = resolve_float64_literal(&acc_args, 1, "eps")?;
        let confidence = resolve_float64_literal(&acc_args, 2, "confidence")?;
        let seed = resolve_seed_literal(&acc_args, 3)?;
        Ok(Box::new(CountMinSketchAccumulator::new(
            eps, confidence, seed,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count_min_sketch"),
                DataType::Binary,
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct CountMinSketchAccumulator {
    sketch: SparkCountMinSketch,
}

impl CountMinSketchAccumulator {
    fn new(eps: f64, confidence: f64, seed: i32) -> Result<Self> {
        Ok(Self {
            sketch: SparkCountMinSketch::new(eps, confidence, seed)?,
        })
    }
}

impl Accumulator for CountMinSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sketch.update_from_array(&values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "count_min_sketch state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                let other = SparkCountMinSketch::deserialize(states.value(row))?;
                self.sketch.merge(&other)?;
            }
        }
        Ok(())
    }
}

fn validate_count_min_sketch_types(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 4 {
        return Err(invalid_arg_count_exec_err(
            "count_min_sketch",
            (4, 4),
            arg_types.len(),
        ));
    }
    match &arg_types[0] {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        data_type => {
            return Err(unsupported_data_type_exec_err(
                "count_min_sketch",
                "TINYINT, SMALLINT, INT, BIGINT, STRING or BINARY",
                data_type,
            ));
        }
    }
    if !matches!(arg_types[1], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double eps argument, got {}",
            arg_types[1]
        );
    }
    if !matches!(arg_types[2], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double confidence argument, got {}",
            arg_types[2]
        );
    }
    if !matches!(arg_types[3], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "count_min_sketch requires an integer seed argument, got {}",
            arg_types[3]
        );
    }
    Ok(())
}

fn resolve_float64_literal(args: &AccumulatorArgs, index: usize, name: &str) -> Result<f64> {
    let value = args.exprs.get(index).ok_or_else(|| {
        DataFusionError::Plan(format!("count_min_sketch requires a {name} argument"))
    })?;
    let value = get_scalar_value(value).map_err(|_| {
        DataFusionError::Plan(format!(
            "count_min_sketch requires {name} to be a constant double value"
        ))
    })?;
    // `validate_count_min_sketch_types` already rejects anything but Float64 for
    // eps/confidence, so only the Float64 literal can reach here.
    match value {
        ScalarValue::Float64(Some(value)) => Ok(value),
        value => Err(DataFusionError::Plan(format!(
            "count_min_sketch requires {name} to be a non-null double literal, got {}",
            value.data_type()
        ))),
    }
}

fn resolve_seed_literal(args: &AccumulatorArgs, index: usize) -> Result<i32> {
    let value = args.exprs.get(index).ok_or_else(|| {
        DataFusionError::Plan("count_min_sketch requires a seed argument".to_string())
    })?;
    let value = get_scalar_value(value).map_err(|_| {
        DataFusionError::Plan("count_min_sketch requires seed to be a constant integer".to_string())
    })?;
    match value {
        ScalarValue::Int32(Some(value)) => Ok(value),
        ScalarValue::Int64(Some(value)) => Ok(value as i32),
        value => Err(DataFusionError::Plan(format!(
            "count_min_sketch requires seed to be a non-null integer literal, got {}",
            value.data_type()
        ))),
    }
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
