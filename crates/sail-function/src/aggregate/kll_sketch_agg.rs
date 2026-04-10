use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::as_int64_array;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::aggregate::utils::get_scalar_value;

/// Default value for the k parameter when not specified.
const DEFAULT_K: u16 = 200;

/// Extract the k parameter from the accumulator arguments (second argument, if
/// present). Falls back to [`DEFAULT_K`] when there is only one argument.
fn extract_k(acc_args: &AccumulatorArgs) -> Result<u16> {
    if acc_args.exprs.len() < 2 {
        return Ok(DEFAULT_K);
    }
    let scalar = get_scalar_value(&acc_args.exprs[1])?;
    let k_i64 = match scalar {
        ScalarValue::Int8(Some(v)) => v as i64,
        ScalarValue::Int16(Some(v)) => v as i64,
        ScalarValue::Int32(Some(v)) => v as i64,
        ScalarValue::Int64(Some(v)) => v,
        ScalarValue::UInt8(Some(v)) => v as i64,
        ScalarValue::UInt16(Some(v)) => v as i64,
        ScalarValue::UInt32(Some(v)) => v as i64,
        ScalarValue::UInt64(Some(v)) => v as i64,
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => {
            return Err(DataFusionError::Plan(
                "kll_sketch_agg requires a non-null integer literal for k".to_string(),
            ))
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "kll_sketch_agg requires an integer literal for k, got {}",
                other.data_type()
            )))
        }
    };
    if !(8..=65535).contains(&k_i64) {
        return Err(DataFusionError::Plan(format!(
            "kll_sketch_agg requires k to be in the range 8..65535, got {k_i64}",
        )));
    }
    Ok(k_i64 as u16)
}

// ---------------------------------------------------------------------------
// Binary serialization helpers
//
// The serialised format is intentionally simple:
//   [k: 2 bytes LE] [n: 8 bytes LE] [values …]
// where each value is encoded in little-endian in its native width
// (8 bytes for i64/f64, 4 bytes for f32).
// ---------------------------------------------------------------------------

fn serialize_header(k: u16, n: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    buf.extend_from_slice(&k.to_le_bytes());
    buf.extend_from_slice(&n.to_le_bytes());
    buf
}

fn deserialize_header(data: &[u8]) -> Result<(u16, u64)> {
    if data.len() < 10 {
        return Err(DataFusionError::Internal(
            "kll_sketch_agg: state binary too short for header".to_string(),
        ));
    }
    let k = u16::from_le_bytes([data[0], data[1]]);
    let n = u64::from_le_bytes(data[2..10].try_into().map_err(|_| {
        DataFusionError::Internal("kll_sketch_agg: failed to parse n from state".to_string())
    })?);
    Ok((k, n))
}

// ===========================================================================
// kll_sketch_agg_bigint
// ===========================================================================

/// Aggregate function that builds a compact binary KLL sketch from BIGINT values.
#[derive(PartialEq, Eq, Hash)]
pub struct KllSketchAggBigintFunction {
    signature: Signature,
}

impl Debug for KllSketchAggBigintFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketchAggBigintFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KllSketchAggBigintFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchAggBigintFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for KllSketchAggBigintFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_agg_bigint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = extract_k(&acc_args)?;
        Ok(Box::new(KllSketchAggBigintAccumulator::new(k)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("sketch", DataType::Binary, true).into()
        ])
    }
}

#[derive(Debug)]
struct KllSketchAggBigintAccumulator {
    k: u16,
    values: Vec<i64>,
}

impl KllSketchAggBigintAccumulator {
    fn new(k: u16) -> Self {
        Self {
            k,
            values: Vec::new(),
        }
    }

    fn to_binary(&self) -> Vec<u8> {
        let mut sorted = self.values.clone();
        sorted.sort();
        let n = sorted.len() as u64;
        let mut buf = serialize_header(self.k, n);
        for v in &sorted {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
}

impl Accumulator for KllSketchAggBigintAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_int64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.values.push(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.to_binary())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<i64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.to_binary()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "kll_sketch_agg_bigint expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let data = binary_array.value(i);
            let (_k, n) = deserialize_header(data)?;
            let values_data = &data[10..];
            let expected_len = (n as usize) * 8;
            if values_data.len() != expected_len {
                return Err(DataFusionError::Internal(format!(
                    "kll_sketch_agg_bigint: expected {} value bytes, got {}",
                    expected_len,
                    values_data.len()
                )));
            }
            for chunk in values_data.chunks_exact(8) {
                let v = i64::from_le_bytes(chunk.try_into().map_err(|_| {
                    DataFusionError::Internal(
                        "kll_sketch_agg_bigint: failed to parse i64 from state".to_string(),
                    )
                })?);
                self.values.push(v);
            }
        }
        Ok(())
    }
}

// ===========================================================================
// kll_sketch_agg_float
// ===========================================================================

/// Aggregate function that builds a compact binary KLL sketch from FLOAT values.
#[derive(PartialEq, Eq, Hash)]
pub struct KllSketchAggFloatFunction {
    signature: Signature,
}

impl Debug for KllSketchAggFloatFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketchAggFloatFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KllSketchAggFloatFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchAggFloatFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float32]),
                    TypeSignature::Exact(vec![DataType::Float32, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for KllSketchAggFloatFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_agg_float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = extract_k(&acc_args)?;
        Ok(Box::new(KllSketchAggFloatAccumulator::new(k)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("sketch", DataType::Binary, true).into()
        ])
    }
}

#[derive(Debug)]
struct KllSketchAggFloatAccumulator {
    k: u16,
    values: Vec<f32>,
}

impl KllSketchAggFloatAccumulator {
    fn new(k: u16) -> Self {
        Self {
            k,
            values: Vec::new(),
        }
    }

    fn to_binary(&self) -> Vec<u8> {
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let n = sorted.len() as u64;
        let mut buf = serialize_header(self.k, n);
        for v in &sorted {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
}

impl Accumulator for KllSketchAggFloatAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "kll_sketch_agg_float expected Float32Array".to_string(),
                )
            })?;
        for value in array.iter().flatten() {
            self.values.push(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.to_binary())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<f32>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.to_binary()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "kll_sketch_agg_float expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let data = binary_array.value(i);
            let (_k, n) = deserialize_header(data)?;
            let values_data = &data[10..];
            let expected_len = (n as usize) * 4;
            if values_data.len() != expected_len {
                return Err(DataFusionError::Internal(format!(
                    "kll_sketch_agg_float: expected {} value bytes, got {}",
                    expected_len,
                    values_data.len()
                )));
            }
            for chunk in values_data.chunks_exact(4) {
                let v = f32::from_le_bytes(chunk.try_into().map_err(|_| {
                    DataFusionError::Internal(
                        "kll_sketch_agg_float: failed to parse f32 from state".to_string(),
                    )
                })?);
                self.values.push(v);
            }
        }
        Ok(())
    }
}

// ===========================================================================
// kll_sketch_agg_double
// ===========================================================================

/// Aggregate function that builds a compact binary KLL sketch from DOUBLE values.
#[derive(PartialEq, Eq, Hash)]
pub struct KllSketchAggDoubleFunction {
    signature: Signature,
}

impl Debug for KllSketchAggDoubleFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketchAggDoubleFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KllSketchAggDoubleFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchAggDoubleFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for KllSketchAggDoubleFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_agg_double"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = extract_k(&acc_args)?;
        Ok(Box::new(KllSketchAggDoubleAccumulator::new(k)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("sketch", DataType::Binary, true).into()
        ])
    }
}

#[derive(Debug)]
struct KllSketchAggDoubleAccumulator {
    k: u16,
    values: Vec<f64>,
}

impl KllSketchAggDoubleAccumulator {
    fn new(k: u16) -> Self {
        Self {
            k,
            values: Vec::new(),
        }
    }

    fn to_binary(&self) -> Vec<u8> {
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let n = sorted.len() as u64;
        let mut buf = serialize_header(self.k, n);
        for v in &sorted {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
}

impl Accumulator for KllSketchAggDoubleAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "kll_sketch_agg_double expected Float64Array".to_string(),
                )
            })?;
        for value in array.iter().flatten() {
            self.values.push(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.to_binary())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.to_binary()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "kll_sketch_agg_double expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let data = binary_array.value(i);
            let (_k, n) = deserialize_header(data)?;
            let values_data = &data[10..];
            let expected_len = (n as usize) * 8;
            if values_data.len() != expected_len {
                return Err(DataFusionError::Internal(format!(
                    "kll_sketch_agg_double: expected {} value bytes, got {}",
                    expected_len,
                    values_data.len()
                )));
            }
            for chunk in values_data.chunks_exact(8) {
                let v = f64::from_le_bytes(chunk.try_into().map_err(|_| {
                    DataFusionError::Internal(
                        "kll_sketch_agg_double: failed to parse f64 from state".to_string(),
                    )
                })?);
                self.values.push(v);
            }
        }
        Ok(())
    }
}
