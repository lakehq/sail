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
use crate::scalar::math::java_random::JavaRandom;

const DEFAULT_K: u16 = 200;
const MAX_LEVEL_SIZE_FACTOR: usize = 2;
const RNG_SEED: u64 = 31183;

trait SketchValue: Copy + Send + Sync + 'static {
    const WIDTH: usize;

    fn write(self, buffer: &mut Vec<u8>);
    fn read(chunk: &[u8]) -> Result<Self>;
    fn sort(values: &mut [Self]);
}

impl SketchValue for i64 {
    const WIDTH: usize = 8;

    fn write(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }

    fn read(chunk: &[u8]) -> Result<Self> {
        let bytes: [u8; Self::WIDTH] = chunk.try_into().map_err(|_| {
            DataFusionError::Internal(
                "kll_sketch_agg_bigint: failed to parse i64 from state".to_string(),
            )
        })?;
        Ok(Self::from_le_bytes(bytes))
    }

    fn sort(values: &mut [Self]) {
        values.sort();
    }
}

impl SketchValue for f32 {
    const WIDTH: usize = 4;

    fn write(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }

    fn read(chunk: &[u8]) -> Result<Self> {
        let bytes: [u8; Self::WIDTH] = chunk.try_into().map_err(|_| {
            DataFusionError::Internal(
                "kll_sketch_agg_float: failed to parse f32 from state".to_string(),
            )
        })?;
        Ok(Self::from_le_bytes(bytes))
    }

    fn sort(values: &mut [Self]) {
        values.sort_by(|a, b| a.total_cmp(b));
    }
}

impl SketchValue for f64 {
    const WIDTH: usize = 8;

    fn write(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }

    fn read(chunk: &[u8]) -> Result<Self> {
        let bytes: [u8; Self::WIDTH] = chunk.try_into().map_err(|_| {
            DataFusionError::Internal(
                "kll_sketch_agg_double: failed to parse f64 from state".to_string(),
            )
        })?;
        Ok(Self::from_le_bytes(bytes))
    }

    fn sort(values: &mut [Self]) {
        values.sort_by(|a, b| a.total_cmp(b));
    }
}

fn k_type_signatures(value_type: DataType) -> Vec<TypeSignature> {
    [
        None,
        Some(DataType::Int8),
        Some(DataType::Int16),
        Some(DataType::Int32),
        Some(DataType::Int64),
        Some(DataType::UInt8),
        Some(DataType::UInt16),
        Some(DataType::UInt32),
        Some(DataType::UInt64),
    ]
    .into_iter()
    .map(|k_type| match k_type {
        Some(k_type) => TypeSignature::Exact(vec![value_type.clone(), k_type]),
        None => TypeSignature::Exact(vec![value_type.clone()]),
    })
    .collect()
}

fn extract_k(acc_args: &AccumulatorArgs) -> Result<u16> {
    if acc_args.exprs.len() < 2 {
        return Ok(DEFAULT_K);
    }
    let scalar = get_scalar_value(&acc_args.exprs[1]).map_err(|_| {
        DataFusionError::Plan(
            "kll_sketch_agg requires a non-null integer literal for k".to_string(),
        )
    })?;
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
            ));
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "kll_sketch_agg requires an integer literal for k, got {}",
                other.data_type()
            )));
        }
    };
    if !(8..=65535).contains(&k_i64) {
        return Err(DataFusionError::Plan(format!(
            "kll_sketch_agg requires k to be in the range 8 to 65535, got {k_i64}",
        )));
    }
    Ok(k_i64 as u16)
}

#[derive(Debug, Clone)]
struct CompactSketch<T: SketchValue> {
    k: u16,
    n: u64,
    levels: Vec<Vec<T>>,
    rng: JavaRandom,
}

impl<T: SketchValue> CompactSketch<T> {
    fn new(k: u16) -> Self {
        Self {
            k,
            n: 0,
            levels: vec![Vec::new()],
            rng: JavaRandom::new(RNG_SEED),
        }
    }

    fn with_state(k: u16, n: u64, levels: Vec<Vec<T>>) -> Self {
        Self {
            k,
            n,
            levels,
            rng: JavaRandom::new(RNG_SEED),
        }
    }

    fn max_level_size(&self) -> usize {
        usize::from(self.k) * MAX_LEVEL_SIZE_FACTOR
    }

    fn update(&mut self, value: T) -> Result<()> {
        self.n = self.n.saturating_add(1);
        self.levels[0].push(value);
        self.compact_from(0)
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        if other.k != self.k {
            return Err(DataFusionError::Internal(format!(
                "kll_sketch_agg: incompatible sketch state k={}, expected {}",
                other.k, self.k
            )));
        }
        self.n = self.n.saturating_add(other.n);
        if self.levels.len() < other.levels.len() {
            self.levels.resize_with(other.levels.len(), Vec::new);
        }
        for (level, incoming) in other.levels.into_iter().enumerate() {
            self.levels[level].extend(incoming);
            self.compact_from(level)?;
        }
        Ok(())
    }

    fn compact_from(&mut self, start_level: usize) -> Result<()> {
        let max_level_size = self.max_level_size();
        for level in start_level.. {
            if self.levels[level].len() <= max_level_size {
                break;
            }
            if self.levels.len() == level + 1 {
                self.levels.push(Vec::new());
            }
            let promoted = self.compact_level(level);
            self.levels[level + 1].extend(promoted);
        }
        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> Vec<T> {
        let values = &mut self.levels[level];
        T::sort(values);
        let offset = usize::from(self.rng.next_f64() >= 0.5);
        let mut promoted = Vec::with_capacity(values.len() / 2);
        let mut retained = Vec::with_capacity(values.len() % 2);
        for (index, value) in values.iter().copied().enumerate() {
            if index % 2 == offset {
                promoted.push(value);
            } else if values.len() % 2 == 1 && retained.is_empty() {
                retained.push(value);
            }
        }
        *values = retained;
        promoted
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let level_count: u32 = self.levels.len().try_into().map_err(|_| {
            DataFusionError::Execution(
                "kll_sketch_agg: level count exceeds u32::MAX during serialization".to_string(),
            )
        })?;
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.k.to_le_bytes());
        buffer.extend_from_slice(&self.n.to_le_bytes());
        buffer.extend_from_slice(&level_count.to_le_bytes());
        for level in &self.levels {
            let len: u32 = level.len().try_into().map_err(|_| {
                DataFusionError::Execution(
                    "kll_sketch_agg: level size exceeds u32::MAX during serialization".to_string(),
                )
            })?;
            buffer.extend_from_slice(&len.to_le_bytes());
            for value in level {
                value.write(&mut buffer);
            }
        }
        Ok(buffer)
    }

    fn deserialize(data: &[u8], label: &str) -> Result<Self> {
        let mut cursor = 0usize;

        let k = read_u16(data, &mut cursor, label, "k")?;
        let n = read_u64(data, &mut cursor, label, "n")?;
        let level_count = read_u32(data, &mut cursor, label, "level_count")? as usize;
        let mut levels = Vec::with_capacity(level_count.max(1));
        for _ in 0..level_count {
            let level_len = read_u32(data, &mut cursor, label, "level_len")? as usize;
            let mut level = Vec::with_capacity(level_len);
            for _ in 0..level_len {
                let end = cursor.saturating_add(T::WIDTH);
                let chunk = data.get(cursor..end).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{label}: truncated state while reading sketch values"
                    ))
                })?;
                level.push(T::read(chunk)?);
                cursor = end;
            }
            levels.push(level);
        }
        if cursor != data.len() {
            return Err(DataFusionError::Internal(format!(
                "{label}: unexpected trailing bytes in serialized state"
            )));
        }
        if levels.is_empty() {
            levels.push(Vec::new());
        }
        Ok(Self::with_state(k, n, levels))
    }
}

fn read_u16(data: &[u8], cursor: &mut usize, label: &str, field: &str) -> Result<u16> {
    let end = cursor.saturating_add(2);
    let chunk = data.get(*cursor..end).ok_or_else(|| {
        DataFusionError::Internal(format!("{label}: truncated state while reading {field}"))
    })?;
    *cursor = end;
    Ok(u16::from_le_bytes(chunk.try_into().map_err(|_| {
        DataFusionError::Internal(format!("{label}: failed to parse {field}"))
    })?))
}

fn read_u32(data: &[u8], cursor: &mut usize, label: &str, field: &str) -> Result<u32> {
    let end = cursor.saturating_add(4);
    let chunk = data.get(*cursor..end).ok_or_else(|| {
        DataFusionError::Internal(format!("{label}: truncated state while reading {field}"))
    })?;
    *cursor = end;
    Ok(u32::from_le_bytes(chunk.try_into().map_err(|_| {
        DataFusionError::Internal(format!("{label}: failed to parse {field}"))
    })?))
}

fn read_u64(data: &[u8], cursor: &mut usize, label: &str, field: &str) -> Result<u64> {
    let end = cursor.saturating_add(8);
    let chunk = data.get(*cursor..end).ok_or_else(|| {
        DataFusionError::Internal(format!("{label}: truncated state while reading {field}"))
    })?;
    *cursor = end;
    Ok(u64::from_le_bytes(chunk.try_into().map_err(|_| {
        DataFusionError::Internal(format!("{label}: failed to parse {field}"))
    })?))
}

#[derive(Debug)]
struct KllSketchAccumulator<T: SketchValue> {
    sketch: CompactSketch<T>,
    label: &'static str,
}

impl<T: SketchValue> KllSketchAccumulator<T> {
    fn new(k: u16, label: &'static str) -> Self {
        Self {
            sketch: CompactSketch::new(k),
            label,
        }
    }

    fn evaluate_binary(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize()?)))
    }

    fn merge_serialized_states(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{} expected binary array for state", self.label))
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let state = CompactSketch::<T>::deserialize(binary_array.value(i), self.label)?;
            if state.k != self.sketch.k {
                return Err(DataFusionError::Internal(format!(
                    "{}: incompatible sketch state k={}, expected {}",
                    self.label, state.k, self.sketch.k
                )));
            }
            self.sketch.merge(state)?;
        }
        Ok(())
    }
}

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
            signature: Signature::one_of(k_type_signatures(DataType::Int64), Volatility::Immutable),
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
        Ok(Box::new(KllSketchAggBigintAccumulator::new(extract_k(
            &acc_args,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("sketch", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct KllSketchAggBigintAccumulator {
    inner: KllSketchAccumulator<i64>,
}

impl KllSketchAggBigintAccumulator {
    fn new(k: u16) -> Self {
        Self {
            inner: KllSketchAccumulator::new(k, "kll_sketch_agg_bigint"),
        }
    }
}

impl Accumulator for KllSketchAggBigintAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_int64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.inner.sketch.update(value)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate_binary()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .inner
                .sketch
                .levels
                .iter()
                .map(|level| level.capacity() * std::mem::size_of::<i64>())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.inner.evaluate_binary()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_serialized_states(states)
    }
}

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
                k_type_signatures(DataType::Float32),
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
        Ok(Box::new(KllSketchAggFloatAccumulator::new(extract_k(
            &acc_args,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("sketch", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct KllSketchAggFloatAccumulator {
    inner: KllSketchAccumulator<f32>,
}

impl KllSketchAggFloatAccumulator {
    fn new(k: u16) -> Self {
        Self {
            inner: KllSketchAccumulator::new(k, "kll_sketch_agg_float"),
        }
    }
}

impl Accumulator for KllSketchAggFloatAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("kll_sketch_agg_float expected Float32Array".to_string())
            })?;
        for value in array.iter().flatten() {
            self.inner.sketch.update(value)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate_binary()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .inner
                .sketch
                .levels
                .iter()
                .map(|level| level.capacity() * std::mem::size_of::<f32>())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.inner.evaluate_binary()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_serialized_states(states)
    }
}

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
                k_type_signatures(DataType::Float64),
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
        Ok(Box::new(KllSketchAggDoubleAccumulator::new(extract_k(
            &acc_args,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("sketch", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct KllSketchAggDoubleAccumulator {
    inner: KllSketchAccumulator<f64>,
}

impl KllSketchAggDoubleAccumulator {
    fn new(k: u16) -> Self {
        Self {
            inner: KllSketchAccumulator::new(k, "kll_sketch_agg_double"),
        }
    }
}

impl Accumulator for KllSketchAggDoubleAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("kll_sketch_agg_double expected Float64Array".to_string())
            })?;
        for value in array.iter().flatten() {
            self.inner.sketch.update(value)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate_binary()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .inner
                .sketch
                .levels
                .iter()
                .map(|level| level.capacity() * std::mem::size_of::<f64>())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.inner.evaluate_binary()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_serialized_states(states)
    }
}
