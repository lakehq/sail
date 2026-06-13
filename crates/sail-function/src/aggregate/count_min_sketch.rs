use std::fmt::Debug;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};

use super::utils::get_scalar_value;
use crate::scalar::hash::utils::spark_compatible_murmur3_hash;
use crate::scalar::math::java_random::JavaRandom;

const PRIME_MODULUS: i64 = (1i64 << 31) - 1;
const COUNT_MIN_SKETCH_VERSION: i32 = 1;

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
        Ok(vec![Field::new(
            format_state_name(args.name, "count_min_sketch"),
            DataType::Binary,
            true,
        )
        .into()])
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

#[derive(Debug)]
struct SparkCountMinSketch {
    depth: usize,
    width: usize,
    total_count: i64,
    hash_a: Vec<i64>,
    table: Vec<i64>,
}

impl SparkCountMinSketch {
    fn new(eps: f64, confidence: f64, seed: i32) -> Result<Self> {
        validate_parameters(eps, confidence)?;
        let width = (2.0 / eps).ceil();
        let depth = (-(-confidence).ln_1p() / 2.0_f64.ln()).ceil();
        if !width.is_finite() || width <= 0.0 || width > i32::MAX as f64 {
            return exec_err!("count_min_sketch computed invalid width {width}");
        }
        if !depth.is_finite() || depth <= 0.0 || depth > i32::MAX as f64 {
            return exec_err!("count_min_sketch computed invalid depth {depth}");
        }
        let width = width as usize;
        let depth = depth as usize;
        let entries = depth.checked_mul(width).ok_or_else(|| {
            DataFusionError::Execution("count_min_sketch table size overflow".to_string())
        })?;
        let mut rng = JavaRandom::new(seed as i64 as u64);
        let hash_a = (0..depth)
            .map(|_| {
                rng.next_i32_bound(i32::MAX)
                    .map(|value| value as i64)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "count_min_sketch random hash bound must be positive".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            depth,
            width,
            total_count: 0,
            hash_a,
            table: vec![0; entries],
        })
    }

    fn size(&self) -> usize {
        self.hash_a.capacity() * std::mem::size_of::<i64>()
            + self.table.capacity() * std::mem::size_of::<i64>()
    }

    fn update_from_array(&mut self, values: &ArrayRef) -> Result<()> {
        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            match values.data_type() {
                DataType::Int8 => {
                    let values = values.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                        DataFusionError::Internal("count_min_sketch expected Int8Array".to_string())
                    })?;
                    self.add_long(values.value(row) as i64);
                }
                DataType::Int16 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "count_min_sketch expected Int16Array".to_string(),
                            )
                        })?;
                    self.add_long(values.value(row) as i64);
                }
                DataType::Int32 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "count_min_sketch expected Int32Array".to_string(),
                            )
                        })?;
                    self.add_long(values.value(row) as i64);
                }
                DataType::Int64 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "count_min_sketch expected Int64Array".to_string(),
                            )
                        })?;
                    self.add_long(values.value(row));
                }
                DataType::Binary => {
                    let values =
                        values
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "count_min_sketch expected BinaryArray".to_string(),
                                )
                            })?;
                    self.add_binary(values.value(row));
                }
                DataType::Utf8 => {
                    let values =
                        values
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "count_min_sketch expected StringArray".to_string(),
                                )
                            })?;
                    self.add_binary(values.value(row).as_bytes());
                }
                DataType::LargeUtf8 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "count_min_sketch expected LargeStringArray".to_string(),
                            )
                        })?;
                    self.add_binary(values.value(row).as_bytes());
                }
                DataType::Utf8View => {
                    let values = values
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "count_min_sketch expected StringViewArray".to_string(),
                            )
                        })?;
                    self.add_binary(values.value(row).as_bytes());
                }
                DataType::Null => {}
                data_type => {
                    return exec_err!("count_min_sketch does not support input type {data_type}");
                }
            }
        }
        Ok(())
    }

    fn add_long(&mut self, item: i64) {
        for row in 0..self.depth {
            let bucket = self.hash_long(item, row);
            self.table[row * self.width + bucket] += 1;
        }
        self.total_count += 1;
    }

    fn add_binary(&mut self, item: &[u8]) {
        let buckets = self.hash_binary(item);
        for (row, bucket) in buckets.into_iter().enumerate() {
            self.table[row * self.width + bucket] += 1;
        }
        self.total_count += 1;
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.depth != other.depth {
            return exec_err!("Cannot merge count_min_sketch values of different depth");
        }
        if self.width != other.width {
            return exec_err!("Cannot merge count_min_sketch values of different width");
        }
        if self.hash_a != other.hash_a {
            return exec_err!("Cannot merge count_min_sketch values of different seed");
        }
        for (left, right) in self.table.iter_mut().zip(other.table.iter()) {
            *left += right;
        }
        self.total_count += other.total_count;
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + (self.depth * 8) + (self.depth * self.width * 8));
        bytes.extend_from_slice(&COUNT_MIN_SKETCH_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.total_count.to_be_bytes());
        bytes.extend_from_slice(&(self.depth as i32).to_be_bytes());
        bytes.extend_from_slice(&(self.width as i32).to_be_bytes());
        for value in &self.hash_a {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        for value in &self.table {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        let version = read_i32_be(bytes, &mut offset, "version")?;
        if version != COUNT_MIN_SKETCH_VERSION {
            return exec_err!("Unexpected Count-Min Sketch version number ({version})");
        }
        let total_count = read_i64_be(bytes, &mut offset, "total_count")?;
        let depth = read_i32_be(bytes, &mut offset, "depth")?;
        let width = read_i32_be(bytes, &mut offset, "width")?;
        if depth <= 0 || width <= 0 {
            return exec_err!("Invalid Count-Min Sketch dimensions: depth={depth}, width={width}");
        }
        let depth = depth as usize;
        let width = width as usize;
        let entries = depth.checked_mul(width).ok_or_else(|| {
            DataFusionError::Execution("count_min_sketch table size overflow".to_string())
        })?;
        let mut hash_a = Vec::with_capacity(depth);
        for _ in 0..depth {
            hash_a.push(read_i64_be(bytes, &mut offset, "hash_a")?);
        }
        let mut table = Vec::with_capacity(entries);
        for _ in 0..entries {
            table.push(read_i64_be(bytes, &mut offset, "table")?);
        }
        if offset != bytes.len() {
            return exec_err!("Count-Min Sketch has trailing bytes");
        }
        Ok(Self {
            depth,
            width,
            total_count,
            hash_a,
            table,
        })
    }

    fn hash_long(&self, item: i64, row: usize) -> usize {
        let mut hash = self.hash_a[row].wrapping_mul(item);
        hash = hash.wrapping_add(hash >> 32);
        hash &= PRIME_MODULUS;
        (hash as i32 % self.width as i32) as usize
    }

    fn hash_binary(&self, item: &[u8]) -> Vec<usize> {
        let hash1 = spark_compatible_murmur3_hash(item, 0) as i32;
        let hash2 = spark_compatible_murmur3_hash(item, hash1 as u32) as i32;
        (0..self.depth)
            .map(|row| {
                let hash = hash1.wrapping_add((row as i32).wrapping_mul(hash2));
                (hash % self.width as i32).wrapping_abs() as usize
            })
            .collect()
    }
}

fn validate_parameters(eps: f64, confidence: f64) -> Result<()> {
    if !eps.is_finite() || eps <= 0.0 {
        return exec_err!("count_min_sketch requires eps to be positive, got {eps}");
    }
    if !confidence.is_finite() || confidence <= 0.0 || confidence >= 1.0 {
        return exec_err!(
            "count_min_sketch requires confidence to be in the range (0.0, 1.0), got {confidence}"
        );
    }
    Ok(())
}

fn validate_count_min_sketch_types(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 4 {
        return exec_err!(
            "count_min_sketch requires 4 arguments, got {}",
            arg_types.len()
        );
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
            return exec_err!("count_min_sketch does not support input type {data_type}");
        }
    }
    if !matches!(arg_types[1], DataType::Float32 | DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a floating point eps argument, got {}",
            arg_types[1]
        );
    }
    if !matches!(arg_types[2], DataType::Float32 | DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a floating point confidence argument, got {}",
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
            "count_min_sketch requires {name} to be a constant floating point value"
        ))
    })?;
    match value {
        ScalarValue::Float32(Some(value)) => Ok(value as f64),
        ScalarValue::Float64(Some(value)) => Ok(value),
        value => Err(DataFusionError::Plan(format!(
            "count_min_sketch requires {name} to be a non-null floating point literal, got {}",
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

fn read_i32_be(bytes: &[u8], offset: &mut usize, name: &'static str) -> Result<i32> {
    Ok(i32::from_be_bytes(read_array(bytes, offset, name)?))
}

fn read_i64_be(bytes: &[u8], offset: &mut usize, name: &'static str) -> Result<i64> {
    Ok(i64::from_be_bytes(read_array(bytes, offset, name)?))
}

fn read_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
    name: &'static str,
) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| {
        DataFusionError::Execution(format!("Count-Min Sketch offset overflow reading {name}"))
    })?;
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        DataFusionError::Execution(format!("Count-Min Sketch is truncated reading {name}"))
    })?;
    let mut out = [0; N];
    out.copy_from_slice(slice);
    *offset = end;
    Ok(out)
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
