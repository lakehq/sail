use std::any::Any;
use std::fmt::Debug;
use std::hash::Hasher;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use twox_hash::XxHash64;

use crate::aggregate::hll_utils::{
    scalar_to_lg_config_k, HllSketch, DEFAULT_LG_CONFIG_K, HLL_HASH_SEED, HLL_MAGIC,
};
use crate::aggregate::utils::get_scalar_value;

/// Aggregate function that builds a HyperLogLog sketch from input values.
#[derive(PartialEq, Eq, Hash)]
pub struct HllSketchAggFunction {
    signature: Signature,
}

impl Debug for HllSketchAggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HllSketchAggFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for HllSketchAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketchAggFunction {
    pub fn new() -> Self {
        Self {
            // Two arguments: input value (any type) and lgConfigK (integer literal).
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for HllSketchAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hll_sketch_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let lg_config_k = if acc_args.exprs.len() >= 2 {
            let scalar = get_scalar_value(&acc_args.exprs[1])?;
            scalar_to_lg_config_k(&scalar)?
        } else {
            DEFAULT_LG_CONFIG_K
        };
        Ok(Box::new(HllSketchAccumulator::new(lg_config_k)?))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("sketch", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct HllSketchAccumulator {
    sketch: HllSketch,
}

impl HllSketchAccumulator {
    fn new(lg_config_k: u8) -> Result<Self> {
        Ok(Self {
            sketch: HllSketch::new(lg_config_k)?,
        })
    }

    fn hash_and_update(&mut self, hasher: XxHash64) {
        self.sketch.update_hash(hasher.finish());
    }

    /// Falls back to `ScalarValue` conversion for types without a fast path.
    fn update_scalar_at(&mut self, array: &ArrayRef, index: usize) -> Result<()> {
        if array.is_null(index) {
            return Ok(());
        }
        let scalar = ScalarValue::try_from_array(array, index)?;
        let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
        hash_scalar(&scalar, &mut hasher)?;
        self.hash_and_update(hasher);
        Ok(())
    }
}

/// Macro to generate a typed fast-path loop that hashes primitive values
/// directly from Arrow arrays without going through `ScalarValue`.
macro_rules! hash_primitive_array {
    ($self:expr, $array:expr, $array_type:ty, |$val:ident| $hash_expr:expr) => {{
        let typed = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "hll_sketch_agg: expected {} array",
                    stringify!($array_type)
                ))
            })?;
        for i in 0..typed.len() {
            if typed.is_null(i) {
                continue;
            }
            let $val = typed.value(i);
            let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
            hasher.write(&$hash_expr);
            $self.hash_and_update(hasher);
        }
        Ok(())
    }};
}

/// Fast-path batch update: hashes directly from typed Arrow arrays for common
/// primitive/string/binary types, falling back to per-row `ScalarValue`
/// conversion only for uncommon types (timestamps, decimals, etc.).
fn update_batch_typed(acc: &mut HllSketchAccumulator, array: &ArrayRef) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("hll_sketch_agg: expected BooleanArray".to_string())
                })?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write_u8(u8::from(typed.value(i)));
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::Int8 => {
            hash_primitive_array!(acc, array, Int8Array, |v| i64::from(v).to_le_bytes())
        }
        DataType::Int16 => {
            hash_primitive_array!(acc, array, Int16Array, |v| i64::from(v).to_le_bytes())
        }
        DataType::Int32 => {
            hash_primitive_array!(acc, array, Int32Array, |v| i64::from(v).to_le_bytes())
        }
        DataType::Int64 => {
            hash_primitive_array!(acc, array, Int64Array, |v| v.to_le_bytes())
        }
        DataType::UInt8 => {
            hash_primitive_array!(acc, array, UInt8Array, |v| u64::from(v).to_le_bytes())
        }
        DataType::UInt16 => {
            hash_primitive_array!(acc, array, UInt16Array, |v| u64::from(v).to_le_bytes())
        }
        DataType::UInt32 => {
            hash_primitive_array!(acc, array, UInt32Array, |v| u64::from(v).to_le_bytes())
        }
        DataType::UInt64 => {
            hash_primitive_array!(acc, array, UInt64Array, |v| v.to_le_bytes())
        }
        DataType::Float32 => {
            hash_primitive_array!(acc, array, Float32Array, |v| v.to_bits().to_le_bytes())
        }
        DataType::Float64 => {
            hash_primitive_array!(acc, array, Float64Array, |v| v.to_bits().to_le_bytes())
        }
        DataType::Utf8 => {
            let typed = array.as_string::<i32>();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i).as_bytes());
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::LargeUtf8 => {
            let typed = array.as_string::<i64>();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i).as_bytes());
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::Utf8View => {
            let typed = array.as_string_view();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i).as_bytes());
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::Binary => {
            let typed = array.as_binary::<i32>();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i));
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::LargeBinary => {
            let typed = array.as_binary::<i64>();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i));
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        DataType::BinaryView => {
            let typed = array.as_binary_view();
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let mut hasher = XxHash64::with_seed(HLL_HASH_SEED);
                hasher.write(typed.value(i));
                acc.hash_and_update(hasher);
            }
            Ok(())
        }
        // Fall back to ScalarValue for less common types (timestamps,
        // decimals, fixed-size binary, etc.).
        _ => {
            for i in 0..array.len() {
                acc.update_scalar_at(array, i)?;
            }
            Ok(())
        }
    }
}

fn hash_scalar(scalar: &ScalarValue, hasher: &mut XxHash64) -> Result<()> {
    use ScalarValue::*;
    match scalar {
        Boolean(Some(v)) => hasher.write_u8(u8::from(*v)),
        Int8(Some(v)) => hasher.write(&i64::from(*v).to_le_bytes()),
        Int16(Some(v)) => hasher.write(&i64::from(*v).to_le_bytes()),
        Int32(Some(v)) => hasher.write(&i64::from(*v).to_le_bytes()),
        Int64(Some(v)) => hasher.write(&v.to_le_bytes()),
        UInt8(Some(v)) => hasher.write(&u64::from(*v).to_le_bytes()),
        UInt16(Some(v)) => hasher.write(&u64::from(*v).to_le_bytes()),
        UInt32(Some(v)) => hasher.write(&u64::from(*v).to_le_bytes()),
        UInt64(Some(v)) => hasher.write(&v.to_le_bytes()),
        Float32(Some(v)) => hasher.write(&v.to_bits().to_le_bytes()),
        Float64(Some(v)) => hasher.write(&v.to_bits().to_le_bytes()),
        Utf8(Some(v)) | LargeUtf8(Some(v)) | Utf8View(Some(v)) => hasher.write(v.as_bytes()),
        Binary(Some(v)) | LargeBinary(Some(v)) | BinaryView(Some(v)) => hasher.write(v),
        FixedSizeBinary(_, Some(v)) => hasher.write(v),
        Date32(Some(v)) => hasher.write(&i64::from(*v).to_le_bytes()),
        Date64(Some(v)) => hasher.write(&v.to_le_bytes()),
        TimestampSecond(Some(v), _)
        | TimestampMillisecond(Some(v), _)
        | TimestampMicrosecond(Some(v), _)
        | TimestampNanosecond(Some(v), _) => hasher.write(&v.to_le_bytes()),
        Decimal128(Some(v), _, _) => hasher.write(&v.to_le_bytes()),
        Decimal256(Some(v), _, _) => hasher.write(&v.to_le_bytes()),
        // Null values are filtered out before reaching this function.
        Boolean(None)
        | Int8(None)
        | Int16(None)
        | Int32(None)
        | Int64(None)
        | UInt8(None)
        | UInt16(None)
        | UInt32(None)
        | UInt64(None)
        | Float32(None)
        | Float64(None)
        | Utf8(None)
        | LargeUtf8(None)
        | Utf8View(None)
        | Binary(None)
        | LargeBinary(None)
        | BinaryView(None)
        | FixedSizeBinary(_, None)
        | Date32(None)
        | Date64(None)
        | TimestampSecond(None, _)
        | TimestampMillisecond(None, _)
        | TimestampMicrosecond(None, _)
        | TimestampNanosecond(None, _)
        | Decimal128(None, _, _)
        | Decimal256(None, _, _)
        | Null => {}
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "hll_sketch_agg does not support input type {}",
                other.data_type()
            )));
        }
    }
    Ok(())
}

impl Accumulator for HllSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        update_batch_typed(self, &values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.to_bytes())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.heap_size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.to_bytes()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "hll_sketch_agg expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let bytes = binary_array.value(i);
            if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
                return exec_err!("hll_sketch_agg received an invalid sketch state");
            }
            let other = HllSketch::from_bytes(bytes)?;
            self.sketch.merge(&other)?;
        }
        Ok(())
    }
}
