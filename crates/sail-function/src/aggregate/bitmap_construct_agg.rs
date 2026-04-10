use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::as_int64_array;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::types::{
    logical_int16, logical_int32, logical_int64, logical_int8, logical_uint16, logical_uint32,
    logical_uint64, logical_uint8, NativeType,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

use super::utils::BITMAP_NUM_BYTES;

/// The number of bits in the bitmap (8 * 4 * 1024 = 32768).
const BITMAP_NUM_BITS: i64 = (8 * BITMAP_NUM_BYTES) as i64;

/// An aggregate function that constructs a bitmap from bit positions.
///
/// Returns a binary value of `BITMAP_NUM_BYTES` bytes with the positions
/// of the bits set from all input values. The input values should be
/// bit positions (typically from `bitmap_bit_position()`). Positions are
/// wrapped into the range `0..BITMAP_NUM_BITS` using modulo arithmetic.
#[derive(PartialEq, Eq, Hash)]
pub struct BitmapConstructAggFunction {
    signature: Signature,
}

impl Debug for BitmapConstructAggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitmapConstructAggFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for BitmapConstructAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapConstructAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_int64()),
                    vec![
                        TypeSignatureClass::Native(logical_int8()),
                        TypeSignatureClass::Native(logical_int16()),
                        TypeSignatureClass::Native(logical_int32()),
                        TypeSignatureClass::Native(logical_int64()),
                        TypeSignatureClass::Native(logical_uint8()),
                        TypeSignatureClass::Native(logical_uint16()),
                        TypeSignatureClass::Native(logical_uint32()),
                        TypeSignatureClass::Native(logical_uint64()),
                    ],
                    NativeType::Int64,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for BitmapConstructAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_construct_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitmapConstructAggAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("bitmap", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct BitmapConstructAggAccumulator {
    bitmap: Vec<u8>,
}

impl BitmapConstructAggAccumulator {
    fn new() -> Self {
        Self {
            bitmap: vec![0u8; BITMAP_NUM_BYTES],
        }
    }

    fn set_bit(&mut self, position: i64) {
        // Wrap positions into the bitmap range using modulo arithmetic.
        let pos = position.rem_euclid(BITMAP_NUM_BITS) as usize;
        let byte_index = pos / 8;
        let bit_index = pos % 8;
        self.bitmap[byte_index] |= 1 << bit_index;
    }
}

impl Accumulator for BitmapConstructAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_int64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.set_bit(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.bitmap.clone())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.bitmap.capacity()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.bitmap.clone()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "bitmap_construct_agg expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let other = binary_array.value(i);
                if other.len() != self.bitmap.len() {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "bitmap_construct_agg expected state length {}, got {}",
                        self.bitmap.len(),
                        other.len()
                    )));
                }
                for (j, byte) in other.iter().enumerate() {
                    self.bitmap[j] |= byte;
                }
            }
        }
        Ok(())
    }
}
