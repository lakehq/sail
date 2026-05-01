use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;

use super::utils::BITMAP_NUM_BYTES;

/// An aggregate function that returns a bitmap that is the bitwise AND
/// of all of the bitmaps from the input column.
///
/// The input column should be bitmaps created from `bitmap_construct_agg()`.
/// Input values shorter than `BITMAP_NUM_BYTES` are treated as zero-padded
/// on the right; consequently, every buffer byte beyond the input length
/// is reset to zero on each update.
/// The result is always a binary value of `BITMAP_NUM_BYTES` bytes.
/// If no rows are observed, the result is the all-ones buffer.
#[derive(PartialEq, Eq, Hash)]
pub struct BitmapAndAggFunction {
    signature: Signature,
}

impl Debug for BitmapAndAggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitmapAndAggFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for BitmapAndAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapAndAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BitmapAndAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_and_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitmapAndAggAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("bitmap", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct BitmapAndAggAccumulator {
    bitmap: Vec<u8>,
}

impl BitmapAndAggAccumulator {
    fn new() -> Self {
        Self {
            bitmap: vec![0xFFu8; BITMAP_NUM_BYTES],
        }
    }

    /// AND the input bytes into the buffer, then zero-fill any buffer
    /// bytes beyond the input length (since the input has implicit zeros
    /// there). This matches Spark's `bitmapAndMerge` semantics.
    fn and_input_bytes(&mut self, other: &[u8]) {
        let n = std::cmp::min(self.bitmap.len(), other.len());
        for (dst, &src) in self.bitmap.iter_mut().zip(other.iter()).take(n) {
            *dst &= src;
        }
        for byte in &mut self.bitmap[n..] {
            *byte = 0;
        }
    }

    /// AND the merge state (which is the same length as the buffer) into
    /// the buffer without the trailing zero-fill, since the state already
    /// represents a full-size bitmap.
    fn and_state_bytes(&mut self, other: &[u8]) {
        for (dst, &src) in self.bitmap.iter_mut().zip(other.iter()) {
            *dst &= src;
        }
    }
}

impl Accumulator for BitmapAndAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let binary_array = values[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "bitmap_and_agg expected binary array for input".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let other = binary_array.value(i);
                if other.len() > BITMAP_NUM_BYTES {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "bitmap_and_agg input length {} exceeds maximum {}",
                        other.len(),
                        BITMAP_NUM_BYTES
                    )));
                }
                self.and_input_bytes(other);
            }
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
                    "bitmap_and_agg expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let other = binary_array.value(i);
                if other.len() != self.bitmap.len() {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "bitmap_and_agg expected state length {}, got {}",
                        self.bitmap.len(),
                        other.len()
                    )));
                }
                self.and_state_bytes(other);
            }
        }
        Ok(())
    }
}
