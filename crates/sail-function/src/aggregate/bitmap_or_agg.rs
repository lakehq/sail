use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;

use super::utils::BITMAP_NUM_BYTES;

/// An aggregate function that returns a bitmap that is the bitwise OR
/// of all of the bitmaps from the input column.
///
/// The input column should be bitmaps created from `bitmap_construct_agg()`.
/// Input values shorter than `BITMAP_NUM_BYTES` are zero-padded on the right.
/// The result is always a binary value of `BITMAP_NUM_BYTES` bytes.
#[derive(PartialEq, Eq, Hash)]
pub struct BitmapOrAggFunction {
    signature: Signature,
}

impl Debug for BitmapOrAggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitmapOrAggFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for BitmapOrAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapOrAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BitmapOrAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_or_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitmapOrAggAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("bitmap", DataType::Binary, true).into()])
    }
}

#[derive(Debug)]
struct BitmapOrAggAccumulator {
    bitmap: Vec<u8>,
}

impl BitmapOrAggAccumulator {
    fn new() -> Self {
        Self {
            bitmap: vec![0u8; BITMAP_NUM_BYTES],
        }
    }

    fn or_bytes(&mut self, other: &[u8]) {
        for (dst, &src) in self.bitmap.iter_mut().zip(other.iter()) {
            *dst |= src;
        }
    }
}

impl Accumulator for BitmapOrAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let binary_array = values[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "bitmap_or_agg expected binary array for input".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let other = binary_array.value(i);
                if other.len() > BITMAP_NUM_BYTES {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "bitmap_or_agg input length {} exceeds maximum {}",
                        other.len(),
                        BITMAP_NUM_BYTES
                    )));
                }
                self.or_bytes(other);
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
                    "bitmap_or_agg expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let other = binary_array.value(i);
                if other.len() != self.bitmap.len() {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "bitmap_or_agg expected state length {}, got {}",
                        self.bitmap.len(),
                        other.len()
                    )));
                }
                self.or_bytes(other);
            }
        }
        Ok(())
    }
}
