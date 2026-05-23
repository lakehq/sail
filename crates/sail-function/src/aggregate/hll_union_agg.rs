use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_common::cast::as_fixed_size_binary_array;

use crate::aggregate::hll_utils::{
    is_coercible_to_binary, scalar_to_allow_diff, HllSketch, HLL_MAGIC,
};
use crate::aggregate::utils::get_scalar_value;

/// Aggregate function that merges HyperLogLog sketches via union.
#[derive(PartialEq, Eq, Hash)]
pub struct HllUnionAggFunction {
    signature: Signature,
}

impl Debug for HllUnionAggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HllUnionAggFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for HllUnionAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HllUnionAggFunction {
    pub fn new() -> Self {
        Self {
            // Two arguments: input binary sketch and allowDifferentLgConfigK boolean literal.
            // Actual type validation happens in `coerce_types`.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for HllUnionAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hll_union_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let allow_different = if acc_args.exprs.len() >= 2 {
            let scalar = get_scalar_value(&acc_args.exprs[1])?;
            scalar_to_allow_diff(&scalar)?
        } else {
            false
        };
        Ok(Box::new(HllUnionAccumulator::new(allow_different)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("sketch", DataType::Binary, true).into()])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "hll_union_agg expects 2 arguments, got {}",
                arg_types.len()
            )));
        }
        if !is_coercible_to_binary(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "hll_union_agg expects a binary or string sketch as the first argument, got {}",
                arg_types[0]
            )));
        }
        if arg_types[1] != DataType::Boolean {
            return Err(DataFusionError::Plan(format!(
                "hll_union_agg expects a boolean as the second argument, got {}",
                arg_types[1]
            )));
        }
        // Coerce all binary variants to Binary for uniform downstream handling.
        Ok(vec![DataType::Binary, DataType::Boolean])
    }
}

#[derive(Debug)]
struct HllUnionAccumulator {
    sketch: Option<HllSketch>,
    allow_different: bool,
}

impl HllUnionAccumulator {
    fn new(allow_different: bool) -> Self {
        Self {
            sketch: None,
            allow_different,
        }
    }

    fn merge_sketch(&mut self, other: HllSketch) -> Result<()> {
        match self.sketch.as_mut() {
            None => {
                self.sketch = Some(other);
                Ok(())
            }
            Some(s) => {
                if self.allow_different {
                    s.merge_lossy(&other)
                } else {
                    s.merge(&other)
                }
            }
        }
    }

    fn ingest_binary_array(&mut self, array: &ArrayRef) -> Result<()> {
        match array.data_type() {
            DataType::Binary => {
                let bin = array.as_binary::<i32>();
                for i in 0..bin.len() {
                    if bin.is_null(i) {
                        continue;
                    }
                    self.ingest_sketch_bytes(bin.value(i))?;
                }
            }
            DataType::LargeBinary => {
                let bin = array.as_binary::<i64>();
                for i in 0..bin.len() {
                    if bin.is_null(i) {
                        continue;
                    }
                    self.ingest_sketch_bytes(bin.value(i))?;
                }
            }
            DataType::BinaryView => {
                let bin = array.as_binary_view();
                for i in 0..bin.len() {
                    if bin.is_null(i) {
                        continue;
                    }
                    self.ingest_sketch_bytes(bin.value(i))?;
                }
            }
            DataType::FixedSizeBinary(_) => {
                let bin = as_fixed_size_binary_array(array)?;
                for i in 0..bin.len() {
                    if bin.is_null(i) {
                        continue;
                    }
                    self.ingest_sketch_bytes(bin.value(i))?;
                }
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "hll_union_agg expected a binary array, got {other}"
                )));
            }
        }
        Ok(())
    }

    fn ingest_sketch_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
            return exec_err!("hll_union_agg received an invalid sketch");
        }
        let other = HllSketch::from_bytes(bytes)?;
        self.merge_sketch(other)
    }
}

impl Accumulator for HllUnionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        self.ingest_binary_array(&values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.sketch {
            None => Ok(ScalarValue::Binary(None)),
            Some(s) => Ok(ScalarValue::Binary(Some(s.to_bytes()))),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.as_ref().map(|s| s.heap_size()).unwrap_or(0)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match &self.sketch {
            None => Ok(vec![ScalarValue::Binary(None)]),
            Some(s) => Ok(vec![ScalarValue::Binary(Some(s.to_bytes()))]),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // State is always serialized as Binary.
        let binary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "hll_union_agg expected binary array for state".to_string(),
                )
            })?;
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            self.ingest_sketch_bytes(binary_array.value(i))?;
        }
        Ok(())
    }
}
