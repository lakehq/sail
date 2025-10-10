use std::any::Any;
use std::fmt::{Debug, Formatter};

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion_common::arrow::datatypes::Float64Type;
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::signature::{Signature, Volatility};

#[derive(PartialEq, Eq, Hash)]
pub struct TrySumFunction {
    signature: Signature,
}

impl Default for TrySumFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl TrySumFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Debug for TrySumFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrySumFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct TrySumAccumulator {
    dtype: DataType,
    sum_i64: Option<i64>,
    sum_f64: Option<f64>,
    failed: bool,
}

impl TrySumAccumulator {
    fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            sum_i64: None,
            sum_f64: None,
            failed: false,
        }
    }

    #[inline]
    fn null_of_dtype(&self) -> ScalarValue {
        match self.dtype {
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float64 => ScalarValue::Float64(None),
            _ => ScalarValue::Null,
        }
    }

    fn update_i64(&mut self, arr: &PrimitiveArray<Int64Type>) {
        if self.failed {
            return;
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_i64 = match self.sum_i64 {
                None => Some(v),
                Some(acc) => match acc.checked_add(v) {
                    Some(s) => Some(s),
                    None => {
                        self.failed = true;
                        return;
                    }
                },
            };
        }
    }

    fn update_f64(&mut self, arr: &PrimitiveArray<Float64Type>) {
        if self.failed {
            return;
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + v);
        }
    }
}

impl Accumulator for TrySumAccumulator {
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum_scalar = match self.dtype {
            DataType::Int64 => self
                .sum_i64
                .map(|x| ScalarValue::Int64(Some(x)))
                .unwrap_or(ScalarValue::Int64(None)),
            DataType::Float64 => self
                .sum_f64
                .map(|x| ScalarValue::Float64(Some(x)))
                .unwrap_or(ScalarValue::Float64(None)),
            _ => ScalarValue::Null,
        };

        Ok(vec![
            if self.failed {
                self.null_of_dtype()
            } else {
                sum_scalar
            },
            ScalarValue::Boolean(Some(self.failed)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() || self.failed {
            return Ok(());
        }
        let arr = &values[0];

        match self.dtype {
            DataType::Int64 => self.update_i64(
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("try_sum: expected Int64".to_string())
                    })?,
            ),
            DataType::Float64 => self.update_f64(
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("try_sum: expected Float64".to_string())
                    })?,
            ),
            ref dt => {
                return Err(DataFusionError::Execution(format!(
                    "try_sum: type not supported update_batch: {dt:?}"
                )))
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Execution(format!(
                "try_sum: number invalid: {} (esperado 2)",
                states.len()
            )));
        }

        // 1) merge flag failed
        let failed_arr = states[1]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("try_sum: state[1] need be Boolean".to_string())
            })?;

        for i in 0..failed_arr.len() {
            if failed_arr.is_valid(i) && failed_arr.value(i) {
                self.failed = true;
                break;
            }
        }
        if self.failed {
            return Ok(());
        }

        match self.dtype {
            DataType::Int64 => {
                let a = states[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("try_sum: state[0] need be Int64".to_string())
                    })?;
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    self.sum_i64 = match self.sum_i64 {
                        None => Some(v),
                        Some(acc) => match acc.checked_add(v) {
                            Some(s) => Some(s),
                            None => {
                                self.failed = true;
                                return Ok(());
                            }
                        },
                    };
                }
            }
            DataType::Float64 => {
                let a = states[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("try_sum: stadte[0] need be Float64".to_string())
                    })?;
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + v);
                }
            }
            ref dt => {
                return Err(DataFusionError::Execution(format!(
                    "try_sum: type not suported in merge_batch: {dt:?}"
                )))
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.failed {
            return Ok(self.null_of_dtype());
        }

        let out = match self.dtype {
            DataType::Int64 => self
                .sum_i64
                .map(|x| ScalarValue::Int64(Some(x)))
                .unwrap_or(ScalarValue::Int64(None)),

            DataType::Float64 => self
                .sum_f64
                .map(|x| ScalarValue::Float64(Some(x)))
                .unwrap_or(ScalarValue::Float64(None)),

            _ => ScalarValue::Null,
        };

        Ok(out)
    }
}

impl AggregateUDFImpl for TrySumFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let dtype = acc_args.return_field.data_type().clone();

        // Basic, need mor types
        match dtype {
            DataType::Int64 | DataType::Float64 => Ok(Box::new(TrySumAccumulator::new(dtype))),
            dt => Err(DataFusionError::Execution(format!(
                "try_sum: type not suported yet: {dt:?}"
            ))),
        }
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "try_sum: only 1 expected{}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Int64 | DataType::Float64 => Ok(vec![arg_types[0].clone()]),
            dt => Err(DataFusionError::Plan(format!(
                "try_sum: type not suported yet: {dt:?}"
            ))),
        }
    }
}
