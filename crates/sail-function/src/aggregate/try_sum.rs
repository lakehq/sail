use std::any::Any;
use std::fmt::{Debug, Formatter};

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::arrow::datatypes::Float64Type;
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
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

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

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
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let sum_dt = args.return_field.data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), sum_dt, true).into(),
            Field::new(
                format_state_name(args.name, "failed"),
                DataType::Boolean,
                false,
            )
            .into(),
        ])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        use DataType::*;
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "try_sum: exactly 1 argument expected, got {}", arg_types.len()
            )));
        }
        let coerced = match arg_types[0] {
            // cualquier entero firmado/unsigned → Int64 (como Spark)
            Int8 | Int16 | Int32 | Int64
            | UInt8 | UInt16 | UInt32 | UInt64 => Int64,

            // floats → Float64
            Float16 | Float32 | Float64 => Float64,

            ref dt => return Err(DataFusionError::Plan(format!(
                "try_sum: unsupported type: {dt:?}"
            ))),
        };
        Ok(vec![coerced])
    }


    fn default_value(&self, _data_type: &DataType) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Null)
    }
}
