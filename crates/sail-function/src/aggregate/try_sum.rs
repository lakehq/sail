use std::any::Any;
use std::fmt::{Debug, Formatter};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, PrimitiveArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::arrow::datatypes::Float64Type;
use datafusion_common::{downcast_value, DataFusionError, Result as DFResult, ScalarValue};
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
    sum_dec128: Option<i128>, // <-- NUEVO
    failed: bool,
}

impl TrySumAccumulator {
    fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            sum_i64: None,
            sum_f64: None,
            sum_dec128: None,
            failed: false,
        }
    }

    #[inline]
    fn null_of_dtype(&self) -> ScalarValue {
        match self.dtype {
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s), // <-- NUEVO
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

    fn update_dec128(&mut self, arr: &Decimal128Array, p: u8, _s: i8) {
        if self.failed {
            return;
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_dec128 = match self.sum_dec128 {
                None => Some(v),
                Some(acc) => match acc.checked_add(v) {
                    Some(sum) => Some(sum),
                    None => {
                        self.failed = true;
                        return;
                    }
                },
            };
            if let Some(sum) = self.sum_dec128 {
                if exceeds_decimal128_precision(sum, p) {
                    self.failed = true;
                    return;
                }
            }
        }
    }
}

macro_rules! sum_scalar_match {
    ($self:ident, {
            $(
                $pat:pat => ($field:ident, $some_ctor:expr, $null_sv:expr)
            ),+ $(,)?
        }) => {{
            match &$self.dtype {
                $(
                    &$pat => {
                        match $self.$field {
                            Some(v) => $some_ctor(v),
                            None    => $null_sv,
                        }
                    }
                ),+,
                _ => ScalarValue::Null,
            }
        }};
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
            DataType::Decimal128(p, s) => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("try_sum: expected Decimal128".to_string())
                    })?;
                self.update_dec128(a, p, s);
            }
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
        let out = sum_scalar_match!(self, {
            DataType::Int64   => (sum_i64, |x| ScalarValue::Int64(Some(x)),   ScalarValue::Int64(None)),
            DataType::Float64 => (sum_f64, |x| ScalarValue::Float64(Some(x)), ScalarValue::Float64(None)),
                DataType::Decimal128(p, s) => (sum_dec128,|x| ScalarValue::Decimal128(Some(x), p, s),    ScalarValue::Decimal128(None, p, s)),
        });
        Ok(out)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum_scalar = sum_scalar_match!(self, {
            DataType::Int64   => (sum_i64, |x| ScalarValue::Int64(Some(x)),   ScalarValue::Int64(None)),
            DataType::Float64 => (sum_f64, |x| ScalarValue::Float64(Some(x)), ScalarValue::Float64(None)),
            DataType::Decimal128(p, s) => (sum_dec128,|x| ScalarValue::Decimal128(Some(x), p, s),    ScalarValue::Decimal128(None, p, s)),
        });

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
                let a = downcast_value!(states[0], Int64Array);
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
                let a = downcast_value!(states[0], Float64Array);
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + v);
                }
            }
            DataType::Decimal128(p, _s) => {
                let a = downcast_value!(states[0], Decimal128Array);
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    self.sum_dec128 = match self.sum_dec128 {
                        None => Some(v),
                        Some(acc) => match acc.checked_add(v) {
                            Some(sum) => Some(sum),
                            None => {
                                self.failed = true;
                                return Ok(());
                            }
                        },
                    };
                    if let Some(sum) = self.sum_dec128 {
                        if exceeds_decimal128_precision(sum, p) {
                            self.failed = true;
                            return Ok(());
                        }
                    }
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

// Helpers to determine is exceeds decimal
fn pow10_i128(p: u8) -> Option<i128> {
    let mut v: i128 = 1;
    for _ in 0..p {
        v = v.checked_mul(10)?;
    }
    Some(v)
}

fn exceeds_decimal128_precision(sum: i128, p: u8) -> bool {
    if let Some(max_plus_one) = pow10_i128(p) {
        let max = max_plus_one - 1;
        sum > max || sum < -max
    } else {
        true
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
            DataType::Int64 | DataType::Float64 | DataType::Decimal128(_, _) => {
                Ok(Box::new(TrySumAccumulator::new(dtype)))
            }
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

    fn default_value(&self, _data_type: &DataType) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion_common::arrow::array::Float64Array;
    use datafusion_common::{Result as DFResult, ScalarValue};

    use super::*;
    // -------- Helpers --------

    fn int64(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values)) as ArrayRef
    }

    fn f64(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values)) as ArrayRef
    }

    fn dec128(p: u8, s: i8, vals: Vec<Option<i128>>) -> DFResult<ArrayRef> {
        let base = Decimal128Array::from(vals);
        let arr = base.with_precision_and_scale(p, s).map_err(|e| {
            DataFusionError::Execution(format!("invalid precision/scale ({p},{s}): {e}"))
        })?;
        Ok(Arc::new(arr) as ArrayRef)
    }

    // -------- update_batch + evaluate --------

    #[test]
    fn try_sum_int_basic() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.update_batch(&[int64((0..10).map(Some).collect())])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(45)));
        Ok(())
    }

    #[test]
    fn try_sum_int_with_nulls() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.update_batch(&[int64(vec![None, Some(2), Some(3), None, Some(5)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(10)));
        Ok(())
    }

    #[test]
    fn try_sum_float_basic() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.5), Some(2.5), None, Some(3.0)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Float64(Some(7.0)));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_basic() -> DFResult<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(123), Some(477)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(600), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_with_nulls() -> DFResult<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(150), None, Some(200)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(350), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_overflow_sets_failed() -> DFResult<()> {
        let p = 5u8;
        let s = 0i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(90_000), Some(20_000)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(None, p, s));
        assert!(acc.failed);
        Ok(())
    }

    #[test]
    fn try_sum_decimal_merge_ok_and_failure_propagation() -> DFResult<()> {
        let p = 10u8;
        let s = 2i8;

        let mut p_ok = TrySumAccumulator::new(DataType::Decimal128(p, s));
        p_ok.update_batch(&[dec128(p, s, vec![Some(100), Some(200)])?])?;
        let s_ok = p_ok
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<DFResult<Vec<_>>>()?;

        let mut p_fail = TrySumAccumulator::new(DataType::Decimal128(p, s));
        p_fail.update_batch(&[dec128(p, s, vec![Some(i128::MAX), Some(1)])?])?;
        let s_fail = p_fail
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<DFResult<Vec<_>>>()?;

        let mut final_acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        final_acc.merge_batch(&s_ok)?;
        final_acc.merge_batch(&s_fail)?;

        assert!(final_acc.failed);
        assert_eq!(final_acc.evaluate()?, ScalarValue::Decimal128(None, p, s));
        Ok(())
    }

    #[test]
    fn try_sum_int_overflow_sets_failed() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        // i64::MAX + 1 => overflow => failed => result NULL
        acc.update_batch(&[int64(vec![Some(i64::MAX), Some(1)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(None));
        assert!(acc.failed);
        Ok(())
    }

    #[test]
    fn try_sum_int_negative_overflow_sets_failed() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        // i64::MIN - 1 → overflow negative
        acc.update_batch(&[int64(vec![Some(i64::MIN), Some(-1)])])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(None));
        assert!(acc.failed);
        Ok(())
    }

    // -------- state + merge_batch --------

    #[test]
    fn try_sum_state_two_fields_and_merge_ok() -> DFResult<()> {
        // acumulador 1 ve [10, 5] -> sum=15
        let mut acc1 = TrySumAccumulator::new(DataType::Int64);
        acc1.update_batch(&[int64(vec![Some(10), Some(5)])])?;
        let state1 = acc1.state()?; // [sum, failed]
        assert_eq!(state1.len(), 2);

        // acumulador 2 ve [20, NULL] -> sum=20
        let mut acc2 = TrySumAccumulator::new(DataType::Int64);
        acc2.update_batch(&[int64(vec![Some(20), None])])?;
        let state2 = acc2.state()?; // [sum, failed]

        let state1_arrays: Vec<ArrayRef> = state1
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<DFResult<_>>()?;

        let state2_arrays: Vec<ArrayRef> = state2
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<DFResult<_>>()?;

        // final accumulator
        let mut final_acc = TrySumAccumulator::new(DataType::Int64);

        final_acc.merge_batch(&state1_arrays)?;
        final_acc.merge_batch(&state2_arrays)?;

        // sum total = 15 + 20 = 35
        assert!(!final_acc.failed);
        assert_eq!(final_acc.evaluate()?, ScalarValue::Int64(Some(35)));
        Ok(())
    }

    #[test]
    fn try_sum_merge_propagates_failure() -> DFResult<()> {
        // sum=NULL, failed=true
        let failed_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let failed_flag = Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef;

        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.merge_batch(&[failed_sum, failed_flag])?;

        assert!(acc.failed);
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(None));
        Ok(())
    }

    #[test]
    fn try_sum_merge_empty_partition_is_not_failure() -> DFResult<()> {
        // sum=NULL, failed=false
        let empty_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let ok_flag = Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef;

        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.update_batch(&[int64(vec![Some(7), Some(8)])])?; // 15

        acc.merge_batch(&[empty_sum, ok_flag])?;

        assert!(!acc.failed);
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(Some(15)));
        Ok(())
    }

    // -------- signature --------

    #[test]
    fn try_sum_return_type_matches_input() -> DFResult<()> {
        let f = TrySumFunction::new();
        assert_eq!(f.return_type(&[DataType::Int64])?, DataType::Int64);
        assert_eq!(f.return_type(&[DataType::Float64])?, DataType::Float64);
        Ok(())
    }

    #[test]
    fn try_sum_state_and_evaluate_consistency() -> DFResult<()> {
        let mut acc = TrySumAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.0), Some(2.0)])])?;
        let eval = acc.evaluate()?;
        let state = acc.state()?;
        assert_eq!(state[0], eval);
        assert_eq!(state[1], ScalarValue::Boolean(Some(false)));
        Ok(())
    }
}
