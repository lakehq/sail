use std::any::Any;
use std::fmt::{Debug, Formatter};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    PrimitiveArray,
};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion_common::arrow::datatypes::Float64Type;
use datafusion_common::{downcast_value, DataFusionError, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::signature::{Signature, Volatility};

#[derive(PartialEq, Eq, Hash)]
pub struct TryAvgFunction {
    signature: Signature,
}

impl Default for TryAvgFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl TryAvgFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Debug for TryAvgFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TryAvgFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct TryAvgAccumulator {
    dtype: DataType,
    sum_i64: Option<i64>,
    sum_f64: Option<f64>,
    sum_dec128: Option<i128>,
    count: i64,
    failed: bool,
}

impl TryAvgAccumulator {
    fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            sum_i64: None,
            sum_f64: None,
            sum_dec128: None,
            count: 0,
            failed: false,
        }
    }

    fn null_of_dtype(&self) -> ScalarValue {
        match self.dtype {
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
            _ => ScalarValue::Null,
        }
    }

    fn inc_count(&mut self) {
        if self.failed {
            return;
        }
        self.count = match self.count.checked_add(1) {
            Some(v) => v,
            None => {
                self.failed = true;
                0
            }
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
            self.inc_count();
            if self.failed {
                return;
            }
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
            self.inc_count();
            if self.failed {
                return;
            }
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
            self.inc_count();
            if self.failed {
                return;
            }
        }
    }

    fn avg_scalar(&self) -> ScalarValue {
        if self.failed || self.count == 0 {
            return self.null_of_dtype();
        }

        match &self.dtype {
            DataType::Float64 => {
                let total_f64 = if let Some(s) = self.sum_f64 {
                    s
                } else if let Some(s) = self.sum_i64 {
                    s as f64
                } else {
                    return ScalarValue::Float64(None);
                };

                let denom = self.count as f64;
                if denom == 0.0 {
                    ScalarValue::Float64(None)
                } else {
                    ScalarValue::Float64(Some(total_f64 / denom))
                }
            }
            DataType::Decimal128(p, s) => {
                if let Some(total) = self.sum_dec128 {
                    let denom = self.count as i128;
                    if denom == 0 {
                        return ScalarValue::Decimal128(None, *p, *s);
                    }

                    if let Some(q) = total.checked_div(denom) {
                        if exceeds_decimal128_precision(q, *p) {
                            ScalarValue::Decimal128(None, *p, *s)
                        } else {
                            ScalarValue::Decimal128(Some(q), *p, *s)
                        }
                    } else {
                        ScalarValue::Decimal128(None, *p, *s)
                    }
                } else {
                    ScalarValue::Decimal128(None, *p, *s)
                }
            }
            _ => ScalarValue::Null,
        }
    }
}

impl Accumulator for TryAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        if values.is_empty() || self.failed {
            return Ok(());
        }

        match self.dtype {
            DataType::Float64 => {
                if let Some(int_arr) = values[0].as_any().downcast_ref::<Int64Array>() {
                    self.update_i64(int_arr);
                } else if let Some(f_arr) = values[0].as_any().downcast_ref::<Float64Array>() {
                    self.update_f64(f_arr);
                } else {
                    return Err(DataFusionError::Execution(
                        "try_avg: unsupported array type for Float64 output".to_string(),
                    ));
                }
            }
            DataType::Decimal128(p, s) => {
                let array = values[0].as_primitive::<Decimal128Type>();
                self.update_dec128(array, p, s);
            }
            ref dt => {
                return Err(DataFusionError::Execution(format!(
                    "try_avg: type not supported update_batch: {dt:?}"
                )))
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(self.avg_scalar())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let sum_scalar = if self.failed {
            self.null_of_dtype()
        } else if let Some(v) = self.sum_i64 {
            ScalarValue::Int64(Some(v))
        } else if let Some(v) = self.sum_f64 {
            ScalarValue::Float64(Some(v))
        } else if let Some(v) = self.sum_dec128 {
            match &self.dtype {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(v), *p, *s),
                _ => ScalarValue::Null,
            }
        } else {
            match &self.dtype {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, *p, *s),
                DataType::Float64 => ScalarValue::Float64(None),
                _ => ScalarValue::Null,
            }
        };
        Ok(vec![
            sum_scalar,
            if self.failed {
                ScalarValue::Int64(None)
            } else {
                ScalarValue::Int64(Some(self.count))
            },
            ScalarValue::Boolean(Some(self.failed)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        // states layout:
        // 0 -> sum parcial (Int64Array o Float64Array o Decimal128Array)
        // 1 -> count (Int64Array)
        // 2 -> failed (BooleanArray)

        // 1) merge failed
        {
            let failed_arr = downcast_value!(states[2], BooleanArray);
            for i in 0..failed_arr.len() {
                if failed_arr.is_valid(i) && failed_arr.value(i) {
                    self.failed = true;
                    return Ok(());
                }
            }
        }

        // 2) merge count
        {
            let count_arr = downcast_value!(states[1], Int64Array);
            for value in count_arr.iter().flatten() {
                self.count = match self.count.checked_add(value) {
                    Some(v) => v,
                    None => {
                        self.failed = true;
                        return Ok(());
                    }
                };
            }
        }

        // 3) merge sums con checks de overflow/precision
        match self.dtype {
            DataType::Decimal128(p, _s) => {
                let array = downcast_value!(states[0], Decimal128Array);
                for value in array.iter().flatten() {
                    let next = match self.sum_dec128 {
                        None => value,
                        Some(acc) => match acc.checked_add(value) {
                            Some(sum) => sum,
                            None => {
                                self.failed = true;
                                return Ok(());
                            }
                        },
                    };

                    if exceeds_decimal128_precision(next, p) {
                        self.failed = true;
                        return Ok(());
                    }

                    self.sum_dec128 = Some(next);
                }
            }
            DataType::Float64 => {
                if let Some(int_arr) = states[0].as_any().downcast_ref::<Int64Array>() {
                    for value in int_arr.iter().flatten() {
                        self.sum_i64 = match self.sum_i64 {
                            None => Some(value),
                            Some(acc) => match acc.checked_add(value) {
                                Some(s) => Some(s),
                                None => {
                                    self.failed = true;
                                    return Ok(());
                                }
                            },
                        };
                    }
                } else if let Some(f_arr) = states[0].as_any().downcast_ref::<Float64Array>() {
                    for value in f_arr.iter().flatten() {
                        self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + value);
                    }
                } else {
                    return Err(DataFusionError::Execution(
                        "try_avg: merge_batch unsupported partial sum type for Float64 output"
                            .to_string(),
                    ));
                }
            }

            ref dt => {
                return Err(DataFusionError::Execution(format!(
                    "try_avg: type not supported in merge_batch: {dt:?}"
                )))
            }
        }

        Ok(())
    }
}

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

impl AggregateUDFImpl for TryAvgFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        use DataType::*;

        let dt = &arg_types[0];
        let result_type = match *dt {
            Null => Float64,
            Decimal128(p, s) => {
                let p2 = std::cmp::min(p + 10, 38);
                Decimal128(p2, s)
            }
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Float64,
            Float16 | Float32 | Float64 => Float64,
            ref other => {
                return Err(DataFusionError::Plan(format!(
                    "try_avg: unsupported type: {other:?}"
                )))
            }
        };

        Ok(result_type)
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        let dtype = acc_args.return_field.data_type().clone();
        Ok(Box::new(TryAvgAccumulator::new(dtype)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> datafusion::common::Result<Vec<FieldRef>> {
        let sum_dt = args.return_field.data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), sum_dt.clone(), true).into(),
            Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "failed"),
                DataType::Boolean,
                false,
            )
            .into(),
        ])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        use DataType::*;
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "try_avg: exactly 1 argument expected, got {}",
                arg_types.len()
            )));
        }

        let dt = &arg_types[0];
        let coerced = match dt {
            Null => Float64,
            Decimal128(p, s) => Decimal128(*p, *s),
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
            Float16 | Float32 | Float64 => Float64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "try_avg: unsupported type: {other:?}"
                )))
            }
        };
        Ok(vec![coerced])
    }

    fn default_value(&self, _data_type: &DataType) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion_common::arrow::array::Float64Array;
    use datafusion_common::ScalarValue;

    use super::*;

    // -------- Helpers --------

    fn int64(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values)) as ArrayRef
    }

    fn f64(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values)) as ArrayRef
    }

    fn dec128(p: u8, s: i8, vals: Vec<Option<i128>>) -> datafusion::common::Result<ArrayRef> {
        let base = Decimal128Array::from(vals);
        let arr = base.with_precision_and_scale(p, s).map_err(|e| {
            DataFusionError::Execution(format!("invalid precision/scale ({p},{s}): {e}"))
        })?;
        Ok(Arc::new(arr) as ArrayRef)
    }

    // -------- update_batch + evaluate (avg semantics) --------

    #[test]
    fn try_avg_int_basic() -> datafusion::common::Result<()> {
        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[int64((0..10).map(Some).collect())])?;
        let out = acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = out {
            assert!((v - 4.5).abs() < 1e-12, "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(_))),
            "expected Float64(Some), got {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_avg_int_with_nulls() -> datafusion::common::Result<()> {
        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[int64(vec![None, Some(2), Some(3), None, Some(5)])])?;
        let out = acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = out {
            assert!((v - (10.0 / 3.0)).abs() < 1e-12, "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(_))),
            "expected Float64(Some), got {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_avg_float_basic() -> datafusion::common::Result<()> {
        // 1.5 + 2.5 + 3.0 = 7.0, count=3 => avg=7/3 ~= 2.3333...
        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.5), Some(2.5), None, Some(3.0)])])?;
        let out = acc.evaluate()?;

        if let ScalarValue::Float64(Some(v)) = out {
            assert!((v - (7.0 / 3.0)).abs() < 1e-12, "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(_))),
            "expected Float64(Some), got {out:?}"
        );
        Ok(())
    }

    #[test]
    fn float_overflow_behaves_like_spark_sum_infinite() -> datafusion::common::Result<()> {
        // sum ≈ 1e308 + 1e308 = inf, count=2 => avg=inf/2 = inf
        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1e308), Some(1e308)])])?;

        let out = acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = out {
            assert!(v.is_infinite() && v.is_sign_positive(), "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(v)) if v.is_infinite() && v.is_sign_positive()),
            "expected +Infinity, got {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_avg_decimal_basic() -> datafusion::common::Result<()> {
        // p=10,s=2 ; vals [123,477] => sum=600,count=2 => avg=300
        let p = 20u8;
        let s = 2i8;
        // salida decimal widened DECIMAL(20,2)
        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(p, s));
        // input DECIMAL(10,2) -> valores sin null
        acc.update_batch(&[dec128(10, s, vec![Some(123), Some(477)])?])?;
        let out = acc.evaluate()?;

        // 600/2 = 300
        assert_eq!(out, ScalarValue::Decimal128(Some(300), p, s));
        Ok(())
    }

    #[test]
    fn try_avg_decimal_with_nulls() -> datafusion::common::Result<()> {
        // vals [150,NULL,200] => sum=350,count=2 => avg=175
        let p = 20u8;
        let s = 2i8;
        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(10, s, vec![Some(150), None, Some(200)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(175), p, s));
        Ok(())
    }

    #[test]
    fn try_avg_decimal_overflow_sets_failed() -> datafusion::common::Result<()> {
        let p = 15u8;
        let s = 0i8;
        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(5, s, vec![Some(90_000), Some(20_000)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(55_000), p, s));
        Ok(())
    }

    // -------- state + merge_batch --------

    #[test]
    fn try_avg_state_and_merge_ok() -> datafusion::common::Result<()> {
        // acc1: [10,5] -> sum=15,count=2
        let mut acc1 = TryAvgAccumulator::new(DataType::Float64);
        acc1.update_batch(&[int64(vec![Some(10), Some(5)])])?;
        let state1 = acc1.state()?;
        assert_eq!(state1.len(), 3);

        let mut acc2 = TryAvgAccumulator::new(DataType::Float64);
        acc2.update_batch(&[int64(vec![Some(20), None])])?;
        let state2 = acc2.state()?;

        let state1_arrays: Vec<ArrayRef> = state1
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<datafusion::common::Result<_>>()?;

        let state2_arrays: Vec<ArrayRef> = state2
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<datafusion::common::Result<_>>()?;

        let mut final_acc = TryAvgAccumulator::new(DataType::Float64);

        final_acc.merge_batch(&state1_arrays)?;
        final_acc.merge_batch(&state2_arrays)?;

        assert!(!final_acc.failed);
        let out = final_acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = out {
            assert!((v - (35.0 / 3.0)).abs() < 1e-12, "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(_))),
            "expected Float64(Some), got {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_avg_merge_propagates_failure() -> datafusion::common::Result<()> {
        let failed_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let failed_count = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let failed_flag = Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef;

        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.merge_batch(&[failed_sum, failed_count, failed_flag])?;

        assert!(acc.failed);
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Float64(None));
        Ok(())
    }

    #[test]
    fn try_avg_merge_empty_partition_is_not_failure() -> datafusion::common::Result<()> {
        let empty_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let zero_count = Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef;
        let ok_flag = Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef;

        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[int64(vec![Some(7), Some(8)])])?; // sum=15,count=2 => avg=7.5
        acc.merge_batch(&[empty_sum, zero_count, ok_flag])?;

        assert!(!acc.failed);
        let out = acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = out {
            assert!((v - 7.5).abs() < 1e-12, "got {v}");
        }
        assert!(
            matches!(out, ScalarValue::Float64(Some(_))),
            "expected Float64(Some), got {out:?}"
        );
        Ok(())
    }

    // -------- signature / return_type --------

    #[test]
    fn try_avg_return_type_matches_input() -> datafusion::common::Result<()> {
        let f = TryAvgFunction::new();
        assert_eq!(f.return_type(&[DataType::Int64])?, DataType::Float64);
        assert_eq!(f.return_type(&[DataType::Float64])?, DataType::Float64);
        assert_eq!(
            f.return_type(&[DataType::Decimal128(10, 2)])?,
            DataType::Decimal128(20, 2)
        );
        Ok(())
    }

    #[test]
    fn try_avg_state_and_evaluate_consistency_float() -> datafusion::common::Result<()> {
        let mut acc = TryAvgAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.0), Some(2.0)])])?;
        // sum=3.0,count=2 => avg=1.5
        let eval = acc.evaluate()?;
        if let ScalarValue::Float64(Some(v)) = eval {
            assert!((v - 1.5).abs() < 1e-12, "unexpected avg {v}");
        }
        assert!(
            matches!(eval, ScalarValue::Float64(Some(_))),
            "unexpected eval result {eval:?}"
        );

        let st = acc.state()?;
        assert_eq!(st.len(), 3);
        assert_eq!(st[2], ScalarValue::Boolean(Some(false)));
        Ok(())
    }

    // -------------------------
    // DECIMAL widening tests
    // -------------------------

    #[test]
    fn decimal_10_2_sum_and_schema_widened() -> datafusion::common::Result<()> {
        // input: DECIMAL(10,2)  -> return DECIMAL(20,2)
        let f = TryAvgFunction::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(10, 2)])?,
            DataType::Decimal128(20, 2),
            "Spark needs precision+10"
        );

        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(20, 2));
        acc.update_batch(&[dec128(10, 2, vec![Some(123), Some(477)])?])?;
        // sum=600,count=2 => avg=300
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(Some(300), 20, 2));
        Ok(())
    }

    #[test]
    fn decimal_5_0_widened_avg() -> datafusion::common::Result<()> {
        // input: DECIMAL(5,0) -> return DECIMAL(15,0)
        // vals [90000,20000] => sum=110000,count=2 => avg=55000
        let f = TryAvgFunction::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(5, 0)])?,
            DataType::Decimal128(15, 0)
        );

        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(15, 0));
        acc.update_batch(&[dec128(5, 0, vec![Some(90_000), Some(20_000)])?])?;
        assert_eq!(
            acc.evaluate()?,
            ScalarValue::Decimal128(Some(55_000), 15, 0)
        );
        Ok(())
    }

    #[test]
    fn decimal_38_0_max_precision_overflows_to_null() -> datafusion::common::Result<()> {
        let f = TryAvgFunction::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(38, 0)])?,
            DataType::Decimal128(38, 0)
        );
        let ten_pow_38_minus_1 = {
            let p10 = super::pow10_i128(38)
                .ok_or_else(|| DataFusionError::Internal("10^38 overflow".into()))?;
            p10 - 1
        };

        // sum > 10^38-1 => overflow => failed => avg NULL
        let mut acc = TryAvgAccumulator::new(DataType::Decimal128(38, 0));
        acc.update_batch(&[dec128(38, 0, vec![Some(ten_pow_38_minus_1), Some(1)])?])?;

        assert!(acc.failed, "need fail in overflow p=38");
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(None, 38, 0));
        Ok(())
    }
}
