use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, DurationMicrosecondBuilder, PrimitiveBuilder};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{DurationMicrosecondType, Float64Type, Int32Type};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::invalid_arg_count_exec_err;
use crate::extension::function::functions_nested_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkMakeDtInterval {
    signature: Signature,
}

impl Default for SparkMakeDtInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeDtInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeDtInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_dt_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            let n: usize = std::cmp::max(args.number_rows, 1);
            let mut b: PrimitiveBuilder<DurationMicrosecondType> =
                DurationMicrosecondBuilder::with_capacity(n);
            for _ in 0..n {
                b.append_value(0_i64);
            }
            return Ok(ColumnarValue::Array(Arc::new(b.finish())));
        }
        make_scalar_function(make_dt_interval_kernel)(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 4 {
            return Err(invalid_arg_count_exec_err(
                "make_dt_interval",
                (1, 4),
                arg_types.len(),
            ));
        }

        Ok((0..arg_types.len())
            .map(|i| {
                if i == 3 {
                    DataType::Float64
                } else {
                    DataType::Int32
                }
            })
            .collect())
    }
}
fn make_dt_interval_kernel(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    // 0 args is in invoke_with_args
    if args.is_empty() || args.len() > 7 {
        return exec_err!("make_interval expects between 0 and 7, got {}", args.len());
    }

    let n_rows = args[0].len();
    debug_assert!(args.iter().all(|a| a.len() == n_rows));

    let days = args.first().map(|a| a.as_primitive::<Int32Type>());
    let hours = args.get(1).map(|a| a.as_primitive::<Int32Type>());
    let mins = args.get(2).map(|a| a.as_primitive::<Int32Type>());
    let secs = args.get(3).map(|a| a.as_primitive::<Float64Type>());

    let mut builder = DurationMicrosecondBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present = days.as_ref().is_some_and(|a| a.is_null(i))
            || hours.as_ref().is_some_and(|a| a.is_null(i))
            || mins.as_ref().is_some_and(|a| a.is_null(i))
            || secs
                .as_ref()
                .is_some_and(|a| a.is_null(i) || a.value(i).is_infinite() || a.value(i).is_nan());

        if any_null_present {
            builder.append_null();
            continue;
        }

        // default values 0 or 0.0
        let d = days.as_ref().map_or(0, |a| a.value(i));
        let h = hours.as_ref().map_or(0, |a| a.value(i));
        let mi = mins.as_ref().map_or(0, |a| a.value(i));
        let s = secs.as_ref().map_or(0.0, |a| a.value(i));

        match make_interval_dt_nano(d, h, mi, s)? {
            Some(v) => builder.append_value(v),
            None => {
                builder.append_null();
                continue;
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}
pub fn make_interval_dt_nano(day: i32, hour: i32, min: i32, sec: f64) -> Result<Option<i64>> {
    if !sec.is_finite() {
        return Ok(None);
    }

    // checks if overflow
    let total_hours = match day.checked_mul(24).and_then(|v| v.checked_add(hour)) {
        Some(hr) => hr,
        None => return Ok(None),
    };
    let total_min = match total_hours.checked_mul(60).and_then(|v| v.checked_add(min)) {
        Some(n) => n,
        None => return Ok(None),
    };
    let nanos_from_min = match (total_min as i64).checked_mul(60_000_000_000) {
        Some(n) => n,
        None => return Ok(None),
    };

    let total_nanos = match (sec as i64)
        .checked_mul(60_000_000_000)
        .and_then(|v| v.checked_add(nanos_from_min))
    {
        Some(n) => n,
        None => return Ok(None),
    };

    Ok(Some(total_nanos))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{DurationMicrosecondArray, Float64Array, Int32Array};
    use arrow::datatypes::DataType::Duration;
    use arrow::datatypes::Field;
    use arrow::datatypes::TimeUnit::Microsecond;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    use super::*;

    fn run_make_dt_interval(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        make_dt_interval_kernel(&arrs)
    }

    #[test]
    fn nulls_propagate_per_row() -> Result<()> {
        let days = Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let hours = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let mins = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let secs = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
        Ok(())
    }

    #[test]
    fn overflow_should_be_null() -> Result<()> {
        let days = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let hours = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        assert_eq!(out.len(), 1);
        assert!(out.is_null(0), "row 0 should be NULL due to overflow");
        Ok(())
    }

    fn invoke_make_dt_interval_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", Duration(Microsecond), true).into(),
        };
        SparkMakeDtInterval::new().invoke_with_args(args)
    }

    #[test]
    fn zero_args_returns_zero_duration() -> Result<()> {
        let number_rows: usize = 3;

        let res: ColumnarValue = invoke_make_dt_interval_with_args(vec![], number_rows)?;
        let arr = res.into_array(number_rows)?;
        let arr = arr
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        assert_eq!(arr.len(), number_rows);
        for i in 0..number_rows {
            assert!(!arr.is_null(i));
            assert_eq!(arr.value(i), 0_i64);
        }
        Ok(())
    }

    #[test]
    fn one_day_minus_24_hours_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(1), Some(-1)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(-24), Some(24)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_secs = Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }

    #[test]
    fn one_hour_minus_60_mins_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(-1), Some(1)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(60), Some(-60)])) as ArrayRef;
        let arr_secs = Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }

    #[test]
    fn one_mins_minus_60_secs_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(-1), Some(1)])) as ArrayRef;
        let arr_secs = Arc::new(Float64Array::from(vec![Some(60.0), Some(-60.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("expected DurationMicrosecondArray".into()))?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }
}
