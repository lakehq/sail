use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, DurationMicrosecondBuilder, PrimitiveBuilder};
use arrow::datatypes::{DurationMicrosecondType, Float64Type, Int32Type};
use arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use crate::extension::function::functions_nested_utils::make_scalar_function;

use crate::extension::function::error_utils::invalid_arg_count_exec_err;

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
            .map(|i| if i == 3 { DataType::Float64 } else { DataType::Int32 })
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

    let days  = args.get(0).map(|a| a.as_primitive::<Int32Type>());
    let hours = args.get(1).map(|a| a.as_primitive::<Int32Type>());
    let mins  = args.get(2).map(|a| a.as_primitive::<Int32Type>());
    let secs  = args.get(3).map(|a| a.as_primitive::<Float64Type>());

    let mut builder = DurationMicrosecondBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present =  days.as_ref().is_some_and(|a| a.is_null(i))
            || hours.as_ref().is_some_and(|a| a.is_null(i))
            || mins.as_ref().is_some_and(|a| a.is_null(i))
            || secs.as_ref().is_some_and(|a| {
            a.is_null(i) || a.value(i).is_infinite() || a.value(i).is_nan()
        });

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
pub fn make_interval_dt_nano(
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> Result<Option<i64>> {

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

    let total_nanos = match (sec as i64).checked_mul(60_000_000_000)
        .and_then(|v| v.checked_add(nanos_from_min)) {
        Some(n) => n,
        None => return Ok(None),
    };

    Ok(Some(total_nanos))
}