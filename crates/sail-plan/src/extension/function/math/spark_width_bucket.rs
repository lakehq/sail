use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array,
    ArrayRef,
    Int32Builder,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::cast::{
    as_duration_microsecond_array, as_float64_array, as_int32_array, as_interval_ym_array,
};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::functions_nested_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkWidthBucket {
    signature: Signature,
}

impl Default for SparkWidthBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkWidthBucket {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkWidthBucket {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "width_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(width_bucket_kern)(&args)
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        if types.len() != 4 {
            return Err(invalid_arg_count_exec_err(
                "width_bucket",
                (4, 4),
                types.len(),
            ));
        }
        let (v, lo, hi, n) = (&types[0], &types[1], &types[2], &types[3]);

        let is_num = |t: &DataType| {
            matches!(
                t,
                Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Decimal128(_, _)
            )
        };
        let is_int = |t: &DataType| matches!(t, Int8 | Int16 | Int32 | Int64);

        if is_num(v) && is_num(lo) && is_num(hi) && is_int(n) {
            return Ok(vec![Float64, Float64, Float64, Int32]);
        }

        let all_duration =
            matches!(v, Duration(_)) && matches!(lo, Duration(_)) && matches!(hi, Duration(_));
        if all_duration && is_int(n) {
            return Ok(vec![
                Duration(TimeUnit::Microsecond),
                Duration(TimeUnit::Microsecond),
                Duration(TimeUnit::Microsecond),
                Int32,
            ]);
        }

        if matches!(v, Interval(IntervalUnit::YearMonth))
            && matches!(lo, Interval(IntervalUnit::YearMonth))
            && matches!(hi, Interval(IntervalUnit::YearMonth))
            && is_int(n)
        {
            return Ok(vec![
                Interval(IntervalUnit::YearMonth),
                Interval(IntervalUnit::YearMonth),
                Interval(IntervalUnit::YearMonth),
                Int32,
            ]);
        }

        Err(unsupported_data_types_exec_err(
            "width_bucket",
            "Float/Decimal OR Duration(*) OR Interval(YearMonth) for first 3 args; Int for 4th",
            types,
        ))
    }
}

fn width_bucket_kern(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [v, minv, maxv, nb] = args else {
        return Err(invalid_arg_count_exec_err(
            "width_bucket",
            (4, 4),
            args.len(),
        ));
    };

    match v.data_type() {
        DataType::Float64 => {
            let v = as_float64_array(v)?;
            let lo = as_float64_array(minv)?;
            let hi = as_float64_array(maxv)?;
            let n = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_float64(v, lo, hi, n)))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let v = as_duration_microsecond_array(v)?;
            let lo = as_duration_microsecond_array(minv)?;
            let hi = as_duration_microsecond_array(maxv)?;
            let n = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i64_as_float(v, lo, hi, n)))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let v = as_interval_ym_array(v)?;
            let lo = as_interval_ym_array(minv)?;
            let hi = as_interval_ym_array(maxv)?;
            let n = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i32_as_float(v, lo, hi, n)))
        }

        other => Err(unsupported_data_types_exec_err(
            "width_bucket",
            "Float64 | Duration(Microsecond) | Interval(YearMonth)",
            &[
                other.clone(),
                minv.data_type().clone(),
                maxv.data_type().clone(),
                nb.data_type().clone(),
            ],
        )),
    }
}

fn width_bucket_float64(
    v: &arrow::array::Float64Array,
    min: &arrow::array::Float64Array,
    max: &arrow::array::Float64Array,
    n_bucket: &arrow::array::Int32Array,
) -> arrow::array::Int32Array {
    let len = v.len();
    let mut b = Int32Builder::with_capacity(len);

    for i in 0..len {
        if v.is_null(i) || min.is_null(i) || max.is_null(i) || n_bucket.is_null(i) {
            b.append_null();
            continue;
        }
        let x = v.value(i);
        let l = min.value(i);
        let h = max.value(i);
        let buckets = n_bucket.value(i);

        if buckets <= 0 || x.is_nan() || l.is_nan() || h.is_nan() {
            b.append_null();
            continue;
        }
        let ord = match l.partial_cmp(&h) {
            Some(o) => o,
            None => {
                b.append_null();
                continue;
            }
        };

        if matches!(ord, Ordering::Equal) {
            b.append_null();
            continue;
        }
        let asc = matches!(ord, Ordering::Less);

        if asc {
            if x < l {
                b.append_value(0);
                continue;
            }
            if x > h {
                b.append_value(buckets + 1);
                continue;
            }
        } else {
            if x > l {
                b.append_value(0);
                continue;
            }
            if x < h {
                b.append_value(buckets + 1);
                continue;
            }
        }

        let width = (h - l) / (buckets as f64);
        if width == 0.0 || !width.is_finite() {
            b.append_null();
            continue;
        }
        let mut bucket = ((x - l) / width).floor() as i32 + 1;
        if bucket > buckets {
            bucket = buckets;
        }
        if bucket < 1 {
            bucket = 1;
        }
        b.append_value(bucket);
    }

    b.finish()
}

fn width_bucket_i64_as_float(
    v: &arrow::array::DurationMicrosecondArray,
    min: &arrow::array::DurationMicrosecondArray,
    max: &arrow::array::DurationMicrosecondArray,
    n_bucket: &arrow::array::Int32Array,
) -> arrow::array::Int32Array {
    let len = v.len();
    let mut b = Int32Builder::with_capacity(len);

    for i in 0..len {
        if v.is_null(i) || min.is_null(i) || max.is_null(i) || n_bucket.is_null(i) {
            b.append_null();
            continue;
        }
        let x = v.value(i) as f64;
        let l = min.value(i) as f64;
        let h = max.value(i) as f64;
        let buckets = n_bucket.value(i);

        if buckets <= 0 {
            b.append_null();
            continue;
        }

        let ord = match l.partial_cmp(&h) {
            Some(o) => o,
            None => {
                b.append_null();
                continue;
            }
        };
        if matches!(ord, Ordering::Equal) {
            b.append_null();
            continue;
        }
        let asc = matches!(ord, Ordering::Less);

        if asc {
            if x < l {
                b.append_value(0);
                continue;
            }
            if x > h {
                b.append_value(buckets + 1);
                continue;
            }
        } else {
            if x > l {
                b.append_value(0);
                continue;
            }
            if x < h {
                b.append_value(buckets + 1);
                continue;
            }
        }

        let width = (h - l) / (buckets as f64);
        if width == 0.0 || !width.is_finite() {
            b.append_null();
            continue;
        }
        let mut bucket = ((x - l) / width).floor() as i32 + 1;
        if bucket > buckets {
            bucket = buckets;
        }
        if bucket < 1 {
            bucket = 1;
        }
        b.append_value(bucket);
    }

    b.finish()
}

// ---------- Interval(YearMonth): valores i32 (meses) ----------
fn width_bucket_i32_as_float(
    v: &arrow::array::IntervalYearMonthArray,
    min: &arrow::array::IntervalYearMonthArray,
    max: &arrow::array::IntervalYearMonthArray,
    n_bucket: &arrow::array::Int32Array,
) -> arrow::array::Int32Array {
    let len = v.len();
    let mut b = Int32Builder::with_capacity(len);

    for i in 0..len {
        if v.is_null(i) || min.is_null(i) || max.is_null(i) || n_bucket.is_null(i) {
            b.append_null();
            continue;
        }
        let x = v.value(i) as f64;
        let l = min.value(i) as f64;
        let h = max.value(i) as f64;
        let buckets = n_bucket.value(i);

        if buckets <= 0 {
            b.append_null();
            continue;
        }

        let ord = match l.partial_cmp(&h) {
            Some(o) => o,
            None => {
                b.append_null();
                continue;
            }
        };
        if matches!(ord, Ordering::Equal) {
            b.append_null();
            continue;
        }
        let asc = matches!(ord, Ordering::Less);

        if asc {
            if x < l {
                b.append_value(0);
                continue;
            }
            if x > h {
                b.append_value(buckets + 1);
                continue;
            }
        } else {
            if x > l {
                b.append_value(0);
                continue;
            }
            if x < h {
                b.append_value(buckets + 1);
                continue;
            }
        }

        let width = (h - l) / (buckets as f64);
        if width == 0.0 || !width.is_finite() {
            b.append_null();
            continue;
        }
        let mut bucket = ((x - l) / width).floor() as i32 + 1;
        if bucket > buckets {
            bucket = buckets;
        }
        if bucket < 1 {
            bucket = 1;
        }
        b.append_value(bucket);
    }

    b.finish()
}
