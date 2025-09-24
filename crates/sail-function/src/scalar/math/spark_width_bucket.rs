use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, Float64Array, Int32Array, Int32Builder,
    IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::IntervalUnit::YearMonth;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{
    as_duration_microsecond_array, as_float64_array, as_int32_array, as_interval_ym_array,
};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
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

        if matches!(v, Interval(YearMonth))
            && matches!(lo, Interval(YearMonth))
            && matches!(hi, Interval(YearMonth))
            && is_int(n)
        {
            return Ok(vec![
                Interval(YearMonth),
                Interval(YearMonth),
                Interval(YearMonth),
                Int32,
            ]);
        }

        Err(unsupported_data_types_exec_err(
            "width_bucket",
            "Float/Decimal OR Duration OR Interval(YearMonth) for first 3 args; Int for 4th",
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
            let min = as_float64_array(minv)?;
            let max = as_float64_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_float64(v, min, max, n_bucket)))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let v = as_duration_microsecond_array(v)?;
            let min = as_duration_microsecond_array(minv)?;
            let max = as_duration_microsecond_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i64_as_float(v, min, max, n_bucket)))
        }
        DataType::Interval(YearMonth) => {
            let v = as_interval_ym_array(v)?;
            let min = as_interval_ym_array(minv)?;
            let max = as_interval_ym_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i32_as_float(v, min, max, n_bucket)))
        }

        other => Err(unsupported_data_types_exec_err(
            "width_bucket",
            "Float/Decimal OR Duration OR Interval(YearMonth) for first 3 args; Int for 4th",
            &[
                other.clone(),
                minv.data_type().clone(),
                maxv.data_type().clone(),
                nb.data_type().clone(),
            ],
        )),
    }
}

macro_rules! width_bucket_kernel_impl {
    ($name:ident, $arr_ty:ty, $to_f64:expr, $check_nan:expr) => {
        pub(crate) fn $name(
            v: &$arr_ty,
            min: &$arr_ty,
            max: &$arr_ty,
            n_bucket: &Int32Array,
        ) -> Int32Array {
            let len = v.len();
            let mut b = Int32Builder::with_capacity(len);

            for i in 0..len {
                if v.is_null(i) || min.is_null(i) || max.is_null(i) || n_bucket.is_null(i) {
                    b.append_null();
                    continue;
                }
                let x = ($to_f64)(v, i);
                let l = ($to_f64)(min, i);
                let h = ($to_f64)(max, i);
                let buckets = n_bucket.value(i);

                if buckets <= 0 {
                    b.append_null();
                    continue;
                }
                if $check_nan {
                    if !x.is_finite() || !l.is_finite() || !h.is_finite() {
                        b.append_null();
                        continue;
                    }
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
                    if x >= h {
                        b.append_value(buckets + 1);
                        continue;
                    }
                } else {
                    if x > l {
                        b.append_value(0);
                        continue;
                    }
                    if x <= h {
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
                if bucket < 1 {
                    bucket = 1;
                }
                if bucket > buckets + 1 {
                    bucket = buckets + 1;
                }

                b.append_value(bucket);
            }

            b.finish()
        }
    };
}

width_bucket_kernel_impl!(
    width_bucket_float64,
    Float64Array,
    |arr: &Float64Array, i: usize| arr.value(i),
    true
);

width_bucket_kernel_impl!(
    width_bucket_i64_as_float,
    DurationMicrosecondArray,
    |arr: &DurationMicrosecondArray, i: usize| arr.value(i) as f64,
    false
);

width_bucket_kernel_impl!(
    width_bucket_i32_as_float,
    IntervalYearMonthArray,
    |arr: &IntervalYearMonthArray, i: usize| arr.value(i) as f64,
    false
);
