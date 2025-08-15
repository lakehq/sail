use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Builder};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_float64_array, as_int32_array};
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
                "spark_try_subtract",
                (2, 2),
                types.len(),
            ));
        }
        let is_num = |t: &DataType| {
            matches!(
                t,
                Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Decimal128(_, _)
            )
        };
        let is_int = |t: &DataType| matches!(t, Int8 | Int16 | Int32 | Int64);

        let (v, lo, hi, n) = (&types[0], &types[1], &types[2], &types[3]);

        if !(is_num(v) && is_num(lo) && is_num(hi) && is_int(n)) {
            return Err(unsupported_data_types_exec_err(
                "spark_try_subtract",
                "Float,Float,Float, Int",
                types,
            ));
        }

        Ok(vec![Float64, Float64, Float64, Int32])
    }
}

fn width_bucket_kern(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [v, minv, maxv, nb] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_width_bucket",
            (4, 4),
            args.len(),
        ));
    };
    // only float64 + int32
    // check coerce_types o use fixed values
    let v = as_float64_array(v)?;
    let minv = as_float64_array(minv)?;
    let maxv = as_float64_array(maxv)?;
    let nb = as_int32_array(nb)?;

    let len = v.len();
    let mut b = Int32Builder::with_capacity(len);

    for i in 0..len {
        if v.is_null(i) || minv.is_null(i) || maxv.is_null(i) || nb.is_null(i) {
            b.append_null();
            continue;
        }
        let x = v.value(i);
        let lo = minv.value(i);
        let hi = maxv.value(i);
        let n = nb.value(i);

        let valid_range = matches!(lo.partial_cmp(&hi), Some(std::cmp::Ordering::Less));
        if n <= 0 || !valid_range {
            b.append_null();
            continue;
        }

        if x < lo {
            b.append_value(0);
            continue;
        }
        if x > hi {
            b.append_value(n + 1);
            continue;
        }

        let width = (hi - lo) / (n as f64);
        let mut bucket = ((x - lo) / width).floor() as i32 + 1;
        if bucket > n {
            bucket = n;
        }
        b.append_value(bucket);
    }

    Ok(Arc::new(b.finish()))
}
