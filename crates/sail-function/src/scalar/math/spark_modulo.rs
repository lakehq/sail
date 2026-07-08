use datafusion::arrow::array::{Array, ArrayRef, Scalar, new_null_array};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::kernels::numeric::rem;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::math::utils::try_op::{arith_input_types, arith_result_type};

/// Spark `%`/`mod` and `try_mod`, unified. `safe = true` is `try_mod`
/// (remainder-by-zero → NULL, ANSI-invariant); `safe = false` is `%`/`mod` and
/// honors `ansi_mode` (remainder-by-zero → `REMAINDER_BY_ZERO` under ANSI, NULL
/// under non-ANSI). Integer/float keep their type, `float % float` stays float
/// (never promoted to double), and decimal follows Spark's remainder
/// precision/scale rule (handled by `BinaryTypeCoercer` for `Operator::Modulo`).
///
/// The zero-divisor handling (`try_rem` below) is adapted from datafusion-spark's
/// `SparkMod::try_rem` (`datafusion-spark` 54.0.0):
/// https://github.com/davidlghellin/datafusion/blob/958f7d982fd9105a69fdc9f0162cda4450c3e107/datafusion/spark/src/function/math/modulus.rs#L36-L53
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkModulo {
    signature: Signature,
    ansi_mode: bool,
    safe: bool,
}

impl Default for SparkModulo {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkModulo {
    pub fn new(ansi_mode: bool, safe: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
            safe,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    /// Remainder by zero raises only for `%`/`mod` under ANSI mode; `try_mod` and
    /// non-ANSI `%` return NULL.
    fn error_on_zero(&self) -> bool {
        self.ansi_mode && !self.safe
    }
}

/// Spark's `[REMAINDER_BY_ZERO]` error (SQLSTATE 22012), raised by `%`/`mod`
/// under ANSI mode; `try_mod` and non-ANSI `%` return NULL instead.
pub fn remainder_by_zero_err() -> DataFusionError {
    exec_datafusion_err!(
        "[REMAINDER_BY_ZERO] Remainder by zero. Use `try_mod` to tolerate divisor being 0 \
         and return NULL instead."
    )
}

/// `rem(left, right)` with Spark's zero-divisor handling. When `error_on_zero`
/// (ANSI `%`), a zero divisor raises `REMAINDER_BY_ZERO`. Otherwise the zero
/// divisors are nulled before the remainder so those positions return NULL
/// while the rest compute normally. Arrow's `rem` already returns `0` for
/// `INT_MIN % -1` (no overflow) and NaN for float `x % ±inf`.
fn try_rem(left: &ArrayRef, right: &ArrayRef, error_on_zero: bool) -> Result<ArrayRef> {
    // Detect zero divisors up front: arrow's `rem` errors on integer/decimal
    // `x % 0` but returns NaN for float `x % 0`, so relying on its error would
    // miss the float case. `eq` yields NULL (not true) for NULL divisors, so
    // only genuine zeros are counted.
    let zero = Scalar::new(ScalarValue::new_zero(right.data_type())?.to_array()?);
    let is_zero = eq(right, &zero)?;
    if error_on_zero {
        if is_zero.true_count() > 0 {
            return Err(remainder_by_zero_err());
        }
        return Ok(rem(left, right)?);
    }
    // Legacy / try_mod: null out zero divisors so those rows return NULL while
    // the rest compute normally.
    let null = Scalar::new(new_null_array(right.data_type(), 1));
    let safe_right = zip(&is_zero, &null, right)?;
    Ok(rem(left, &safe_right)?)
}

impl ScalarUDFImpl for SparkModulo {
    fn name(&self) -> &str {
        if self.safe { "try_mod" } else { "spark_modulo" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_modulo",
                (2, 2),
                arg_types.len(),
            ));
        };
        arith_result_type(left, Operator::Modulo, right)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_modulo",
                (2, 2),
                args.len(),
            ));
        };
        Ok(ColumnarValue::Array(try_rem(
            left,
            right,
            self.error_on_zero(),
        )?))
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = types else {
            return Err(invalid_arg_count_exec_err(
                "spark_modulo",
                (2, 2),
                types.len(),
            ));
        };
        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }
        let (l, r) = arith_input_types(left, Operator::Modulo, right)?;
        Ok(vec![l, r])
    }
}
