use std::sync::Arc;

use datafusion::arrow::array::Decimal128Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::numeric::div;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::{BigInt, Signed, ToPrimitive, Zero};

use crate::functions_nested_utils::make_scalar_function;

/// Exact decimal division with Spark's HALF_UP rounding at the resolved result scale.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalDivide {
    signature: Signature,
    pub ansi_mode: bool,
}

impl DecimalDivide {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            ansi_mode,
        }
    }
}

fn decimal(value: ScalarValue) -> Result<(BigInt, i8)> {
    let (value, scale) = match value {
        ScalarValue::Decimal128(Some(value), _, scale) => (value, scale),
        ScalarValue::Int8(Some(value)) => (i128::from(value), 0),
        ScalarValue::Int16(Some(value)) => (i128::from(value), 0),
        ScalarValue::Int32(Some(value)) => (i128::from(value), 0),
        ScalarValue::Int64(Some(value)) => (i128::from(value), 0),
        value => return exec_err!("decimal division does not support {value:?}"),
    };
    Ok((BigInt::from(value), scale))
}

impl ScalarUDFImpl for DecimalDivide {
    fn name(&self) -> &str {
        "spark_decimal_divide"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [left, right] = arg_types else {
            return internal_err!("decimal division requires two arguments");
        };
        BinaryTypeCoercer::new(left, &Operator::Divide, right).get_result_type()
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Decimal128(precision, scale) = *args.return_field.data_type() else {
            // An input initially typed as decimal can widen to floating point
            // during analysis (for example through UNION).
            return make_scalar_function(|values| {
                let left = cast(&values[0], args.return_field.data_type())?;
                let right = cast(&values[1], args.return_field.data_type())?;
                Ok(div(&left, &right)?)
            })(&args.args);
        };
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut result = Vec::with_capacity(arrays[0].len());
        let bound = BigInt::from(10).pow(u32::from(precision));
        for index in 0..arrays[0].len() {
            let left = ScalarValue::try_from_array(&arrays[0], index)?;
            let right = ScalarValue::try_from_array(&arrays[1], index)?;
            if left.is_null() || right.is_null() {
                result.push(None);
                continue;
            }
            let (mut numerator, left_scale) = decimal(left)?;
            let (mut denominator, right_scale) = decimal(right)?;
            if denominator.is_zero() {
                if self.ansi_mode {
                    return exec_err!("Division by zero");
                }
                result.push(None);
                continue;
            }
            let shift = i32::from(scale) + i32::from(right_scale) - i32::from(left_scale);
            let factor = BigInt::from(10).pow(shift.unsigned_abs());
            if shift >= 0 {
                numerator *= factor;
            } else {
                denominator *= factor;
            }
            let mut quotient = &numerator / &denominator;
            let remainder = &numerator % &denominator;
            if remainder.abs() * 2 >= denominator.abs() {
                quotient += numerator.signum() * denominator.signum();
            }
            if quotient.abs() >= bound {
                if self.ansi_mode {
                    return exec_err!("Decimal division overflow");
                }
                result.push(None);
            } else {
                result.push(quotient.to_i128());
            }
        }
        let array =
            Arc::new(Decimal128Array::from(result).with_precision_and_scale(precision, scale)?);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                array.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}
