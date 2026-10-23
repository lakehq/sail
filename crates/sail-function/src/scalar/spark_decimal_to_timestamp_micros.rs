use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, i256};
use datafusion_common::cast::as_decimal256_array;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

const MICROS_PER_SECOND: i128 = 1_000_000;

/// `CAST(decimal AS TIMESTAMP)`: the microseconds since the epoch, as an `Int64`.
///
/// Spark's `Cast.decimalToTimestamp` (Cast.scala:791-793) is
/// `(d.toBigDecimal * MICROS_PER_SECOND).longValue`. `BigDecimal.longValue` drops the fraction
/// (toward zero) and keeps only the low 64 bits of what is left, so an overflow WRAPS instead of
/// saturating or raising. There is no ANSI or TRY branch: CAST and TRY_CAST, ANSI on and off,
/// follow this one rule. Arrow's own decimal-to-timestamp cast raises on overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalToTimestampMicros {
    signature: Signature,
}

impl Default for SparkDecimalToTimestampMicros {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDecimalToTimestampMicros {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDecimalToTimestampMicros {
    fn name(&self) -> &str {
        "spark_decimal_to_timestamp_micros"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called; return_field_from_args is used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(decimal_to_timestamp_micros, vec![])(&args.args)
    }
}

/// The low 64 bits of `trunc(unscaled * 10^-scale * 10^6)`, as two's complement.
///
/// The product is exact in 256 bits for any Spark decimal (at most 38 digits). A wrapping
/// multiply is enough even beyond that: the low bits of a product do not depend on the high
/// ones. Rust's integer division truncates toward zero, as `BigDecimal.longValue` does.
fn decimal_to_micros(unscaled: i256, scale: i8) -> i64 {
    let ten = i256::from_i128(10);
    let numerator = unscaled.wrapping_mul(i256::from_i128(MICROS_PER_SECOND));
    let power = ten.wrapping_pow(u32::from(scale.unsigned_abs()));
    let micros = if scale >= 0 {
        numerator.checked_div(power).unwrap_or(i256::ZERO)
    } else {
        numerator.wrapping_mul(power)
    };
    let (low, _) = micros.to_parts();
    low as u64 as i64
}

fn decimal_to_timestamp_micros(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_decimal_to_timestamp_micros",
            (1, 1),
            args.len(),
        ));
    };
    let scale = match array.data_type() {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_decimal_to_timestamp_micros",
                "DECIMAL",
                other,
            ));
        }
    };
    // Widening to 256 bits keeps the scale and every digit.
    let widened = cast(array, &DataType::Decimal256(76, scale))?;
    let result: Int64Array = as_decimal256_array(&widened)?
        .iter()
        .map(|value| value.map(|value| decimal_to_micros(value, scale)))
        .collect();
    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Decimal128Array;

    use super::*;

    fn micros(unscaled: i128, scale: i8) -> i64 {
        decimal_to_micros(i256::from_i128(unscaled), scale)
    }

    #[test]
    fn test_values_measured_on_spark() {
        // No overflow, and the fraction drops toward zero.
        assert_eq!(micros(15, 1), 1_500_000);
        assert_eq!(micros(-15, 1), -1_500_000);
        assert_eq!(micros(19_999_999, 7), 1_999_999);
        assert_eq!(micros(-19_999_999, 7), -1_999_999);
        assert_eq!(micros(9, 7), 0);
        assert_eq!(micros(1_000_000_000_000_000_001, 18), 1_000_000);
        // Overflow wraps.
        assert_eq!(micros(9_223_372_036_854_775_807, 0), -1_000_000);
        assert_eq!(micros(9_223_372_036_854_775_808, 0), 0);
        assert_eq!(micros(-9_223_372_036_854_775_808, 0), 0);
        assert_eq!(micros(-9_223_372_036_854_775_809, 0), -1_000_000);
        assert_eq!(micros(18_446_744_073_709_551_616, 6), 0);
        // Decimals of 35 digits or more: the product is exact, not rounded to 34 digits.
        assert_eq!(
            micros(99_999_999_999_999_999_999_999_999_999_999_999_999, 0),
            80237960547581376
        );
        assert_eq!(
            micros(-99_999_999_999_999_999_999_999_999_999_999_999_999, 0),
            -80237960547581376
        );
        assert_eq!(
            micros(12_345_678_901_234_567_890_123_456_789_012_345_678, 0),
            9103449233449086848
        );
        assert_eq!(
            micros(10_000_000_000_000_000_000_000_000_000_000_000_005, 0),
            7386721425543678784
        );
        assert_eq!(
            micros(12_345_678_901_234_567_890_123_456_789_012_345, 0),
            -4657922801415745472
        );
        // The edge of the range: the last value that does not wrap, and the first that does.
        assert_eq!(micros(9_223_372_036_854_775_807, 6), i64::MAX);
        assert_eq!(micros(9_223_372_036_854_775_808, 6), i64::MIN);
    }

    #[test]
    fn test_a_null_stays_null_and_the_scale_is_read_from_the_type() -> Result<()> {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(15), None, Some(-15)])
                .with_precision_and_scale(5, 1)?,
        );
        let result = decimal_to_timestamp_micros(&[array])?;
        let result = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| datafusion_common::DataFusionError::Internal("Int64".to_string()))?;
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(1_500_000), None, Some(-1_500_000)]
        );
        Ok(())
    }
}
