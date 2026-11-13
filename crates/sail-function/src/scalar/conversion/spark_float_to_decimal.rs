use std::cell::Cell;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DecimalType, Field, FieldRef, Float32Type, Float64Type,
};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

/// Casts FLOAT and DOUBLE values to DECIMAL the way Spark does.
///
/// Spark builds the decimal from the string form of the double (`Double.toString`,
/// FLOAT values are widened to DOUBLE first) and rounds that string half up at the
/// target scale, so `0.145` becomes `0.15` although the nearest double is slightly
/// below `0.145`. Arrow multiplies the value by a power of ten in floating point and
/// rounds the product, which gives `0.14` here and adds noise to the low digits of
/// large values. NaN and infinity are NULL even in ANSI mode, as in Spark.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFloatToDecimal {
    signature: Signature,
    precision: u8,
    scale: i8,
    null_on_overflow: bool,
}

impl SparkFloatToDecimal {
    pub fn new(precision: u8, scale: i8, null_on_overflow: bool) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
            precision,
            scale,
            null_on_overflow,
        }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }

    pub fn null_on_overflow(&self) -> bool {
        self.null_on_overflow
    }

    fn cast(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [array] = args else {
            return Err(invalid_arg_count_exec_err(self.name(), (1, 1), args.len()));
        };
        let Some(&maximum) =
            Decimal128Type::MAX_FOR_EACH_PRECISION.get(usize::from(self.precision))
        else {
            return internal_err!("invalid decimal precision: {}", self.precision);
        };
        let values = match array.data_type() {
            DataType::Float64 => array.as_primitive::<Float64Type>().clone(),
            DataType::Float32 => array
                .as_primitive::<Float32Type>()
                .unary::<_, Float64Type>(f64::from),
            other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "FLOAT or DOUBLE",
                    other,
                ));
            }
        };
        let overflow = Cell::new(None);
        let output = values.unary_opt::<_, Decimal128Type>(|value| {
            if !value.is_finite() {
                return None;
            }
            let unscaled = float_to_decimal128(value, self.scale, maximum);
            if unscaled.is_none() && overflow.get().is_none() {
                overflow.set(Some(value));
            }
            unscaled
        });
        if let Some(value) = overflow.get()
            && !self.null_on_overflow
        {
            return exec_err!(
                "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] {value} cannot be represented as Decimal({}, {}). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return NULL instead.",
                self.precision,
                self.scale
            );
        }
        Ok(Arc::new(
            output.with_precision_and_scale(self.precision, self.scale)?,
        ))
    }
}

impl ScalarUDFImpl for SparkFloatToDecimal {
    fn name(&self) -> &str {
        "spark_float_to_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(self.precision, self.scale),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(|args| self.cast(args), vec![])(&args.args)
    }
}

/// Powers of ten that are exact in `f64`.
const POWERS_OF_TEN: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// Returns the unscaled value of the finite `value` at `scale`, rounded half up from
/// the shortest decimal string that parses back to `value`, or `None` if it is
/// greater than `maximum` in absolute value.
///
/// Java 19 and later print that string in `Double.toString`. Java 17 sometimes prints
/// more digits than needed (JDK-4511638), so for such values Spark on Java 17 may
/// give a result that differs in the low digits.
fn float_to_decimal128(value: f64, scale: i8, maximum: i128) -> Option<i128> {
    let magnitude = value.abs();
    let unscaled = round_scaled(magnitude, scale)
        .or_else(|| short_decimal(magnitude, scale))
        .or_else(|| round_shortest(magnitude, scale))?;
    if unscaled > maximum {
        return None;
    }
    Some(if value.is_sign_negative() {
        -unscaled
    } else {
        unscaled
    })
}

/// Rounds `magnitude * 10^scale` computed in `f64`, unless it is within a few ULPs
/// of a half.
///
/// The shortest string is within half an ULP of `magnitude`, so after scaling it is
/// within one ULP of the exact product, and the computed product is within half an
/// ULP of it. Away from a half, both round to the same integer.
fn round_scaled(magnitude: f64, scale: i8) -> Option<i128> {
    let factor = POWERS_OF_TEN.get(usize::try_from(scale).ok()?)?;
    let scaled = magnitude * factor;
    let floor = scaled.floor();
    let fraction = scaled - floor;
    // The margin reaches a half from 2^49 on, so larger products are declined.
    if (fraction - 0.5).abs() <= 4.0 * f64::EPSILON * scaled {
        return None;
    }
    // An infinite product saturates, which overflows every precision.
    #[expect(clippy::cast_possible_truncation)]
    let floor = floor as i128;
    Some(floor + i128::from(fraction > 0.5))
}

/// Returns the unscaled value if `magnitude` is a decimal with at most 15 significant
/// digits and at most 6 fractional digits that fit in `scale`, such as a price.
///
/// No two decimals of up to 15 significant digits parse to the same `f64`, so such a
/// decimal that parses back to `magnitude` is its shortest string, and fits `scale`
/// without rounding. The division is exact for the candidate and the power of ten.
fn short_decimal(magnitude: f64, scale: i8) -> Option<i128> {
    let digits = scale.clamp(0, 6);
    let factor = POWERS_OF_TEN[usize::try_from(digits).ok()?];
    let candidate = (magnitude * factor).round();
    if candidate >= 1e15 || candidate / factor != magnitude {
        return None;
    }
    #[expect(clippy::cast_possible_truncation)]
    let candidate = candidate as i128;
    candidate.checked_mul(10_i128.checked_pow(u32::try_from(scale - digits).ok()?)?)
}

/// Rounds the shortest decimal string of `magnitude` half up at `scale`.
fn round_shortest(magnitude: f64, scale: i8) -> Option<i128> {
    let mut buffer = ryu::Buffer::new();
    let text = buffer.format_finite(magnitude);
    let (mantissa, exponent) = match text.split_once('e') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (text, 0),
    };
    // At most 17 significant digits and a trailing zero, which fit in `u64`.
    let (mut digits, mut count, mut fraction, mut point) = (0_u64, 0_u32, 0_i32, false);
    for byte in mantissa.bytes() {
        if byte == b'.' {
            point = true;
        } else {
            digits = digits * 10 + u64::from(byte - b'0');
            count += u32::from(digits != 0);
            fraction += i32::from(point);
        }
    }
    // The value is `digits * 10^(exponent - fraction)`.
    let shift = exponent - fraction + i32::from(scale);
    if shift >= 0 {
        return i128::from(digits).checked_mul(10_i128.checked_pow(shift.unsigned_abs())?);
    }
    let dropped = shift.unsigned_abs();
    if dropped > count {
        return Some(0);
    }
    let divisor = 10_u64.pow(dropped);
    let round_up = digits / (divisor / 10) % 10 >= 5;
    Some(i128::from(digits / divisor + u64::from(round_up)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Decimal128Array, Float32Array, Float64Array};

    use super::*;

    fn cast(udf: &SparkFloatToDecimal, input: ArrayRef) -> Result<Vec<Option<i128>>> {
        let output = udf.cast(&[input])?;
        assert_eq!(
            output.data_type(),
            &DataType::Decimal128(udf.precision, udf.scale)
        );
        let Some(output) = output.as_any().downcast_ref::<Decimal128Array>() else {
            return internal_err!("expected Decimal128Array");
        };
        Ok(output.iter().collect())
    }

    #[test]
    fn test_double_rounds_half_up_from_its_decimal_string() -> Result<()> {
        let cases = [
            (0.145, 10, 2, Some(15)),
            (-0.145, 10, 2, Some(-15)),
            (0.125, 10, 2, Some(13)),
            (2.675, 10, 2, Some(268)),
            (1.005, 10, 2, Some(101)),
            (0.14755, 38, 4, Some(1476)),
            (0.00015, 38, 4, Some(2)),
            (1e-20, 38, 4, Some(0)),
            (1e-20, 38, 20, Some(1)),
            (5e-21, 38, 20, Some(1)),
            (4.9e-5, 10, 4, Some(0)),
            (5e-6, 10, 4, Some(0)),
            (-0.0, 10, 2, Some(0)),
            (1.25, 2, 1, Some(13)),
            (295161.0, 31, 15, Some(295_161_000_000_000_000_000)),
            (1.1e20, 38, 4, Some(1_100_000_000_000_000_000_000_000)),
            (1e37, 38, 0, Some(10_i128.pow(37))),
            (1e38, 38, 0, None),
            (f64::MAX, 38, 0, None),
            (9.994, 3, 2, Some(999)),
            (9.995, 3, 2, None),
            (-9.995, 3, 2, None),
            (0.95, 1, 1, None),
            (83.14, 4, 3, None),
            (3_588_193_230_991.656_2, 38, 4, Some(35_881_932_309_916_562)),
            (
                5_479_696_775_611_615.0,
                38,
                6,
                Some(5_479_696_775_611_615_000_000),
            ),
            (
                2_866_479_652_961_231.5,
                38,
                6,
                Some(2_866_479_652_961_231_500_000),
            ),
            (1e300, 38, 22, None),
        ];
        for (value, precision, scale, expected) in cases {
            let udf = SparkFloatToDecimal::new(precision, scale, true);
            let input = Arc::new(Float64Array::from(vec![value])) as ArrayRef;
            assert_eq!(cast(&udf, input)?, vec![expected], "value: {value:e}");
        }
        Ok(())
    }

    #[test]
    fn test_shortcuts_agree_with_the_shortest_string() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let unit = f64::from_bits(0x3ff0_0000_0000_0000 | (next() >> 12)) - 1.0;
            let exponent = i32::try_from(next() % 30).unwrap_or(0) - 12;
            let places = i32::try_from(next() % 7).unwrap_or(0);
            let price = (unit * 1e9).round() / 10_f64.powi(places);
            let wide = (1.0 + 9.0 * unit) * 10_f64.powi(exponent);
            let tie = ((unit * 1e6).round() + 0.5) / 10_f64.powi(places);
            let near = f64::from_bits((tie.to_bits() + next() % 5).saturating_sub(2));
            for magnitude in [unit, price, wide, tie, near] {
                for scale in 0..=20 {
                    let expected = round_shortest(magnitude, scale);
                    for shortcut in [round_scaled, short_decimal] {
                        if let Some(unscaled) = shortcut(magnitude, scale) {
                            assert_eq!(Some(unscaled), expected, "{magnitude:e} at scale {scale}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_float_is_widened_to_double_first() -> Result<()> {
        let udf = SparkFloatToDecimal::new(38, 18, true);
        let input = Arc::new(Float32Array::from(vec![0.1_f32])) as ArrayRef;
        assert_eq!(cast(&udf, input)?, vec![Some(100_000_001_490_116_120)]);
        let udf = SparkFloatToDecimal::new(10, 2, true);
        let input = Arc::new(Float32Array::from(vec![0.145_f32, 0.125_f32])) as ArrayRef;
        assert_eq!(cast(&udf, input)?, vec![Some(14), Some(13)]);
        Ok(())
    }

    #[test]
    fn test_non_finite_values_are_null_and_overflow_follows_the_mode() -> Result<()> {
        let values = vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ];
        for null_on_overflow in [true, false] {
            let udf = SparkFloatToDecimal::new(10, 2, null_on_overflow);
            let input = Arc::new(Float64Array::from(values.clone())) as ArrayRef;
            assert_eq!(cast(&udf, input)?, vec![None; 4]);
        }
        let udf = SparkFloatToDecimal::new(4, 3, false);
        let input = Arc::new(Float64Array::from(vec![1.0, 83.14])) as ArrayRef;
        match udf.cast(&[input]) {
            Ok(_) => internal_err!("expected an overflow error"),
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("83.14 cannot be represented as Decimal(4, 3)"),
                    "{e}"
                );
                Ok(())
            }
        }
    }
}
