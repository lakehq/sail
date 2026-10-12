use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type, DecimalType, Field,
    FieldRef, Float32Type, Float64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

/// `CAST(decimal AS DOUBLE | FLOAT)`, rounding ONCE from the exact decimal value.
///
/// Spark's `Decimal.toDouble` / `toFloat` is `BigDecimal.doubleValue` / `floatValue`
/// (`Decimal.scala:245-247`): one correctly rounded step. Arrow's own kernel is
/// `unscaled as f64 / 10f64.powi(scale)`, which rounds the integer, rounds the power of
/// ten and rounds the quotient. A cast through a string is exact but allocates per row.
///
/// Three paths, each correctly rounded (nearest, ties to even):
/// 1. both operands exactly representable -> one IEEE division/multiplication;
/// 2. `0 <= scale <= 19` -> one `u128` division plus a manual round of the quotient;
/// 3. anything else -> parse `"{unscaled}e{-scale}"`, which Rust rounds correctly.
///
/// TODO: remove once Sail's arrow includes apache/arrow-rs#10509, which makes arrow's own
/// decimal -> float cast round once. Then drop this UDF, its codec entries and the
/// `Decimal32/64/128 -> Float` arm in `resolver/expression/cast.rs`; the `cast.feature`
/// scenarios stay as the proof. Keep it only if arrow's slow path is still measurably slower
/// (see apache/arrow-rs#11397).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastDecimalToFloat {
    signature: Signature,
    target: FloatTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FloatTarget {
    Float32,
    Float64,
}

impl SparkCastDecimalToFloat {
    pub fn new_float64() -> Self {
        Self::new(FloatTarget::Float64)
    }

    pub fn new_float32() -> Self {
        Self::new(FloatTarget::Float32)
    }

    fn new(target: FloatTarget) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
        }
    }

    fn output_type(&self) -> DataType {
        match self.target {
            FloatTarget::Float32 => DataType::Float32,
            FloatTarget::Float64 => DataType::Float64,
        }
    }
}

impl ScalarUDFImpl for SparkCastDecimalToFloat {
    fn name(&self) -> &str {
        match self.target {
            FloatTarget::Float32 => "spark_cast_decimal_to_float",
            FloatTarget::Float64 => "spark_cast_decimal_to_double",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called; return_field_from_args is used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.output_type(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target = self.target;
        let name = self.name().to_string();
        make_scalar_function(
            move |args: &[ArrayRef]| decimal_to_float(&name, target, args),
            vec![],
        )(&args.args)
    }
}

fn decimal_to_float(name: &str, target: FloatTarget, args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(name, (1, 1), args.len()));
    };
    match (array.data_type(), target) {
        (DataType::Decimal32(_, scale), FloatTarget::Float64) => {
            convert_f64(array.as_primitive::<Decimal32Type>(), *scale)
        }
        (DataType::Decimal64(_, scale), FloatTarget::Float64) => {
            convert_f64(array.as_primitive::<Decimal64Type>(), *scale)
        }
        (DataType::Decimal128(_, scale), FloatTarget::Float64) => {
            convert_f64(array.as_primitive::<Decimal128Type>(), *scale)
        }
        (DataType::Decimal32(_, scale), FloatTarget::Float32) => {
            convert_f32(array.as_primitive::<Decimal32Type>(), *scale)
        }
        (DataType::Decimal64(_, scale), FloatTarget::Float32) => {
            convert_f32(array.as_primitive::<Decimal64Type>(), *scale)
        }
        (DataType::Decimal128(_, scale), FloatTarget::Float32) => {
            convert_f32(array.as_primitive::<Decimal128Type>(), *scale)
        }
        (other, _) => Err(unsupported_data_type_exec_err(name, "DECIMAL", other)),
    }
}

fn convert_f64<T>(array: &PrimitiveArray<T>, scale: i8) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: ArrowNativeTypeOp + Into<i128>,
{
    let scale = i32::from(scale);
    let fast = exact_power_f64(scale);
    // One check per array: when every stored value is exact the whole array is one
    // IEEE operation per row, with no per-row `Result` to stop the loop vectorizing.
    if let Some((power, divide)) = fast
        && array
            .values()
            .iter()
            .all(|v| fits_exactly((*v).into(), F64_EXACT_INT_LIMIT))
    {
        let result = if divide {
            array.unary::<_, Float64Type>(|v| (v.into() as i64 as f64) / power)
        } else {
            array.unary::<_, Float64Type>(|v| (v.into() as i64 as f64) * power)
        };
        return Ok(Arc::new(result));
    }
    let result = array.try_unary::<_, Float64Type, ArrowError>(|v| {
        decimal_to_f64_with(v.into(), scale, fast)
            .ok_or_else(|| ArrowError::ComputeError("decimal to double".to_string()))
    })?;
    Ok(Arc::new(result))
}

fn convert_f32<T>(array: &PrimitiveArray<T>, scale: i8) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: ArrowNativeTypeOp + Into<i128>,
{
    let scale = i32::from(scale);
    let fast = exact_power_f32(scale);
    if let Some((power, divide)) = fast
        && array
            .values()
            .iter()
            .all(|v| fits_exactly((*v).into(), F32_EXACT_INT_LIMIT))
    {
        let result = if divide {
            array.unary::<_, Float32Type>(|v| (v.into() as i32 as f32) / power)
        } else {
            array.unary::<_, Float32Type>(|v| (v.into() as i32 as f32) * power)
        };
        return Ok(Arc::new(result));
    }
    let result = array.try_unary::<_, Float32Type, ArrowError>(|v| {
        decimal_to_f32_with(v.into(), scale, fast)
            .ok_or_else(|| ArrowError::ComputeError("decimal to float".to_string()))
    })?;
    Ok(Arc::new(result))
}

#[inline(always)]
fn fits_exactly(unscaled: i128, limit: u128) -> bool {
    unscaled.unsigned_abs() < limit
}

/// The scale is the same for the whole array, so look the power of ten up once.
/// `(power, divide)`: `divide` for `scale >= 0`, multiply for a negative scale.
fn exact_power_f64(scale: i32) -> Option<(f64, bool)> {
    if let Ok(index) = usize::try_from(scale) {
        POW10_F64.get(index).map(|power| (*power, true))
    } else {
        let index = usize::try_from(-i64::from(scale)).ok()?;
        POW10_F64.get(index).map(|power| (*power, false))
    }
}

fn exact_power_f32(scale: i32) -> Option<(f32, bool)> {
    if let Ok(index) = usize::try_from(scale) {
        POW10_F32.get(index).map(|power| (*power, true))
    } else {
        let index = usize::try_from(-i64::from(scale)).ok()?;
        POW10_F32.get(index).map(|power| (*power, false))
    }
}

/// `1e0..=1e22` as literals: every one is exact in `f64` (`5^22 < 2^53`). `powi` is not.
const POW10_F64: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];
/// `1e0..=1e10` as literals: every one is exact in `f32` (`5^10 < 2^24`).
const POW10_F32: [f32; 11] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];
/// `10^0..=10^19` fit in `u64`.
const POW10_U64: [u64; 20] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
];

const F64_EXACT_INT_LIMIT: u128 = 1 << 53;
const F32_EXACT_INT_LIMIT: u128 = 1 << 24;

#[cfg(test)]
fn decimal_to_f64(unscaled: i128, scale: i32) -> Option<f64> {
    decimal_to_f64_with(unscaled, scale, exact_power_f64(scale))
}

#[inline(always)]
fn decimal_to_f64_with(unscaled: i128, scale: i32, fast: Option<(f64, bool)>) -> Option<f64> {
    let magnitude = unscaled.unsigned_abs();
    if let Some((power, divide)) = fast
        && magnitude < F64_EXACT_INT_LIMIT
    {
        // The integer is exact (|v| < 2^53), so only the one operation below rounds.
        let value = unscaled as i64 as f64;
        return Some(if divide { value / power } else { value * power });
    }
    decimal_to_f64_slow(unscaled, magnitude, scale)
}

#[cold]
#[inline(never)]
fn decimal_to_f64_slow(unscaled: i128, magnitude: u128, scale: i32) -> Option<f64> {
    if let Ok(scale) = u32::try_from(scale)
        && let Some((mantissa, exponent)) = divide_rounded(magnitude, scale, 53)
    {
        let value = mantissa as f64 * f64::from_bits(((exponent + 1023) as u64) << 52);
        return Some(if unscaled < 0 { -value } else { value });
    }
    format!("{unscaled}e{}", -i64::from(scale)).parse().ok()
}

#[cfg(test)]
fn decimal_to_f32(unscaled: i128, scale: i32) -> Option<f32> {
    decimal_to_f32_with(unscaled, scale, exact_power_f32(scale))
}

#[inline(always)]
fn decimal_to_f32_with(unscaled: i128, scale: i32, fast: Option<(f32, bool)>) -> Option<f32> {
    let magnitude = unscaled.unsigned_abs();
    if let Some((power, divide)) = fast
        && magnitude < F32_EXACT_INT_LIMIT
    {
        let value = unscaled as i32 as f32;
        return Some(if divide { value / power } else { value * power });
    }
    decimal_to_f32_slow(unscaled, magnitude, scale)
}

#[cold]
#[inline(never)]
fn decimal_to_f32_slow(unscaled: i128, magnitude: u128, scale: i32) -> Option<f32> {
    if let Ok(scale) = u32::try_from(scale)
        && let Some((mantissa, exponent)) = divide_rounded(magnitude, scale, 24)
    {
        let value = mantissa as f32 * f32::from_bits(((exponent + 127) as u32) << 23);
        return Some(if unscaled < 0 { -value } else { value });
    }
    format!("{unscaled}e{}", -i64::from(scale)).parse().ok()
}

/// `magnitude / 10^scale` rounded to `bits` significant bits (nearest, ties to even), as
/// `(mantissa, exponent)` meaning `mantissa * 2^exponent`. `None` when the operands do not
/// fit the single-division scheme: `scale > 19`, zero, or a magnitude needing all 128 bits.
fn divide_rounded(magnitude: u128, scale: u32, bits: u32) -> Option<(u64, i32)> {
    let divisor = u128::from(*POW10_U64.get(usize::try_from(scale).ok()?)?);
    if magnitude == 0 {
        return None;
    }
    let width = 128 - magnitude.leading_zeros();
    if width > 127 {
        return None;
    }
    // Left-align to 127 bits: the quotient keeps >= 62 significant bits (the divisor
    // is < 2^64), enough for a 53-bit mantissa, the round bit and a sticky remainder.
    let left = 127 - width;
    let numerator = magnitude << left;
    let quotient = numerator / divisor;
    let remainder = numerator - quotient * divisor;
    let quotient_width = 128 - quotient.leading_zeros();
    let shift = quotient_width.checked_sub(bits)?;
    if shift == 0 {
        return None;
    }
    let mut mantissa = (quotient >> shift) as u64;
    let dropped = quotient & ((1u128 << shift) - 1);
    let half = 1u128 << (shift - 1);
    if dropped > half || (dropped == half && (remainder != 0 || mantissa & 1 == 1)) {
        mantissa += 1;
    }
    Some((mantissa, shift as i32 - left as i32))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn next(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// The reference is Rust's correctly rounded float parser on the exact decimal.
    #[test]
    fn matches_correctly_rounded_parse_on_every_path() -> Result<()> {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        for round in 0..400_000 {
            // Cover every magnitude from tiny to 126 bits, and every scale incl. negative.
            let width = 1 + next(&mut seed) % 127;
            let raw = ((u128::from(next(&mut seed)) << 64) | u128::from(next(&mut seed)))
                >> (128 - width);
            let unscaled = raw as i128 * if round % 2 == 0 { 1 } else { -1 };
            let scale = (next(&mut seed) % 62) as i32 - 12;
            let text = format!("{unscaled}e{}", -scale);
            let expected64: f64 = text
                .parse()
                .map_err(|_| datafusion_common::DataFusionError::Internal(text.clone()))?;
            let expected32: f32 = text
                .parse()
                .map_err(|_| datafusion_common::DataFusionError::Internal(text.clone()))?;
            assert_eq!(
                decimal_to_f64(unscaled, scale).map(f64::to_bits),
                Some(expected64.to_bits()),
                "f64 {text}"
            );
            assert_eq!(
                decimal_to_f32(unscaled, scale).map(f32::to_bits),
                Some(expected32.to_bits()),
                "f32 {text}"
            );
        }
        Ok(())
    }

    #[test]
    fn ties_round_to_even_and_a_sticky_remainder_rounds_up() {
        // 2^53 + 1 and 2^53 + 3 are exact ties between two doubles.
        assert_eq!(
            decimal_to_f64(9007199254740993, 0),
            Some(9007199254740992.0)
        );
        assert_eq!(
            decimal_to_f64(9007199254740995, 0),
            Some(9007199254740996.0)
        );
        // 2^60 + 128 is a tie; one extra fractional digit breaks it upward.
        assert_eq!(
            decimal_to_f64(1152921504606847104, 0),
            Some(1152921504606846976.0)
        );
        assert_eq!(
            decimal_to_f64(11529215046068471041, 1).map(f64::to_bits),
            Some(0x43b0_0000_0000_0001)
        );
        // 2^24 + 1 and 2^24 + 3 are exact ties between two floats.
        assert_eq!(decimal_to_f32(16777217, 0), Some(16777216.0));
        assert_eq!(decimal_to_f32(16777219, 0), Some(16777220.0));
    }

    /// The per-array fast loop and the general loop must agree row by row, with nulls and
    /// with one wide value forcing the whole array off the fast loop.
    #[test]
    fn array_paths_match_the_scalar_kernel() -> Result<()> {
        use datafusion::arrow::array::{Array, Decimal128Array, Float32Array, Float64Array};

        for values in [
            vec![
                Some(1234567_i128),
                None,
                Some(-9007199254740991),
                Some(0),
                Some(-5),
            ],
            vec![
                Some(1234567_i128),
                None,
                Some(123456789012345678900),
                Some(-5),
            ],
        ] {
            for scale in [-3_i8, 0, 2, 10, 22, 23, 38] {
                let array =
                    Decimal128Array::from(values.clone()).with_precision_and_scale(38, scale)?;
                let doubles = convert_f64(&array, scale)?;
                let floats = convert_f32(&array, scale)?;
                let doubles = doubles.as_any().downcast_ref::<Float64Array>();
                let floats = floats.as_any().downcast_ref::<Float32Array>();
                let (Some(doubles), Some(floats)) = (doubles, floats) else {
                    return datafusion_common::internal_err!("unexpected output type");
                };
                for (index, value) in values.iter().enumerate() {
                    let scale = i32::from(scale);
                    assert_eq!(
                        doubles
                            .is_valid(index)
                            .then(|| doubles.value(index).to_bits()),
                        value
                            .and_then(|v| decimal_to_f64(v, scale))
                            .map(f64::to_bits),
                        "f64 row {index} scale {scale}"
                    );
                    assert_eq!(
                        floats
                            .is_valid(index)
                            .then(|| floats.value(index).to_bits()),
                        value
                            .and_then(|v| decimal_to_f32(v, scale))
                            .map(f32::to_bits),
                        "f32 row {index} scale {scale}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn extremes_do_not_panic() {
        for scale in [-38, -1, 0, 1, 19, 20, 22, 23, 37, 38] {
            for value in [0, 1, -1, i128::MAX, i128::MIN, 10_i128.pow(38) - 1] {
                let _ = decimal_to_f64(value, scale);
                let _ = decimal_to_f32(value, scale);
            }
        }
    }
}
