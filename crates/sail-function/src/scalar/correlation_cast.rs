use std::sync::Arc;

use arrow::array::builder::Decimal128Builder;
use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Float64Array, new_null_array};
use arrow::compute::kernels::cast_utils::Parser;
use arrow::compute::{CastOptions, cast_with_options, parse_string_to_decimal_native};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Decimal64Type, Decimal128Type, DecimalType,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;

/// Native strict casts used by correlation keys, with their physical target field retained.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CorrelationCast {
    signature: Signature,
}

impl Default for CorrelationCast {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrelationCast {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    pub fn supports(from: &DataType, to: &DataType) -> bool {
        match (from, to) {
            (DataType::Float32, DataType::Float64) => true,
            (DataType::Float32 | DataType::Float64, DataType::Decimal128(p, s)) => {
                (1..=38).contains(p) && (0..=(*p as i8)).contains(s)
            }
            (
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64,
                DataType::Decimal128(p, s),
            ) => (1..=38).contains(p) && (0..=(*p as i8)).contains(s),
            (DataType::Decimal128(p, s), DataType::Float64) => {
                (1..=18).contains(p) && (0..=(*p as i8)).contains(s)
            }
            (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Float64) => true,
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Decimal128(p, s),
            ) => (1..=38).contains(p) && (0..=i16::from(*p)).contains(&i16::from(*s)),
            (DataType::Decimal128(p, s), DataType::Decimal128(q, t)) => {
                (1..=18).contains(p)
                    && (1..=18).contains(q)
                    && (0..=(*p as i8)).contains(s)
                    && (0..=(*q as i8)).contains(t)
                    && t < s
            }
            (DataType::Decimal128(_, 0..=18), DataType::Int32 | DataType::Int64) => true,
            (
                DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, zone),
                DataType::Date32,
            ) => zone
                .as_ref()
                .is_none_or(|zone| is_fixed_zero_offset_timezone(zone)),
            _ => false,
        }
    }
}

impl ScalarUDFImpl for CorrelationCast {
    fn name(&self) -> &str {
        "correlation_cast"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("physical correlation CAST retains its native target field")
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("correlation_cast expects one argument");
        };
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(cast_array(array, args.return_type())?))
            }
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &cast_array(&value.to_array()?, args.return_type())?,
                0,
            )?)),
        }
    }
}

fn cast_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if !CorrelationCast::supports(array.data_type(), target) {
        return internal_err!("unsupported physical correlation CAST");
    }
    match (array.data_type(), target) {
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Float64) => {
            Ok(cast_double(array)?)
        }
        (DataType::Decimal128(_, _), DataType::Float64) => {
            Ok(cast_decimal_double(array.as_primitive::<Decimal128Type>())?)
        }
        (DataType::Float32, DataType::Float64) => {
            if !array.is_empty() && array.null_count() == array.len() {
                return Ok(new_null_array(target, array.len()));
            }
            // Widening cannot fail; the native checked numeric loop need not visit validity bits.
            Ok(Arc::new(
                array
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float64Type>(f64::from),
            ))
        }
        (DataType::Float32, DataType::Decimal128(p, s)) => {
            floating_decimal(array.as_primitive::<Float32Type>(), *p, *s, f64::from)
        }
        (DataType::Float64, DataType::Decimal128(p, s)) => {
            floating_decimal(array.as_primitive::<Float64Type>(), *p, *s, |value| value)
        }
        (DataType::Int8, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<Int8Type>(), *p, *s)
        }
        (DataType::Int16, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<Int16Type>(), *p, *s)
        }
        (DataType::Int32, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<Int32Type>(), *p, *s)
        }
        (DataType::Int64, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<Int64Type>(), *p, *s)
        }
        (DataType::UInt8, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<UInt8Type>(), *p, *s)
        }
        (DataType::UInt16, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<UInt16Type>(), *p, *s)
        }
        (DataType::UInt32, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<UInt32Type>(), *p, *s)
        }
        (DataType::UInt64, DataType::Decimal128(p, s)) => {
            integral_decimal(array.as_primitive::<UInt64Type>(), *p, *s)
        }
        (DataType::Decimal128(_, _), DataType::Decimal128(p, s)) => Ok(downscale_decimal(
            array.as_primitive::<Decimal128Type>(),
            *p,
            *s,
        )?),
        (_, DataType::Decimal128(p, s)) => {
            let decimal = match array.data_type() {
                DataType::Utf8 => decimal_values(array.as_string::<i32>().iter(), *p, *s)?,
                DataType::LargeUtf8 => decimal_values(array.as_string::<i64>().iter(), *p, *s)?,
                DataType::Utf8View => decimal_values(array.as_string_view().iter(), *p, *s)?,
                _ => return internal_err!("expected string correlation CAST"),
            };
            Ok(Arc::new(decimal))
        }
        (DataType::Decimal128(_, 0), DataType::Int64) => {
            let array = array.as_primitive::<Decimal128Type>();
            if array.null_count() == 0
                && array
                    .values()
                    .iter()
                    .all(|value| i64::try_from(*value).is_ok())
            {
                return Ok(Arc::new(array.unary::<_, Int64Type>(|value| value as i64)));
            }
            Ok(Arc::new(array.try_unary::<_, Int64Type, ArrowError>(
                |value| {
                    i64::try_from(value).map_err(|_| {
                        ArrowError::CastError(format!("value of {value:?} is out of range Int64"))
                    })
                },
            )?))
        }
        (DataType::Decimal128(_, _), DataType::Int32 | DataType::Int64) => Ok(
            scaled_decimal_integer(array.as_primitive::<Decimal128Type>(), target)?,
        ),
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Date32) => Ok(Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .unary::<_, Date32Type>(|value| {
                    // The whole i64 microsecond range fits Date32. Floor is required before the epoch.
                    value.div_euclid(86_400_000_000) as i32
                }),
        )),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), DataType::Date32) => Ok(Arc::new(
            array
                .as_primitive::<TimestampNanosecondType>()
                .unary::<_, Date32Type>(|value| value.div_euclid(86_400_000_000_000) as i32),
        )),
        _ => internal_err!("unsupported physical correlation CAST"),
    }
}

fn native_numeric(array: &dyn Array, target: &DataType) -> Result<ArrayRef> {
    Ok(cast_with_options(
        array,
        target,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?)
}

fn integral_decimal<T>(
    array: &arrow::array::PrimitiveArray<T>,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let target = DataType::Decimal128(precision, scale);
    // Native fallible casts skip invalid slots and choose the first native error.
    if array.null_count() != 0 {
        return native_numeric(array, &target);
    }
    let multiplier = 10_i128.pow(scale as u32);
    let limit = Decimal128Type::MAX_FOR_EACH_PRECISION[usize::from(precision)] / multiplier;
    let in_range = |value: T::Native| {
        let value = value.into();
        (-limit..=limit).contains(&value)
    };
    if !array.values().iter().copied().all(in_range) {
        return native_numeric(array, &target);
    }
    Ok(Arc::new(
        array
            .unary::<_, Decimal128Type>(|value| value.into() * multiplier)
            .with_precision_and_scale(precision, scale)?,
    ))
}

fn floating_decimal<T, F>(
    array: &arrow::array::PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    widen: F,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> f64,
{
    let target = DataType::Decimal128(precision, scale);
    if array.null_count() != 0 {
        return native_numeric(array, &target);
    }
    let maximum = Decimal128Type::MAX_FOR_EACH_PRECISION[usize::from(precision)];
    let mut bound = maximum as f64;
    if bound as i128 > maximum {
        bound = bound.next_down();
    }
    let multiplier = 10_f64.powi(i32::from(scale));
    let mut values = Vec::with_capacity(array.len());
    for &value in array.values() {
        let rounded = (multiplier * widen(value)).round();
        if rounded
            .abs()
            .partial_cmp(&bound)
            .is_none_or(|order| order.is_gt())
        {
            // This also catches NaN/infinity; preserve native error text and priority.
            return native_numeric(array, &target);
        }
        values.push(integral_float_i128(rounded));
    }
    Ok(Arc::new(
        Decimal128Array::new(values.into(), None).with_precision_and_scale(precision, scale)?,
    ))
}

#[inline]
fn integral_float_i128(value: f64) -> i128 {
    if value.abs() < 9_223_372_036_854_775_808.0 {
        return i128::from(value as i64);
    }
    // The caller proved a finite integer within decimal precision <= 38.
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32 - 1023;
    let significand = (bits & ((1_u64 << 52) - 1)) | (1_u64 << 52);
    let magnitude = i128::from(significand) << (exponent - 52);
    if value.is_sign_negative() {
        -magnitude
    } else {
        magnitude
    }
}

fn cast_decimal_double(array: &Decimal128Array) -> std::result::Result<ArrayRef, ArrowError> {
    let outside = |value: i128| i64::try_from(value).is_err();
    // Check raw null payloads too: native unary evaluates every value slot.
    if array.values().first().is_some_and(|value| outside(*value))
        || array
            .values()
            .iter()
            .fold(false, |found, value| found | outside(*value))
    {
        return cast_with_options(
            array,
            &DataType::Float64,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        );
    }
    let divisor = 10_f64.powi(i32::from(array.scale()));
    Ok(Arc::new(array.unary::<_, Float64Type>(|value| {
        (value as i64) as f64 / divisor
    })))
}

fn downscale_decimal(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
) -> std::result::Result<ArrayRef, ArrowError> {
    let target = DataType::Decimal128(precision, scale);
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    if array.null_count() == array.len() {
        return cast_with_options(array, &target, &options);
    }
    let outside = |value: i128| i64::try_from(value).is_err();
    // Native trusted-precision casts visit null payloads as well as valid values.
    if array.values().first().is_some_and(|value| outside(*value))
        || array
            .values()
            .iter()
            .fold(false, |found, value| found | outside(*value))
    {
        return cast_with_options(array, &target, &options);
    }
    if array.scale() - scale == 1 {
        downscale_values::<true>(array, precision, scale)
    } else {
        downscale_values::<false>(array, precision, scale)
    }
}

fn downscale_values<const DIVIDE_BY_TEN: bool>(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
) -> std::result::Result<ArrayRef, ArrowError> {
    let delta = array.scale() - scale;
    let divisor = if DIVIDE_BY_TEN {
        10
    } else {
        Decimal64Type::MAX_FOR_EACH_PRECISION[delta as usize] + 1
    };
    let half = divisor / 2;
    let infallible = (array.precision() as i8) - delta < precision as i8;
    let max = i128::from(Decimal64Type::MAX_FOR_EACH_PRECISION[usize::from(precision)]);
    let downscale = |value: i128| {
        let value = value as i64;
        let quotient = if DIVIDE_BY_TEN {
            value / 10
        } else {
            value / divisor
        };
        let remainder = if DIVIDE_BY_TEN {
            value % 10
        } else {
            value % divisor
        };
        // Positive divisor >= 10 keeps the rounded quotient within i64.
        i128::from(match value >= 0 {
            true if remainder >= half => quotient + 1,
            false if remainder <= -half => quotient - 1,
            _ => quotient,
        })
    };
    // Preserve native metadata trust, null evaluation and first-error order.
    let output = if infallible {
        array.unary::<_, Decimal128Type>(downscale)
    } else {
        array.try_unary::<_, Decimal128Type, ArrowError>(|value| {
            let adjusted = downscale(value);
            if adjusted < -max || adjusted > max {
                Decimal128Type::validate_decimal_precision(adjusted, precision, scale)?;
            }
            Ok(adjusted)
        })?
    };
    Ok(Arc::new(output.with_precision_and_scale(precision, scale)?))
}

fn scaled_decimal_integer(
    array: &Decimal128Array,
    target: &DataType,
) -> std::result::Result<ArrayRef, ArrowError> {
    let scale = array.scale();
    if (0..=18).contains(&scale)
        && array
            .values()
            .iter()
            .all(|value| i64::try_from(*value).is_ok())
    {
        let divisor = 10_i64.pow(scale as u32);
        match target {
            DataType::Int64 => {
                return Ok(Arc::new(
                    array.unary::<_, Int64Type>(|value| (value as i64) / divisor),
                ));
            }
            DataType::Int32 => {
                return Ok(Arc::new(array.try_unary::<_, Int32Type, ArrowError>(
                    |value| {
                        let value = (value as i64) / divisor;
                        i32::try_from(value).map_err(|_| {
                            ArrowError::CastError(format!(
                                "value of {value:?} is out of range Int32"
                            ))
                        })
                    },
                )?));
            }
            _ => {}
        }
    }
    cast_with_options(
        array,
        target,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
}

// Arrow uses the same standard float grammar; retain its parser as the fallback.
fn parse_double(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .or_else(|| value.trim_ascii().parse::<f64>().ok())
        .or_else(|| Float64Type::parse(value))
}

fn cast_double(array: &ArrayRef) -> std::result::Result<ArrayRef, ArrowError> {
    fn values<'a>(
        rows: impl Iterator<Item = Option<&'a str>>,
    ) -> std::result::Result<Vec<f64>, ArrowError> {
        rows.map(|value| match value {
            None => Ok(0.0),
            Some(value) => parse_double(value).ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Cannot cast string '{value}' to value of Float64 type"
                ))
            }),
        })
        .collect()
    }
    let values = match array.data_type() {
        DataType::Utf8 => values(array.as_string::<i32>().iter())?,
        DataType::LargeUtf8 => values(array.as_string::<i64>().iter())?,
        DataType::Utf8View => values(array.as_string_view().iter())?,
        _ => return Err(ArrowError::CastError("expected string input".into())),
    };
    Ok(Arc::new(Float64Array::try_new(
        values.into(),
        array.nulls().cloned(),
    )?))
}

fn decimal_values<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    precision: u8,
    scale: i8,
) -> std::result::Result<Decimal128Array, ArrowError> {
    let factor = 10_i128.pow(scale as u32);
    // The supported precision and scale were checked before dispatch.
    let maximum = Decimal128Type::MAX_FOR_EACH_PRECISION[usize::from(precision)];
    let minimum = -maximum;
    let mut builder = Decimal128Builder::with_capacity(values.size_hint().0);
    for value in values {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        // Skip integer parsing for fractions; keep native rounding and error behavior.
        let integer = (value.len() <= 39 && !value.contains('.'))
            .then(|| {
                (if value
                    .as_bytes()
                    .first()
                    .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-'))
                    && value.as_bytes().last().is_some_and(u8::is_ascii_digit)
                {
                    value
                } else {
                    value.trim()
                })
                .parse::<i128>()
                .ok()
            })
            .flatten()
            .and_then(|v| v.checked_mul(factor));
        let parsed = match integer {
            Some(value) => value,
            None => parse_string_to_decimal_native::<Decimal128Type>(value, scale as usize)
                .map_err(|_| {
                    ArrowError::CastError(format!(
                        "Cannot cast string '{value}' to value of {} type",
                        Decimal128Type::DATA_TYPE
                    ))
                })?,
        };
        if parsed < minimum || parsed > maximum {
            // Retain native first-row error text and priority.
            Decimal128Type::validate_decimal_precision(parsed, precision, scale)?;
        }
        builder.append_value(parsed);
    }
    builder.finish().with_precision_and_scale(precision, scale)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, Date32Array, Decimal128Array, Int64Array, LargeStringArray, StringArray,
        StringViewArray,
    };
    use datafusion::arrow::compute::{CastOptions, cast_with_options};
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::common::DataFusionError;

    fn native(a: &ArrayRef, t: &DataType) -> Result<ArrayRef, DataFusionError> {
        cast_with_options(
            a,
            t,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(Into::into)
    }
    fn decimal(a: &ArrayRef, p: u8, s: i8) -> Result<ArrayRef, DataFusionError> {
        super::cast_array(a, &DataType::Decimal128(p, s))
    }
    fn integer(a: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
        super::cast_array(a, &DataType::Int64)
    }
    fn date(a: &ArrayRef) -> ArrayRef {
        super::cast_array(a, &DataType::Date32).unwrap()
    }
    fn exact(a: Result<ArrayRef, DataFusionError>, b: Result<ArrayRef, DataFusionError>) {
        match (a, b) {
            (Ok(a), Ok(b)) => assert_eq!(a.to_data(), b.to_data()),
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            x => panic!("mismatch {x:?}"),
        }
    }

    #[test]
    fn numeric_decimal_casts_preserve_small_and_wide_native_results() {
        use arrow::array::{Float32Array, Float64Array, Int32Array};
        let floats = [
            0.0, -0.0, 1.005, -1.005, 1.2345, -1.2345, 100_000.0, -100_000.0,
        ];
        for (precision, scale) in [(12, 2), (18, 0), (30, 15), (38, 20), (38, 38)] {
            let target = DataType::Decimal128(precision, scale);
            for array in [
                Arc::new(Float64Array::from(floats.to_vec())) as ArrayRef,
                Arc::new(Float32Array::from(floats.map(|v| v as f32).to_vec())) as ArrayRef,
            ] {
                for i in 0..array.len() {
                    let item = array.slice(i, 1);
                    exact(super::cast_array(&item, &target), native(&item, &target));
                }
                exact(super::cast_array(&array, &target), native(&array, &target));
            }
        }
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, -1, 999_999_999, -999_999_999]));
        let target = DataType::Decimal128(9, 0);
        exact(super::cast_array(&ints, &target), native(&ints, &target));
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![
            0,
            1,
            -1,
            (1_i64 << 53) + 1,
            999_999_999_999_999_999,
            -999_999_999_999_999_999,
        ]));
        let target = DataType::Decimal128(38, 20);
        exact(super::cast_array(&longs, &target), native(&longs, &target));
    }

    #[test]
    fn numeric_decimal_errors_nulls_and_precision_boundaries_match_native() {
        use arrow::array::{Float32Array, Float64Array, Int32Array};
        use arrow::datatypes::{Decimal128Type, DecimalType};
        for precision in 1..=38 {
            let bound = Decimal128Type::MAX_FOR_EACH_PRECISION[usize::from(precision)] as f64;
            let target = DataType::Decimal128(precision, 0);
            for value in [
                bound.next_down(),
                bound,
                bound.next_up(),
                -bound.next_down(),
                -bound,
                -bound.next_up(),
            ] {
                let array: ArrayRef = Arc::new(Float64Array::from(vec![value]));
                exact(super::cast_array(&array, &target), native(&array, &target));
            }
        }
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(1.25),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(-f64::INFINITY),
            Some(f64::MAX),
        ]));
        let f32s: ArrayRef = Arc::new(Float32Array::from(vec![None, Some(1.25), Some(f32::NAN)]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(1),
            Some(1_000_000_000),
            Some(i32::MIN),
        ]));
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(1),
            Some(1_000_000_000_000_000_000),
            Some(i64::MIN),
        ]));
        for (array, target) in [
            (floats, DataType::Decimal128(30, 15)),
            (f32s, DataType::Decimal128(12, 2)),
            (ints, DataType::Decimal128(9, 0)),
            (longs, DataType::Decimal128(38, 20)),
        ] {
            for i in 0..array.len() {
                let item = array.slice(i, 1);
                exact(super::cast_array(&item, &target), native(&item, &target));
            }
            exact(super::cast_array(&array, &target), native(&array, &target));
        }
    }
    fn native_valid_numeric_rows(array: &ArrayRef, target: &DataType) -> ArrayRef {
        let oracle = cast_with_options(
            array,
            target,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )
        .unwrap();
        let valid = arrow::array::BooleanArray::from(
            (0..array.len())
                .map(|index| oracle.is_valid(index))
                .collect::<Vec<_>>(),
        );
        let result = arrow::compute::filter(array.as_ref(), &valid).unwrap();
        assert_eq!(result.null_count(), 0);
        result
    }

    #[test]
    fn numeric_decimal_valid_f64_exponents_and_mantissas_match_native() {
        use arrow::array::Float64Array;

        let mut bits = Vec::new();
        for exponent in 0_u64..2047 {
            for mantissa in [
                0,
                1,
                0x7_ffff_ffff_ffff,
                0x8_0000_0000_0000,
                0xf_ffff_ffff_fffe,
                0xf_ffff_ffff_ffff,
            ] {
                for sign in [0, 1_u64 << 63] {
                    bits.push(sign | (exponent << 52) | mantissa);
                }
            }
        }
        let mut state = 0x0028_4329_2026_1029_u64;
        for _ in 0..4096 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let exponent = (state >> 52) % 2047;
            bits.push((state & 0x800f_ffff_ffff_ffff) | (exponent << 52));
        }
        for value in [1.005_f64, -1.005, 2.675, -2.675, 0.005, -0.005] {
            bits.extend([
                value.next_down().to_bits(),
                value.to_bits(),
                value.next_up().to_bits(),
            ]);
        }
        bits.sort_unstable();
        bits.dedup();
        let values = bits.into_iter().map(f64::from_bits).collect::<Vec<_>>();
        assert!(values.iter().all(|value| value.is_finite()));
        let source: ArrayRef = Arc::new(Float64Array::from(values));
        for (precision, scale) in [
            (9, 0),
            (12, 2),
            (18, 0),
            (19, 0),
            (30, 15),
            (38, 0),
            (38, 20),
            (38, 38),
        ] {
            let target = DataType::Decimal128(precision, scale);
            let valid = native_valid_numeric_rows(&source, &target);
            assert!(valid.len() > 1000);
            let expected = native(&valid, &target).unwrap();
            if target == DataType::Decimal128(38, 0) {
                let expected = expected.as_any().downcast_ref::<Decimal128Array>().unwrap();
                assert!(
                    expected
                        .values()
                        .iter()
                        .filter(|value| value.unsigned_abs() >= (1_u128 << 63))
                        .count()
                        > 100
                );
            }
            exact(super::cast_array(&valid, &target), Ok(expected));
            let slice = valid.slice(1, valid.len() - 2);
            exact(super::cast_array(&slice, &target), native(&slice, &target));
            let empty = valid.slice(1, 0);
            exact(super::cast_array(&empty, &target), native(&empty, &target));
        }
    }

    #[test]
    fn numeric_decimal_small_integer_domains_match_native() {
        use arrow::array::{Int8Array, Int16Array};

        let sources: [ArrayRef; 2] = [
            Arc::new(Int8Array::from((i8::MIN..=i8::MAX).collect::<Vec<_>>())),
            Arc::new(Int16Array::from((i16::MIN..=i16::MAX).collect::<Vec<_>>())),
        ];
        for source in sources {
            for (precision, scale) in [(2, 0), (3, 0), (5, 0), (9, 2), (38, 20), (38, 38)] {
                let target = DataType::Decimal128(precision, scale);
                let valid = native_valid_numeric_rows(&source, &target);
                assert!(!valid.is_empty());
                exact(super::cast_array(&valid, &target), native(&valid, &target));
                let slice = valid.slice(1, valid.len() - 1);
                exact(super::cast_array(&slice, &target), native(&slice, &target));
                // Keep exact native diagnostics for the same unfiltered boundary inputs.
                exact(
                    super::cast_array(&source, &target),
                    native(&source, &target),
                );
            }
        }
    }

    #[test]
    fn matches_native_decimal_errors_and_utc_day_arithmetic() {
        let values = [
            None,
            Some("0"),
            Some("-0"),
            Some("+0"),
            Some("1"),
            Some("-1"),
            Some("+1"),
            Some("001"),
            Some("-001"),
            Some("+001"),
            Some("\u{2003}42\u{2003}"),
            Some("\t-42\n"),
            Some(".5"),
            Some("1."),
            Some("1.235"),
            Some("-1.235"),
            Some("9.995"),
            Some("99.999"),
            Some("1e2"),
            Some("1E-2"),
            Some("NaN"),
            Some("inf"),
            Some("+"),
            Some("-"),
            Some(""),
            Some("."),
            Some("1..2"),
            Some("1 2"),
            Some("1_2"),
            Some("٠"),
            Some("9223372036854775808"),
            Some("-9223372036854775809"),
            Some("99999999999999999999999999999999999999"),
            Some("100000000000000000000000000000000000000"),
            Some("170141183460469231731687303715884105727"),
            Some("-170141183460469231731687303715884105728"),
            Some("0000000000000000000000000000000000000000000000000000001"),
        ];
        let mut decimal_cases = 0;
        for precision in [1, 9, 19, 38] {
            for scale in [0, 1, precision as i8] {
                let target = DataType::Decimal128(precision, scale);
                for value in values {
                    for a in [
                        Arc::new(StringArray::from(vec![None, value, None])) as ArrayRef,
                        Arc::new(LargeStringArray::from(vec![None, value, None])) as ArrayRef,
                        Arc::new(StringViewArray::from(vec![None, value, None])) as ArrayRef,
                    ] {
                        for a in [Arc::clone(&a), a.slice(1, 2), a.slice(0, 2)] {
                            exact(decimal(&a, precision, scale), native(&a, &target));
                            decimal_cases += 1;
                        }
                    }
                }
            }
        }
        for precision in [1, 4, 12, 19, 38] {
            for scale in [0, 1, precision as i8] {
                for layout in 0..3 {
                    for late in [0, 1, 127, 4095] {
                        for unusual in [
                            "1.235",
                            "1e2",
                            "bad",
                            "99999999999999999999999999999999999999999999999999",
                        ] {
                            let mut rows = vec![Some("42"); 4096];
                            rows[late] = Some(unusual);
                            rows[4094] = Some("invalid_later");
                            rows[17] = None;
                            let a: ArrayRef = match layout {
                                0 => Arc::new(StringArray::from(rows)),
                                1 => Arc::new(LargeStringArray::from(rows)),
                                _ => Arc::new(StringViewArray::from(rows)),
                            };
                            exact(
                                decimal(&a, precision, scale),
                                native(&a, &DataType::Decimal128(precision, scale)),
                            );
                            decimal_cases += 1;
                        }
                    }
                }
            }
        }
        let mut integer_cases = 0;
        let bounds = [
            i128::MIN,
            i64::MIN as i128 - 1,
            i64::MIN as i128,
            -1,
            0,
            1,
            i64::MAX as i128,
            i64::MAX as i128 + 1,
            i128::MAX,
        ];
        for precision in 1..=38 {
            for value in bounds {
                let nulls = Int64Array::from(vec![None, Some(1), None]).nulls().cloned();
                let a: ArrayRef = Arc::new(
                    Decimal128Array::new(vec![i128::MIN, value, i128::MAX].into(), nulls)
                        .with_precision_and_scale(precision, 0)
                        .unwrap(),
                );
                for a in [Arc::clone(&a), a.slice(1, 1), a.slice(1, 2), a.slice(0, 2)] {
                    exact(integer(&a), native(&a, &DataType::Int64));
                    integer_cases += 1;
                }
            }
        }
        for rows in [
            vec![
                Some(i64::MIN as i128),
                Some(-1),
                Some(0),
                Some(i64::MAX as i128),
            ],
            vec![Some(i64::MAX as i128 + 1), Some(i64::MIN as i128 - 1)],
            vec![None, Some(i64::MIN as i128 - 1), Some(i64::MAX as i128 + 1)],
            vec![],
            vec![None, None],
        ] {
            let a: ArrayRef = Arc::new(
                Decimal128Array::from(rows)
                    .with_precision_and_scale(19, 0)
                    .unwrap(),
            );
            exact(integer(&a), native(&a, &DataType::Int64));
            integer_cases += 1;
        }
        let mut date_cases = 0;
        let mut date_equal = 0;
        let mut date_improvement = 0;
        let times = [
            i64::MIN,
            i64::MIN + 1,
            -8_334_601_228_800_000_001,
            -8_334_601_228_800_000_000,
            -86_400_000_000_001,
            -86_400_000_000_000,
            -86_400_000_000 + 1,
            -1_001,
            -1_000,
            -999,
            -1,
            0,
            1,
            999,
            1000,
            1001,
            86_400_000_000 - 1,
            86_400_000_000,
            86_400_000_000 + 1,
            8_210_266_876_799_999_999,
            8_210_266_876_800_000_000,
            i64::MAX - 1,
            i64::MAX,
        ];
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            for zone in [
                None,
                Some("UTC"),
                Some("Etc/UTC"),
                Some("+00:00"),
                Some("-00:00"),
                Some("Z"),
                Some("UT"),
            ] {
                for value in times {
                    let integer: ArrayRef =
                        Arc::new(Int64Array::from(vec![None, Some(value), None]));
                    let a =
                        native(&integer, &DataType::Timestamp(unit, zone.map(Into::into))).unwrap();
                    for a in [Arc::clone(&a), a.slice(1, 2), a.slice(0, 2)] {
                        let actual = date(&a);
                        let want = value.div_euclid(if unit == TimeUnit::Microsecond {
                            86_400_000_000
                        } else {
                            86_400_000_000_000
                        }) as i32;
                        for v in actual
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap()
                            .iter()
                            .flatten()
                        {
                            assert_eq!(v, want);
                        }
                        match native(&a, &DataType::Date32) {
                            Ok(expected) => {
                                assert_eq!(actual.to_data(), expected.to_data());
                                date_equal += 1;
                            }
                            Err(e) => {
                                assert!(
                                    e.to_string().contains("Cannot convert")
                                        || e.to_string().contains("Invalid timezone")
                                );
                                date_improvement += 1;
                            }
                        }
                        date_cases += 1;
                    }
                }
            }
        }
        println!(
            "PASS decimal string exact output/error cases={decimal_cases}; scale0 integer exact output/error cases={integer_cases}; timestamp-date cases={date_cases}, old-domain exact={date_equal}, corrected Chrono/alias errors={date_improvement}"
        );
    }
    #[test]
    fn scalar_timestamp_cast_keeps_shape_and_uses_floor_for_the_full_range() -> super::Result<()> {
        use arrow::datatypes::Field;
        use datafusion_common::ScalarValue;
        use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
        let function = super::CorrelationCast::new();
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            for zone in [None, Some("UTC"), Some("Z")] {
                for value in [None, Some(i64::MIN), Some(-1), Some(0), Some(i64::MAX)] {
                    let data_type = DataType::Timestamp(unit, zone.map(Into::into));
                    let scalar = match unit {
                        TimeUnit::Microsecond => {
                            ScalarValue::TimestampMicrosecond(value, zone.map(Into::into))
                        }
                        _ => ScalarValue::TimestampNanosecond(value, zone.map(Into::into)),
                    };
                    let actual = function.invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Scalar(scalar)],
                        arg_fields: vec![Arc::new(Field::new("raw", data_type, true))],
                        number_rows: 1,
                        return_field: Arc::new(Field::new("day", DataType::Date32, true)),
                        config_options: Arc::default(),
                    })?;
                    let divisor = if unit == TimeUnit::Microsecond {
                        86_400_000_000
                    } else {
                        86_400_000_000_000
                    };
                    let expected = ScalarValue::Date32(value.map(|v| v.div_euclid(divisor) as i32));
                    assert!(matches!(actual, ColumnarValue::Scalar(value) if value == expected));
                }
            }
        }
        Ok(())
    }
    #[test]
    fn widening_preserves_native_nan_bits_zeros_and_sliced_validity() -> super::Result<()> {
        use arrow::array::{AsArray, Float32Array};
        use arrow::datatypes::Float64Type;
        let values = [
            0,
            0x8000_0000,
            1,
            0x8000_0001,
            0x007f_ffff,
            0x0080_0000,
            0x7f7f_ffff,
            0xff7f_ffff,
            0x7f80_0000,
            0xff80_0000,
            0x7f80_0001,
            0x7fc0_0001,
            0xff80_0001,
            0xffc0_0001,
            0x7fff_ffff,
        ];
        for nullable in [false, true] {
            let nulls = nullable
                .then(|| {
                    Float32Array::from(
                        (0..values.len())
                            .map(|i| (i % 3 != 0).then_some(0.0))
                            .collect::<Vec<_>>(),
                    )
                    .nulls()
                    .cloned()
                })
                .flatten();
            let array: ArrayRef = Arc::new(Float32Array::new(
                values
                    .into_iter()
                    .map(f32::from_bits)
                    .collect::<Vec<_>>()
                    .into(),
                nulls,
            ));
            for array in [Arc::clone(&array), array.slice(1, 11), array.slice(0, 0)] {
                let actual = super::cast_array(&array, &DataType::Float64)?;
                let expected = native(&array, &DataType::Float64)?;
                assert_eq!(actual.nulls(), expected.nulls());
                for i in 0..array.len() {
                    if array.is_valid(i) {
                        assert_eq!(
                            actual.as_primitive::<Float64Type>().value(i).to_bits(),
                            expected.as_primitive::<Float64Type>().value(i).to_bits()
                        );
                    }
                }
            }
        }
        let nulls: ArrayRef = Arc::new(Float32Array::new_null(8192));
        exact(
            super::cast_array(&nulls, &DataType::Float64),
            native(&nulls, &DataType::Float64),
        );
        Ok(())
    }
    #[test]
    fn scaled_decimal_integer_casts_keep_native_values_nulls_and_errors() -> super::Result<()> {
        for scale in [0, 1, 2, 18] {
            for value in [
                i128::MIN,
                i64::MIN as i128 - 1,
                i64::MIN as i128,
                i32::MIN as i128 * 100 - 99,
                -101,
                -99,
                -1,
                0,
                1,
                99,
                101,
                i32::MAX as i128 * 100 + 99,
                i64::MAX as i128,
                i64::MAX as i128 + 1,
                i128::MAX,
            ] {
                for mode in 0..3 {
                    let nulls = (mode != 0)
                        .then(|| Int64Array::from(vec![None, Some(0), None]).nulls().cloned())
                        .flatten();
                    let values = if mode == 2 {
                        vec![i128::MIN, value, i128::MAX]
                    } else {
                        vec![0, value, 0]
                    };
                    let array: ArrayRef = Arc::new(
                        Decimal128Array::new(values.into(), nulls)
                            .with_precision_and_scale(38, scale)?,
                    );
                    for array in [Arc::clone(&array), array.slice(1, 1), array.slice(0, 0)] {
                        for target in [DataType::Int32, DataType::Int64] {
                            exact(super::cast_array(&array, &target), native(&array, &target));
                        }
                    }
                }
            }
        }
        for scale in [-1, 19, 38] {
            for target in [DataType::Int32, DataType::Int64] {
                assert!(!super::CorrelationCast::supports(
                    &DataType::Decimal128(38, scale),
                    &target
                ));
            }
        }
        Ok(())
    }
    #[test]
    fn double_parser_matches_native_bits_ascii_whitespace_and_errors() -> super::Result<()> {
        let values = [
            None,
            Some("0"),
            Some("-0"),
            Some("+0"),
            Some("-0.0"),
            Some("1.25"),
            Some("1e309"),
            Some("-1e-9999"),
            Some("nan"),
            Some("-NaN"),
            Some("INF"),
            Some("-Infinity"),
            Some("\t -1.5 \n"),
            Some("\u{2003}1.0"),
            Some("0x1p0"),
            Some("1_000"),
            Some(""),
            Some("bad"),
            Some("1.00000000000000011102230246251565404236316680908203125"),
        ];
        for value in values {
            for array in [
                Arc::new(StringArray::from(vec![None, value, None])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec![None, value, None])) as ArrayRef,
                Arc::new(StringViewArray::from(vec![None, value, None])) as ArrayRef,
            ] {
                for array in [Arc::clone(&array), array.slice(1, 2), array.slice(0, 0)] {
                    exact(
                        super::cast_array(&array, &DataType::Float64),
                        native(&array, &DataType::Float64),
                    );
                }
            }
        }
        for values in [
            vec![Some("invalid first"), Some("invalid last")],
            vec![None, Some("1"), Some("invalid last")],
        ] {
            let array: ArrayRef = Arc::new(StringArray::from(values));
            exact(
                super::cast_array(&array, &DataType::Float64),
                native(&array, &DataType::Float64),
            );
        }
        Ok(())
    }
    #[test]
    fn decimal_double_preserves_bits_null_payloads_and_slices() {
        use arrow::array::AsArray;
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Float64Type;
        let values = vec![
            i128::MIN,
            i64::MIN as i128 - 1,
            i64::MIN as i128,
            0,
            i64::MAX as i128,
            i64::MAX as i128 + 1,
            i128::MAX,
        ];
        for (precision, scale) in [(10, 2), (18, 0), (18, 18)] {
            let array = Decimal128Array::new(
                values.clone().into(),
                Some(NullBuffer::from(vec![
                    false, true, true, true, true, true, false,
                ])),
            )
            .with_precision_and_scale(precision, scale)
            .unwrap();
            for array in [
                array.clone(),
                array.slice(1, 5),
                array.slice(2, 3),
                array.slice(0, 0),
            ] {
                let array: ArrayRef = Arc::new(array);
                let expected = native(&array, &DataType::Float64).unwrap();
                let actual = super::cast_array(&array, &DataType::Float64).unwrap();
                assert_eq!(expected.nulls(), actual.nulls());
                assert_eq!(
                    expected
                        .as_primitive::<Float64Type>()
                        .values()
                        .iter()
                        .map(|x| x.to_bits())
                        .collect::<Vec<_>>(),
                    actual
                        .as_primitive::<Float64Type>()
                        .values()
                        .iter()
                        .map(|x| x.to_bits())
                        .collect::<Vec<_>>()
                );
            }
        }
        for (precision, scale) in [(19, 0), (38, 18), (18, -1)] {
            assert!(!super::CorrelationCast::supports(
                &DataType::Decimal128(precision, scale),
                &DataType::Float64
            ));
        }
    }
    #[test]
    fn decimal_downscale_preserves_rounding_errors_and_nulls() {
        use arrow::array::AsArray;
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Decimal128Type;
        let cases = [
            vec![-15, -14, -5, -4, 0, 4, 5, 14, 15],
            vec![
                i128::MIN,
                i64::MIN as i128,
                -125,
                0,
                125,
                i64::MAX as i128,
                i128::MAX,
            ],
            vec![125, 9_999_999_995, -9_999_999_995, i128::MAX],
            vec![125, -9_999_999_995, 9_999_999_995, i128::MIN],
        ];
        for (p, s, q, t) in [(10, 2, 9, 1), (10, 2, 18, 1), (1, 1, 18, 0), (18, 18, 1, 0)] {
            for values in &cases {
                let len = values.len();
                for nulls in [
                    None,
                    Some(NullBuffer::from(vec![false; len])),
                    Some(NullBuffer::from(
                        (0..len).map(|i| i > 0 && i + 1 < len).collect::<Vec<_>>(),
                    )),
                ] {
                    let array = Decimal128Array::new(values.clone().into(), nulls)
                        .with_precision_and_scale(p, s)
                        .unwrap();
                    for array in [array.clone(), array.slice(1, len - 1), array.slice(0, 0)] {
                        let array: ArrayRef = Arc::new(array);
                        let target = DataType::Decimal128(q, t);
                        let expected = native(&array, &target);
                        let actual = super::cast_array(&array, &target);
                        if let (Ok(a), Ok(b)) = (&expected, &actual) {
                            assert_eq!(
                                a.as_primitive::<Decimal128Type>().values(),
                                b.as_primitive::<Decimal128Type>().values()
                            );
                        }
                        exact(expected, actual);
                    }
                }
            }
        }
        for (from, to) in [
            (DataType::Decimal128(38, 2), DataType::Decimal128(9, 1)),
            (DataType::Decimal128(18, 2), DataType::Decimal128(19, 1)),
            (DataType::Decimal128(18, 2), DataType::Decimal128(18, -1)),
            (DataType::Decimal128(18, 1), DataType::Decimal128(18, 2)),
        ] {
            assert!(!super::CorrelationCast::supports(&from, &to));
        }
    }

    #[test]
    fn unsigned_decimal_boundaries_nulls_and_slices_match_native() {
        use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};

        let arrays: [ArrayRef; 4] = [
            Arc::new(UInt8Array::from(vec![0, 1, u8::MAX - 1, u8::MAX])),
            Arc::new(UInt16Array::from(vec![0, 1, u16::MAX - 1, u16::MAX])),
            Arc::new(UInt32Array::from(vec![0, 1, u32::MAX - 1, u32::MAX])),
            Arc::new(UInt64Array::from(vec![
                0,
                1,
                (1_u64 << 53) + 1,
                (1_u64 << 63) + 1,
                999_999_999_999_999_999,
                1_000_000_000_000_000_000,
                9_999_999_999_999_999_999,
                10_000_000_000_000_000_000,
                u64::MAX,
            ])),
        ];
        for array in arrays {
            for (precision, scale) in [
                (2, 0),
                (9, 0),
                (19, 0),
                (20, 0),
                (38, 18),
                (38, 19),
                (38, 20),
            ] {
                let target = DataType::Decimal128(precision, scale);
                let valid = native_valid_numeric_rows(&array, &target);
                assert!(!valid.is_empty());
                exact(super::cast_array(&valid, &target), native(&valid, &target));
                exact(super::cast_array(&array, &target), native(&array, &target));
                let slice = array.slice(1, array.len() - 1);
                exact(super::cast_array(&slice, &target), native(&slice, &target));
                let empty = array.slice(1, 0);
                exact(super::cast_array(&empty, &target), native(&empty, &target));
            }
        }
        // Hidden overflow must be skipped by native validity, while a visible overflow still fails.
        let hidden: ArrayRef = Arc::new(UInt64Array::new(
            vec![u64::MAX, 1].into(),
            Some(vec![false, true].into()),
        ));
        let visible: ArrayRef = Arc::new(UInt64Array::from(vec![None, Some(u64::MAX)]));
        for target in [
            DataType::Decimal128(19, 0),
            DataType::Decimal128(38, 19),
            DataType::Decimal128(38, 20),
        ] {
            assert!(native(&hidden, &target).is_ok());
            assert!(native(&visible, &target).is_err());
            exact(
                super::cast_array(&hidden, &target),
                native(&hidden, &target),
            );
            exact(
                super::cast_array(&visible, &target),
                native(&visible, &target),
            );
        }
    }
}
