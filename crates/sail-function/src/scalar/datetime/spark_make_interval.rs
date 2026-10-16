use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, IntervalMonthDayNanoBuilder, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType::Interval;
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float64Type, Int32Type, Int64Type,
    IntervalMonthDayNano, UInt32Type, UInt64Type, i256,
};
use datafusion_common::types::{NativeType, logical_int32};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkMakeInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeInterval {
    pub fn new() -> Self {
        let int32 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );

        let variants = vec![
            TypeSignature::Nullary,
            // year
            TypeSignature::Coercible(vec![int32.clone()]),
            // year, month
            TypeSignature::Coercible(vec![int32.clone(), int32.clone()]),
            // year, month, week
            TypeSignature::Coercible(vec![int32.clone(), int32.clone(), int32.clone()]),
            // year, month, week, day
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour, minute
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour, minute, second
            // Retain exact numeric seconds instead of losing precision through Float64.
            TypeSignature::UserDefined,
        ];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
            ansi_mode: false,
        }
    }

    pub fn with_ansi_mode(mut self, ansi_mode: bool) -> Self {
        self.ansi_mode = ansi_mode;
        self
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn name(&self) -> &str {
        "make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Interval(MonthDayNano))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = !self.ansi_mode
            || args.arg_fields.iter().any(|f| f.is_nullable())
            || args
                .arg_fields
                .get(6)
                .is_some_and(|f| seconds_nullable(f.data_type()));
        Ok(Arc::new(Field::new(
            self.name(),
            Interval(MonthDayNano),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                Some(IntervalMonthDayNano::new(0, 0, 0)),
            )));
        }
        make_scalar_function(
            |arrays| make_interval_kernel(arrays, self.ansi_mode),
            vec![],
        )(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_interval_types(self.name(), arg_types, 6)
    }
}

fn make_interval_kernel(args: &[ArrayRef], ansi_mode: bool) -> Result<ArrayRef, DataFusionError> {
    let n_rows = args[0].len();

    let years = args[0]
        .as_primitive_opt::<Int32Type>()
        .ok_or_else(|| plan_datafusion_err!("make_interval arg[0] must be Int32"))?;
    let months = args
        .get(1)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>()
                .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[1] must be Int32"))
        })
        .transpose()?;
    let weeks = args
        .get(2)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>()
                .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[2] must be Int32"))
        })
        .transpose()?;
    let days: Option<&PrimitiveArray<Int32Type>> = args
        .get(3)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>()
                .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[3] must be Int32"))
        })
        .transpose()?;
    let hours: Option<&PrimitiveArray<Int32Type>> = args
        .get(4)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>()
                .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[4] must be Int32"))
        })
        .transpose()?;
    let mins: Option<&PrimitiveArray<Int32Type>> = args
        .get(5)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>()
                .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[5] must be Int32"))
        })
        .transpose()?;
    let secs = args
        .get(6)
        .map(|a| IntervalSeconds::new(a, 9))
        .transpose()?;

    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present = years.is_null(i)
            || months.as_ref().is_some_and(|a| a.is_null(i))
            || weeks.as_ref().is_some_and(|a| a.is_null(i))
            || days.as_ref().is_some_and(|a| a.is_null(i))
            || hours.as_ref().is_some_and(|a| a.is_null(i))
            || mins.as_ref().is_some_and(|a| a.is_null(i))
            || secs.as_ref().is_some_and(|a| a.is_null(i));

        if any_null_present {
            builder.append_null();
            continue;
        }

        // default values 0 or 0.0
        let y = years.value(i);
        let mo = months.as_ref().map_or(0, |a| a.value(i));
        let w = weeks.as_ref().map_or(0, |a| a.value(i));
        let d = days.as_ref().map_or(0, |a| a.value(i));
        let h = hours.as_ref().map_or(0, |a| a.value(i));
        let mi = mins.as_ref().map_or(0, |a| a.value(i));
        let s = secs.as_ref().map_or(Some(0), |a| a.value(i));

        match s.and_then(|seconds| make_interval_month_day_nano(y, mo, w, d, h, mi, seconds)) {
            Some(v) => builder.append_value(v),
            None if ansi_mode && s.is_none() => {
                return exec_err!("[NUMERIC_VALUE_OUT_OF_RANGE] seconds overflow in make_interval");
            }
            None if ansi_mode => {
                return exec_err!("[ARITHMETIC_OVERFLOW] overflow in make_interval");
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn make_interval_month_day_nano(
    year: i32,
    month: i32,
    week: i32,
    day: i32,
    hour: i32,
    min: i32,
    second_nanos: i128,
) -> Option<IntervalMonthDayNano> {
    // checks if overflow
    let months = year.checked_mul(12).and_then(|v| v.checked_add(month))?;
    let total_days = week.checked_mul(7).and_then(|v| v.checked_add(day))?;

    // Widen intermediate values so valid opposite-sign components can cancel.
    let hours_nanos = i128::from(hour) * 3_600_000_000_000;
    let mins_nanos = i128::from(min) * 60_000_000_000;

    let total_nanos = hours_nanos
        .checked_add(mins_nanos)
        .and_then(|v| v.checked_add(second_nanos))?;
    // FIXME: Spark's microsecond calendar interval has a wider elapsed-time range than Arrow's nanos.
    let total_nanos = i64::try_from(total_nanos).ok()?;

    Some(IntervalMonthDayNano::new(months, total_days, total_nanos))
}

pub(super) fn coerce_interval_types(
    name: &str,
    arg_types: &[DataType],
    seconds_index: usize,
) -> Result<Vec<DataType>> {
    if arg_types.len() > seconds_index + 1 {
        return plan_err!(
            "{name} requires between 0 and {} arguments",
            seconds_index + 1
        );
    }
    arg_types
        .iter()
        .enumerate()
        .map(|(index, mut data_type)| {
            while let DataType::Dictionary(_, value_type) = data_type {
                data_type = value_type.as_ref();
            }
            let native: NativeType = data_type.into();
            if index < seconds_index {
                return if native.is_integer() || matches!(native, NativeType::Null) {
                    Ok(DataType::Int32)
                } else {
                    plan_err!("{name} component arguments must be integers")
                };
            }
            match data_type {
                DataType::Null | DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    Ok(DataType::Float64)
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(DataType::Int32),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(DataType::UInt32),
                DataType::Int64 | DataType::UInt64 | DataType::Decimal256(_, _) => {
                    Ok(data_type.clone())
                }
                DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale) => {
                    Ok(DataType::Decimal128(*precision, *scale))
                }
                _ => plan_err!("{name} seconds must be numeric"),
            }
        })
        .collect()
}

pub(super) fn seconds_nullable(data_type: &DataType) -> bool {
    // Spark's DECIMAL(18,6) seconds coercion can produce NULL even for non-null inputs.
    match data_type {
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Int64
        | DataType::UInt64 => true,
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let integer_digits = i16::from(*precision) - i16::from(*scale);
            integer_digits > 12 || (integer_digits == 12 && *scale > 6)
        }
        _ => false,
    }
}

enum SecondsValues<'a> {
    Int32(&'a [i32]),
    UInt32(&'a [u32]),
    Int64(&'a [i64]),
    UInt64(&'a [u64]),
    Float64(&'a [f64]),
    Decimal128(&'a [i128], Option<i128>, bool),
    Decimal256(&'a [i256], Option<i256>, bool),
}

// The two constructors need the same exact rounding, at Sail's respective micro/nanosecond scales.
pub(super) struct IntervalSeconds<'a> {
    values: SecondsValues<'a>,
    nulls: Option<&'a NullBuffer>,
    scale: u32,
    factor: i128,
}

impl<'a> IntervalSeconds<'a> {
    pub(super) fn new(array: &'a ArrayRef, scale: u32) -> Result<Self> {
        let values = match array.data_type() {
            DataType::Int32 => SecondsValues::Int32(array.as_primitive::<Int32Type>().values()),
            DataType::UInt32 => SecondsValues::UInt32(array.as_primitive::<UInt32Type>().values()),
            DataType::Int64 => SecondsValues::Int64(array.as_primitive::<Int64Type>().values()),
            DataType::UInt64 => SecondsValues::UInt64(array.as_primitive::<UInt64Type>().values()),
            DataType::Float64 => {
                SecondsValues::Float64(array.as_primitive::<Float64Type>().values())
            }
            DataType::Decimal128(_, source_scale) => {
                let shift = scale as i32 - i32::from(*source_scale);
                SecondsValues::Decimal128(
                    array.as_primitive::<Decimal128Type>().values(),
                    10i128.checked_pow(shift.unsigned_abs()),
                    shift >= 0,
                )
            }
            DataType::Decimal256(_, source_scale) => {
                let shift = scale as i32 - i32::from(*source_scale);
                SecondsValues::Decimal256(
                    array.as_primitive::<Decimal256Type>().values(),
                    i256::from(10).checked_pow(shift.unsigned_abs()),
                    shift >= 0,
                )
            }
            data_type => return plan_err!("unsupported interval seconds type {data_type}"),
        };
        Ok(Self {
            values,
            nulls: array.nulls(),
            scale,
            factor: 10i128.pow(scale),
        })
    }

    pub(super) fn is_null(&self, index: usize) -> bool {
        self.nulls.is_some_and(|nulls| nulls.is_null(index))
            || matches!(&self.values, SecondsValues::Float64(values) if !values[index].is_finite())
    }

    pub(super) fn value(&self, index: usize) -> Option<i128> {
        match &self.values {
            SecondsValues::Int32(values) => Some(i128::from(values[index]) * self.factor),
            SecondsValues::UInt32(values) => Some(i128::from(values[index]) * self.factor),
            SecondsValues::Int64(values) => Some(i128::from(values[index]) * self.factor),
            SecondsValues::UInt64(values) => Some(i128::from(values[index]) * self.factor),
            SecondsValues::Float64(values) => {
                round_float_seconds(values[index], self.scale, self.factor)
            }
            SecondsValues::Decimal128(values, factor, multiply) => {
                rescale_decimal128(values[index], *factor, *multiply)
            }
            SecondsValues::Decimal256(values, factor, multiply) => {
                rescale_decimal256(values[index], *factor, *multiply)
            }
        }
    }
}

fn rescale_decimal128(value: i128, factor: Option<i128>, multiply: bool) -> Option<i128> {
    if value == 0 {
        return Some(0);
    }
    if multiply {
        return value.checked_mul(factor?);
    }
    let Some(factor) = factor else {
        return Some(0);
    };
    let result = value / factor;
    let remainder = value % factor;
    Some(
        result
            + if remainder.unsigned_abs() >= (factor / 2) as u128 {
                value.signum()
            } else {
                0
            },
    )
}

fn rescale_decimal256(value: i256, factor: Option<i256>, multiply: bool) -> Option<i128> {
    if value == i256::ZERO {
        return Some(0);
    }
    if multiply {
        return value.checked_mul(factor?)?.to_i128();
    }
    let Some(factor) = factor else {
        return Some(0);
    };
    let result = value / factor;
    let remainder = value % factor;
    let carry = if remainder.wrapping_abs() >= factor / i256::from(2) {
        i256::from(if value.is_negative() { -1 } else { 1 })
    } else {
        i256::ZERO
    };
    result.checked_add(carry)?.to_i128()
}

fn round_float_seconds(value: f64, scale: u32, factor: i128) -> Option<i128> {
    let scaled = value * (factor as i64) as f64;
    let magnitude = scaled.abs();
    // Bound conversion and multiplication errors before using native rounding near a half-unit.
    if magnitude < 4_503_599_627_370_496.0
        && (magnitude.fract() - 0.5).abs() > 2.0 * f64::EPSILON * magnitude
    {
        return Some(i128::from(scaled.round() as i64));
    }
    // Spark rounds the decimal representation HALF_UP, which binary multiplication can misround.
    let text = value.to_string();
    let unsigned = text.strip_prefix('-').unwrap_or(&text);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut result = whole.parse::<i128>().ok()?.checked_mul(factor)?;
    let mut digits = fraction.bytes();
    let mut fractional = 0i128;
    for _ in 0..scale {
        fractional = fractional * 10 + i128::from(digits.next().unwrap_or(b'0') - b'0');
    }
    result = result.checked_add(fractional)?;
    if digits.next().is_some_and(|digit| digit >= b'5') {
        result = result.checked_add(1)?;
    }
    Some(if value.is_sign_negative() {
        -result
    } else {
        result
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use arrow::array::{Float64Array, Int32Array, IntervalMonthDayNanoArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{
        Result, assert_eq_or_internal_err, internal_datafusion_err, internal_err,
    };

    use super::*;
    fn run_make_interval_month_day_nano(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        make_interval_kernel(&arrs, false)
    }

    #[test]
    fn nulls_propagate_per_row() {
        let year = Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let month = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let week = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let day = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let hour = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let min = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            None,
            Some(7),
            Some(8),
            Some(9),
        ]));
        let sec = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]));

        let out =
            run_make_interval_month_day_nano(vec![year, month, week, day, hour, min, sec]).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }

    #[test]
    fn error_months_overflow_should_be_null() {
        // months = year*12 + month → NULL
        let year = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out =
            run_make_interval_month_day_nano(vec![year, month, week, day, hour, min, sec]).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_days_overflow_should_be_null() {
        // months = year*12 + month →  NULL
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out =
            run_make_interval_month_day_nano(vec![year, month, week, day, hour, min, sec]).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_min_overflow_should_be_null() {
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out =
            run_make_interval_month_day_nano(vec![year, month, week, day, hour, min, sec]).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_sec_overflow_should_be_null() {
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(f64::MAX)])) as ArrayRef;

        let out =
            run_make_interval_month_day_nano(vec![year, month, week, day, hour, min, sec]).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }

    #[test]
    fn happy_path_all_present_single_row() {
        // 1y 2m 3w 4d 5h 6m 7.25s
        let year = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(5)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(6)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![Some(7.25)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![year, month, week, day, hour, mins, secs])
            .unwrap();
        assert_eq!(out.data_type(), &Interval(MonthDayNano));

        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out.null_count(), 0);

        let v: IntervalMonthDayNano = out.value(0);
        assert_eq!(v.months, 12 + 2); // 14
        assert_eq!(v.days, 3 * 7 + 4); // 25
        let expected_nanos = (5_i64 * 3600 + 6 * 60 + 7) * 1_000_000_000 + 250_000_000;
        assert_eq!(v.nanoseconds, expected_nanos);
    }

    #[test]
    fn negative_components_and_fractional_seconds() {
        // -1y -2m  -1w -1d  -1h -1m  -1.5s
        let year = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(-2)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![Some(-1.5)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![year, month, week, day, hour, mins, secs])
            .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.null_count(), 0);
        let v = out.value(0);

        assert_eq!(v.months, -12 + (-2)); // -14
        assert_eq!(v.days, -7 + (-1)); // -8

        // -(1h + 1m + 1.5s) en nanos
        let expected_nanos = -((3600_i64 + 60 + 1) * 1_000_000_000 + 500_000_000);
        assert_eq!(v.nanoseconds, expected_nanos);
    }

    fn invoke_make_interval_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", Interval(MonthDayNano), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        SparkMakeInterval::new().invoke_with_args(args)
    }

    #[test]
    fn zero_args_returns_zero_seconds() -> Result<()> {
        let number_rows = 2;
        let res: ColumnarValue = invoke_make_interval_with_args(vec![], number_rows)?;

        match res {
            ColumnarValue::Array(arr) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!("expected IntervalMonthDayNanoArray")
                    })?;
                assert_eq_or_internal_err!(
                    arr.len(),
                    number_rows,
                    "expected array length {number_rows}"
                );
                for i in 0..number_rows {
                    let iv = arr.value(i);
                    assert_eq_or_internal_err!(
                        (iv.months, iv.days, iv.nanoseconds),
                        (0, 0, 0),
                        "row {i}: expected (0,0,0), got ({},{},{})",
                        iv.months,
                        iv.days,
                        iv.nanoseconds
                    );
                }
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(iv))) => {
                assert_eq_or_internal_err!(
                    (iv.months, iv.days, iv.nanoseconds),
                    (0, 0, 0),
                    "expected scalar 0s, got ({},{},{})",
                    iv.months,
                    iv.days,
                    iv.nanoseconds
                );
            }
            other => {
                return internal_err!(
                    "expected Array or Scalar IntervalMonthDayNano, got {other:?}"
                );
            }
        }

        Ok(())
    }
}
