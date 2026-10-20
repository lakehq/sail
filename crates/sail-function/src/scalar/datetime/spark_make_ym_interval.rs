use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, IntervalYearMonthArray, new_null_array,
};
use datafusion::arrow::compute::{cast, try_binary};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float64Type, Int32Type, Int64Type,
    IntervalUnit, i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::spark_cast_string_to_int32::parse_spark_legacy_string_to_i32;

const MONTHS_PER_YEAR: i32 = 12;

/// Spark `make_ym_interval([years[, months]])` (`MakeYMInterval`, `intervalExpressions.scala`).
///
/// The arguments are `ImplicitCastInputTypes(IntegerType, IntegerType)`, so a number or a string
/// reaches the function through Spark's `Cast` to INT, whose rules depend on ANSI mode. The cast is
/// done here rather than by the planner, because the nullability of the result depends on the
/// types the arguments had before it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeYmInterval {
    signature: Signature,
    ansi_mode: bool,
}

impl SparkMakeYmInterval {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkMakeYmInterval {
    fn name(&self) -> &str {
        "spark_make_ym_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    /// `MakeYMInterval` is null-intolerant, so the result is nullable when an argument is. The
    /// implicit cast to INT is itself nullable from a string, a fraction or a decimal
    /// (`Cast.forceNullable`), whatever the value.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .iter()
            .any(|f| f.is_nullable() || cast_to_int_is_nullable(f.data_type()));
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Interval(IntervalUnit::YearMonth),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "make_ym_interval",
                (0, 2),
                args.len(),
            ));
        }

        // Omitted arguments default to zero (Spark fills `Literal(0)` for absent years/months).
        let zero = ColumnarValue::Scalar(ScalarValue::Int32(Some(0)));
        let mut args = args.into_iter();
        let year = args.next().unwrap_or_else(|| zero.clone());
        let month = args.next().unwrap_or(zero);

        // Spark's `MakeYMInterval` is null-intolerant: a NULL in a supplied argument yields NULL.
        let scalar_null = |value: &ColumnarValue| matches!(value, ColumnarValue::Scalar(scalar) if scalar.is_null());
        if scalar_null(&year) || scalar_null(&month) {
            return Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(None)));
        }

        let years = cast_to_int(&year.to_array(number_rows)?, self.ansi_mode)?;
        let months = cast_to_int(&month.to_array(number_rows)?, self.ansi_mode)?;

        // `try_binary` propagates array nulls and lets us surface Spark's overflow error, which is
        // raised regardless of `spark.sql.ansi.enabled` (year-month interval math uses exact ops).
        let result: IntervalYearMonthArray = try_binary(&years, &months, checked_ym_interval)?
            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    /// The arguments keep their types, since [`cast_to_int`] converts them with Spark's rules.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 2 {
            return plan_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `make_ym_interval` requires [0, 1, 2] \
                 parameters but the actual number is {}.",
                arg_types.len()
            );
        }
        for (i, arg_type) in arg_types.iter().enumerate() {
            if !is_castable_to_int(arg_type) {
                let ordinal = if i == 0 { "first" } else { "second" };
                return plan_err!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \
                     \"make_ym_interval\" due to data type mismatch: The {ordinal} parameter \
                     requires the \"INT\" type, however the argument has the type {arg_type}."
                );
            }
        }
        Ok(arg_types.to_vec())
    }
}

fn is_castable_to_int(data_type: &DataType) -> bool {
    data_type.is_integer()
        || data_type.is_floating()
        || matches!(
            data_type,
            DataType::Null
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
        || is_string(data_type)
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn cast_to_int_is_nullable(data_type: &DataType) -> bool {
    data_type.is_floating()
        || is_string(data_type)
        || matches!(
            data_type,
            DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

fn cast_overflow(value: impl std::fmt::Display, type_name: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_OVERFLOW] The value {value} of the type \"{type_name}\" cannot be cast to \"INT\" \
         due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. If \
         necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error."
    ))
}

fn cast_invalid_input(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"INT\" because it is malformed. Correct the value as per the syntax, or change its \
         target type. Use `try_cast` to tolerate malformed input and return NULL instead. If \
         necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error."
    ))
}

/// An argument cast to INT the way Spark's `Cast` does it. With ANSI on, a value out of range or
/// a malformed string is an error. With ANSI off, a BIGINT or DECIMAL out of range keeps its low 32
/// bits, a fraction saturates and NaN is 0 (Scala's `toInt`), and a malformed string is NULL.
fn cast_to_int(array: &ArrayRef, ansi_mode: bool) -> Result<Int32Array> {
    let data_type = array.data_type();
    if matches!(data_type, DataType::Null) {
        return Ok(new_null_array(&DataType::Int32, array.len())
            .as_primitive::<Int32Type>()
            .clone());
    }
    if matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16
    ) {
        return Ok(cast(array, &DataType::Int32)?
            .as_primitive::<Int32Type>()
            .clone());
    }
    if data_type.is_integer() {
        let values = cast(array, &DataType::Int64)?;
        return values
            .as_primitive::<Int64Type>()
            .iter()
            .map(|v| match v {
                Some(v) if ansi_mode => i32::try_from(v)
                    .map(Some)
                    .map_err(|_| cast_overflow(format!("{v}L"), "BIGINT")),
                Some(v) => Ok(Some(v as i32)),
                None => Ok(None),
            })
            .collect();
    }
    if data_type.is_floating() {
        let values = cast(array, &DataType::Float64)?;
        return values
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| match v {
                Some(v) if ansi_mode => {
                    let t = v.trunc();
                    if t.is_nan() || t < f64::from(i32::MIN) || t > f64::from(i32::MAX) {
                        Err(cast_overflow(format!("{v:?}D"), "DOUBLE"))
                    } else {
                        Ok(Some(t as i32))
                    }
                }
                Some(v) => Ok(Some(v as i32)),
                None => Ok(None),
            })
            .collect();
    }
    match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale) => {
            let scale = *scale;
            let type_name = data_type.to_string().to_uppercase();
            let values = cast(array, &DataType::Decimal128(38, scale))?;
            let divisor = 10_i128.checked_pow(u32::try_from(scale.max(0)).unwrap_or(0));
            values
                .as_primitive::<Decimal128Type>()
                .iter()
                .map(|v| {
                    let Some(v) = v else { return Ok(None) };
                    let integral = match divisor {
                        Some(d) => v / d,
                        None => 0,
                    };
                    if ansi_mode {
                        i32::try_from(integral)
                            .map(Some)
                            .map_err(|_| cast_overflow(format!("{integral}BD"), &type_name))
                    } else {
                        Ok(Some(integral as i32))
                    }
                })
                .collect()
        }
        DataType::Decimal256(_, scale) => {
            let scale = *scale;
            let type_name = data_type.to_string().to_uppercase();
            let divisor = i256::from_i128(10).checked_pow(u32::try_from(scale.max(0)).unwrap_or(0));
            array
                .as_primitive::<Decimal256Type>()
                .iter()
                .map(|v| {
                    let Some(v) = v else { return Ok(None) };
                    let integral = match divisor {
                        Some(d) => v.wrapping_div(d),
                        None => i256::ZERO,
                    };
                    match integral.to_i128().and_then(|x| i32::try_from(x).ok()) {
                        Some(x) => Ok(Some(x)),
                        None if ansi_mode => {
                            Err(cast_overflow(format!("{integral}BD"), &type_name))
                        }
                        None => Ok(Some(integral.as_i128() as i32)),
                    }
                })
                .collect()
        }
        _ if is_string(data_type) => {
            let values = cast(array, &DataType::Utf8)?;
            values
                .as_string::<i32>()
                .iter()
                .map(|v| {
                    let Some(v) = v else { return Ok(None) };
                    let parsed = parse_spark_legacy_string_to_i32(v);
                    if ansi_mode {
                        // `UTF8String.toIntExact` takes no fraction, unlike the legacy parse.
                        match parsed {
                            Some(x) if !v.contains('.') => Ok(Some(x)),
                            _ => Err(cast_invalid_input(v)),
                        }
                    } else {
                        Ok(parsed)
                    }
                })
                .collect()
        }
        other => plan_err!("make_ym_interval cannot cast {other} to INT"),
    }
}

/// Computes `years * 12 + months`, raising Spark's interval overflow error on wrap.
///
/// Matches Spark `MakeYMInterval`, which uses `Math.multiplyExact`/`Math.addExact` and therefore
/// errors on Int32 overflow independently of `spark.sql.ansi.enabled`.
fn checked_ym_interval(years: i32, months: i32) -> std::result::Result<i32, ArrowError> {
    years
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|total| total.checked_add(months))
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer overflow while operating with intervals.".to_string(),
            )
        })
}
