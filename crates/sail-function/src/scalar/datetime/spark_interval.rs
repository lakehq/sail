use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int32Type, IntervalMonthDayNano,
    IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion_common::arrow::array::{AsArray, PrimitiveArray};
use datafusion_common::arrow::datatypes::IntervalMonthDayNanoType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::literal::interval::{IntervalValue, parse_year_month_interval_string};
use sail_sql_analyzer::parser::parse_interval;

macro_rules! define_interval_udf {
    ($udf:ident, $name:expr_2021, $return_type:expr_2021, $primitive_type:ty, $func:expr_2021, $scalar:expr_2021 $(,)?) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $udf {
            signature: Signature,
        }

        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::coercible(
                        vec![Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        ))],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $udf {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs { args, .. } = args;
                let arg = args.one()?;
                match arg {
                    ColumnarValue::Array(array) => {
                        let array: PrimitiveArray<$primitive_type> = match array.data_type() {
                            DataType::Utf8 => as_string_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
                            DataType::LargeUtf8 => as_large_string_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
                            DataType::Utf8View => as_string_view_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
                            _ => return exec_err!("expected string array for intervals"),
                        };
                        Ok(ColumnarValue::Array(Arc::new(array)))
                    }
                    ColumnarValue::Scalar(scalar) => {
                        let value = match scalar.try_as_str() {
                            Some(x) => x.map(|x| $func(x)).transpose()?,
                            _ => return exec_err!("expected string scalar for intervals"),
                        };
                        Ok(ColumnarValue::Scalar($scalar(value)))
                    }
                }
            }
        }
    };
}

define_interval_udf!(
    SparkYearMonthInterval,
    "spark_year_month_interval",
    DataType::Interval(IntervalUnit::YearMonth),
    IntervalYearMonthType,
    string_to_year_month_interval,
    ScalarValue::IntervalYearMonth,
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct YearMonthIntervalMonths {
    signature: Signature,
}

impl Default for YearMonthIntervalMonths {
    fn default() -> Self {
        Self::new()
    }
}

impl YearMonthIntervalMonths {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Interval(IntervalUnit::YearMonth)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for YearMonthIntervalMonths {
    fn name(&self) -> &str {
        "year_month_interval_months"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = take_function_args(self.name(), args.arg_fields)?;
        // The numeric output must not retain interval qualifier metadata.
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int32,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.one()? {
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(months)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(months)))
            }
            ColumnarValue::Array(array)
                if array.data_type() == &DataType::Interval(IntervalUnit::YearMonth) =>
            {
                Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<IntervalYearMonthType>()
                        .reinterpret_cast::<Int32Type>(),
                )))
            }
            _ => exec_err!("expected year month interval"),
        }
    }
}

/// Casts an `Int64` (already widened by the resolver so the multiply below cannot
/// overflow `i64`) into `Interval(YearMonth)`, applying `IntervalUtils.longToYearMonthInterval`'s
/// overflow check (`Math.multiplyExact`/`toIntExact`, unconditional regardless of ANSI mode).
///
/// A dedicated UDF -- rather than a `CASE WHEN overflow THEN raise_error(..) ELSE CAST(..)`
/// expression -- is required to get nullability right: DataFusion derives a `Case`'s
/// nullability from its branches, and `raise_error` always declares itself nullable, which
/// would make the whole cast nullable even though Spark's `Cast.forceNullable` does not list
/// numeric -> YearMonthIntervalType (so it should stay exactly as nullable as the input).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkYearMonthIntervalFromInt64 {
    signature: Signature,
    multiplier: i64,
    is_try: bool,
}

impl SparkYearMonthIntervalFromInt64 {
    pub fn new(multiplier: i64, is_try: bool) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            multiplier,
            is_try,
        }
    }

    pub fn multiplier(&self) -> i64 {
        self.multiplier
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn convert(&self, value: i64) -> Result<Option<i32>> {
        match value
            .checked_mul(self.multiplier)
            .and_then(|months| i32::try_from(months).ok())
        {
            Some(months) => Ok(Some(months)),
            None if self.is_try => Ok(None),
            None => exec_err!(
                "[CAST_OVERFLOW] The value '{value}' cannot be cast to \"INTERVAL YEAR TO MONTH\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead."
            ),
        }
    }
}

impl ScalarUDFImpl for SparkYearMonthIntervalFromInt64 {
    fn name(&self) -> &str {
        "spark_year_month_interval_from_int64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Interval(IntervalUnit::YearMonth),
            self.is_try || field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.one()? {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                let months = value.map(|v| self.convert(v)).transpose()?.flatten();
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(
                    months,
                )))
            }
            ColumnarValue::Array(array) => {
                let array = array
                    .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                    .iter()
                    .map(|value| {
                        value
                            .map(|v| self.convert(v))
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<PrimitiveArray<IntervalYearMonthType>>>()?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => exec_err!("expected Int64"),
        }
    }
}

/// Casts an `Int64` into `Duration(Microsecond)`, applying
/// `IntervalUtils.longToDayTimeInterval`'s overflow check (`Math.multiplyExact`,
/// unconditional regardless of ANSI mode).
///
/// A dedicated UDF for the same nullability reason as `SparkYearMonthIntervalFromInt64`:
/// a `CASE WHEN overflow THEN raise_error(..) ELSE CAST(..)` would force the whole
/// expression nullable (`raise_error` always declares itself nullable), but Spark's
/// `Cast.forceNullable` does not list numeric -> DayTimeIntervalType.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDayTimeIntervalFromInt64 {
    signature: Signature,
    multiplier: i64,
    is_try: bool,
}

impl SparkDayTimeIntervalFromInt64 {
    pub fn new(multiplier: i64, is_try: bool) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            multiplier,
            is_try,
        }
    }

    pub fn multiplier(&self) -> i64 {
        self.multiplier
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn convert(&self, value: i64) -> Result<Option<i64>> {
        match value.checked_mul(self.multiplier) {
            Some(micros) => Ok(Some(micros)),
            None if self.is_try => Ok(None),
            None => exec_err!(
                "[CAST_OVERFLOW] The value '{value}' cannot be cast to \"INTERVAL DAY TO SECOND\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead."
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDayTimeIntervalFromInt64 {
    fn name(&self) -> &str {
        "spark_day_time_interval_from_int64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(TimeUnit::Microsecond))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Duration(TimeUnit::Microsecond),
            self.is_try || field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.one()? {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                let micros = value.map(|v| self.convert(v)).transpose()?.flatten();
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(
                    micros,
                )))
            }
            ColumnarValue::Array(array) => {
                let array = array
                    .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                    .iter()
                    .map(|value| {
                        value
                            .map(|v| self.convert(v))
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<PrimitiveArray<DurationMicrosecondType>>>()?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => exec_err!("expected Int64"),
        }
    }
}

define_interval_udf!(
    SparkDayTimeInterval,
    "spark_day_time_interval",
    DataType::Duration(TimeUnit::Microsecond),
    DurationMicrosecondType,
    string_to_day_time_interval,
    ScalarValue::DurationMicrosecond,
);

define_interval_udf!(
    SparkCalendarInterval,
    "spark_calendar_interval",
    DataType::Interval(IntervalUnit::MonthDayNano),
    IntervalMonthDayNanoType,
    string_to_calendar_interval,
    ScalarValue::IntervalMonthDayNano,
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDayTimeIntervalToCalendarInterval {
    signature: Signature,
}

impl Default for SparkDayTimeIntervalToCalendarInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDayTimeIntervalToCalendarInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Duration(TimeUnit::Microsecond)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDayTimeIntervalToCalendarInterval {
    fn name(&self) -> &str {
        "spark_day_time_interval_to_calendar_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => array
                        .as_primitive::<DurationMicrosecondType>()
                        .iter()
                        .map(|value| {
                            value
                                .map(day_time_interval_to_calendar_interval)
                                .transpose()
                        })
                        .collect::<Result<PrimitiveArray<IntervalMonthDayNanoType>>>()?,
                    data_type => {
                        return exec_err!(
                            "expected microsecond day-time interval, got {data_type}"
                        );
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(value)) => {
                let value = value
                    .map(day_time_interval_to_calendar_interval)
                    .transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    value,
                )))
            }
            value => exec_err!("expected microsecond day-time interval, got {value:?}"),
        }
    }
}

// TODO: support alternative form of interval strings
//   In Spark, interval strings can be specified in two forms.
//   For example, the `INTERVAL HOUR` type can have the following string representations.
//   1. `[+|-]h`
//   2. `INTERVAL [+|-]'[+|-]h' HOUR`
//   The first form cannot be parsed since the start and end field information is lost in
//   Arrow types. Types such as `INTERVAL DAY` and `INTERVAL HOUR` has the same physical type
//   in Arrow, and we cannot distinguish `[+|-]d` from `[+|-]h`.

fn string_to_year_month_interval(value: &str) -> Result<i32> {
    parse_year_month_interval_string(value).map_err(|e| exec_datafusion_err!("{e}"))
}

fn string_to_day_time_interval(value: &str) -> Result<i64> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
        IntervalValue::YearMonth { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            exec_err!("expected day time interval, but got: {value}")
        }
    }
}

fn string_to_calendar_interval(value: &str) -> Result<IntervalMonthDayNano> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::YearMonth { months, .. } => Ok(IntervalMonthDayNano {
            months,
            days: 0,
            nanoseconds: 0,
        }),
        IntervalValue::Microsecond { microseconds, .. } => {
            day_time_interval_to_calendar_interval(microseconds)
        }
        IntervalValue::MonthDayNanosecond {
            months,
            days,
            nanoseconds,
        } => Ok(IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        }),
    }
}

fn day_time_interval_to_calendar_interval(microseconds: i64) -> Result<IntervalMonthDayNano> {
    const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

    let days = i32::try_from(microseconds / MICROSECONDS_PER_DAY).map_err(|_| {
        exec_datafusion_err!("microseconds overflow for calendar interval: {microseconds}")
    })?;
    Ok(IntervalMonthDayNano {
        months: 0,
        days,
        nanoseconds: microseconds % MICROSECONDS_PER_DAY * 1_000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_time_interval_preserves_calendar_days_and_microsecond_remainder() -> Result<()> {
        const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

        assert_eq!(
            day_time_interval_to_calendar_interval(MICROSECONDS_PER_DAY + 5)?,
            IntervalMonthDayNano::new(0, 1, 5_000)
        );
        assert_eq!(
            day_time_interval_to_calendar_interval(-MICROSECONDS_PER_DAY - 5)?,
            IntervalMonthDayNano::new(0, -1, -5_000)
        );
        Ok(())
    }
}
