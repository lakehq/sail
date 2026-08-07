use std::str::FromStr;
use std::sync::Arc;

use chrono::{
    DateTime, Days, LocalResult, Months, NaiveDateTime, Offset, TimeDelta, TimeZone, Utc,
};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    ListArray, NullBufferBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, DurationMicrosecondType, Field, FieldRef, Int8Type,
    Int16Type, Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion::arrow::temporal_conversions::{as_datetime, as_datetime_with_timezone};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;

use crate::functions_nested_utils::make_scalar_function;

const MAX_ROUNDED_ARRAY_LENGTH: i64 = i32::MAX as i64 - 15;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_MONTH: i64 = 28 * MICROS_PER_DAY;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSequence {
    signature: Signature,
    session_timezone: Arc<str>,
    ansi_mode: bool,
}

impl SparkSequence {
    pub fn new(session_timezone: Arc<str>, ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            session_timezone,
            ansi_mode,
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    fn parse_session_timezone(&self) -> Result<Tz> {
        Tz::from_str(&self.session_timezone).map_err(|error| {
            exec_datafusion_err!(
                "Spark `sequence` function: failed to parse timezone {}: {error}",
                self.session_timezone
            )
        })
    }
}

impl ScalarUDFImpl for SparkSequence {
    fn name(&self) -> &str {
        "spark_sequence"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let start_type = match arg_types {
            [start, _] | [start, _, _] => start.clone(),
            _ => return invalid_sequence_arity(arg_types.len()),
        };

        Ok(DataType::List(Arc::new(Field::new_list_field(
            start_type, false,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());

        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        match args.first().map(ColumnarValue::data_type) {
            Some(DataType::Int8) => make_scalar_function(gen_sequence_i8)(&args),
            Some(DataType::Int16) => make_scalar_function(gen_sequence_i16)(&args),
            Some(DataType::Int32) => make_scalar_function(gen_sequence_i32)(&args),
            Some(DataType::Int64) => make_scalar_function(gen_sequence_i64)(&args),
            Some(DataType::Date32) => {
                let timezone = self.parse_session_timezone()?;
                make_scalar_function(move |arrays| gen_sequence_date(arrays, timezone))(&args)
            }
            Some(DataType::Timestamp(TimeUnit::Microsecond, output_timezone)) => {
                let arithmetic_timezone = match output_timezone {
                    Some(_) => self.parse_session_timezone()?,
                    None => Tz::from_str("UTC").map_err(|error| {
                        exec_datafusion_err!(
                            "Spark `sequence` function: failed to parse UTC timezone: {error}"
                        )
                    })?,
                };
                let output_timezone = output_timezone.clone();
                make_scalar_function(move |arrays| {
                    gen_sequence_timestamp(arrays, arithmetic_timezone, output_timezone.clone())
                })(&args)
            }
            Some(other) => wrong_sequence_input_types(&[other]),
            None => invalid_sequence_arity(0),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 2 | 3) {
            return invalid_sequence_arity(arg_types.len());
        }

        let mut coerced = arg_types
            .iter()
            .map(|data_type| normalize_sequence_type(data_type, &self.session_timezone))
            .collect::<Result<Vec<_>>>()?;

        let mut selected = vec![0, 1];
        if coerced.len() == 3 && !is_temporal_step_type(&coerced[2]) {
            selected.push(2);
        }

        let mut common_type = coerced[selected[0]].clone();
        for index in selected.iter().copied().skip(1) {
            common_type = wider_sequence_type(&common_type, &coerced[index], self.ansi_mode)
                .ok_or_else(|| sequence_wrong_input_types_error(&coerced))?;
        }
        for index in selected {
            coerced[index] = common_type.clone();
        }

        validate_sequence_types(&coerced)?;
        Ok(coerced)
    }
}

fn invalid_sequence_arity<T>(arity: usize) -> Result<T> {
    exec_err!("Spark `sequence` function requires 2 or 3 arguments, got {arity}")
}

fn sequence_wrong_input_types_error(arg_types: &[DataType]) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES: Spark `sequence` function cannot accept argument types ({})",
        arg_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn wrong_sequence_input_types<T>(arg_types: &[DataType]) -> Result<T> {
    Err(sequence_wrong_input_types_error(arg_types))
}

fn normalize_sequence_type(data_type: &DataType, session_timezone: &Arc<str>) -> Result<DataType> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null => {
            Ok(data_type.clone())
        }
        DataType::UInt8 => Ok(DataType::Int16),
        DataType::UInt16 => Ok(DataType::Int32),
        DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
        DataType::Date32 | DataType::Date64 => Ok(DataType::Date32),
        DataType::Timestamp(_, None) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Timestamp(_, Some(_)) => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        )),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(DataType::Utf8),
        DataType::Duration(_) => Ok(DataType::Duration(TimeUnit::Microsecond)),
        DataType::Interval(_) => Ok(data_type.clone()),
        _ => wrong_sequence_input_types(std::slice::from_ref(data_type)),
    }
}

fn wider_sequence_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if right.is_null() {
        return Some(left.clone());
    }

    if let (Some(left_rank), Some(right_rank)) = (integral_rank(left), integral_rank(right)) {
        return Some(integral_type(left_rank.max(right_rank)));
    }

    match (left, right) {
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, timezone))
        | (DataType::Timestamp(TimeUnit::Microsecond, timezone), DataType::Date32) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
        )
        | (
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ) => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(timezone)),
        )),
        (DataType::Utf8, other) | (other, DataType::Utf8) => {
            if !ansi_mode {
                return Some(DataType::Utf8);
            }
            if integral_rank(other).is_some() {
                Some(DataType::Int64)
            } else {
                match other {
                    DataType::Date32 => Some(DataType::Date32),
                    DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                        Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
                    }
                    _ => None,
                }
            }
        }
        _ => None,
    }
}

fn integral_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn integral_type(rank: u8) -> DataType {
    match rank {
        0 => DataType::Int8,
        1 => DataType::Int16,
        2 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn is_temporal_bound_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

fn is_temporal_step_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Interval(
            IntervalUnit::MonthDayNano | IntervalUnit::YearMonth | IntervalUnit::DayTime
        ) | DataType::Duration(TimeUnit::Microsecond)
    )
}

fn validate_sequence_types(arg_types: &[DataType]) -> Result<()> {
    let [start, stop, step @ ..] = arg_types else {
        return invalid_sequence_arity(arg_types.len());
    };
    if start != stop {
        return wrong_sequence_input_types(arg_types);
    }

    if integral_rank(start).is_some() {
        if step.is_empty() || step.first() == Some(start) {
            return Ok(());
        }
    } else if is_temporal_bound_type(start)
        && (step.is_empty() || step.first().is_some_and(is_temporal_step_type))
    {
        return Ok(());
    }

    wrong_sequence_input_types(arg_types)
}

fn valid_boundaries(start: i64, stop: i64, step: i64) -> bool {
    (step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop)
}

fn illegal_sequence_boundaries<T>(start: i64, stop: i64, step: i64) -> Result<T> {
    exec_err!("Illegal sequence boundaries: {start} to {stop} by {step}")
}

fn collection_size_limit_error<T>(length: i128) -> Result<T> {
    exec_err!(
        "COLLECTION_SIZE_LIMIT_EXCEEDED.PARAMETER: Cannot create array with {length} elements; the limit is {MAX_ROUNDED_ARRAY_LENGTH}"
    )
}

fn sequence_length(start: i64, stop: i64, step: i64) -> Result<usize> {
    if !valid_boundaries(start, stop, step) {
        return illegal_sequence_boundaries(start, stop, step);
    }

    let length = if start == stop {
        1_i128
    } else {
        1_i128 + (i128::from(stop) - i128::from(start)) / i128::from(step)
    };

    if length > i128::from(MAX_ROUNDED_ARRAY_LENGTH) {
        return collection_size_limit_error(length);
    }

    usize::try_from(length)
        .map_err(|_| exec_datafusion_err!("sequence length does not fit in usize"))
}

fn checked_batch_length(current: usize, additional: usize) -> Result<usize> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| exec_datafusion_err!("sequence output length overflow"))?;
    i32::try_from(total)
        .map_err(|_| exec_datafusion_err!("sequence output exceeds Arrow List capacity"))?;
    Ok(total)
}

fn reserve<T>(values: &mut Vec<T>, additional: usize) -> Result<()> {
    values
        .try_reserve(additional)
        .map_err(|error| exec_datafusion_err!("failed to reserve sequence output: {error}"))
}

fn list_array(
    element_type: DataType,
    offsets: Vec<i32>,
    values: ArrayRef,
    validity: &mut NullBufferBuilder,
) -> Result<ArrayRef> {
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new_list_field(element_type, false)),
        OffsetBuffer::new(offsets.into()),
        values,
        validity.finish(),
    )?))
}

fn append_null_offset<T>(
    values: &[T],
    offsets: &mut Vec<i32>,
    validity: &mut NullBufferBuilder,
) -> Result<()> {
    offsets.push(
        i32::try_from(values.len())
            .map_err(|_| exec_datafusion_err!("sequence output exceeds Arrow List capacity"))?,
    );
    validity.append_null();
    Ok(())
}

fn append_row<T>(
    values: &mut Vec<T>,
    row: Vec<T>,
    offsets: &mut Vec<i32>,
    validity: &mut NullBufferBuilder,
) -> Result<()> {
    if row.len() as i128 > i128::from(MAX_ROUNDED_ARRAY_LENGTH) {
        return collection_size_limit_error(row.len() as i128);
    }
    let total = checked_batch_length(values.len(), row.len())?;
    reserve(values, row.len())?;
    values.extend(row);
    offsets.push(
        i32::try_from(total)
            .map_err(|_| exec_datafusion_err!("sequence output exceeds Arrow List capacity"))?,
    );
    validity.append_non_null();
    Ok(())
}

fn integral_row<T>(start: T, stop: T, step: T, max_values: usize) -> Result<Vec<T>>
where
    T: Copy + Into<i128> + TryFrom<i128>,
{
    let start_i128 = start.into();
    let stop_i128 = stop.into();
    let step_i128 = step.into();
    let start_i64 = i64::try_from(start_i128)
        .map_err(|_| exec_datafusion_err!("sequence start does not fit in i64"))?;
    let stop_i64 = i64::try_from(stop_i128)
        .map_err(|_| exec_datafusion_err!("sequence stop does not fit in i64"))?;
    let step_i64 = i64::try_from(step_i128)
        .map_err(|_| exec_datafusion_err!("sequence step does not fit in i64"))?;
    let length = sequence_length(start_i64, stop_i64, step_i64)?;
    if length > max_values {
        return exec_err!("sequence output exceeds Arrow List capacity");
    }
    integral_row_with_length(start, step, length)
}

fn integral_row_with_length<T>(start: T, step: T, length: usize) -> Result<Vec<T>>
where
    T: Copy + Into<i128> + TryFrom<i128>,
{
    let mut values = Vec::new();
    reserve(&mut values, length)?;
    extend_integral_values(&mut values, start, step, length)?;
    Ok(values)
}

fn extend_integral_values<T>(values: &mut Vec<T>, start: T, step: T, length: usize) -> Result<()>
where
    T: Copy + Into<i128> + TryFrom<i128>,
{
    let start = start.into();
    let step = step.into();
    for index in 0..length {
        let value = start + step * index as i128;
        values.push(
            T::try_from(value)
                .map_err(|_| exec_datafusion_err!("sequence element is out of range"))?,
        );
    }
    Ok(())
}

macro_rules! impl_sequence_for_type {
    ($name:ident, $native_type:ty, $arrow_type:ty, $array_type:ty, $data_type:expr_2021) => {
        fn $name(args: &[ArrayRef]) -> Result<ArrayRef> {
            let (start_array, stop_array, step_array) = match args {
                [start, stop] => (
                    start.as_primitive::<$arrow_type>(),
                    stop.as_primitive::<$arrow_type>(),
                    None,
                ),
                [start, stop, step] => (
                    start.as_primitive::<$arrow_type>(),
                    stop.as_primitive::<$arrow_type>(),
                    Some(step.as_primitive::<$arrow_type>()),
                ),
                _ => return invalid_sequence_arity(args.len()),
            };

            let mut rows = Vec::with_capacity(start_array.len());
            let mut total_values = 0_usize;
            for index in 0..start_array.len() {
                if start_array.is_null(index)
                    || stop_array.is_null(index)
                    || step_array.is_some_and(|step| step.is_null(index))
                {
                    rows.push(None);
                    continue;
                }

                let start = start_array.value(index);
                let stop = stop_array.value(index);
                let step = step_array.map_or_else(
                    || {
                        if start <= stop {
                            1 as $native_type
                        } else {
                            -1 as $native_type
                        }
                    },
                    |step| step.value(index),
                );
                let length = sequence_length(start as i64, stop as i64, step as i64)?;
                total_values = checked_batch_length(total_values, length)?;
                rows.push(Some((start, step, length)));
            }

            let mut values = Vec::new();
            reserve(&mut values, total_values)?;
            let mut offsets = Vec::with_capacity(rows.len() + 1);
            offsets.push(0);
            let mut validity = NullBufferBuilder::new(rows.len());

            for row in rows {
                match row {
                    None => append_null_offset(&values, &mut offsets, &mut validity)?,
                    Some((start, step, length)) => {
                        extend_integral_values(&mut values, start, step, length)?;
                        offsets.push(i32::try_from(values.len()).map_err(|_| {
                            exec_datafusion_err!("sequence output exceeds Arrow List capacity")
                        })?);
                        validity.append_non_null();
                    }
                }
            }

            list_array(
                $data_type,
                offsets,
                Arc::new(<$array_type>::from(values)),
                &mut validity,
            )
        }
    };
}

impl_sequence_for_type!(gen_sequence_i8, i8, Int8Type, Int8Array, DataType::Int8);
impl_sequence_for_type!(
    gen_sequence_i16,
    i16,
    Int16Type,
    Int16Array,
    DataType::Int16
);
impl_sequence_for_type!(
    gen_sequence_i32,
    i32,
    Int32Type,
    Int32Array,
    DataType::Int32
);
impl_sequence_for_type!(
    gen_sequence_i64,
    i64,
    Int64Type,
    Int64Array,
    DataType::Int64
);

#[derive(Debug, Clone, Copy)]
enum TemporalStep {
    Calendar { months: i32, days: i32, micros: i64 },
    YearMonth { months: i32 },
    DayTime { micros: i64 },
}

impl TemporalStep {
    fn default_for(start: i64, stop: i64) -> Self {
        Self::Calendar {
            months: 0,
            days: if start <= stop { 1 } else { -1 },
            micros: 0,
        }
    }

    fn parts(self) -> Result<(i32, i32, i64)> {
        match self {
            Self::Calendar {
                months,
                days,
                micros,
            } => Ok((months, days, micros)),
            Self::YearMonth { months } => Ok((months, 0, 0)),
            Self::DayTime { micros } => {
                let days = micros / MICROS_PER_DAY;
                let days = i32::try_from(days)
                    .map_err(|_| exec_datafusion_err!("sequence interval days overflow"))?;
                let remainder = micros - i64::from(days) * MICROS_PER_DAY;
                Ok((0, days, remainder))
            }
        }
    }
}

fn temporal_step_at(array: &ArrayRef, index: usize) -> Result<Option<TemporalStep>> {
    if array.is_null(index) {
        return Ok(None);
    }

    match array.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = array
                .as_primitive::<IntervalMonthDayNanoType>()
                .value(index);
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(value);
            Ok(Some(TemporalStep::Calendar {
                months,
                days,
                micros: nanos / 1_000,
            }))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let value = array.as_primitive::<IntervalYearMonthType>().value(index);
            Ok(Some(TemporalStep::YearMonth {
                months: IntervalYearMonthType::to_months(value),
            }))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let value = array.as_primitive::<IntervalDayTimeType>().value(index);
            let (days, millis) = IntervalDayTimeType::to_parts(value);
            let micros = i64::from(days)
                .checked_mul(MICROS_PER_DAY)
                .and_then(|value| value.checked_add(i64::from(millis) * 1_000))
                .ok_or_else(|| exec_datafusion_err!("sequence day-time interval overflow"))?;
            Ok(Some(TemporalStep::DayTime { micros }))
        }
        DataType::Duration(TimeUnit::Microsecond) => Ok(Some(TemporalStep::DayTime {
            micros: array.as_primitive::<DurationMicrosecondType>().value(index),
        })),
        data_type => wrong_sequence_input_types(std::slice::from_ref(data_type)),
    }
}

fn estimated_temporal_step(months: i32, days: i32, micros: i64) -> Result<i64> {
    let estimated = i128::from(micros)
        + i128::from(months) * i128::from(MICROS_PER_MONTH)
        + i128::from(days) * i128::from(MICROS_PER_DAY);
    i64::try_from(estimated)
        .map_err(|_| exec_datafusion_err!("sequence interval estimate does not fit in i64"))
}

fn scaled_i32(value: i32, index: usize, component: &str) -> Result<i32> {
    i32::try_from(i128::from(value) * index as i128)
        .map_err(|_| exec_datafusion_err!("sequence {component} interval component overflow"))
}

fn scaled_i64(value: i64, index: usize, component: &str) -> Result<i64> {
    i64::try_from(i128::from(value) * index as i128)
        .map_err(|_| exec_datafusion_err!("sequence {component} interval component overflow"))
}

fn checked_add_months(datetime: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    if months >= 0 {
        datetime.checked_add_months(Months::new(months as u32))
    } else {
        datetime.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

fn checked_add_days(datetime: NaiveDateTime, days: i32) -> Option<NaiveDateTime> {
    if days >= 0 {
        datetime.checked_add_days(Days::new(days as u64))
    } else {
        datetime.checked_sub_days(Days::new(u64::from(days.unsigned_abs())))
    }
}

fn localize_with_preferred_offset(
    timezone: Tz,
    datetime: NaiveDateTime,
    preferred_offset: i32,
) -> Result<DateTime<Tz>> {
    match timezone.from_local_datetime(&datetime) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(first, second) => {
            if first.offset().fix().local_minus_utc() == preferred_offset {
                Ok(first)
            } else if second.offset().fix().local_minus_utc() == preferred_offset {
                Ok(second)
            } else {
                Ok(first)
            }
        }
        LocalResult::None => {
            Ok(localize_with_fallback(&timezone, &datetime)?.with_timezone(&timezone))
        }
    }
}

fn add_ltz_interval(start: i64, months: i32, days: i32, micros: i64, timezone: Tz) -> Result<i64> {
    let mut datetime = as_datetime_with_timezone::<TimestampMicrosecondType>(start, timezone)
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence timestamp {start}"))?;

    if months != 0 {
        let preferred_offset = datetime.offset().fix().local_minus_utc();
        let local = checked_add_months(datetime.naive_local(), months)
            .ok_or_else(|| exec_datafusion_err!("cannot add {months} months to {start}"))?;
        datetime = localize_with_preferred_offset(timezone, local, preferred_offset)?;
    }
    if days != 0 {
        let preferred_offset = datetime.offset().fix().local_minus_utc();
        let local = checked_add_days(datetime.naive_local(), days)
            .ok_or_else(|| exec_datafusion_err!("cannot add {days} days to {start}"))?;
        datetime = localize_with_preferred_offset(timezone, local, preferred_offset)?;
    }

    datetime
        .with_timezone(&Utc)
        .checked_add_signed(TimeDelta::microseconds(micros))
        .map(|value| value.timestamp_micros())
        .ok_or_else(|| exec_datafusion_err!("cannot add {micros} microseconds to {start}"))
}

fn add_ntz_interval(start: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    let mut datetime = as_datetime::<TimestampMicrosecondType>(start)
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence timestamp {start}"))?;
    datetime = checked_add_months(datetime, months)
        .ok_or_else(|| exec_datafusion_err!("cannot add {months} months to {start}"))?;
    datetime = checked_add_days(datetime, days)
        .ok_or_else(|| exec_datafusion_err!("cannot add {days} days to {start}"))?;
    datetime = datetime
        .checked_add_signed(TimeDelta::microseconds(micros))
        .ok_or_else(|| exec_datafusion_err!("cannot add {micros} microseconds to {start}"))?;
    TimestampMicrosecondType::from_naive_datetime(datetime, None)
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence timestamp {datetime}"))
}

fn add_temporal_interval(
    start: i64,
    index: usize,
    months: i32,
    days: i32,
    micros: i64,
    timezone: Tz,
    timestamp_ntz: bool,
) -> Result<i64> {
    let months = scaled_i32(months, index, "month")?;
    let days = scaled_i32(days, index, "day")?;
    let micros = scaled_i64(micros, index, "microsecond")?;
    if timestamp_ntz {
        add_ntz_interval(start, months, days, micros)
    } else {
        add_ltz_interval(start, months, days, micros, timezone)
    }
}

fn temporal_timestamp_row(
    start: i64,
    stop: i64,
    step: TemporalStep,
    timezone: Tz,
    timestamp_ntz: bool,
    max_values: usize,
) -> Result<Vec<i64>> {
    let (months, days, micros) = step.parts()?;
    if months == 0 && days == 0 {
        return integral_row(start, stop, micros, max_values);
    }

    let estimated_step = estimated_temporal_step(months, days, micros)?;
    let estimated_length = sequence_length(start, stop, estimated_step)?;
    let mut values = Vec::new();
    reserve(&mut values, estimated_length.min(max_values))?;
    let ascending = estimated_step > 0;
    let mut index = 0_usize;
    loop {
        let value =
            add_temporal_interval(start, index, months, days, micros, timezone, timestamp_ntz)?;
        if (ascending && value > stop) || (!ascending && value < stop) {
            break;
        }
        if values.len() as i128 >= i128::from(MAX_ROUNDED_ARRAY_LENGTH) {
            return collection_size_limit_error(values.len() as i128 + 1);
        }
        if values.len() >= max_values {
            return exec_err!("sequence output exceeds Arrow List capacity");
        }
        if values.len() == values.capacity() {
            reserve(&mut values, 1)?;
        }
        values.push(value);
        index = index
            .checked_add(1)
            .ok_or_else(|| exec_datafusion_err!("sequence index overflow"))?;
    }
    Ok(values)
}

fn date_to_micros(date: i32, timezone: Tz) -> Result<i64> {
    let datetime = Date32Type::to_naive_date_opt(date)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence date {date}"))?;
    Ok(localize_with_fallback(&timezone, &datetime)?.timestamp_micros())
}

fn micros_to_date(micros: i64, timezone: Tz) -> Result<i32> {
    let date = as_datetime_with_timezone::<TimestampMicrosecondType>(micros, timezone)
        .map(|datetime| datetime.date_naive())
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence timestamp {micros}"))?;
    Ok(Date32Type::from_naive_date(date))
}

fn temporal_date_row(
    start: i32,
    stop: i32,
    step: TemporalStep,
    timezone: Tz,
    max_values: usize,
) -> Result<Vec<i32>> {
    let (months, days, micros) = step.parts()?;
    if months == 0 && days == 0 {
        return exec_err!("sequence step must be an interval with day granularity for DATE values");
    }
    if months == 0 && micros == 0 {
        return integral_row(start, stop, days, max_values);
    }

    let start_micros = date_to_micros(start, timezone)?;
    let stop_micros = date_to_micros(stop, timezone)?;
    let estimated_step = estimated_temporal_step(months, days, micros)?;
    let estimated_length = sequence_length(start_micros, stop_micros, estimated_step)?;
    let mut values = Vec::new();
    reserve(&mut values, estimated_length.min(max_values))?;
    let ascending = estimated_step > 0;
    let mut index = 0_usize;
    loop {
        let value =
            add_temporal_interval(start_micros, index, months, days, micros, timezone, false)?;
        if (ascending && value > stop_micros) || (!ascending && value < stop_micros) {
            break;
        }
        if values.len() as i128 >= i128::from(MAX_ROUNDED_ARRAY_LENGTH) {
            return collection_size_limit_error(values.len() as i128 + 1);
        }
        if values.len() >= max_values {
            return exec_err!("sequence output exceeds Arrow List capacity");
        }
        if values.len() == values.capacity() {
            reserve(&mut values, 1)?;
        }
        values.push(micros_to_date(value, timezone)?);
        index = index
            .checked_add(1)
            .ok_or_else(|| exec_datafusion_err!("sequence index overflow"))?;
    }
    Ok(values)
}

fn gen_sequence_date(args: &[ArrayRef], timezone: Tz) -> Result<ArrayRef> {
    let (start_array, stop_array, step_array) = match args {
        [start, stop] => (
            start.as_primitive::<Date32Type>(),
            stop.as_primitive::<Date32Type>(),
            None,
        ),
        [start, stop, step] => (
            start.as_primitive::<Date32Type>(),
            stop.as_primitive::<Date32Type>(),
            Some(step),
        ),
        _ => return invalid_sequence_arity(args.len()),
    };

    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(start_array.len() + 1);
    offsets.push(0);
    let mut validity = NullBufferBuilder::new(start_array.len());
    for index in 0..start_array.len() {
        if start_array.is_null(index) || stop_array.is_null(index) {
            append_null_offset(&values, &mut offsets, &mut validity)?;
            continue;
        }

        let start = start_array.value(index);
        let stop = stop_array.value(index);
        let step = match step_array {
            Some(array) => match temporal_step_at(array, index)? {
                Some(step) => step,
                None => {
                    append_null_offset(&values, &mut offsets, &mut validity)?;
                    continue;
                }
            },
            None => TemporalStep::default_for(i64::from(start), i64::from(stop)),
        };
        let remaining = i32::MAX as usize - values.len();
        let row = temporal_date_row(start, stop, step, timezone, remaining)?;
        append_row(&mut values, row, &mut offsets, &mut validity)?;
    }

    list_array(
        DataType::Date32,
        offsets,
        Arc::new(Date32Array::from(values)),
        &mut validity,
    )
}

fn gen_sequence_timestamp(
    args: &[ArrayRef],
    timezone: Tz,
    output_timezone: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let (start_array, stop_array, step_array) = match args {
        [start, stop] => (
            start.as_primitive::<TimestampMicrosecondType>(),
            stop.as_primitive::<TimestampMicrosecondType>(),
            None,
        ),
        [start, stop, step] => (
            start.as_primitive::<TimestampMicrosecondType>(),
            stop.as_primitive::<TimestampMicrosecondType>(),
            Some(step),
        ),
        _ => return invalid_sequence_arity(args.len()),
    };

    let timestamp_ntz = output_timezone.is_none();
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(start_array.len() + 1);
    offsets.push(0);
    let mut validity = NullBufferBuilder::new(start_array.len());
    for index in 0..start_array.len() {
        if start_array.is_null(index) || stop_array.is_null(index) {
            append_null_offset(&values, &mut offsets, &mut validity)?;
            continue;
        }

        let start = start_array.value(index);
        let stop = stop_array.value(index);
        let step = match step_array {
            Some(array) => match temporal_step_at(array, index)? {
                Some(step) => step,
                None => {
                    append_null_offset(&values, &mut offsets, &mut validity)?;
                    continue;
                }
            },
            None => TemporalStep::default_for(start, stop),
        };
        let remaining = i32::MAX as usize - values.len();
        let row = temporal_timestamp_row(start, stop, step, timezone, timestamp_ntz, remaining)?;
        append_row(&mut values, row, &mut offsets, &mut validity)?;
    }

    let values = TimestampMicrosecondArray::from(values).with_timezone_opt(output_timezone.clone());
    list_array(
        DataType::Timestamp(TimeUnit::Microsecond, output_timezone),
        offsets,
        Arc::new(values),
        &mut validity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_length_uses_wide_arithmetic_and_checked_limits() -> Result<()> {
        assert_eq!(sequence_length(i64::MIN, i64::MAX, i64::MAX)?, 3);
        assert_eq!(
            integral_row(i64::MIN, i64::MAX, i64::MAX, usize::MAX)?,
            vec![i64::MIN, -1, i64::MAX - 1]
        );
        assert_eq!(
            sequence_length(0, MAX_ROUNDED_ARRAY_LENGTH - 1, 1)?,
            MAX_ROUNDED_ARRAY_LENGTH as usize
        );
        assert!(sequence_length(0, MAX_ROUNDED_ARRAY_LENGTH, 1).is_err());
        assert!(checked_batch_length(i32::MAX as usize, 1).is_err());
        Ok(())
    }
}
