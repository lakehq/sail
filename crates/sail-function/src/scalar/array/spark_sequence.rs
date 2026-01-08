use std::any::Any;
use std::iter::from_fn;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Date32Builder, Int16Array, Int32Array, Int64Array, Int8Array,
    ListArray, ListBuilder, NullBufferBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Date32Type, Field, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalMonthDayNanoType, IntervalUnit, TimeUnit, TimestampMicrosecondType,
};
use datafusion::arrow::temporal_conversions::as_datetime_with_timezone;
use datafusion_common::{exec_datafusion_err, exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSequence {
    signature: Signature,
}

impl Default for SparkSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSequence {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSequence {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_sequence"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.iter().any(|t| t.is_null()) {
            Ok(DataType::Null)
        } else {
            Ok(DataType::List(Arc::new(Field::new_list_field(
                arg_types[0].clone(),
                true,
            ))))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.iter().any(|arg| arg.data_type().is_null()) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        match args[0].data_type() {
            DataType::Int8 => make_scalar_function(gen_sequence_i8)(&args),
            DataType::Int16 => make_scalar_function(gen_sequence_i16)(&args),
            DataType::Int32 => make_scalar_function(gen_sequence_i32)(&args),
            DataType::Int64 => make_scalar_function(gen_sequence_i64)(&args),
            DataType::Date32 => make_scalar_function(gen_sequence_date)(&args),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                make_scalar_function(gen_sequence_timestamp)(&args)
            }
            other => {
                exec_err!(
                    "Spark `sequence` function: Expected INT, DATE or TIMESTAMP, got: {other}"
                )
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 3 {
            return exec_err!(
                "Spark `sequence` function requires 1 to 3 arguments, got {}",
                arg_types.len()
            );
        }
        arg_types
            .iter()
            .map(|arg_type| {
                if arg_type.is_signed_integer() {
                    Ok(arg_type.clone())
                } else {
                    match arg_type {
                        DataType::UInt8 => Ok(DataType::Int16),
                        DataType::UInt16 => Ok(DataType::Int32),
                        DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
                        DataType::Null => Ok(DataType::Null),
                        DataType::Timestamp(_time_unit, tz) => {
                            Ok(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
                        }
                        DataType::Date32
                        | DataType::Date64
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View => Ok(DataType::Date32),
                        DataType::Interval(_) | DataType::Duration(_) => {
                            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                        }
                        other => {
                            exec_err!("Spark `sequence` function: unsupported type: {other}")
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>>>()
    }
}

fn gen_sequence_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "Spark `sequence` function requires 3 arguments for TIMESTAMP, got {}",
            args.len()
        );
    }

    let (start_array, start_tz, stop_array, stop_tz) =
        match (&args[0].data_type(), &args[1].data_type()) {
            (
                DataType::Timestamp(TimeUnit::Microsecond, start_tz),
                DataType::Timestamp(TimeUnit::Microsecond, stop_tz),
            ) => Ok((
                args[0].as_primitive::<TimestampMicrosecondType>(),
                start_tz,
                args[1].as_primitive::<TimestampMicrosecondType>(),
                stop_tz,
            )),
            _ => {
                internal_err!(
                    "Spark `sequence` function: expected timestamp arguments, got {args:?}"
                )
            }
        }?;
    let step_array = args[2].as_primitive::<IntervalMonthDayNanoType>();

    let values_builder = TimestampMicrosecondBuilder::new();
    let values_builder = if let Some(start_tz) = start_tz {
        values_builder.with_timezone(Arc::clone(start_tz))
    } else {
        values_builder
    };
    let mut list_builder = ListBuilder::new(values_builder);

    let start_tz = Tz::from_str(start_tz.as_deref().unwrap_or("+00")).map_err(|err| {
        exec_datafusion_err!(
            "Spark `sequence` function: failed to parse timezone {start_tz:?}: {err}"
        )
    })?;
    let stop_tz = Tz::from_str(stop_tz.as_deref().unwrap_or("+00")).map_err(|err| {
        exec_datafusion_err!(
            "Spark `sequence` function: failed to parse timezone {stop_tz:?}: {err}"
        )
    })?;

    for index in 0..start_array.len() {
        if start_array.is_null(index) || stop_array.is_null(index) || step_array.is_null(index) {
            list_builder.append_null();
            continue;
        }

        let start = start_array.value(index);
        let stop = stop_array.value(index);
        let step = step_array.value(index);

        let (months, days, ns) = IntervalMonthDayNanoType::to_parts(step);
        if months == 0 && days == 0 && ns == 0 {
            return exec_err!(
                "Spark `sequence` function cannot generate timestamp range less than 1 nanosecond."
            );
        }

        let negative = TimestampMicrosecondType::add_month_day_nano(start, step, start_tz)
            .ok_or(exec_datafusion_err!(
                "Spark `sequence` function cannot generate timestamp range for start + step: {start} + {step:?}"
            ))?
            < start;
        let stop_dt = as_datetime_with_timezone::<TimestampMicrosecondType>(stop, stop_tz).ok_or(
            exec_datafusion_err!(
                "Spark `sequence` function cannot generate timestamp for stop: {stop}: {stop_tz:?}"
            ),
        )?;
        let mut current = start;
        let mut current_dt = as_datetime_with_timezone::<TimestampMicrosecondType>(current, start_tz)
            .ok_or(exec_datafusion_err!(
                "Spark `sequence` function cannot generate timestamp for start: {current}: {start_tz:?}"
            ))?;

        let values = from_fn(|| {
            if (negative && current_dt < stop_dt) || (!negative && current_dt > stop_dt) {
                return None;
            }

            let prev_current = current;
            if let Some(ts) = TimestampMicrosecondType::add_month_day_nano(current, step, start_tz)
            {
                current = ts;
                current_dt =
                    as_datetime_with_timezone::<TimestampMicrosecondType>(current, start_tz)?;
                Some(Some(prev_current))
            } else {
                None
            }
        });

        list_builder.append_value(values);
    }

    let arr = Arc::new(list_builder.finish());

    Ok(arr)
}

fn gen_sequence_date(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "Spark `sequence` function requires 3 arguments for DATE, got {}",
            args.len()
        );
    }

    let (start_array, stop_array, step_array) = (
        Some(args[0].as_primitive::<Date32Type>()),
        args[1].as_primitive::<Date32Type>(),
        Some(args[2].as_primitive::<IntervalMonthDayNanoType>()),
    );

    let values_builder = Date32Builder::new();
    let mut list_builder = ListBuilder::new(values_builder);

    match (start_array, step_array) {
        (Some(start_array), Some(step_array)) => {
            for index in 0..stop_array.len() {
                if start_array.is_null(index)
                    || stop_array.is_null(index)
                    || step_array.is_null(index)
                {
                    list_builder.append_null();
                    continue;
                }

                let start = start_array.value(index);
                let stop = stop_array.value(index);
                let step = step_array.value(index);

                let (months, days, _nanoseconds) = IntervalMonthDayNanoType::to_parts(step);
                if months == 0 && days == 0 {
                    return exec_err!(
                        "Spark `sequence` function cannot generate date range less than 1 day."
                    );
                }

                let negative = months < 0 || days < 0;
                let mut new_date = start;
                let values = from_fn(|| {
                    if (negative && new_date < stop) || (!negative && new_date > stop) {
                        None
                    } else {
                        let current_date = new_date;
                        new_date = Date32Type::add_month_day_nano(new_date, step);
                        Some(Some(current_date))
                    }
                });

                list_builder.append_value(values);
            }
        }
        _ => {
            for _index in 0..stop_array.len() {
                list_builder.append_null()
            }
        }
    }

    Ok(Arc::new(list_builder.finish()))
}

macro_rules! impl_sequence_for_type {
    ($name:ident, $native_type:ty, $array_type:ty, $array_builder:ty, $data_type:expr, $zero:expr, $one:expr, $max:expr) => {
        fn $name(args: &[ArrayRef]) -> Result<ArrayRef> {
            let (start_array, stop_array, step_array) = match args.len() {
                1 => (None, args[0].as_primitive::<$array_type>(), None),
                2 => (
                    Some(args[0].as_primitive::<$array_type>()),
                    args[1].as_primitive::<$array_type>(),
                    None,
                ),
                3 => (
                    Some(args[0].as_primitive::<$array_type>()),
                    args[1].as_primitive::<$array_type>(),
                    Some(args[2].as_primitive::<$array_type>()),
                ),
                other => {
                    return exec_err!(
                        "Spark `sequence` function requires 1 to 3 arguments, got {other}"
                    )
                }
            };

            let mut values = vec![];
            let mut offsets = vec![0];
            let mut valid = NullBufferBuilder::new(stop_array.len());

            for (index, stop) in stop_array.iter().enumerate() {
                let start = start_array.map_or(Some($zero), |array| {
                    if !array.is_null(index) {
                        Some(array.value(index))
                    } else {
                        None
                    }
                });
                let step = step_array.map_or(Some($one), |array| {
                    if !array.is_null(index) {
                        Some(array.value(index))
                    } else {
                        None
                    }
                });

                match (start, stop, step) {
                    (None, _, _) | (_, None, _) | (_, _, None) => {
                        offsets.push(values.len() as i32);
                        valid.append_null();
                    }
                    (Some(start), Some(stop), Some(step)) => {
                        let step = if step == $zero { $one } else { step };

                        let decreasing = step < $zero;
                        let step_abs = step.unsigned_abs().to_usize().ok_or_else(|| {
                            exec_datafusion_err!("step {step} can't fit into usize")
                        })?;

                        let step_by_iter: Box<dyn Iterator<Item = $native_type>> = match decreasing
                        {
                            true => Box::new((stop..=start).rev()),
                            false => Box::new(start..=stop),
                        };

                        values.extend(step_by_iter.step_by(step_abs));
                        offsets.push(values.len() as i32);
                        valid.append_non_null();
                    }
                };
            }

            Ok(Arc::new(ListArray::try_new(
                Arc::new(Field::new_list_field($data_type, true)),
                OffsetBuffer::new(offsets.into()),
                Arc::new(<$array_builder>::from(values)),
                valid.finish(),
            )?))
        }
    };
}

impl_sequence_for_type!(
    gen_sequence_i8,
    i8,
    Int8Type,
    Int8Array,
    DataType::Int8,
    0i8,
    1i8,
    i8::MAX
);
impl_sequence_for_type!(
    gen_sequence_i16,
    i16,
    Int16Type,
    Int16Array,
    DataType::Int16,
    0i16,
    1i16,
    i16::MAX
);
impl_sequence_for_type!(
    gen_sequence_i32,
    i32,
    Int32Type,
    Int32Array,
    DataType::Int32,
    0i32,
    1i32,
    i32::MAX
);
impl_sequence_for_type!(
    gen_sequence_i64,
    i64,
    Int64Type,
    Int64Array,
    DataType::Int64,
    0i64,
    1i64,
    i64::MAX
);
