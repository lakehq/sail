use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, Duration, Weekday};
use datafusion::arrow::array::{new_null_array, ArrayRef, AsArray, Date32Array, StringArrayType};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkNextDay {
    signature: Signature,
}

impl Default for SparkNextDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNextDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNextDay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `next_day` function requires 2 arguments, got {}",
                args.len()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(date), ColumnarValue::Scalar(day_of_week)) => {
                match (date, day_of_week) {
                    (ScalarValue::Date32(days), ScalarValue::Utf8(day_of_week) | ScalarValue::Utf8View(day_of_week) | ScalarValue::LargeUtf8(day_of_week)) => {
                        if let Some(days) = days {
                            if let Some(day_of_week) = day_of_week {
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                                    spark_next_day(*days, day_of_week.as_str()),
                                )))
                            } else {
                                // TODO: if spark.sql.ansi.enabled is false,
                                //  returns NULL instead of an error for a malformed dayOfWeek.
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                            }
                        } else {
                            Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                        }
                    }
                    _ => exec_err!("Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"),
                }
            }
            (ColumnarValue::Array(date_array), ColumnarValue::Scalar(day_of_week)) => {
                match (date_array.data_type(), day_of_week) {
                    (DataType::Date32, ScalarValue::Utf8(day_of_week) | ScalarValue::Utf8View(day_of_week) | ScalarValue::LargeUtf8(day_of_week)) => {
                        if let Some(day_of_week) = day_of_week {
                            let result: Date32Array = date_array
                                .as_primitive::<Date32Type>()
                                .unary_opt(|days| spark_next_day(days, day_of_week.as_str()))
                                .with_data_type(DataType::Date32);
                            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                        } else {
                            // TODO: if spark.sql.ansi.enabled is false,
                            //  returns NULL instead of an error for a malformed dayOfWeek.
                            Ok(ColumnarValue::Array(Arc::new(new_null_array(&DataType::Date32, date_array.len()))))
                        }
                    }
                    _ => exec_err!("Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"),
                }
            }
            (ColumnarValue::Array(date_array), ColumnarValue::Array(day_of_week_array)) => {
                let result = match (date_array.data_type(), day_of_week_array.data_type()) {
                    (
                        DataType::Date32,
                        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                    ) => {
                        let date_array: &Date32Array = date_array.as_primitive::<Date32Type>();
                        match day_of_week_array.data_type() {
                            DataType::Utf8 => {
                                let day_of_week_array = day_of_week_array.as_string::<i32>();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            DataType::LargeUtf8 => {
                                let day_of_week_array = day_of_week_array.as_string::<i64>();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            DataType::Utf8View => {
                                let day_of_week_array = day_of_week_array.as_string_view();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            other => {
                                exec_err!("Spark `next_day` function: second arg must be string. Got {other:?}")
                            }
                        }
                    }
                    (left, right) => {
                        exec_err!(
                            "Spark `next_day` function: first arg must be date, second arg must be string. Got {left:?}, {right:?}"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            _ => exec_err!("Unsupported args {args:?} for Spark function `next_day`"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `next_day` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }

        let current_native_type: NativeType = (&arg_types[0]).into();
        if matches!(current_native_type, NativeType::Date)
            || matches!(current_native_type, NativeType::String)
            || matches!(current_native_type, NativeType::Null)
        {
            if matches!(&arg_types[1], DataType::Utf8)
                || matches!(&arg_types[1], DataType::Utf8View)
                || matches!(&arg_types[1], DataType::LargeUtf8)
            {
                Ok(vec![DataType::Date32, arg_types[1].clone()])
            } else {
                plan_err!(
                    "The second argument of the Spark `next_day` function must be a string, but got {}",
                    &arg_types[1]
                )
            }
        } else {
            plan_err!(
                "The first argument of the Spark `next_day` function can only be a date or string, but got {}", &arg_types[0]
            )
        }
    }
}

fn process_next_day_arrays<'a, S>(
    date_array: &Date32Array,
    day_of_week_array: &'a S,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let result = date_array
        .iter()
        .zip(day_of_week_array.iter())
        .map(|(days, day_of_week)| {
            if let Some(days) = days {
                if let Some(day_of_week) = day_of_week {
                    spark_next_day(days, day_of_week)
                } else {
                    // TODO: if spark.sql.ansi.enabled is false,
                    //  returns NULL instead of an error for a malformed dayOfWeek.
                    None
                }
            } else {
                None
            }
        })
        .collect::<Date32Array>();
    Ok(Arc::new(result) as ArrayRef)
}

fn spark_next_day(days: i32, day_of_week: &str) -> Option<i32> {
    let date = Date32Type::to_naive_date(days);

    let day_of_week = day_of_week.trim().to_uppercase();
    let day_of_week = match day_of_week.as_str() {
        "MO" | "MON" | "MONDAY" => Some("MONDAY"),
        "TU" | "TUE" | "TUESDAY" => Some("TUESDAY"),
        "WE" | "WED" | "WEDNESDAY" => Some("WEDNESDAY"),
        "TH" | "THU" | "THURSDAY" => Some("THURSDAY"),
        "FR" | "FRI" | "FRIDAY" => Some("FRIDAY"),
        "SA" | "SAT" | "SATURDAY" => Some("SATURDAY"),
        "SU" | "SUN" | "SUNDAY" => Some("SUNDAY"),
        _ => {
            // TODO: if spark.sql.ansi.enabled is false,
            //  returns NULL instead of an error for a malformed dayOfWeek.
            None
        }
    };

    if let Some(day_of_week) = day_of_week {
        let day_of_week = day_of_week.parse::<Weekday>();
        match day_of_week {
            Ok(day_of_week) => Some(Date32Type::from_naive_date(
                date + Duration::days((7 - date.weekday().days_since(day_of_week)) as i64),
            )),
            Err(_) => {
                // TODO: if spark.sql.ansi.enabled is false,
                //  returns NULL instead of an error for a malformed dayOfWeek.
                None
            }
        }
    } else {
        None
    }
}
