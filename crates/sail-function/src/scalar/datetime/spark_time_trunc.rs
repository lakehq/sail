use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, PrimitiveArray, PrimitiveBuilder, StringArrayType,
};
use datafusion::arrow::datatypes::{DataType, Time64MicrosecondType, TimeUnit};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeTrunc {
    signature: Signature,
}

impl Default for SparkTimeTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Returns the truncation divisor in microseconds for a given unit string.
/// Returns `None` for unrecognized units.
fn truncation_divisor(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "MICROSECOND" => Some(1),
        "MILLISECOND" => Some(1_000),
        "SECOND" => Some(1_000_000),
        "MINUTE" => Some(60_000_000),
        "HOUR" => Some(3_600_000_000),
        _ => None,
    }
}

impl ScalarUDFImpl for SparkTimeTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_time_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        let [unit_arg, time_arg] = args.as_slice() else {
            return exec_err!(
                "Spark `time_trunc` function requires 2 arguments, got {}",
                args.len()
            );
        };

        match (unit_arg, time_arg) {
            // (Scalar unit, Scalar time) — return a scalar
            (ColumnarValue::Scalar(unit_sv), ColumnarValue::Scalar(time_sv)) => {
                let unit_opt = match unit_sv {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        Some(s.as_str())
                    }
                    ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => {
                        None
                    }
                    _ => return exec_err!("time_trunc: unit must be a string"),
                };
                let time_opt = match time_sv {
                    ScalarValue::Time64Microsecond(v) => *v,
                    _ => return exec_err!("time_trunc: time must be TIME"),
                };
                let result = match (unit_opt, time_opt) {
                    (Some(unit), Some(time)) => {
                        let divisor = match truncation_divisor(unit) {
                            Some(d) => d,
                            None => return exec_err!(
                                "time_trunc: unsupported unit '{}'. Supported: HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND",
                                unit
                            ),
                        };
                        Some(time - (time % divisor))
                    }
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(
                    result,
                )))
            }
            // (Scalar unit, Array time) — constant divisor across all rows
            (ColumnarValue::Scalar(unit_sv), ColumnarValue::Array(time_array)) => {
                let unit_opt = match unit_sv {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        Some(s.as_str())
                    }
                    ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => {
                        None
                    }
                    _ => return exec_err!("time_trunc: unit must be a string"),
                };
                match unit_opt {
                    None => Ok(ColumnarValue::Array(new_null_array(
                        &DataType::Time64(TimeUnit::Microsecond),
                        number_rows,
                    ))),
                    Some(unit) => {
                        let divisor = match truncation_divisor(unit) {
                            Some(d) => d,
                            None => return exec_err!(
                                "time_trunc: unsupported unit '{}'. Supported: HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND",
                                unit
                            ),
                        };
                        let times = time_array.as_primitive::<Time64MicrosecondType>();
                        let result = times
                            .iter()
                            .map(|t| t.map(|v| v - (v % divisor)))
                            .collect::<PrimitiveArray<Time64MicrosecondType>>();
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                }
            }
            // (Array unit, Scalar or Array time) — per-row unit lookup
            (ColumnarValue::Array(unit_array), time_arg) => {
                let times = match time_arg {
                    ColumnarValue::Array(a) => a.as_primitive::<Time64MicrosecondType>().to_owned(),
                    ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(v))) => {
                        PrimitiveArray::<Time64MicrosecondType>::from_value(*v, number_rows)
                    }
                    ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None)) => {
                        return Ok(ColumnarValue::Array(new_null_array(
                            &DataType::Time64(TimeUnit::Microsecond),
                            number_rows,
                        )));
                    }
                    _ => return exec_err!("time_trunc: time must be TIME"),
                };
                let result = match unit_array.data_type() {
                    DataType::Utf8 => {
                        trunc_time_rows(unit_array.as_string::<i32>(), &times, number_rows)
                    }
                    DataType::LargeUtf8 => {
                        trunc_time_rows(unit_array.as_string::<i64>(), &times, number_rows)
                    }
                    DataType::Utf8View => {
                        trunc_time_rows(unit_array.as_string_view(), &times, number_rows)
                    }
                    _ => exec_err!("time_trunc: unit must be a string"),
                }?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `time_trunc` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
        ])
    }
}

/// Per-row truncation when unit comes from a string array column.
fn trunc_time_rows<'a, S>(
    unit_array: &'a S,
    times: &PrimitiveArray<Time64MicrosecondType>,
    number_rows: usize,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let mut builder = PrimitiveBuilder::<Time64MicrosecondType>::with_capacity(number_rows);
    for (unit_opt, time_opt) in unit_array.iter().zip(times.iter()) {
        match (unit_opt, time_opt) {
            (None, _) | (_, None) => builder.append_null(),
            (Some(unit), Some(val)) => match truncation_divisor(unit) {
                Some(divisor) => builder.append_value(val - (val % divisor)),
                None => {
                    return exec_err!(
                        "time_trunc: unsupported unit '{}'. Supported: HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND",
                        unit
                    )
                }
            },
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
