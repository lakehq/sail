use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Decimal128Array, DurationMicrosecondArray};
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::arrow::temporal_conversions::MICROSECONDS;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_part::DatePartFunc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDatePart {
    inner: DatePartFunc,
}

impl Default for SparkDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDatePart {
    pub fn new() -> Self {
        Self {
            inner: DatePartFunc::new(),
        }
    }

    fn invoke_seconds(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;

        args.get(1).map_or_else(
            || {
                exec_err!(
                    "Spark `date_part` function requires 2 arguments, got {}",
                    arg_fields.len()
                )
            },
            |second_arg| {
                match second_arg.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => {
                        truncate_duration_microseconds(second_arg.clone())
                    }
                    _ => self.inner.invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "microseconds".to_string(),
                            ))),
                            second_arg.clone(),
                        ],
                        arg_fields: arg_fields.clone(),
                        number_rows,
                        return_field: Arc::new(Field::new(
                            return_field.name(),
                            DataType::Int32,
                            true,
                        )),
                        config_options,
                    }),
                }
                .and_then(|value| value.cast_to(&DataType::Decimal128(8, 0), None))
                .and_then(|value| {
                    let (is_scalar, array) = match value {
                        ColumnarValue::Array(arr) => (false, arr),
                        ColumnarValue::Scalar(scalar) => (true, scalar.to_array()?),
                    };

                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .and_then(|arr| arr.clone().with_precision_and_scale(8, 6).ok())
                        .map_or_else(
                            || {
                                exec_err!(
                                    "Spark `date_part`: Error when cast microseconds to decimal"
                                )
                            },
                            |divided| {
                                if is_scalar {
                                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                                        &divided, 0,
                                    )?))
                                } else {
                                    Ok(ColumnarValue::Array(Arc::new(divided)))
                                }
                            },
                        )
                })
            },
        )
    }
}

impl ScalarUDFImpl for SparkDatePart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;

        field
            .and_then(|sv| sv.try_as_str())
            .flatten()
            .filter(|part| !part.is_empty())
            .filter(|part| {
                IntervalUnit::from_str(part).is_ok_and(|unit| matches!(unit, IntervalUnit::Second))
            })
            .map(|_| {
                Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Decimal128(8, 6),
                    true,
                )))
            })
            .unwrap_or_else(|| self.inner.return_field_from_args(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.return_field.data_type() {
            DataType::Decimal128(8, 6) => self.invoke_seconds(args),
            _ => self.inner.invoke_with_args(args),
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

fn truncate_duration_microseconds(value: ColumnarValue) -> Result<ColumnarValue> {
    let (is_scalar, array) = match value {
        ColumnarValue::Array(arr) => (false, arr),
        ColumnarValue::Scalar(scalar) => (true, scalar.to_array()?),
    };

    array
        .as_any()
        .downcast_ref::<DurationMicrosecondArray>()
        .map(|arr| {
            Arc::new(
                arr.iter()
                    .map(|v| v.map(|d| d % (60 * MICROSECONDS)))
                    .collect::<DurationMicrosecondArray>(),
            ) as ArrayRef
        })
        .map_or_else(
            || exec_err!("Spark `date_part`: Error truncating interval to seconds"),
            |result_array| {
                if is_scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &result_array,
                        0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(result_array))
                }
            },
        )
}
