use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Decimal128Array, DurationMicrosecondArray};
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::arrow::temporal_conversions::MICROSECONDS;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
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
                        truncate_duration_microseconds(second_arg.clone(), 60 * MICROSECONDS)
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

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.return_field.data_type() {
            DataType::Decimal128(8, 6) => self.invoke_seconds(args),
            _ => {
                let modulus = args
                    .args
                    .first()
                    .and_then(|part| match part {
                        ColumnarValue::Scalar(part) => part.try_as_str().flatten(),
                        _ => None,
                    })
                    .and_then(|part| IntervalUnit::from_str(part).ok())
                    .and_then(|unit| match unit {
                        IntervalUnit::Hour => Some(24 * 60 * 60 * MICROSECONDS),
                        IntervalUnit::Minute => Some(60 * 60 * MICROSECONDS),
                        _ => None,
                    });
                if let (Some(modulus), Some(value)) = (modulus, args.args.get(1))
                    && matches!(value.data_type(), DataType::Duration(TimeUnit::Microsecond))
                {
                    args.args[1] = truncate_duration_microseconds(value.clone(), modulus)?;
                }
                self.inner.invoke_with_args(args)
            }
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

fn truncate_duration_microseconds(value: ColumnarValue, modulus: i64) -> Result<ColumnarValue> {
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
                    .map(|v| v.map(|d| d % modulus))
                    .collect::<DurationMicrosecondArray>(),
            ) as ArrayRef
        })
        .map_or_else(
            || exec_err!("Spark `date_part`: Error truncating interval component"),
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, StringArray};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn invoke_duration_part_value(
        part: ColumnarValue,
        values: Vec<Option<i64>>,
    ) -> Result<Vec<Option<i32>>> {
        let number_rows = values.len();
        let duration_type = DataType::Duration(TimeUnit::Microsecond);
        let result = SparkDatePart::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                part,
                ColumnarValue::Array(Arc::new(DurationMicrosecondArray::from(values))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("part", DataType::Utf8, false)),
                Arc::new(Field::new("value", duration_type, true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Int32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        Ok(result
            .into_array(number_rows)?
            .as_primitive::<Int32Type>()
            .iter()
            .collect())
    }

    fn invoke_duration_part(part: &str, values: Vec<Option<i64>>) -> Result<Vec<Option<i32>>> {
        invoke_duration_part_value(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part.to_string()))),
            values,
        )
    }

    #[test]
    fn duration_hour_and_minute_are_components() -> Result<()> {
        let values = vec![
            Some(
                2 * 24 * 60 * 60 * MICROSECONDS
                    + 5 * 60 * 60 * MICROSECONDS
                    + 3 * 60 * MICROSECONDS,
            ),
            Some(
                -(2 * 24 * 60 * 60 * MICROSECONDS
                    + 5 * 60 * 60 * MICROSECONDS
                    + 3 * 60 * MICROSECONDS),
            ),
            None,
        ];

        assert_eq!(
            invoke_duration_part("hour", values.clone())?,
            vec![Some(5), Some(-5), None]
        );
        assert_eq!(
            invoke_duration_part("minute", values)?,
            vec![Some(3), Some(-3), None]
        );
        Ok(())
    }

    #[test]
    fn duration_component_rejects_non_scalar_part() {
        assert!(
            invoke_duration_part_value(
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["hour"]))),
                vec![Some(0)],
            )
            .is_err()
        );
    }

    #[test]
    fn duration_component_rejects_non_duration_value() {
        assert!(
            truncate_duration_microseconds(
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
                MICROSECONDS,
            )
            .is_err()
        );
    }
}
