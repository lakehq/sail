use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::kernels::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::datetime::to_local_time::ToLocalTimeFunc;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::utils::datetime::timezone_offset;

// Preserve timestamp nullability and units while sharing the named-zone rules.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkToLocalTime {
    inner: ToLocalTimeFunc,
}

impl SparkToLocalTime {
    pub(crate) fn convert(value: &ColumnarValue) -> Result<ColumnarValue> {
        let (unit, timezone) = match value.data_type() {
            DataType::Timestamp(unit, Some(timezone)) => (unit, timezone),
            DataType::Timestamp(_, None) => return Ok(value.clone()),
            DataType::Null => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    None, None,
                )));
            }
            data_type => {
                return internal_err!("to_local_time requires a timestamp, got {data_type}");
            }
        };
        let timezone = timezone.parse::<datafusion::arrow::array::timezone::Tz>()?;
        let units = time_unit_to_multiplier(&unit);
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let values = value.cast_to(&DataType::Int64, None)?.to_array(1)?;
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_datafusion_err!("expected timestamp integers"))?;
        let values = values
            .iter()
            .map(|value| {
                value
                    .map(|value| {
                        let seconds = value.div_euclid(units);
                        let offset = timezone_offset(&timezone, seconds)?;
                        let shifted = i128::from(value)
                            + i128::from(offset.local_minus_utc()) * i128::from(units);
                        let shifted = i64::try_from(shifted)
                            .map_err(|_| exec_datafusion_err!("local timestamp out of range"))?;
                        if DateTime::from_timestamp(seconds, 0).is_none()
                            || DateTime::from_timestamp(shifted.div_euclid(units), 0).is_none()
                        {
                            return exec_err!("local timestamp out of range");
                        }
                        Ok(shifted)
                    })
                    .transpose()
            })
            .collect::<Result<Int64Array>>()?;
        let values = cast::cast(&values, &DataType::Timestamp(unit, None))?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &values, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(values))
        }
    }
}

impl ScalarUDFImpl for SparkToLocalTime {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields[0].is_nullable();
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::convert(&args.args[0])
    }
}
