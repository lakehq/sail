use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::datetime::timestamp::{
    parse_timestamp, parse_timezone, TimestampValue,
};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkTimestamp {
    timezone: Arc<str>,
    signature: Signature,
}

impl SparkTimestamp {
    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            timezone,
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }

    fn string_to_timestamp_microseconds<T: AsRef<str>>(&self, value: T) -> Result<i64> {
        let timestamp = parse_timestamp(value.as_ref())?;
        let datetime = match timestamp {
            TimestampValue::WithTimeZone(x) => x,
            TimestampValue::WithoutTimeZone(x) => {
                // FIXME: avoid expensive time zone parsing
                let tz = parse_timezone(&self.timezone)?;
                tz.localize_with_fallback(&x)?
            }
        };
        Ok(datetime.timestamp_micros())
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(&self.timezone)),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|x| self.string_to_timestamp_microseconds(x))
                        .transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `timestamp`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    value,
                    Some(Arc::from("UTC")),
                )))
            }
        }
    }
}
