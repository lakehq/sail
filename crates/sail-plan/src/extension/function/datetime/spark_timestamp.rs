use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::utils::ItemTaker;

trait TimestampParser: Debug + Send + Sync {
    fn string_to_microseconds(&self, value: &str) -> Result<i64>;
}

#[derive(Debug)]
struct TimestampLtzParser {
    timezone: Tz,
}

impl TimestampParser for TimestampLtzParser {
    fn string_to_microseconds(&self, value: &str) -> Result<i64> {
        let (datetime, timezone) = parse_timestamp(value)
            .and_then(|x| x.into_naive())
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        let timezone = if timezone.is_empty() {
            self.timezone
        } else {
            timezone.parse()?
        };
        let datetime = localize_with_fallback(&timezone, &datetime)?;
        Ok(datetime.timestamp_micros())
    }
}

#[derive(Debug)]
struct TimestampNtzParser {}

impl TimestampParser for TimestampNtzParser {
    fn string_to_microseconds(&self, value: &str) -> Result<i64> {
        let (datetime, _timezone) = parse_timestamp(value)
            .and_then(|x| x.into_naive())
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        Ok(datetime.and_utc().timestamp_micros())
    }
}

#[derive(Debug)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: Box<dyn TimestampParser>,
    signature: Signature,
}

impl SparkTimestamp {
    pub fn try_new(timezone: Option<Arc<str>>) -> Result<Self> {
        let parser: Box<dyn TimestampParser> = if let Some(ref timezone) = timezone {
            let timezone = timezone
                .as_ref()
                .parse()
                .map_err(|e| plan_datafusion_err!("{e}"))?;
            Box::new(TimestampLtzParser { timezone })
        } else {
            Box::new(TimestampNtzParser {})
        };
        Ok(Self {
            timezone,
            parser,
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
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
            self.timezone.clone(),
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
                        .map(|x| x.map(|x| self.parser.string_to_microseconds(x)).transpose())
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| x.map(|x| self.parser.string_to_microseconds(x)).transpose())
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| x.map(|x| self.parser.string_to_microseconds(x)).transpose())
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                let array = array.with_timezone_opt(self.timezone.clone());
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|x| self.parser.string_to_microseconds(x))
                        .transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `timestamp`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    value,
                    self.timezone.clone(),
                )))
            }
        }
    }
}
