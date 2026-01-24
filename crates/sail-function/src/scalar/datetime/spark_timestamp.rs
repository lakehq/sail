use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_timestamp;

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}

impl TimestampParser {
    fn string_to_microseconds(&self, value: &str, is_try: bool) -> Result<Option<i64>> {
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let (datetime, timezone) = match parsed {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(exec_datafusion_err!("{e}")),
                };
                let timezone: Tz = if timezone.is_empty() {
                    match default_timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                };
                let datetime = match localize_with_fallback(&timezone, &datetime) {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(e),
                };
                Ok(Some(datetime.timestamp_micros()))
            }
            TimestampParser::Ntz => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let (datetime, _timezone) = match parsed {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(exec_datafusion_err!("{e}")),
                };
                Ok(Some(datetime.and_utc().timestamp_micros()))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: TimestampParser,
    signature: Signature,
    is_try: bool,
}

impl SparkTimestamp {
    /// Creates a SparkTimestamp.
    ///
    /// When `is_try` is true, returns NULL on invalid input (for try_cast).
    /// When `is_try` is false, throws an error on invalid input (for cast).
    pub fn try_new(timezone: Option<Arc<str>>, is_try: bool) -> Result<Self> {
        let parser = if let Some(ref timezone) = timezone {
            TimestampParser::Ltz {
                default_timezone: timezone.as_ref().to_string(),
            }
        } else {
            TimestampParser::Ntz
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
            is_try,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn is_try(&self) -> bool {
        self.is_try
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
        let is_try = self.is_try;
        match arg {
            ColumnarValue::Array(array) => {
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| self.parser.string_to_microseconds(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| self.parser.string_to_microseconds(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| self.parser.string_to_microseconds(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                let array = array.with_timezone_opt(self.timezone.clone());
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| self.parser.string_to_microseconds(v, is_try))
                        .transpose()?
                        .flatten(),
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
