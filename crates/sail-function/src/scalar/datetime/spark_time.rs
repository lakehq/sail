use std::any::Any;
use std::sync::Arc;

use chrono::{NaiveTime, Timelike};
use datafusion::arrow::array::Time64MicrosecondArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_time;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTime {
    signature: Signature,
    is_try: bool,
}

impl SparkTime {
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
            is_try,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn string_to_time_micros(value: &str, is_try: bool) -> Result<Option<i64>> {
        let result = parse_time(value)
            .map_err(|e| exec_datafusion_err!("{e}"))
            .and_then(|t| NaiveTime::try_from(t).map_err(|e| exec_datafusion_err!("{e}")))
            .map(|naive_time| {
                let seconds_from_midnight = naive_time.num_seconds_from_midnight() as i64;
                let nanoseconds = naive_time.nanosecond() as i64;
                seconds_from_midnight * 1_000_000 + nanoseconds / 1_000
            });
        match result {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl ScalarUDFImpl for SparkTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        let is_try = self.is_try;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| Self::string_to_time_micros(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Time64MicrosecondArray>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| Self::string_to_time_micros(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Time64MicrosecondArray>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| Self::string_to_time_micros(v, is_try))
                                .transpose()
                                .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Time64MicrosecondArray>>()?,
                    _ => return exec_err!("expected string array for `time`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| Self::string_to_time_micros(v, is_try))
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!("expected string scalar for `time`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(value)))
            }
        }
    }
}
