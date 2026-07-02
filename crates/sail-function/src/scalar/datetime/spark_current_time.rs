use std::collections::HashMap;
use std::sync::Arc;

use chrono::{TimeZone, Timelike};
use chrono_tz::Tz;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;

const NAME_PREFIX: &str = "spark_current_time_";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCurrentTime {
    signature: Signature,
    precision: i32,
    name: String,
}

impl SparkCurrentTime {
    pub fn new(precision: i32) -> Self {
        debug_assert!(Self::is_valid_precision(precision));
        Self {
            signature: Signature::nullary(Volatility::Stable),
            precision,
            name: format!("{NAME_PREFIX}{precision}"),
        }
    }

    pub fn precision_from_name(name: &str) -> Option<i32> {
        let precision = name.strip_prefix(NAME_PREFIX)?.parse::<i32>().ok()?;
        Self::is_valid_precision(precision).then_some(precision)
    }

    pub fn precision(&self) -> i32 {
        self.precision
    }

    fn is_valid_precision(precision: i32) -> bool {
        (0..=6).contains(&precision)
    }
}

impl ScalarUDFImpl for SparkCurrentTime {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = HashMap::from([(
            SAIL_SPARK_TIME_PRECISION_METADATA_KEY.to_string(),
            self.precision.to_string(),
        )]);
        Ok(Arc::new(
            Field::new(self.name(), DataType::Time64(TimeUnit::Nanosecond), false)
                .with_metadata(metadata),
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified current_time() function")
    }

    fn simplify(&self, _args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let now = info.query_execution_start_time().unwrap_or_default();
        let nanos = info
            .config_options()
            .execution
            .time_zone
            .as_ref()
            .and_then(|tz| tz.parse::<Tz>().ok())
            .map_or_else(
                || datetime_to_time_nanos(&now),
                |tz| {
                    let local_now = tz.from_utc_datetime(&now.naive_utc());
                    datetime_to_time_nanos(&local_now)
                },
            );
        let nanos = truncate_time_to_precision(nanos, self.precision);
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time64Nanosecond(Some(nanos)),
            None,
        )))
    }
}

fn datetime_to_time_nanos<T: TimeZone>(dt: &chrono::DateTime<T>) -> i64 {
    (dt.hour() as i64) * 3_600_000_000_000
        + (dt.minute() as i64) * 60_000_000_000
        + (dt.second() as i64) * 1_000_000_000
        + i64::from(dt.nanosecond())
}

fn truncate_time_to_precision(nanos: i64, precision: i32) -> i64 {
    let divisor = match precision {
        0 => 1_000_000_000,
        1 => 100_000_000,
        2 => 10_000_000,
        3 => 1_000_000,
        4 => 100_000,
        5 => 10_000,
        6 => 1_000,
        _ => unreachable!("current_time precision is validated when constructing the UDF"),
    };
    nanos / divisor * divisor
}
