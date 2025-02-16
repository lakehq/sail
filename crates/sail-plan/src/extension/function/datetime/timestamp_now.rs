use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct TimestampNow {
    signature: Signature,
    timezone: Arc<str>,
    time_unit: TimeUnit,
}

impl TimestampNow {
    pub fn new(timezone: Arc<str>, time_unit: TimeUnit) -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            timezone,
            time_unit,
        }
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn time_unit(&self) -> &TimeUnit {
        &self.time_unit
    }
}

impl ScalarUDFImpl for TimestampNow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "timestamp_now"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            *self.time_unit(),
            Some(self.timezone().into()),
        ))
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified timestamp_now() function")
    }

    fn simplify(&self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        let now = info.execution_props().query_execution_start_time;
        let now = match self.time_unit() {
            TimeUnit::Second => Some(now.timestamp()),
            TimeUnit::Millisecond => Some(now.timestamp_millis()),
            TimeUnit::Microsecond => Some(now.timestamp_micros()),
            TimeUnit::Nanosecond => now.timestamp_nanos_opt(),
        };
        let expr = Expr::Cast(datafusion_expr::Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Int64(now))),
            data_type: DataType::Timestamp(*self.time_unit(), Some(self.timezone().into())),
        });
        Ok(ExprSimplifyResult::Simplified(expr))
    }
}
