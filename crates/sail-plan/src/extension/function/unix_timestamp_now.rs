use std::any::Any;

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(crate) struct UnixTimestampNow {
    signature: Signature,
}

impl Default for UnixTimestampNow {
    fn default() -> Self {
        Self::new()
    }
}

impl UnixTimestampNow {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::uniform(0, vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for UnixTimestampNow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unix_timestamp_now"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified unix_timestamp_now() function")
    }

    fn simplify(&self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        let now = info
            .execution_props()
            .query_execution_start_time
            .timestamp();
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Int64(Some(now)),
        )))
    }
}
