use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{exec_err, internal_err, ExprSchema, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkFromUtcTimestamp {
    signature: Signature,
    time_unit: TimeUnit,
}

impl SparkFromUtcTimestamp {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            time_unit,
        }
    }

    pub fn time_unit(&self) -> &TimeUnit {
        &self.time_unit
    }
}

impl ScalarUDFImpl for SparkFromUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_from_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("`return_type` should not be called, call `return_type_from_exprs` instead")
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `from_utc_timestamp` function requires 2 arguments, got {}",
                args.len()
            );
        }
        match &args[1] {
            Expr::Literal(ScalarValue::Utf8(tz))
            | Expr::Literal(ScalarValue::Utf8View(tz))
            | Expr::Literal(ScalarValue::LargeUtf8(tz)) => Ok(DataType::Timestamp(
                *self.time_unit(),
                tz.as_ref().map(|tz| Arc::from(tz.to_string())),
            )),
            _ => exec_err!(
                "Second argument for `from_utc_timestamp` must be string, received {:?}",
                arg_types[1]
            ),
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("`invoke` should not be called on a simplified `from_utc_timestamp` function")
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `from_utc_timestamp` function requires 2 arguments, got {}",
                args.len()
            );
        }
        let (timestamp, timezone) = args.two()?;
        match &timezone {
            Expr::Literal(ScalarValue::Utf8(tz))
            | Expr::Literal(ScalarValue::Utf8View(tz))
            | Expr::Literal(ScalarValue::LargeUtf8(tz)) => {
                let expr = Expr::Cast(datafusion_expr::Cast {
                    expr: Box::new(timestamp),
                    data_type: DataType::Timestamp(
                        *self.time_unit(),
                        tz.as_ref().map(|tz| Arc::from(tz.to_string())),
                    ),
                });
                Ok(ExprSimplifyResult::Simplified(expr))
            }
            other => exec_err!(
                "Second argument for `from_utc_timestamp` must be string, received {other:?}"
            ),
        }
    }
}
