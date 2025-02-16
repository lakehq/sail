use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Volatility,
};
use datafusion_expr_common::signature::{Signature, TypeSignature, TIMEZONE_WILDCARD};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkFromUtcTimestamp {
    signature: Signature,
    time_unit: TimeUnit,
}

impl SparkFromUtcTimestamp {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::LargeUtf8,
                    ]),
                ],
                Volatility::Immutable,
            ),
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
        internal_err!("`return_type` should not be called, call `return_type_from_args` instead")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        if args.arg_types.len() != 2 {
            return exec_err!(
                "Spark `from_utc_timestamp` function requires 2 arguments, got {}",
                args.arg_types.len()
            );
        }
        // FIXME: Second arg can be ColumnarValue::Array, but DataFusion doesn't support that.
        match &args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(tz))
            | Some(ScalarValue::Utf8View(tz))
            | Some(ScalarValue::LargeUtf8(tz)) => {
                Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    *self.time_unit(),
                    tz.as_ref().map(|tz| Arc::from(tz.to_string())),
                )))
            }
            other => exec_err!(
                "Second argument for `from_utc_timestamp` must be string, received {other:?}"
            ),
        }
    }

    // TODO: Implement this method after the FIXME above is fixed so we can accept array input.
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
        match timezone {
            Expr::Literal(ScalarValue::Utf8(tz))
            | Expr::Literal(ScalarValue::Utf8View(tz))
            | Expr::Literal(ScalarValue::LargeUtf8(tz)) => {
                let expr = Expr::Cast(datafusion_expr::Cast {
                    expr: Box::new(timestamp),
                    data_type: DataType::Timestamp(
                        *self.time_unit(),
                        tz.map(|tz| Arc::from(tz.to_string())),
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
