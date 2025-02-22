use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::encoding::expr_fn::decode;
use datafusion::functions::encoding::inner::DecodeFunc;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{expr, Expr, ScalarUDF, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

use crate::extension::function::math::spark_hex_unhex::SparkUnHex;
use crate::extension::function::string::spark_base64::SparkUnbase64;
use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkToBinary {
    signature: Signature,
}

impl Default for SparkToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkToBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    // This will only be called by TryToBinary
    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 && args.len() != 2 {
            return exec_err!(
                "Spark `to_binary` function requires 1 or 2 arguments, got {}",
                args.len()
            );
        }
        if args.len() == 1 {
            SparkUnHex::new().invoke_batch(args, number_rows)
        } else {
            match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "utf-8" || s.trim().to_lowercase() == "utf8" =>
                {
                    args[0].cast_to(&DataType::Binary, None)
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "hex" =>
                {
                    SparkUnHex::new().invoke_batch(&args[0..1], number_rows)
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "base64" =>
                {
                    SparkUnbase64::new().invoke_batch(&args[0..1], number_rows)
                }
                _ => DecodeFunc::new().invoke_batch(args, number_rows),
            }
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if args.len() != 1 && args.len() != 2 {
            return exec_err!(
                "Spark `to_binary` function requires 1 or 2 arguments, got {}",
                args.len()
            );
        }
        if args.len() == 1 {
            let expr = args.one()?;
            Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(SparkUnHex::new())),
                    args: vec![expr],
                },
            )))
        } else {
            let (expr, format) = args.two()?;
            match &format {
                Expr::Literal(ScalarValue::Utf8(Some(s)))
                | Expr::Literal(ScalarValue::Utf8View(Some(s)))
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)))
                    if s.trim().to_lowercase() == "utf-8" || s.trim().to_lowercase() == "utf8" =>
                {
                    Ok(ExprSimplifyResult::Simplified(Expr::Cast(expr::Cast {
                        expr: Box::new(expr),
                        data_type: DataType::Binary,
                    })))
                }
                Expr::Literal(ScalarValue::Utf8(Some(s)))
                | Expr::Literal(ScalarValue::Utf8View(Some(s)))
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)))
                    if s.trim().to_lowercase() == "hex" =>
                {
                    Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                        expr::ScalarFunction {
                            func: Arc::new(ScalarUDF::from(SparkUnHex::new())),
                            args: vec![expr],
                        },
                    )))
                }
                Expr::Literal(ScalarValue::Utf8(Some(s)))
                | Expr::Literal(ScalarValue::Utf8View(Some(s)))
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)))
                    if s.trim().to_lowercase() == "base64" =>
                {
                    Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                        expr::ScalarFunction {
                            func: Arc::new(ScalarUDF::from(SparkUnbase64::new())),
                            args: vec![expr],
                        },
                    )))
                }
                _ => Ok(ExprSimplifyResult::Simplified(decode(expr, format))),
            }
        }
    }
}

#[derive(Debug)]
pub struct SparkTryToBinary {
    signature: Signature,
}

impl Default for SparkTryToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTryToBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let result = SparkToBinary::new().invoke_batch(args, number_rows);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        }
    }
}
