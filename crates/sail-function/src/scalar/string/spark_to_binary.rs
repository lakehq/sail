use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions::encoding::expr_fn::decode;
use datafusion::functions::encoding::inner::DecodeFunc;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, expr};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::math::spark_unhex::SparkUnHex;
use crate::scalar::string::spark_base64::SparkUnbase64;

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        "spark_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    // Spark: `ToBinary` is `RuntimeReplaceable` and its nullability comes from the
    // format-dependent replacement (stringExpressions.scala:3249): `hex` and the
    // single-argument form resolve to `Unhex` and `utf-8` to `Encode`, both nullable, while
    // `base64` resolves to `UnBase64`, a null-intolerant `UnaryExpression` with no `nullable`
    // override, so that branch follows the input.
    //
    // `simplify` below rewrites the call for execution, but the output field is resolved from
    // this hook beforehand, so the branch has to be made here: delegating to the simplified
    // expression would report `to_binary(.., 'base64')` nullable while `unbase64` is not.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let format = args
            .scalar_arguments
            .get(1)
            .copied()
            .flatten()
            .and_then(|value| value.try_as_str())
            .flatten()
            .map(|format| format.trim().to_lowercase());
        let nullable = match format.as_deref() {
            Some("base64") => args.arg_fields.iter().any(|field| field.is_nullable()),
            _ => true,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            nullable,
        )))
    }

    // This will only be called by TryToBinary
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 && args.args.len() != 2 {
            return exec_err!(
                "Spark `to_binary` function requires 1 or 2 arguments, got {}",
                args.args.len()
            );
        }
        if args.args.len() == 1 {
            SparkUnHex::new().invoke_with_args(args)
        } else {
            match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "utf-8" || s.trim().to_lowercase() == "utf8" =>
                {
                    args.args[0].cast_to(&DataType::Binary, None)
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "hex" =>
                {
                    let ScalarFunctionArgs {
                        args,
                        arg_fields,
                        number_rows,
                        return_field,
                        config_options,
                    } = args;
                    let args = ScalarFunctionArgs {
                        args: args[0..1].to_vec(),
                        arg_fields: arg_fields[0..1].to_vec(),
                        number_rows,
                        return_field,
                        config_options,
                    };
                    SparkUnHex::new().invoke_with_args(args)
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    if s.trim().to_lowercase() == "base64" =>
                {
                    let ScalarFunctionArgs {
                        args,
                        arg_fields,
                        number_rows,
                        return_field,
                        config_options,
                    } = args;
                    let args = ScalarFunctionArgs {
                        args: args[0..1].to_vec(),
                        arg_fields: arg_fields[0..1].to_vec(),
                        number_rows,
                        return_field,
                        config_options,
                    };
                    SparkUnbase64::new().invoke_with_args(args)
                }
                _ => DecodeFunc::new().invoke_with_args(args),
            }
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
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
                Expr::Literal(ScalarValue::Utf8(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::Utf8View(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _metadata)
                    if s.trim().to_lowercase() == "utf-8" || s.trim().to_lowercase() == "utf8" =>
                {
                    Ok(ExprSimplifyResult::Simplified(Expr::Cast(expr::Cast::new(
                        Box::new(expr),
                        DataType::Binary,
                    ))))
                }
                Expr::Literal(ScalarValue::Utf8(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::Utf8View(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _metadata)
                    if s.trim().to_lowercase() == "hex" =>
                {
                    Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                        expr::ScalarFunction {
                            func: Arc::new(ScalarUDF::from(SparkUnHex::new())),
                            args: vec![expr],
                        },
                    )))
                }
                Expr::Literal(ScalarValue::Utf8(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::Utf8View(Some(s)), _metadata)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _metadata)
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        "spark_try_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    // `try_*` swallows the failure and yields NULL, so the output is always nullable
    // (TryEval.scala:51).
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result = SparkToBinary::new().invoke_with_args(args);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        }
    }
}
