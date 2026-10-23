use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::encoding::inner::DecodeFunc;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

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
        sail_type_system::policies::scalar_string_spark_to_binary_typing::spark_to_binary_return_type(_arg_types)
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
        sail_type_system::policies::scalar_string_spark_to_binary_typing::spark_try_to_binary_return_type(_arg_types)
    }

    /// Spark: `TryToBinary` is `RuntimeReplaceable` over `TryEval`, whose
    /// `nullable = true` is unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_string_spark_to_binary_typing::spark_try_to_binary_return_field_from_args(self.name(), _args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result = SparkToBinary::new().invoke_with_args(args);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        }
    }
}
