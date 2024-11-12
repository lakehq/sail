use std::any::Any;
use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::string::concat::ConcatFunc;
use datafusion_common::utils::list_ndims;
use datafusion_common::{plan_err, Result};
use datafusion_expr::type_coercion::binary::get_wider_type;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions_nested::concat::ArrayConcat;

#[derive(Debug)]
pub struct SparkConcat {
    signature: Signature,
}

impl Default for SparkConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// [Credit]: <https://github.com/apache/datafusion/blob/7b2284c8a0b49234e9607bfef10d73ef788d9458/datafusion/functions-nested/src/concat.rs#L274-L301>
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types
            .iter()
            .any(|arg_type| matches!(arg_type, DataType::List(_)))
        {
            let mut expr_type = DataType::Null;
            let mut max_dims = 0;
            for arg_type in arg_types {
                match arg_type {
                    DataType::List(field) => {
                        if !field.data_type().equals_datatype(&DataType::Null) {
                            let dims = list_ndims(arg_type);
                            expr_type = match max_dims.cmp(&dims) {
                                Ordering::Greater => expr_type,
                                Ordering::Equal => get_wider_type(&expr_type, arg_type)?,
                                Ordering::Less => {
                                    max_dims = dims;
                                    arg_type.clone()
                                }
                            };
                        }
                    }
                    _ => {
                        return plan_err!(
                            "The array_concat function can only accept list as the args."
                        )
                    }
                }
            }
            Ok(expr_type)
        } else {
            Ok(arg_types
                .iter()
                .find(|&arg_type| matches!(arg_type, &DataType::Utf8View))
                .or_else(|| {
                    arg_types
                        .iter()
                        .find(|&arg_type| matches!(arg_type, &DataType::LargeUtf8))
                })
                .unwrap_or(&DataType::Utf8)
                .clone())
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            #[allow(deprecated)] // TODO use invoke_batch
            ArrayConcat::new().invoke(args)
        } else {
            #[allow(deprecated)] // TODO use invoke_batch
            ConcatFunc::new().invoke(args)
        }
    }
}
