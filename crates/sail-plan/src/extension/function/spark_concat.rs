use std::any::Any;
use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::string::concat::ConcatFunc;
use datafusion_common::utils::list_ndims;
use datafusion_common::{plan_err, ExprSchema, Result};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable, ScalarUDFImpl, Signature, Volatility};
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

    /// [Credit]: <https://github.com/apache/datafusion/blob/7ccc6d7c55ae9dbcb7dee031f394bf11a03000ba/datafusion/functions-nested/src/concat.rs#L276-L310>
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
                                Ordering::Equal => {
                                    if expr_type == DataType::Null {
                                        arg_type.clone()
                                    } else if !expr_type.equals_datatype(arg_type) {
                                        return plan_err!("It is not possible to concatenate arrays of different types. Expected: {expr_type}, got: {arg_type}");
                                    } else {
                                        expr_type
                                    }
                                }
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

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        if args.is_empty() {
            true
        } else {
            args.iter().any(|arg| arg.nullable(schema).unwrap_or(true))
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            ArrayConcat::new().invoke_batch(args, number_rows)
        } else {
            ConcatFunc::new().invoke_batch(args, number_rows)
        }
    }
}
