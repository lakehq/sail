use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::udf_utils::{any_arg_nullable, arg_data_types};

/// A placeholder UDF used to represent a list of expressions that come from the output
/// of generator expressions or wildcard expressions.
/// The UDF must only exist as a top-level expression in projection nodes,
/// otherwise a planning error will be raised during logical plan analysis.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MultiExpr {
    signature: Signature,
}

impl Default for MultiExpr {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiExpr {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }

    fn output_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}

impl ScalarUDFImpl for MultiExpr {
    fn name(&self) -> &str {
        "multi_expr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        self.output_type(&[])
    }

    // Internal plumbing; nullability follows the inputs.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = self.output_type(&arg_data_types(&args))?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            any_arg_nullable(&args),
        )))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
