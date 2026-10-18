use std::sync::Arc;

use datafusion::arrow::compute::kernels::numeric::neg;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NegateDuration {
    signature: Signature,
}

impl Default for NegateDuration {
    fn default() -> Self {
        Self::new()
    }
}

impl NegateDuration {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NegateDuration {
    fn name(&self) -> &str {
        "negate_duration"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg = &args.arg_fields[0];
        Ok(Arc::new(Field::new(
            self.name(),
            arg.data_type().clone(),
            arg.is_nullable(),
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "`negate_duration` function requires 1 argument, got {}",
                args.len()
            );
        };
        if !matches!(arg.data_type(), DataType::Duration(_)) {
            return exec_err!("Unsupported arg {arg:?} for function negate_duration");
        }
        make_scalar_function(|args| Ok(neg(args[0].as_ref())?))(&args)
    }
}
