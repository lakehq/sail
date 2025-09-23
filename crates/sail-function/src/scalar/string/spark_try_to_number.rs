use std::any::{type_name, Any};
use std::fmt::Debug;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::common::DataFusionError;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::functions_nested_utils::downcast_arg;
use crate::scalar::string::spark_to_number::{NumberComponents, RegexSpec, SparkToNumber};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToNumber {
    signature: Signature,
}

impl SparkTryToNumber {
    pub const NAME: &'static str = "try_to_number";

    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl Default for SparkTryToNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkTryToNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        SparkToNumber::new().return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result = SparkToNumber::new().invoke_with_args(args.clone());
        match result {
            Ok(result) => Ok(result),
            Err(_) => {
                let ScalarFunctionArgs { args, .. } = args;
                let args = ColumnarValue::values_to_arrays(&args)?;
                let NumberComponents {
                    precision, scale, ..
                } = get_precision_and_scale(&args)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    None, precision, scale,
                )))
            }
        }
    }
}

fn get_precision_and_scale(args: &[ArrayRef]) -> Result<NumberComponents> {
    let format: &str = downcast_arg!(&args[1], StringArray).value(0);
    let format_spec: RegexSpec = RegexSpec::try_from(format)?;
    NumberComponents::try_from(&format_spec)
}
