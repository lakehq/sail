use crate::extension::function::string::spark_to_number::SparkToNumber;
use arrow::datatypes::DataType;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::any::Any;
use std::fmt::Display;

#[derive(Debug)]
pub struct SparkTryToNumber(SparkToNumber);
impl SparkTryToNumber {
    pub const NAME: &'static str = "try_to_number";

    pub fn new() -> Self {
        Self {
            0: SparkToNumber::new(),
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
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn signature(&self) -> &Signature {
        todo!()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        todo!()
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        todo!()
    }
}
