use std::any::Any;
use std::fmt::Display;
use arrow::datatypes::DataType;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::columnar_value::ColumnarValue;

#[derive(Debug)]
pub struct SparkTryToNumber {
    signature: Signature,
}

impl SparkTryToNumber {}

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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion_common::Result<ColumnarValue> {
        todo!()
    }
}
