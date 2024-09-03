use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{as_large_list_array, as_list_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::min_max;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{Accumulator, ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct ArrayMin {
    signature: Signature,
}

impl ArrayMin {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!("ArrayMin can only accept List, LargeList or FixedSizeList."),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("array_min needs one argument");
        }

        let args = ColumnarValue::values_to_arrays(args)?;
        let (array, data_type) = match &args[0].data_type() {
            DataType::List(field) => {
                let array = as_list_array(&args[0]);
                Ok((array.values(), field.data_type()))
            }
            DataType::LargeList(field) => {
                let array = as_large_list_array(&args[0]);
                Ok((array.values(), field.data_type()))
            }
            _ => exec_err!(
                "array_element does not support type: {:?}",
                &args[0].data_type()
            ),
        }?;

        let mut min = min_max::MinAccumulator::try_new(data_type)?;
        min.update_batch(&[Arc::clone(array)])?;
        Ok(ColumnarValue::Scalar(min.evaluate()?))
    }
}
