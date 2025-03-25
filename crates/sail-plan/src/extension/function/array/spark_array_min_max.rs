use std::any::Any;

use datafusion::arrow::array::{
    as_large_list_array, as_list_array, Array, ArrayRef, GenericListArray, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::min_max;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{Accumulator, ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::functions_nested_utils::make_scalar_function;

enum ArrayOp {
    Min,
    Max,
}

#[derive(Debug)]
pub struct ArrayMin {
    signature: Signature,
}

impl Default for ArrayMin {
    fn default() -> Self {
        Self::new()
    }
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
        make_scalar_function(array_min_inner)(args)
    }
}

#[derive(Debug)]
pub struct ArrayMax {
    signature: Signature,
}

impl Default for ArrayMax {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMax {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!("ArrayMax can only accept List, LargeList or FixedSizeList."),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_max_inner)(args)
    }
}

fn array_min_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_min needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Min)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Min)
        }
        _ => exec_err!(
            "array_min does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn array_max_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_max needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Max)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Max)
        }
        _ => exec_err!(
            "array_max does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn compute_min_max_values<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    data_type: &DataType,
    op: ArrayOp,
) -> Result<Vec<ScalarValue>>
where
    i64: TryInto<O>,
{
    let mut result_values = Vec::new();
    for i in 0..array.len() {
        let inner_array = array.value(i);
        let value = match op {
            ArrayOp::Min => {
                let mut min_accumulator = min_max::MinAccumulator::try_new(data_type)?;
                min_accumulator.update_batch(&[inner_array])?;
                min_accumulator.evaluate()?
            }
            ArrayOp::Max => {
                let mut max_accumulator = min_max::MaxAccumulator::try_new(data_type)?;
                max_accumulator.update_batch(&[inner_array])?;
                max_accumulator.evaluate()?
            }
        };
        result_values.push(value);
    }
    Ok(result_values)
}
