use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    as_large_list_array, as_list_array, Array, ArrayRef, BooleanArray, GenericListArray,
    OffsetSizeTrait,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

/// `array_compact` removes all null values from the array.
/// This is the Spark-compatible implementation.
///
/// Returns null if the input array is null.
/// Returns an empty array if all elements are null.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayCompact {
    signature: Signature,
}

impl Default for SparkArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayCompact {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(_) | DataType::LargeList(_) => Ok(arg_types[0].clone()),
            DataType::Null => Ok(DataType::Null),
            _ => plan_err!(
                "spark_array_compact can only accept List or LargeList, got {:?}",
                arg_types[0]
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        make_scalar_function(array_compact_inner)(&args)
    }
}

fn array_compact_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("spark_array_compact needs exactly one argument");
    }
    match args[0].data_type() {
        DataType::List(_) => {
            let list = as_list_array(&args[0]);
            array_compact_generic::<i32>(list)
        }
        DataType::LargeList(_) => {
            let list = as_large_list_array(&args[0]);
            array_compact_generic::<i64>(list)
        }
        other => exec_err!("spark_array_compact does not support type: {:?}", other),
    }
}

fn array_compact_generic<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<ArrayRef> {
    let values = list.values();
    let offsets = list.offsets();

    // Build a boolean filter mask for the flattened values array.
    // We keep a value if its parent row is not null AND the value itself is not null.
    let mut keep: Vec<bool> = Vec::with_capacity(values.len());
    // New offsets after compaction: one entry per row, counting retained values.
    let mut new_offsets: Vec<O> = Vec::with_capacity(list.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total_kept: usize = 0;

    for i in 0..list.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();

        if list.is_null(i) {
            // Null row: mark all values in the range as not kept.
            keep.extend(std::iter::repeat_n(false, end - start));
            // Null row: offset stays the same (empty slice).
            new_offsets.push(O::usize_as(total_kept));
        } else {
            // Non-null row: keep only non-null values.
            for j in start..end {
                let keep_value = values.is_valid(j);
                keep.push(keep_value);
                if keep_value {
                    total_kept += 1;
                }
            }
            new_offsets.push(O::usize_as(total_kept));
        }
    }

    let keep_mask = BooleanArray::from(keep);
    let new_values = compute::filter(values.as_ref(), &keep_mask)?;

    let field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}
