use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, NullArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{arrow_err, exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

use crate::extension::function::struct_function::to_struct_array;

#[derive(Debug)]
pub struct ArraysZip {
    signature: Signature,
}

impl Default for ArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArraysZip {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let data_types = arg_types
            .iter()
            .map(|arg_type| match arg_type {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => Ok(field.data_type().clone()),
                _ => plan_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataType::List(struct_result_field(&data_types)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(arrays_zip_inner, vec![])(&args)
    }
}

fn struct_result_field_names(data_types: &[DataType]) -> Vec<String> {
    (0..data_types.len())
        .map(|i| format!("{i}"))
        .collect::<Vec<_>>()
}

fn struct_result_field(data_types: &[DataType]) -> Arc<Field> {
    let fields = struct_result_field_names(data_types)
        .iter()
        .zip(data_types)
        .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
        .collect::<Vec<_>>();
    Arc::new(Field::new_struct("item", fields, true))
}

fn arrays_zip_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let casted_lists = args
        .iter()
        .map(|arr| match arr.data_type() {
            DataType::List(_field) => Ok(arr.clone()),
            DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
                cast(arr, &DataType::List(field.clone())).map_or_else(|err| arrow_err!(err), Ok)
            }
            _ => exec_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
        })
        .collect::<Result<Vec<_>>>()?;

    let lists = casted_lists
        .iter()
        .map(|arr| arr.as_list::<i32>())
        .collect::<Vec<_>>();

    let num_rows = lists[0].len();
    for list in &lists {
        if list.len() != num_rows {
            return exec_err!("`arrays_zip`: all ListArrays must have the same number of rows");
        }
    }

    let data_types = lists
        .iter()
        .map(|list| list.value_type().clone())
        .collect::<Vec<_>>();
    let field_names = struct_result_field_names(&data_types);

    let struct_arrays = (0..num_rows)
        .map(|row_idx| {
            let arrays_one_row = lists
                .iter()
                .map(|arg| arg.value(row_idx))
                .collect::<Vec<_>>();

            let lens_one_row = lists
                .iter()
                .map(|arg| arg.value_length(row_idx))
                .collect::<Vec<_>>();

            let max_len_one_row = lens_one_row.iter().max().cloned().unwrap_or(0);

            let arrays_padded = arrays_one_row
                .iter()
                .zip(lens_one_row.iter())
                .map(|(arr, len)| {
                    Ok(match max_len_one_row - len {
                        0 => arr.clone(),
                        len_diff => Arc::new(concat(&[
                            arr,
                            &cast(&NullArray::new(len_diff as usize), arr.data_type())?,
                        ])?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            to_struct_array(arrays_padded.as_slice(), field_names.as_slice())
        })
        .collect::<Result<Vec<_>, _>>()?;

    let values = concat(
        struct_arrays
            .iter()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    let mut offsets = vec![0];
    offsets.extend(struct_arrays.iter().scan(0, |acc, arr| {
        *acc += arr.len() as i32;
        Some(*acc)
    }));

    Ok(Arc::new(ListArray::try_new(
        struct_result_field(&data_types),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    )?))
}
