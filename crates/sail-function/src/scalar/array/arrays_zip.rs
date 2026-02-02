use std::any::Any;
use std::ops::BitAnd;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, NullArray,
    OffsetSizeTrait, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{cast, concat};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{arrow_err, exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use sail_common::spec::SAIL_LIST_FIELD_NAME;

use crate::scalar::struct_function::to_struct_array;

#[derive(Debug, PartialEq, Eq, Hash)]
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
        let params = arg_types
            .iter()
            .map(get_list_params)
            .collect::<Result<Vec<_>>>()?;

        let struct_field = struct_result_field(
            &params
                .iter()
                .map(|row| row.field_type.clone())
                .collect::<Vec<_>>(),
        );

        let is_large = params.iter().any(|row| row.is_large);

        let fixed_size_opt = params
            .iter()
            .map(|row| row.fixed_size_opt)
            .try_fold(None, |prev, item| match (prev, item) {
                (None, x) => Ok(x),
                (Some(p), Some(x)) if p == x => Ok(Some(p)),
                _ => Err(()),
            })
            .ok()
            .flatten();

        Ok(match (is_large, fixed_size_opt) {
            (true, _) => DataType::LargeList(struct_field),
            (false, Some(fixed_size)) => DataType::FixedSizeList(struct_field, fixed_size),
            _ => DataType::List(struct_field),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.return_field.data_type() {
            DataType::LargeList(_) => {
                make_scalar_function(arrays_zip_generic::<i64>, vec![])(&args.args)
            }
            DataType::FixedSizeList(_, size) => {
                make_scalar_function(|args| arrays_zip_fixed_size(args, size), vec![])(&args.args)
            }
            _ => make_scalar_function(arrays_zip_generic::<i32>, vec![])(&args.args),
        }
    }
}

struct ListParams {
    field_type: DataType,
    is_large: bool,
    fixed_size_opt: Option<i32>,
}

impl ListParams {
    pub fn new(field_type: DataType, is_large: bool, fixed_size_opt: Option<i32>) -> Self {
        Self {
            field_type,
            is_large,
            fixed_size_opt,
        }
    }
}

fn get_list_params(data_type: &DataType) -> Result<ListParams> {
    match data_type {
        DataType::List(field) | DataType::ListView(field) => {
            Ok(ListParams::new(field.data_type().clone(), false, None))
        }
        DataType::FixedSizeList(field, size) => Ok(ListParams::new(
            field.data_type().clone(),
            false,
            Some(*size),
        )),
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            Ok(ListParams::new(field.data_type().clone(), true, None))
        }
        _ => plan_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
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
    Arc::new(Field::new_struct(SAIL_LIST_FIELD_NAME, fields, true))
}

fn num_rows_data_types_field_names(
    args: &[ArrayRef],
) -> Result<(usize, Vec<DataType>, Vec<String>)> {
    let num_rows = args[0].len();
    for arg in args {
        if arg.len() != num_rows {
            return exec_err!("`arrays_zip`: all ListArrays must have the same number of rows");
        }
    }
    let data_types = args
        .iter()
        .map(|arg| Ok(get_list_params(arg.data_type())?.field_type))
        .collect::<Result<Vec<_>>>()?;
    let field_names = struct_result_field_names(&data_types);
    Ok((num_rows, data_types, field_names))
}

fn combine_validity_masks(arrays: &[ArrayRef]) -> Option<NullBuffer> {
    if arrays.is_empty() {
        return None;
    }

    let validity_masks = arrays
        .iter()
        .filter_map(|arr| arr.nulls())
        .map(|mask| mask.inner())
        .collect::<Vec<_>>();

    if validity_masks.is_empty() {
        return None;
    }

    let combined_validity = validity_masks
        .iter()
        .fold(validity_masks[0].clone(), |acc, &mask| acc.bitand(mask));

    Some(NullBuffer::new(combined_validity))
}

fn arrays_zip_fixed_size(args: &[ArrayRef], fixed_size: &i32) -> Result<ArrayRef> {
    let (_num_rows, data_types, field_names) = num_rows_data_types_field_names(args)?;

    let lists = args
        .iter()
        .map(|arr| arr.as_fixed_size_list())
        .collect::<Vec<_>>();

    let values = to_struct_array(
        lists
            .into_iter()
            .map(|fla| fla.values().clone())
            .collect::<Vec<_>>()
            .as_slice(),
        &field_names,
    )?;

    Ok(Arc::new(FixedSizeListArray::try_new(
        struct_result_field(&data_types),
        *fixed_size,
        values,
        combine_validity_masks(args),
    )?))
}

fn arrays_zip_generic<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let (num_rows, data_types, field_names) = num_rows_data_types_field_names(args)?;
    let validity_mask_opt = combine_validity_masks(args);

    let casted_lists = args
        .iter()
        .map(|arr| match (O::IS_LARGE, arr.data_type()) {
            (false, DataType::List(_)) => Ok(arr.clone()),
            (false, DataType::ListView(field) | DataType::FixedSizeList(field, _)) => {
                cast(arr, &DataType::List(field.clone())).map_or_else(|err| arrow_err!(err), Ok)
            }
            (true, DataType::LargeList(_field)) => Ok(arr.clone()),
            (
                true,
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _)
                | DataType::LargeListView(field),
            ) => cast(arr, &DataType::LargeList(field.clone()))
                .map_or_else(|err| arrow_err!(err), Ok),
            _ => exec_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
        })
        .collect::<Result<Vec<_>>>()?;

    let lists = casted_lists
        .iter()
        .map(|arr| arr.as_list::<O>())
        .collect::<Vec<_>>();

    let zero_offset = O::from_usize(0).ok_or_else(|| {
        DataFusionError::Execution("`arrays_zip`: zero offset should always exist".to_string())
    })?;

    let mut struct_arrays = vec![];
    let mut offsets = vec![zero_offset];
    let mut last_offset = zero_offset;

    for row_idx in 0..num_rows {
        if validity_mask_opt
            .as_ref()
            .is_some_and(|mask| !mask.is_valid(row_idx))
        {
            offsets.push(last_offset);
            continue;
        }

        let arrays_one_row = lists
            .iter()
            .map(|arg| arg.value(row_idx))
            .collect::<Vec<_>>();

        let lens_one_row = lists
            .iter()
            .map(|arg| arg.value_length(row_idx))
            .collect::<Vec<_>>();

        let max_len_one_row = lens_one_row.iter().max().cloned().unwrap_or(zero_offset);

        let arrays_padded = arrays_one_row
            .iter()
            .zip(lens_one_row.iter())
            .map(|(arr, len)| {
                Ok(match (max_len_one_row - *len).as_usize() {
                    0 => arr.clone(),
                    len_diff => Arc::new(concat(&[
                        arr,
                        &cast(&NullArray::new(len_diff), arr.data_type())?,
                    ])?),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let struct_array = to_struct_array(arrays_padded.as_slice(), field_names.as_slice())?;
        let offset = O::from_usize(struct_array.len()).ok_or_else(|| {
            DataFusionError::Execution("`arrays_zip` offset overflow error".to_string())
        })?;

        last_offset += offset;
        offsets.push(last_offset);
        struct_arrays.push(struct_array);
    }

    let values: ArrayRef = if struct_arrays.is_empty() {
        // When all rows are null, struct_arrays is empty. Create an empty struct array.
        let fields: Fields = field_names
            .iter()
            .zip(data_types.iter())
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect();
        // Create empty arrays for each field
        let empty_arrays: Vec<ArrayRef> = data_types.iter().map(|dt| new_empty_array(dt)).collect();
        Arc::new(StructArray::try_new(fields, empty_arrays, None)?)
    } else {
        concat(
            struct_arrays
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
        )?
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        struct_result_field(&data_types),
        OffsetBuffer::<O>::new(offsets.into()),
        values,
        validity_mask_opt,
    )?))
}
