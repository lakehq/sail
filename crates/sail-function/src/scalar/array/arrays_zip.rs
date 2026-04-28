use std::any::Any;
use std::ops::BitAnd;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, AsArray, FixedSizeListArray,
    GenericListArray, NullArray, OffsetSizeTrait, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{cast, concat};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{arrow_err, exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
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
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
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
                .map(|row| row.inner_field.clone())
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
        // Spark: arrays_zip() with no args returns an empty List<Struct<>> per row.
        if args.args.is_empty() {
            return Ok(ColumnarValue::Array(build_empty_zip_result(
                args.number_rows,
            )?));
        }
        // Untyped NULL (Spark: array<void>) makes every output row NULL.
        // Short-circuit to a single typed-null allocation instead of building
        // intermediate all-NULL ListArrays and running the row loop.
        if args
            .args
            .iter()
            .any(|a| matches!(a.data_type(), DataType::Null))
        {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_field.data_type(),
                args.number_rows,
            )));
        }
        // Any arg that is a fully-null ListArray column propagates NULL to
        // every output row. Short-circuit: skip combine_validity_masks and
        // the row loop entirely, return a typed-null array directly.
        if args.args.iter().any(|a| {
            matches!(a, ColumnarValue::Array(arr) if !arr.is_empty() && arr.null_count() == arr.len())
        }) {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_field.data_type(),
                args.number_rows,
            )));
        }
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

fn build_empty_zip_result(num_rows: usize) -> Result<ArrayRef> {
    let struct_field = struct_result_field(&[]);
    // StructArray with zero fields needs explicit length, not inferred from columns.
    let empty_struct = StructArray::new_empty_fields(0, None);
    let offsets = OffsetBuffer::<i32>::new_zeroed(num_rows);
    Ok(Arc::new(GenericListArray::<i32>::try_new(
        struct_field,
        offsets,
        Arc::new(empty_struct),
        None,
    )?))
}

struct ListParams {
    inner_field: Arc<Field>,
    is_large: bool,
    fixed_size_opt: Option<i32>,
}

impl ListParams {
    pub fn new(inner_field: Arc<Field>, is_large: bool, fixed_size_opt: Option<i32>) -> Self {
        Self {
            inner_field,
            is_large,
            fixed_size_opt,
        }
    }
}

fn get_list_params(data_type: &DataType) -> Result<ListParams> {
    match data_type {
        DataType::List(field) | DataType::ListView(field) => {
            Ok(ListParams::new(field.clone(), false, None))
        }
        DataType::FixedSizeList(field, size) => {
            Ok(ListParams::new(field.clone(), false, Some(*size)))
        }
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            Ok(ListParams::new(field.clone(), true, None))
        }
        // Spark coerces bare untyped NULL to `array<void>`.
        DataType::Null => Ok(ListParams::new(
            Arc::new(Field::new_list_field(DataType::Null, true)),
            false,
            None,
        )),
        _ => plan_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
    }
}

fn struct_result_field_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{i}")).collect::<Vec<_>>()
}

/// Build the struct field for the list result, preserving metadata from inner fields.
fn struct_result_field(inner_fields: &[Arc<Field>]) -> Arc<Field> {
    let field_names = struct_result_field_names(inner_fields.len());
    let fields = field_names
        .iter()
        .zip(inner_fields)
        .map(|(name, f)| {
            Field::new(name, f.data_type().clone(), true).with_metadata(f.metadata().clone())
        })
        .collect::<Vec<_>>();
    Arc::new(Field::new_struct(SAIL_LIST_FIELD_NAME, fields, true))
}

fn num_rows_inner_fields_and_names(
    args: &[ArrayRef],
) -> Result<(usize, Vec<Arc<Field>>, Vec<String>)> {
    let num_rows = args[0].len();
    for arg in args {
        if arg.len() != num_rows {
            return exec_err!("`arrays_zip`: all ListArrays must have the same number of rows");
        }
    }
    let inner_fields = args
        .iter()
        .map(|arg| Ok(get_list_params(arg.data_type())?.inner_field))
        .collect::<Result<Vec<_>>>()?;
    let field_names = struct_result_field_names(inner_fields.len());
    Ok((num_rows, inner_fields, field_names))
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
    let (_num_rows, inner_fields, field_names) = num_rows_inner_fields_and_names(args)?;

    let lists = args
        .iter()
        .map(|arr| arr.as_fixed_size_list())
        .collect::<Vec<_>>();

    // Create fields with nullable=true, preserving metadata from inner fields
    let arg_fields: Vec<Arc<Field>> = inner_fields
        .iter()
        .zip(field_names.iter())
        .map(|(f, name)| {
            Arc::new(
                Field::new(name, f.data_type().clone(), true).with_metadata(f.metadata().clone()),
            )
        })
        .collect();

    let values = to_struct_array(
        lists
            .into_iter()
            .map(|fla| fla.values().clone())
            .collect::<Vec<_>>()
            .as_slice(),
        &field_names,
        &arg_fields,
    )?;

    Ok(Arc::new(FixedSizeListArray::try_new(
        struct_result_field(&inner_fields),
        *fixed_size,
        values,
        combine_validity_masks(args),
    )?))
}

fn arrays_zip_generic<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let (num_rows, inner_fields, field_names) = num_rows_inner_fields_and_names(args)?;

    // Create fields with nullable=true, preserving metadata from inner fields
    let arg_fields: Vec<Arc<Field>> = inner_fields
        .iter()
        .zip(field_names.iter())
        .map(|(f, name)| {
            Arc::new(
                Field::new(name, f.data_type().clone(), true).with_metadata(f.metadata().clone()),
            )
        })
        .collect();

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

    // Combine validity masks on the CAST arrays: NullArray has implicit nulls
    // (.nulls() returns None) but the cast path above produces an explicit
    // all-NULL buffer on the resulting ListArray.
    let validity_mask_opt = combine_validity_masks(&casted_lists);

    let lists = casted_lists
        .iter()
        .map(|arr| arr.as_list::<O>())
        .collect::<Vec<_>>();

    let zero_offset = O::from_usize(0).ok_or_else(|| {
        DataFusionError::Execution("`arrays_zip`: zero offset should always exist".to_string())
    })?;

    let mut struct_arrays = Vec::with_capacity(num_rows);
    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(zero_offset);
    let mut last_offset = zero_offset;

    for row_idx in 0..num_rows {
        if validity_mask_opt
            .as_ref()
            .is_some_and(|mask| !mask.is_valid(row_idx))
        {
            offsets.push(last_offset);
            continue;
        }

        let mut arrays_one_row: Vec<ArrayRef> = Vec::with_capacity(lists.len());
        let mut lens_one_row: Vec<O> = Vec::with_capacity(lists.len());
        let mut max_len_one_row = zero_offset;
        let mut all_uniform = true;
        for arg in &lists {
            let arr = arg.value(row_idx);
            let len = arg.value_length(row_idx);
            if !lens_one_row.is_empty() && len != lens_one_row[0] {
                all_uniform = false;
            }
            if len > max_len_one_row {
                max_len_one_row = len;
            }
            arrays_one_row.push(arr);
            lens_one_row.push(len);
        }

        let arrays_padded = if all_uniform {
            arrays_one_row
        } else {
            arrays_one_row
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
                .collect::<Result<Vec<_>>>()?
        };

        let struct_array = to_struct_array(
            arrays_padded.as_slice(),
            field_names.as_slice(),
            &arg_fields,
        )?;
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
            .zip(inner_fields.iter())
            .map(|(name, f)| {
                Field::new(name, f.data_type().clone(), true).with_metadata(f.metadata().clone())
            })
            .collect();
        let empty_arrays: Vec<ArrayRef> = inner_fields
            .iter()
            .map(|f| new_empty_array(f.data_type()))
            .collect();
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
        struct_result_field(&inner_fields),
        OffsetBuffer::<O>::new(offsets.into()),
        values,
        validity_mask_opt,
    )?))
}
