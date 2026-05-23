// [CREDIT]: https://github.com/apache/datafusion/blob/4a41173ba3df9b5d47638599c819a1e6e46ad92b/datafusion/functions-nested/src/set_ops.rs

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, GenericListArray, OffsetSizeTrait,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::DataType::{LargeList, List, Null};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::{take_function_args, ListCoercion};
use datafusion_common::{assert_eq_or_internal_err, internal_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayIntersect {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayIntersect {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayIntersect {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(
                2,
                Some(ListCoercion::FixedSizedListToList),
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_intersect")],
        }
    }
}

impl ScalarUDFImpl for ArrayIntersect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_intersect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array1, array2] = take_function_args(self.name(), arg_types)?;
        match (array1, array2) {
            (Null, Null) => Ok(DataType::new_list(Null, true)),
            (Null, dt) | (dt, Null) => Ok(dt.clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_intersect_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum SetOp {
    Intersect,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOp::Intersect => write!(f, "array_intersect"),
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    if l.is_empty() || l.value_type().is_null() {
        let field = Arc::new(Field::new_list_field(r.value_type(), true));
        return general_array_distinct::<OffsetSize>(r, &field);
    } else if r.is_empty() || r.value_type().is_null() {
        let field = Arc::new(Field::new_list_field(l.value_type(), true));
        return general_array_distinct::<OffsetSize>(l, &field);
    }

    assert_eq_or_internal_err!(
        l.value_type(),
        r.value_type(),
        "{set_op:?} is not implemented for '{l:?}' and '{r:?}'"
    );

    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    let l_first = l.offsets()[0].as_usize();
    let l_len = l.offsets()[l.len()].as_usize() - l_first;
    let rows_l = converter.convert_columns(&[l.values().slice(l_first, l_len)])?;

    let r_first = r.offsets()[0].as_usize();
    let r_len = r.offsets()[r.len()].as_usize() - r_first;
    let rows_r = converter.convert_columns(&[r.values().slice(r_first, r_len)])?;

    let l_values = l.values().slice(l_first, l_len);
    let r_values = r.values().slice(r_first, r_len);
    let combined_values = concat(&[l_values.as_ref(), r_values.as_ref()])?;
    let r_offset = l_len;

    generic_set_loop::<OffsetSize>(l, r, &rows_l, &rows_r, field, &combined_values, r_offset)
}

fn generic_set_loop<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    rows_l: &datafusion::arrow::row::Rows,
    rows_r: &datafusion::arrow::row::Rows,
    field: Arc<Field>,
    combined_values: &ArrayRef,
    r_offset: usize,
) -> Result<ArrayRef> {
    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_first = l.offsets()[0].as_usize();
    let r_first = r.offsets()[0].as_usize();

    let mut result_offsets = Vec::with_capacity(l.len() + 1);
    let mut current_offset = OffsetSize::usize_as(0);
    result_offsets.push(current_offset);
    let initial_capacity = rows_l.num_rows().min(rows_r.num_rows());
    let mut indices: Vec<usize> = Vec::with_capacity(initial_capacity);

    let mut seen = HashSet::new();
    let mut lookup_set = HashSet::new();
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            result_offsets.push(current_offset);
            continue;
        }

        let l_start = l_offsets[i].as_usize() - l_first;
        let l_end = l_offsets[i + 1].as_usize() - l_first;
        let r_start = r_offsets[i].as_usize() - r_first;
        let r_end = r_offsets[i + 1].as_usize() - r_first;

        let l_len = l_end - l_start;
        let r_len = r_end - r_start;

        let (lookup_rows, lookup_range, probe_rows, probe_range, probe_offset) = if l_len < r_len {
            (rows_l, l_start..l_end, rows_r, r_start..r_end, r_offset)
        } else {
            (rows_r, r_start..r_end, rows_l, l_start..l_end, 0)
        };

        seen.clear();
        lookup_set.clear();
        lookup_set.reserve(lookup_range.len());

        for idx in lookup_range {
            lookup_set.insert(lookup_rows.row(idx));
        }

        for idx in probe_range {
            let row = probe_rows.row(idx);
            if lookup_set.contains(&row) && seen.insert(row) {
                indices.push(idx + probe_offset);
            }
        }
        current_offset += OffsetSize::usize_as(seen.len());
        result_offsets.push(current_offset);
    }

    let final_values = if indices.is_empty() {
        new_empty_array(&l.value_type())
    } else if OffsetSize::IS_LARGE {
        let indices = UInt64Array::from(indices.into_iter().map(|i| i as u64).collect::<Vec<_>>());
        take(combined_values.as_ref(), &indices, None)?
    } else {
        let indices = UInt32Array::from(indices.into_iter().map(|i| i as u32).collect::<Vec<_>>());
        take(combined_values.as_ref(), &indices, None)?
    };

    let arr = GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::new(result_offsets.into()),
        final_values,
        NullBuffer::union(l.nulls(), r.nulls()),
    )?;
    Ok(Arc::new(arr))
}

fn general_set_op(array1: &ArrayRef, array2: &ArrayRef, set_op: SetOp) -> Result<ArrayRef> {
    let len = array1.len();
    match (array1.data_type(), array2.data_type()) {
        (Null, Null) => Ok(new_null_array(&DataType::new_list(Null, true), len)),
        (Null, dt @ List(_))
        | (Null, dt @ LargeList(_))
        | (dt @ List(_), Null)
        | (dt @ LargeList(_), Null) => Ok(new_null_array(dt, len)),
        (List(field), List(_)) => {
            let array1 = as_list_array(array1)?;
            let array2 = as_list_array(array2)?;
            generic_set_lists::<i32>(array1, array2, Arc::clone(field), set_op)
        }
        (LargeList(field), LargeList(_)) => {
            let array1 = as_large_list_array(array1)?;
            let array2 = as_large_list_array(array2)?;
            generic_set_lists::<i64>(array1, array2, Arc::clone(field), set_op)
        }
        (data_type1, data_type2) => {
            internal_err!("{set_op} does not support types '{data_type1:?}' and '{data_type2:?}'")
        }
    }
}

fn array_intersect_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_intersect", args)?;
    general_set_op(array1, array2, SetOp::Intersect)
}

fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    if array.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }
    let value_offsets = array.value_offsets();
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len() + 1);
    let mut current_offset = OffsetSize::usize_as(0);
    offsets.push(current_offset);

    let converter = RowConverter::new(vec![SortField::new(dt.clone())])?;

    let first_offset = value_offsets[0].as_usize();
    let visible_len = value_offsets[array.len()].as_usize() - first_offset;
    let rows = converter.convert_columns(&[array.values().slice(first_offset, visible_len)])?;

    let mut indices: Vec<usize> = Vec::with_capacity(rows.num_rows());
    let mut seen = HashSet::new();
    for i in 0..array.len() {
        if array.is_null(i) {
            offsets.push(current_offset);
            continue;
        }

        let start = value_offsets[i].as_usize() - first_offset;
        let end = value_offsets[i + 1].as_usize() - first_offset;
        seen.clear();
        seen.reserve(end - start);

        for idx in start..end {
            let row = rows.row(idx);
            if seen.insert(row) {
                indices.push(idx + first_offset);
            }
        }
        current_offset += OffsetSize::usize_as(seen.len());
        offsets.push(current_offset);
    }

    let final_values = if indices.is_empty() {
        new_empty_array(&dt)
    } else if OffsetSize::IS_LARGE {
        let indices = UInt64Array::from(indices.into_iter().map(|i| i as u64).collect::<Vec<_>>());
        take(array.values().as_ref(), &indices, None)?
    } else {
        let indices = UInt32Array::from(indices.into_iter().map(|i| i as u32).collect::<Vec<_>>());
        take(array.values().as_ref(), &indices, None)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        final_values,
        array.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, AsArray, Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use super::ArrayIntersect;

    fn make_sliced_pair() -> (ListArray, ListArray, Arc<Field>) {
        let l = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5), Some(6)]),
            Some(vec![Some(7), Some(8)]),
        ]);
        let r = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(3)]),
            Some(vec![Some(3), Some(5)]),
            Some(vec![Some(5), Some(7)]),
            Some(vec![Some(7), Some(1)]),
        ]);
        let field = Arc::new(Field::new("item", l.data_type().clone(), true));
        (l.slice(1, 2), r.slice(1, 2), field)
    }

    fn collect_i32_list(list: &ListArray) -> Result<Vec<Vec<i32>>> {
        (0..list.len())
            .map(|i| {
                let arr = list.value(i);
                let Some(arr) = arr.as_any().downcast_ref::<Int32Array>() else {
                    return exec_err!("expected Int32Array");
                };
                Ok(arr.values().to_vec())
            })
            .collect()
    }

    #[test]
    fn test_array_intersect_sliced_lists() -> Result<()> {
        let (l, r, field) = make_sliced_pair();

        let result = ArrayIntersect::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(l)),
                ColumnarValue::Array(Arc::new(r)),
            ],
            arg_fields: vec![Arc::clone(&field), Arc::clone(&field)],
            number_rows: 2,
            return_field: Arc::clone(&field),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let output = result.into_array(2)?;
        let output = output.as_list::<i32>();
        let rows = collect_i32_list(output)?;

        assert_eq!(rows[0], vec![3]);
        assert_eq!(rows[1], vec![5]);
        Ok(())
    }

    #[test]
    fn test_array_intersect_inner_nullability_result_type_match_return_type() -> Result<()> {
        let udf = ArrayIntersect::new();

        for inner_nullable in [true, false] {
            let inner_field = Field::new_list_field(DataType::Int32, inner_nullable);
            let input_field = Field::new_list("input", Arc::new(inner_field.clone()), true);

            let input_array = ListArray::new(
                inner_field.into(),
                OffsetBuffer::new(vec![0, 3].into()),
                Arc::new(Int32Array::new(vec![1, 1, 2].into(), None)),
                None,
            );

            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(input_array.clone())),
                    ColumnarValue::Array(Arc::new(input_array)),
                ],
                arg_fields: vec![input_field.clone().into(), input_field.clone().into()],
                number_rows: 1,
                return_field: input_field.clone().into(),
                config_options: Arc::new(ConfigOptions::default()),
            })?;

            assert_eq!(
                result.data_type(),
                udf.return_type(&[
                    input_field.data_type().clone(),
                    input_field.data_type().clone()
                ])?
            );
        }
        Ok(())
    }
}
