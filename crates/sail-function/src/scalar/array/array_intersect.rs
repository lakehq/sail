// [CREDIT]: https://github.com/apache/datafusion/blob/4a41173ba3df9b5d47638599c819a1e6e46ad92b/datafusion/functions-nested/src/set_ops.rs
// Adapted from DataFusion's set operation implementation with Spark-compatible empty array handling for array intersections.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, GenericListArray, OffsetSizeTrait,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    assert_eq_or_internal_err, exec_err, internal_err, DataFusionError, Result,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array1, array2] = take_function_args(self.name(), arg_types)?;

        match (array1, array2) {
            (DataType::Null, DataType::Null) => Ok(DataType::new_list(DataType::Null, true)),
            (DataType::Null, dt) | (dt, DataType::Null) => Ok(dt.clone()),

            (DataType::List(field1), DataType::List(field2)) => {
                let field = if field1.data_type().is_null() {
                    field2
                } else {
                    field1
                };
                Ok(DataType::List(field.clone()))
            }

            (DataType::LargeList(field1), DataType::LargeList(field2)) => {
                let field = if field1.data_type().is_null() {
                    field2
                } else {
                    field1
                };
                Ok(DataType::LargeList(field.clone()))
            }

            (DataType::List(field1), DataType::LargeList(field2))
            | (DataType::LargeList(field1), DataType::List(field2)) => {
                let field = if field1.data_type().is_null() {
                    field2
                } else {
                    field1
                };
                Ok(DataType::LargeList(field.clone()))
            }

            (dt, _) => Ok(dt.clone()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let return_type = self.return_type(&arg_types)?;

        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_intersect_inner)(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `array_intersect` requires exactly 2 arguments, but got {}",
                arg_types.len()
            );
        }

        let arg_types = arg_types
            .iter()
            .map(|arg_type| match arg_type {
                DataType::Null => Ok(DataType::Null),
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _) => Ok(DataType::List(field.clone())),
                DataType::LargeList(field) | DataType::LargeListView(field) => {
                    Ok(DataType::LargeList(field.clone()))
                }
                other => {
                    exec_err!(
                        "Spark `array_intersect` requires list or null types, but got '{other:?}'"
                    )
                }
            })
            .collect::<Result<Vec<_>>>()?;

        match arg_types.as_slice() {
            [DataType::List(field), DataType::LargeList(_)] => Ok(vec![
                DataType::LargeList(field.clone()),
                arg_types[1].clone(),
            ]),
            [DataType::LargeList(_), DataType::List(field)] => Ok(vec![
                arg_types[0].clone(),
                DataType::LargeList(field.clone()),
            ]),
            _ => Ok(arg_types),
        }
    }
}

fn array_intersect_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_intersect", args)?;
    general_set_op(array1, array2)
}

fn general_set_op(array1: &ArrayRef, array2: &ArrayRef) -> Result<ArrayRef> {
    let len = array1.len();
    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, DataType::Null) => Ok(new_null_array(
            &DataType::new_list(DataType::Null, true),
            len,
        )),
        (DataType::Null, dt @ DataType::List(_))
        | (DataType::Null, dt @ DataType::LargeList(_))
        | (dt @ DataType::List(_), DataType::Null)
        | (dt @ DataType::LargeList(_), DataType::Null) => Ok(new_null_array(dt, len)),
        (DataType::List(field1), DataType::List(field2)) => {
            let field = if field1.data_type().is_null() {
                Arc::clone(field2)
            } else {
                Arc::clone(field1)
            };
            let array1 = as_list_array(array1)?;
            let array2 = as_list_array(array2)?;
            generic_set_lists::<i32>(array1, array2, field)
        }
        (DataType::LargeList(field1), DataType::LargeList(field2)) => {
            let field = if field1.data_type().is_null() {
                Arc::clone(field2)
            } else {
                Arc::clone(field1)
            };
            let array1 = as_large_list_array(array1)?;
            let array2 = as_large_list_array(array2)?;
            generic_set_lists::<i64>(array1, array2, field)
        }
        (data_type1, data_type2) => {
            internal_err!(
                "array_intersect does not support types '{data_type1:?}' and '{data_type2:?}'"
            )
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
) -> Result<ArrayRef> {
    if l.value_type().is_null() || r.value_type().is_null() {
        return null_element_intersection::<OffsetSize>(l, r, field);
    }

    assert_eq_or_internal_err!(
        l.value_type(),
        r.value_type(),
        "array_intersect is not implemented for '{l:?}' and '{r:?}'"
    );

    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    let l_first = l.offsets()[0].as_usize();
    let l_len = l.offsets()[l.len()].as_usize() - l_first;
    let rows_l = converter.convert_columns(&[l.values().slice(l_first, l_len)])?;

    let r_first = r.offsets()[0].as_usize();
    let r_len = r.offsets()[r.len()].as_usize() - r_first;
    let rows_r = converter.convert_columns(&[r.values().slice(r_first, r_len)])?;

    let l_values = l.values().slice(l_first, l_len);

    generic_set_loop::<OffsetSize>(l, r, &rows_l, &rows_r, field, &l_values)
}

fn generic_set_loop<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    rows_l: &arrow::row::Rows,
    rows_r: &arrow::row::Rows,
    field: Arc<Field>,
    l_values: &ArrayRef,
) -> Result<ArrayRef> {
    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_first = l.offsets()[0].as_usize();
    let r_first = r.offsets()[0].as_usize();

    let mut result_offsets = Vec::with_capacity(l.len() + 1);
    result_offsets.push(OffsetSize::usize_as(0));
    let initial_capacity = rows_l.num_rows().min(rows_r.num_rows());
    let mut indices: Vec<usize> = Vec::with_capacity(initial_capacity);

    // Reuse hash sets across iterations
    let mut seen = HashSet::new();
    let mut lookup_set = HashSet::new();
    for i in 0..l.len() {
        let last_offset = *result_offsets.last().ok_or_else(|| {
            DataFusionError::Internal(
                "result_offsets should always have at least one element".to_string(),
            )
        })?;

        if l.is_null(i) || r.is_null(i) {
            result_offsets.push(last_offset);
            continue;
        }

        let l_start = l_offsets[i].as_usize() - l_first;
        let l_end = l_offsets[i + 1].as_usize() - l_first;
        let r_start = r_offsets[i].as_usize() - r_first;
        let r_end = r_offsets[i + 1].as_usize() - r_first;

        seen.clear();

        // Always build the lookup from the right array and probe the left array so
        // that output order matches the left array (Spark's array_intersect contract).
        lookup_set.clear();
        lookup_set.reserve(r_end - r_start);

        for idx in r_start..r_end {
            lookup_set.insert(rows_r.row(idx));
        }

        for idx in l_start..l_end {
            let row = rows_l.row(idx);
            if lookup_set.contains(&row) && seen.insert(row) {
                indices.push(idx);
            }
        }

        result_offsets.push(last_offset + OffsetSize::usize_as(seen.len()));
    }

    // Gather distinct left-side values by index.
    // Use UInt64Array for LargeList to support values arrays exceeding u32::MAX.
    let final_values = if indices.is_empty() {
        new_empty_array(&l.value_type())
    } else if OffsetSize::IS_LARGE {
        let indices = UInt64Array::from(indices.into_iter().map(|i| i as u64).collect::<Vec<_>>());
        take(l_values.as_ref(), &indices, None)?
    } else {
        let indices = UInt32Array::from(indices.into_iter().map(|i| i as u32).collect::<Vec<_>>());
        take(l_values.as_ref(), &indices, None)?
    };

    let arr = GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::new(result_offsets.into()),
        final_values,
        NullBuffer::union(l.nulls(), r.nulls()),
    )?;
    Ok(Arc::new(arr))
}

fn null_element_intersection<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
) -> Result<ArrayRef> {
    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_values = l.values();
    let r_values = r.values();
    let l_null_elem = l.value_type().is_null();
    let r_null_elem = r.value_type().is_null();

    let mut result_offsets = Vec::with_capacity(l.len() + 1);
    result_offsets.push(OffsetSize::usize_as(0));
    let mut null_count = 0usize;

    for i in 0..l.len() {
        let last_offset = *result_offsets.last().ok_or_else(|| {
            DataFusionError::Internal(
                "result_offsets should always have at least one element".to_string(),
            )
        })?;

        if l.is_null(i) || r.is_null(i) {
            result_offsets.push(last_offset);
            continue;
        }

        let l_start = l_offsets[i].as_usize();
        let l_end = l_offsets[i + 1].as_usize();
        let r_start = r_offsets[i].as_usize();
        let r_end = r_offsets[i + 1].as_usize();

        // For DataType::Null element type every element is null; for a typed array we
        // check whether any element in the row is actually null.
        let l_has_null = if l_null_elem {
            l_end > l_start
        } else {
            (l_start..l_end).any(|idx| l_values.is_null(idx))
        };
        let r_has_null = if r_null_elem {
            r_end > r_start
        } else {
            (r_start..r_end).any(|idx| r_values.is_null(idx))
        };

        if l_has_null && r_has_null {
            null_count += 1;
            result_offsets.push(last_offset + OffsetSize::usize_as(1));
        } else {
            result_offsets.push(last_offset);
        }
    }

    let values = new_null_array(field.data_type(), null_count);

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::new(result_offsets.into()),
        values,
        NullBuffer::union(l.nulls(), r.nulls()),
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
        let field = Arc::new(Field::new("list_item", l.data_type().clone(), true));
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
