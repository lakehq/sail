// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions-nested/src/sort.rs

//! [`ScalarUDFImpl`] definitions for array_sort function.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanBufferBuilder, GenericListArray, OffsetSizeTrait,
    PrimitiveArray, UInt32Array, UInt64Array, new_empty_array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{ArrowNativeTypeOp, DataType, FieldRef};
use arrow::row::{RowConverter, Rows, SortField};
use arrow::util::bit_iterator::BitIterator;
use arrow::{compute, downcast_primitive_array};
use datafusion_common::cast::{as_large_list_array, as_list_array, as_string_array};
// TODO: Uncomment after DataFusion 56
// use datafusion_common::utils::{ListCoercion, offset_span};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use sail_common_datafusion::ordering::spark_nested_sort_options;

use crate::functions_nested_utils::make_scalar_function;
use crate::functions_utils::offset_span;

/// Implementation of `array_sort` function
///
/// `array_sort` sorts the elements of an array
///
/// # Example
///
/// `array_sort([3, 1, 2])` returns `[1, 2, 3]`
///
/// Elements are ordered like Spark orders values: a NULL field or element
/// nested inside an element is smaller than any other value. The optional
/// `nulls_order` argument (`NULLS FIRST` by default) places only the NULL
/// elements of the array itself.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySort {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraySort {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySort {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["list_sort".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySort {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_sort_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn array_sort_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    if args[0].is_empty() || args[0].data_type().is_null() {
        return Ok(Arc::clone(&args[0]));
    }

    if args[1..].iter().any(|array| array.is_null(0)) {
        return Ok(new_null_array(args[0].data_type(), args[0].len()));
    }

    let sort_options = if args.len() >= 2 {
        let order = as_string_array(&args[1])?.value(0);
        let descending = order_desc(order)?;
        let nulls_first = if args.len() >= 3 {
            order_nulls_first(as_string_array(&args[2])?.value(0))?
        } else {
            true
        };
        Some(SortOptions {
            descending,
            nulls_first,
        })
    } else {
        None
    };

    match args[0].data_type() {
        DataType::List(field) | DataType::LargeList(field) if field.data_type().is_null() => {
            Ok(Arc::clone(&args[0]))
        }
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            array_sort_generic(array, sort_options)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            array_sort_generic(array, sort_options)
        }
        // Signature should prevent this arm ever occurring
        _ => exec_err!("array_sort expects list for first argument"),
    }
}

fn array_sort_generic<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let values = list_array.values();

    if values.data_type().is_primitive() {
        array_sort_primitive(list_array, sort_options)
    } else {
        array_sort_non_primitive(list_array, sort_options)
    }
}

/// Sort each row of a primitive-typed ListArray using a custom in-place sort
/// kernel.
fn array_sort_primitive<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let values = list_array.values().as_ref();
    downcast_primitive_array! {
        values => sort_primitive_list(values, list_array, sort_options),
        _ => exec_err!("array_sort: unsupported primitive type")
    }
}

fn sort_primitive_list<T: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
    prim_values: &PrimitiveArray<T>,
    list_array: &GenericListArray<OffsetSize>,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let descending = sort_options.is_some_and(|o| o.descending);
    let nulls_first = sort_options.is_none_or(|o| o.nulls_first);
    let list_nulls = list_array.nulls();
    let offsets = list_array.offsets();

    let (values, validity) = match prim_values.nulls() {
        Some(element_nulls) if element_nulls.null_count() > 0 => {
            let (values, validity) = sort_rows_with_nulls(
                prim_values.values(),
                element_nulls,
                offsets,
                list_nulls,
                descending,
                nulls_first,
            );
            (values, Some(validity))
        }
        _ => (
            sort_rows_no_nulls(prim_values.values(), offsets, list_nulls, descending),
            None,
        ),
    };

    let sorted_values = Arc::new(
        PrimitiveArray::<T>::new(values.into(), validity)
            .with_data_type(prim_values.data_type().clone()),
    );

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        // TODO: Uncomment after DataFusion 56
        // Arc::clone(list_array.value_field()),
        list_value_field(list_array)?,
        rebase_offsets(offsets),
        sorted_values,
        list_nulls.cloned(),
    )?))
}

/// Sorts one row's values in place.
///
/// [`ArrowNativeTypeOp::compare`] is a total order, so a descending sort is the
/// reverse of the ascending one. Sorting one way and reversing keeps a single
/// instantiation of the standard library's sort per native type, instead of one
/// per comparator closure.
#[inline]
fn sort_row<N: ArrowNativeTypeOp>(row: &mut [N], descending: bool) {
    row.sort_unstable_by(|a, b| a.compare(*b));
    if descending {
        row.reverse();
    }
}

/// Fast path for primitive values with no element-level nulls. Copies all
/// values into a single `Vec` and sorts each row's slice in-place.
fn sort_rows_no_nulls<N: ArrowNativeTypeOp, O: OffsetSizeTrait>(
    src_values: &[N],
    offsets: &OffsetBuffer<O>,
    list_nulls: Option<&NullBuffer>,
    descending: bool,
) -> Vec<N> {
    let (values_start, total_values) = offset_span(offsets);

    // Copy all values into a mutable buffer
    let mut values = src_values[values_start..values_start + total_values].to_vec();

    for (row_index, window) in offsets.windows(2).enumerate() {
        if list_nulls.is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = window[0].as_usize() - values_start;
        let end = window[1].as_usize() - values_start;
        sort_row(&mut values[start..end], descending);
    }

    values
}

/// Slow path for primitive values with element-level nulls.
fn sort_rows_with_nulls<N: ArrowNativeTypeOp, O: OffsetSizeTrait>(
    src_values: &[N],
    src_nulls: &NullBuffer,
    offsets: &OffsetBuffer<O>,
    list_nulls: Option<&NullBuffer>,
    descending: bool,
    nulls_first: bool,
) -> (Vec<N>, NullBuffer) {
    let (values_start, total_values) = offset_span(offsets);

    let mut out_values: Vec<N> = vec![N::default(); total_values];
    let mut validity = BooleanBufferBuilder::new(total_values);

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0].as_usize();
        let end = window[1].as_usize();
        let row_len = end - start;
        let out_start = start - values_start;

        if list_nulls.is_some_and(|n| n.is_null(row_index)) || row_len == 0 {
            validity.append_n(row_len, false);
            continue;
        }

        // Compact valid values to the start of the row's output region and
        // count them. The loop writes every value to the next slot but
        // advances only past valid values, so it doesn't branch on each
        // validity bit; that branch would be hard to predict when NULLs are
        // scattered through the row.
        let validity_bits =
            BitIterator::new(src_nulls.validity(), src_nulls.offset() + start, row_len);
        let mut write_pos = out_start;
        for (&value, is_valid) in src_values[start..end].iter().zip(validity_bits) {
            out_values[write_pos] = value;
            write_pos += is_valid as usize;
        }
        let valid_count = write_pos - out_start;
        let null_count = row_len - valid_count;

        sort_row(&mut out_values[out_start..write_pos], descending);

        // Build validity bits. With nulls_first, move the sorted values after
        // the NULLs.
        if nulls_first {
            if null_count > 0 {
                out_values.copy_within(out_start..write_pos, out_start + null_count);
            }
            validity.append_n(null_count, false);
            validity.append_n(valid_count, true);
        } else {
            validity.append_n(valid_count, true);
            validity.append_n(null_count, false);
        }
    }

    (out_values, NullBuffer::from(validity.finish()))
}

/// Sort a non-pritive-typed ListArray by converting all rows at once using
/// `RowConverter`, and then sort row indices by comparing encoded bytes (sort
/// direction and null ordering are baked into the encoding), and materialize
/// the result with a single `take()`.
fn array_sort_non_primitive<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let row_count = list_array.len();
    let values = list_array.values();
    let offsets = list_array.offsets();
    let (values_start, total_values) = offset_span(offsets);

    // A NULL nested inside an element is the smallest value. The encoding
    // places the NULL elements of the array on the same side as the nested
    // NULLs, so they are moved to the requested side after sorting.
    let sort_options = sort_options.unwrap_or_default();
    let converter = RowConverter::new(vec![SortField::new_with_options(
        values.data_type().clone(),
        spark_nested_sort_options(sort_options.descending),
    )])?;
    let values_sliced = values.slice(values_start, total_values);
    let rows = converter.convert_columns(&[Arc::clone(&values_sliced)])?;
    let move_nulls = sort_options.nulls_first == sort_options.descending;
    let element_nulls = values_sliced.logical_nulls().filter(|_| move_nulls);

    let mut indices: Vec<OffsetSize> = Vec::with_capacity(total_values);
    let mut new_offsets = Vec::with_capacity(row_count + 1);
    new_offsets.push(OffsetSize::usize_as(0));

    let mut sort_scratch: Vec<usize> = Vec::new();

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0];
        let end = window[1];

        if list_array.is_null(row_index) {
            new_offsets.push(new_offsets[row_index]);
            continue;
        }

        let len = (end - start).as_usize();
        let local_start = start.as_usize() - values_start;

        if len <= 1 {
            indices.extend((local_start..local_start + len).map(OffsetSize::usize_as));
        } else {
            sort_row_indices(&mut sort_scratch, &rows, local_start, len);
            if let Some(element_nulls) = &element_nulls {
                let null_count = element_nulls.slice(local_start, len).null_count();
                if sort_options.nulls_first {
                    sort_scratch.rotate_right(null_count);
                } else {
                    sort_scratch.rotate_left(null_count);
                }
            }
            indices.extend(sort_scratch.iter().map(|&i| OffsetSize::usize_as(i)));
        }

        new_offsets.push(new_offsets[row_index] + (end - start));
    }

    let sorted_values = if indices.is_empty() {
        new_empty_array(values.data_type())
    } else {
        take_by_indices(&values_sliced, indices)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        // TODO: Uncomment after DataFusion 56
        // Arc::clone(list_array.value_field()),
        list_value_field(list_array)?,
        OffsetBuffer::<OffsetSize>::new(new_offsets.into()),
        sorted_values,
        list_array.nulls().cloned(),
    )?))
}

/// Sorts `scratch` to hold the indices `start..start + len` ordered by their
/// encoded rows. Kept free of the offset width so the standard library's sort
/// is instantiated once rather than once per list type.
fn sort_row_indices(scratch: &mut Vec<usize>, rows: &Rows, start: usize, len: usize) {
    scratch.clear();
    scratch.extend(start..start + len);
    scratch.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
}

/// Select elements from `values` at the given `indices` using `compute::take`.
/// We consume `indices` in order to avoid an intermediate copy.
fn take_by_indices<OffsetSize: OffsetSizeTrait>(
    values: &ArrayRef,
    indices: Vec<OffsetSize>,
) -> Result<ArrayRef> {
    let len = indices.len();
    let buffer = arrow::buffer::Buffer::from_vec(indices);
    let indices_array: ArrayRef = if OffsetSize::IS_LARGE {
        Arc::new(UInt64Array::new(
            arrow::buffer::ScalarBuffer::new(buffer, 0, len),
            None,
        ))
    } else {
        Arc::new(UInt32Array::new(
            arrow::buffer::ScalarBuffer::new(buffer, 0, len),
            None,
        ))
    };
    Ok(compute::take(values.as_ref(), &indices_array, None)?)
}

/// Rebase offsets so they start at 0. For non-sliced ListArrays (the common
/// case) offsets already start at 0 and we can clone the Arc-backed buffer
/// cheaply instead of allocating a new Vec.
fn rebase_offsets<OffsetSize: OffsetSizeTrait>(
    offsets: &OffsetBuffer<OffsetSize>,
) -> OffsetBuffer<OffsetSize> {
    offsets.clone().subtract(offsets[0])
}

fn list_value_field<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
) -> Result<FieldRef> {
    match list_array.data_type() {
        DataType::List(field) | DataType::LargeList(field) => Ok(Arc::clone(field)),
        other => exec_err!("array_sort expects list for first argument, got {other}"),
    }
}

fn order_desc(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "DESC" => Ok(true),
        "ASC" => Ok(false),
        _ => exec_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => exec_err!("the third parameter of array_sort expects NULLS FIRST or NULLS LAST"),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryViewArray, Decimal128Array, DictionaryArray, Float64Array,
        Int32Array, LargeBinaryArray, LargeStringArray, ListArray, StringArray, StringViewArray,
        StructArray, UInt64Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type};

    // [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions-nested/src/utils.rs#L861-L903
    fn check_sliced_list_behavior(
        run: impl Fn(&ArrayRef) -> datafusion_common::Result<ArrayRef>,
    ) -> datafusion_common::Result<()> {
        let padding = 8192;
        let visible = vec![Some(3.0), None, Some(4.0), Some(9.0), Some(9.0)];
        let values = Float64Array::from_iter(
            std::iter::repeat_n(Some(1.0), padding)
                .chain(visible.iter().copied())
                .chain(std::iter::repeat_n(Some(1.0), padding)),
        );
        let field = Arc::new(Field::new_list_field(DataType::Float64, true));
        let input = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([padding, 3, 0, 2, padding]),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, true, true, false, true])),
        );
        let compact = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([3, 0, 2]),
            Arc::new(Float64Array::from(visible)),
            Some(NullBuffer::from(vec![true, true, false])),
        );
        for data_type in [input.data_type().clone(), DataType::LargeList(field)] {
            let input = arrow::compute::cast(&input, &data_type)?;
            let compact = arrow::compute::cast(&compact, &data_type)?;
            // Middle slice, empty slice, empty row, and null row with child data.
            for (offset, len) in [(0, 3), (0, 0), (1, 1), (2, 1)] {
                let sliced = input.slice(1 + offset, len);
                let result = run(&sliced)?;
                let expected = run(&compact.slice(offset, len))?;
                assert_eq!(result.as_ref(), expected.as_ref());
                // Compare buffers, since evaluating through a plan re-wraps arrays.
                assert!(
                    result.to_data().ptr_eq(&sliced.to_data())
                        || result.get_buffer_memory_size() < 1024,
                    "{data_type}: {} bytes for {len} rows",
                    result.get_buffer_memory_size()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_sliced_list() -> datafusion_common::Result<()> {
        check_sliced_list_behavior(|input| super::array_sort_inner(&[Arc::clone(input)]))
    }

    #[test]
    fn test_sliced_values() -> datafusion_common::Result<()> {
        // Slicing the child array makes its validity bitmap start partway
        // through a byte.
        let values = Int32Array::from(vec![
            Some(0),
            Some(0),
            Some(0),
            Some(3),
            None,
            Some(1),
            Some(2),
            None,
        ]);
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths([3, 2]),
            Arc::new(values.slice(3, 5)),
            None,
        ));
        for (nulls, expected) in [
            (
                "NULLS FIRST",
                [vec![None, Some(1), Some(3)], vec![None, Some(2)]],
            ),
            (
                "NULLS LAST",
                [vec![Some(1), Some(3), None], vec![Some(2), None]],
            ),
        ] {
            let result = super::array_sort_inner(&[
                Arc::clone(&input),
                Arc::new(StringArray::from(vec!["ASC"; 2])),
                Arc::new(StringArray::from(vec![nulls; 2])),
            ])?;
            let expected =
                ListArray::from_iter_primitive::<Int32Type, _, _>(expected.into_iter().map(Some));
            assert_eq!(result.as_ref(), &expected as &dyn Array, "{nulls}");
        }
        Ok(())
    }

    /// The orders used by Spark's `array_sort(array)` and
    /// `sort_array(array, true)` / `sort_array(array, false)`.
    const ARRAY_SORT: (&str, &str) = ("ASC", "NULLS LAST");
    const SORT_ARRAY_ASC: (&str, &str) = ("ASC", "NULLS FIRST");
    const SORT_ARRAY_DESC: (&str, &str) = ("DESC", "NULLS LAST");

    fn sort(input: &ArrayRef, (order, nulls): (&str, &str)) -> datafusion_common::Result<ArrayRef> {
        super::array_sort_inner(&[
            Arc::clone(input),
            Arc::new(StringArray::from(vec![order; input.len()])),
            Arc::new(StringArray::from(vec![nulls; input.len()])),
        ])
    }

    fn single_list(values: ArrayRef) -> ArrayRef {
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([values.len()]),
            values,
            None,
        ))
    }

    fn sorted_values(result: &ArrayRef) -> ArrayRef {
        result
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|l| l.value(0))
            .unwrap()
    }

    fn struct_of_a(values: Vec<Option<i32>>, nulls: Vec<bool>) -> ArrayRef {
        Arc::new(StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(values))],
            Some(NullBuffer::from(nulls)),
        ))
    }

    fn struct_a_values(array: &ArrayRef) -> Vec<Option<Option<i32>>> {
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let a = array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..array.len())
            .map(|i| array.is_valid(i).then(|| a.is_valid(i).then(|| a.value(i))))
            .collect()
    }

    /// A NULL field is the smallest value; only NULL elements follow the
    /// requested NULL order. Expected values come from Spark 4.2.
    #[test]
    fn test_struct_with_null_field() -> datafusion_common::Result<()> {
        // [{a: 1}, {a: NULL}, NULL, {a: 0}]
        let input = single_list(struct_of_a(
            vec![Some(1), None, None, Some(0)],
            vec![true, true, false, true],
        ));
        for (order, expected) in [
            (
                ARRAY_SORT,
                vec![Some(None), Some(Some(0)), Some(Some(1)), None],
            ),
            (
                SORT_ARRAY_ASC,
                vec![None, Some(None), Some(Some(0)), Some(Some(1))],
            ),
            (
                SORT_ARRAY_DESC,
                vec![Some(Some(1)), Some(Some(0)), Some(None), None],
            ),
            (
                ("DESC", "NULLS FIRST"),
                vec![None, Some(Some(1)), Some(Some(0)), Some(None)],
            ),
        ] {
            let result = sort(&input, order)?;
            assert_eq!(
                struct_a_values(&sorted_values(&result)),
                expected,
                "{order:?}"
            );
        }
        Ok(())
    }

    /// Spark compares arrays element by element, with NULL elements smallest
    /// and a prefix before longer arrays. Expected values come from Spark 4.2.
    #[test]
    fn test_nested_list_with_null_elements() -> datafusion_common::Result<()> {
        // [[1], [NULL], [], [1, NULL], NULL, [NULL, 1]]
        let inner = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(1), None]),
            None,
            Some(vec![None, Some(1)]),
        ]);
        let input = single_list(Arc::new(inner));
        for (order, rows) in [
            (
                ARRAY_SORT,
                vec![
                    Some(vec![]),
                    Some(vec![None]),
                    Some(vec![None, Some(1)]),
                    Some(vec![Some(1)]),
                    Some(vec![Some(1), None]),
                    None,
                ],
            ),
            (
                SORT_ARRAY_ASC,
                vec![
                    None,
                    Some(vec![]),
                    Some(vec![None]),
                    Some(vec![None, Some(1)]),
                    Some(vec![Some(1)]),
                    Some(vec![Some(1), None]),
                ],
            ),
            (
                SORT_ARRAY_DESC,
                vec![
                    Some(vec![Some(1), None]),
                    Some(vec![Some(1)]),
                    Some(vec![None, Some(1)]),
                    Some(vec![None]),
                    Some(vec![]),
                    None,
                ],
            ),
        ] {
            let result = sort(&input, order)?;
            let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(rows);
            assert_eq!(
                sorted_values(&result).as_ref(),
                &expected as &dyn Array,
                "{order:?}"
            );
        }
        Ok(())
    }

    /// NULL elements of non-primitive element types follow the requested
    /// NULL order.
    #[test]
    fn test_non_primitive_null_elements() -> datafusion_common::Result<()> {
        let values = vec![Some("b"), None, Some("a"), Some(""), None];
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values.clone())),
            Arc::new(
                values
                    .clone()
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(LargeBinaryArray::from(
                values
                    .iter()
                    .map(|v| v.map(str::as_bytes))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BinaryViewArray::from(
                values
                    .iter()
                    .map(|v| v.map(str::as_bytes))
                    .collect::<Vec<_>>(),
            )),
        ];
        for values in inputs {
            let input = single_list(Arc::clone(&values));
            for (order, expected) in [
                (ARRAY_SORT, vec![Some(""), Some("a"), Some("b"), None, None]),
                (
                    SORT_ARRAY_ASC,
                    vec![None, None, Some(""), Some("a"), Some("b")],
                ),
                (
                    SORT_ARRAY_DESC,
                    vec![Some("b"), Some("a"), Some(""), None, None],
                ),
            ] {
                let result = sorted_values(&sort(&input, order)?);
                assert_eq!(result.data_type(), values.data_type());
                let result = arrow::compute::cast(&result, &DataType::Utf8)?;
                let expected: ArrayRef = Arc::new(StringArray::from(expected));
                assert_eq!(
                    result.as_ref(),
                    expected.as_ref(),
                    "{} {order:?}",
                    values.data_type()
                );
            }
        }
        Ok(())
    }

    /// NULL elements of primitive element types follow the requested NULL
    /// order.
    #[test]
    fn test_primitive_null_elements() -> datafusion_common::Result<()> {
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(150), None, Some(-225)])
                .with_precision_and_scale(10, 2)?,
        );
        let unsigned: ArrayRef = Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(0)]));
        for values in [decimals, unsigned] {
            let input = single_list(Arc::clone(&values));
            for (order, expected) in [
                (ARRAY_SORT, [Some(2), Some(0), None]),
                (SORT_ARRAY_ASC, [None, Some(2), Some(0)]),
                (SORT_ARRAY_DESC, [Some(0), Some(2), None]),
            ] {
                let result = sorted_values(&sort(&input, order)?);
                let indices = UInt64Array::from(expected.to_vec());
                let expected = arrow::compute::take(values.as_ref(), &indices, None)?;
                assert_eq!(
                    result.as_ref(),
                    expected.as_ref(),
                    "{} {order:?}",
                    values.data_type()
                );
            }
        }
        Ok(())
    }

    /// Each row is sorted on its own, NULL rows stay NULL, and large lists
    /// are supported.
    #[test]
    fn test_multiple_rows_and_large_list() -> datafusion_common::Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let input = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2, 0, 2, 2]),
            Arc::new(StringArray::from(vec![
                None,
                Some("b"),
                Some("y"),
                Some("x"),
                Some("d"),
                Some("c"),
            ])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let large = arrow::compute::cast(&input, &DataType::LargeList(Arc::clone(&field)))?;
        let input: ArrayRef = Arc::new(input);
        let expected: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([2, 0, 0, 2]),
            Arc::new(StringArray::from(vec![
                Some("b"),
                None,
                Some("c"),
                Some("d"),
            ])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        ));
        for input in [input, large] {
            let result = sort(&input, ARRAY_SORT)?;
            assert_eq!(result.data_type(), input.data_type());
            let expected = arrow::compute::cast(&expected, input.data_type())?;
            assert_eq!(result.as_ref(), expected.as_ref());
            // A sliced input gives the same rows.
            let result = sort(&input.slice(1, 3), ARRAY_SORT)?;
            assert_eq!(result.as_ref(), expected.slice(1, 3).as_ref());
        }
        Ok(())
    }
}
