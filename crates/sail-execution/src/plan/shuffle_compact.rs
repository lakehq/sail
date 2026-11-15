//! Copies and compacts arrays so that queued shuffle output holds little more than its rows.

use std::sync::Arc;

use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, ByteView, GenericByteViewArray, GenericListViewArray,
    MutableArrayData, OffsetSizeTrait, UInt64Array, make_array,
};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{
    ByteViewType, DataType, Int16Type, Int32Type, Int64Type, RunEndIndexType, Schema, UnionMode,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

/// Views of strings at most this long hold them inline rather than in a data buffer.
const MAX_INLINE_VIEW_LEN: u32 = 12;

/// The largest data buffer a view can address.
const MAX_VIEW_BUFFER_LEN: usize = i32::MAX as usize;

/// Arrow pads each buffer it allocates to a multiple of this many bytes.
const BUFFER_ALIGNMENT: usize = 64;

/// Copies an array so that it holds only the memory its rows need.
pub(super) fn copy(array: &ArrayRef) -> Result<ArrayRef> {
    copy_ranges(array, &[(0, array.len())])
}

/// Copies the rows in each of `ranges`, in order, into one array.
fn copy_ranges(array: &ArrayRef, ranges: &[(usize, usize)]) -> Result<ArrayRef> {
    let copy = if contains_list_view(array.data_type()) {
        // `MutableArrayData` copies the values of each list view on its own, so values
        // that several lists share would be copied once for each of them.
        let indices: UInt64Array = ranges
            .iter()
            .flat_map(|&(start, end)| start as u64..end as u64)
            .collect();
        take(array.as_ref(), &indices, None)?
    } else {
        let data = array.to_data();
        let rows = ranges.iter().map(|(start, end)| end - start).sum();
        let mut copy = MutableArrayData::new(vec![&data], false, rows);
        for &(start, end) in ranges {
            copy.try_extend(0, start, end)?;
        }
        let mut copy = make_array(copy.freeze());
        // `MutableArrayData` reserves a slot per row for the values of runs and for each
        // child of a union.
        if contains(array.data_type(), &|t| {
            matches!(t, DataType::RunEndEncoded(..) | DataType::Union(..))
        }) && let Some(copy) = Arc::get_mut(&mut copy)
        {
            copy.shrink_to_fit();
        }
        copy
    };
    trim(copy, true)
}

/// Drops the view strings, dictionary values, and list view values that an array holds
/// but mostly does not use, including in its children, and the values that a sliced list,
/// dense union, or run array leaves out.
///
/// When `detach` is set, dictionary and list view values are copied even when all of
/// them are used, since they may still be shared with the array that was copied.
pub(super) fn trim(array: ArrayRef, detach: bool) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => {
            Ok(trim_views(array.as_string_view())?
                .map_or(array, |views| Arc::new(views) as ArrayRef))
        }
        DataType::BinaryView => {
            Ok(trim_views(array.as_binary_view())?
                .map_or(array, |views| Arc::new(views) as ArrayRef))
        }
        DataType::Dictionary(_, _) => trim_dictionary(&array, detach),
        DataType::List(_) | DataType::Map(_, _) => trim_list::<i32>(array, detach),
        DataType::LargeList(_) => trim_list::<i64>(array, detach),
        DataType::ListView(_) => trim_list_view::<i32>(array, detach),
        DataType::LargeListView(_) => trim_list_view::<i64>(array, detach),
        DataType::Union(_, UnionMode::Dense) | DataType::RunEndEncoded(..)
            if !uses_all_children(&array) =>
        {
            copy(&array)
        }
        data_type if data_type.is_nested() || matches!(data_type, DataType::RunEndEncoded(..)) => {
            trim_children(array, detach)
        }
        _ => Ok(array),
    }
}

fn trim_children(array: ArrayRef, detach: bool) -> Result<ArrayRef> {
    let data = array.to_data();
    let mut changed = false;
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            let child = make_array(child.clone());
            let trimmed = trim(Arc::clone(&child), detach)?;
            changed |= !Arc::ptr_eq(&child, &trimmed);
            Ok(trimmed.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(array);
    }
    Ok(make_array(
        data.into_builder().child_data(children).build()?,
    ))
}

/// Trims the values that a list's offsets cover, leaving out the rest of a sliced list's values.
fn trim_list<O: OffsetSizeTrait>(array: ArrayRef, detach: bool) -> Result<ArrayRef> {
    let data = array.to_data();
    let offsets = &data.buffers()[0].typed_data::<O>()[data.offset()..=data.offset() + data.len()];
    let start = offsets[0].as_usize();
    let end = offsets[data.len()].as_usize();
    let values = make_array(data.child_data()[0].slice(start, end - start));
    let trimmed = trim(Arc::clone(&values), detach)?;
    if Arc::ptr_eq(&values, &trimmed) {
        return Ok(array);
    }
    let offsets: ScalarBuffer<O> = offsets.iter().map(|&offset| offset - offsets[0]).collect();
    Ok(make_array(
        ArrayData::builder(data.data_type().clone())
            .len(data.len())
            .nulls(data.nulls().cloned())
            .add_buffer(offsets.into_inner())
            .add_child_data(trimmed.to_data())
            .build()?,
    ))
}

/// Returns whether a dense union or run array uses every value of its children,
/// which slicing leaves in place.
fn uses_all_children(array: &ArrayRef) -> bool {
    fn runs<R: RunEndIndexType>(array: &ArrayRef) -> bool {
        let runs = array.as_run::<R>();
        if runs.is_empty() {
            return runs.values().is_empty();
        }
        runs.get_start_physical_index() == 0
            && runs.get_end_physical_index() + 1 == runs.values().len()
    }
    match array.data_type() {
        DataType::Union(_, UnionMode::Dense) => {
            let data = array.to_data();
            data.child_data().iter().map(ArrayData::len).sum::<usize>() <= array.len()
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => runs::<Int16Type>(array),
            DataType::Int32 => runs::<Int32Type>(array),
            DataType::Int64 => runs::<Int64Type>(array),
            _ => true,
        },
        _ => true,
    }
}

/// Copies the bytes that views use into new buffers if they use at most half of their
/// buffers, copying bytes that several views share only once.
fn trim_views<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
) -> Result<Option<GenericByteViewArray<T>>> {
    let buffers = array.data_buffers();
    if buffers.is_empty() {
        return Ok(None);
    }
    let ranges = array
        .views()
        .iter()
        .enumerate()
        .filter(|&(i, &view)| view as u32 > MAX_INLINE_VIEW_LEN && array.is_valid(i))
        .map(|(_, &view)| {
            let view = ByteView::from(view);
            let start = view.offset as usize;
            (view.buffer_index, start, start + view.length as usize)
        })
        .collect();
    let ranges = merge_ranges(ranges);
    let used: usize = ranges.iter().map(|(_, start, end)| end - start).sum();
    let held: usize = buffers.iter().map(Buffer::capacity).sum();
    if held <= 2 * used {
        return Ok(None);
    }
    let mut blocks = vec![];
    let mut block: Vec<u8> = Vec::with_capacity(used.min(MAX_VIEW_BUFFER_LEN));
    // The buffer and offset each range is copied to.
    let mut placed = Vec::with_capacity(ranges.len());
    for &(buffer, start, end) in &ranges {
        if block.len() + (end - start) > MAX_VIEW_BUFFER_LEN {
            blocks.push(Buffer::from_vec(std::mem::take(&mut block)));
        }
        placed.push((blocks.len() as u32, block.len()));
        block.extend_from_slice(&buffers[buffer as usize][start..end]);
    }
    if !block.is_empty() {
        blocks.push(Buffer::from_vec(block));
    }
    let views: Vec<u128> = array
        .views()
        .iter()
        .enumerate()
        .map(|(i, &view)| {
            if view as u32 <= MAX_INLINE_VIEW_LEN {
                return view;
            }
            if array.is_null(i) {
                return 0;
            }
            let mut view = ByteView::from(view);
            let start = view.offset as usize;
            let range = ranges
                .partition_point(|&(buffer, range_start, _)| {
                    (buffer, range_start) <= (view.buffer_index, start)
                })
                .saturating_sub(1);
            let (block, offset) = placed[range];
            view.buffer_index = block;
            view.offset = (offset + start - ranges[range].1) as u32;
            view.as_u128()
        })
        .collect();
    Ok(Some(GenericByteViewArray::try_new(
        views.into(),
        blocks,
        array.nulls().cloned(),
    )?))
}

/// Drops the dictionary values no key uses, and copies the rest if they hold more
/// memory than they need.
fn trim_dictionary(array: &ArrayRef, detach: bool) -> Result<ArrayRef> {
    let collected = garbage_collect_any_dictionary(array.as_any_dictionary())?;
    let dictionary = collected.as_any_dictionary();
    let values = dictionary.values();
    let values = if Arc::ptr_eq(values, array.as_any_dictionary().values())
        && (detach || oversized(values))
    {
        copy(values)?
    } else {
        trim(Arc::clone(values), detach)?
    };
    Ok(dictionary.with_values(values))
}

/// Copies the values that list views use into a new array if any are unused, keeping
/// each value that several lists share once.
fn trim_list_view<O: OffsetSizeTrait>(array: ArrayRef, detach: bool) -> Result<ArrayRef> {
    let lists = array.as_list_view::<O>();
    let ranges = lists
        .offsets()
        .iter()
        .zip(lists.sizes().iter())
        .enumerate()
        .filter(|&(i, (_, size))| size.as_usize() > 0 && lists.is_valid(i))
        .map(|(_, (offset, size))| ((), offset.as_usize(), (*offset + *size).as_usize()))
        .collect();
    let ranges = merge_ranges(ranges);
    let used: usize = ranges.iter().map(|(_, start, end)| end - start).sum();
    let values = lists.values();
    if !detach && used == values.len() && !oversized(values) {
        return trim_children(array, false);
    }
    let copied: Vec<(usize, usize)> = ranges.iter().map(|&(_, start, end)| (start, end)).collect();
    let new_values = copy_ranges(values, &copied)?;
    let mut starts = Vec::with_capacity(ranges.len());
    let mut position = 0;
    for &(start, end) in &copied {
        starts.push(position);
        position += end - start;
    }
    let (offsets, sizes): (Vec<O>, Vec<O>) = lists
        .offsets()
        .iter()
        .zip(lists.sizes().iter())
        .enumerate()
        .map(|(i, (&offset, &size))| {
            if size.as_usize() == 0 || lists.is_null(i) {
                return (O::zero(), O::zero());
            }
            let offset = offset.as_usize();
            let range = copied
                .partition_point(|&(start, _)| start <= offset)
                .saturating_sub(1);
            let offset = starts[range] + offset - copied[range].0;
            (O::usize_as(offset), size)
        })
        .unzip();
    let field = match lists.data_type() {
        DataType::ListView(field) | DataType::LargeListView(field) => Arc::clone(field),
        _ => return Ok(array),
    };
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        field,
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        new_values,
        lists.nulls().cloned(),
    )?))
}

/// Sorts ranges and merges those that overlap or touch within the same key.
fn merge_ranges<K: Ord + Copy>(mut ranges: Vec<(K, usize, usize)>) -> Vec<(K, usize, usize)> {
    ranges.sort_unstable();
    let mut merged: Vec<(K, usize, usize)> = Vec::with_capacity(ranges.len());
    for (key, start, end) in ranges {
        match merged.last_mut() {
            Some(last) if last.0 == key && start <= last.2 => last.2 = last.2.max(end),
            _ => merged.push((key, start, end)),
        }
    }
    merged
}

/// Returns whether an array holds more than twice the memory its rows need.
///
/// This does not count view strings, dictionary values, or list view values,
/// which `trim` compacts on its own.
pub(super) fn oversized(array: &ArrayRef) -> bool {
    let (held, needed) = footprint(array.as_ref());
    held > 2 * needed
}

/// Returns the bytes of the buffers behind an array, and the bytes its rows need.
fn footprint(array: &dyn Array) -> (usize, usize) {
    let len = array.len();
    let (mut held, mut needed) = array.nulls().map_or((0, 0), |nulls| {
        (nulls.buffer().capacity(), padded(len.div_ceil(8)))
    });
    let (h, n) = match array.data_type() {
        DataType::Boolean => (
            array.as_boolean().values().inner().capacity(),
            padded(len.div_ceil(8)),
        ),
        DataType::FixedSizeBinary(width) => (
            array.as_fixed_size_binary().values().capacity(),
            padded(len * *width as usize),
        ),
        data_type if data_type.is_primitive() => {
            let width = data_type.primitive_width().unwrap_or(0);
            let held = array
                .to_data()
                .buffers()
                .first()
                .map_or(0, Buffer::capacity);
            (held, padded(len * width))
        }
        DataType::Utf8 => bytes_footprint(
            array.as_string::<i32>().offsets(),
            array.as_string::<i32>().values(),
        ),
        DataType::LargeUtf8 => bytes_footprint(
            array.as_string::<i64>().offsets(),
            array.as_string::<i64>().values(),
        ),
        DataType::Binary => bytes_footprint(
            array.as_binary::<i32>().offsets(),
            array.as_binary::<i32>().values(),
        ),
        DataType::LargeBinary => bytes_footprint(
            array.as_binary::<i64>().offsets(),
            array.as_binary::<i64>().values(),
        ),
        DataType::Utf8View | DataType::BinaryView => {
            let views = array.to_data();
            (
                views.buffers().first().map_or(0, Buffer::capacity),
                padded(len * 16),
            )
        }
        DataType::List(_) => {
            let lists = array.as_list::<i32>();
            list_footprint(lists.offsets(), lists.values())
        }
        DataType::LargeList(_) => {
            let lists = array.as_list::<i64>();
            list_footprint(lists.offsets(), lists.values())
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            list_footprint(map.offsets(), &entries)
        }
        DataType::FixedSizeList(_, _) => footprint(array.as_fixed_size_list().values().as_ref()),
        DataType::Struct(_) => {
            sum_footprints(array.as_struct().columns().iter().map(AsRef::as_ref))
        }
        DataType::ListView(_) => list_view_footprint(array.as_list_view::<i32>()),
        DataType::LargeListView(_) => list_view_footprint(array.as_list_view::<i64>()),
        DataType::Dictionary(_, _) => footprint(array.as_any_dictionary().keys()),
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run_footprint::<Int16Type>(array),
            DataType::Int32 => run_footprint::<Int32Type>(array),
            DataType::Int64 => run_footprint::<Int64Type>(array),
            _ => (0, 0),
        },
        DataType::Union(_, _) => {
            let data = array.to_data();
            let buffers = data.buffers().iter().map(Buffer::capacity).sum::<usize>();
            let ids_and_offsets = padded(len) + (data.buffers().len() - 1) * padded(len * 4);
            let (h, n) = sum_footprints(
                data.child_data()
                    .iter()
                    .map(|child| make_array(child.clone()))
                    .collect::<Vec<_>>()
                    .iter()
                    .map(AsRef::as_ref),
            );
            (buffers + h, ids_and_offsets + n)
        }
        _ => (0, 0),
    };
    held += h;
    needed += n;
    (held, needed)
}

/// Returns the bytes a buffer of `len` bytes takes once allocated.
fn padded(len: usize) -> usize {
    len.next_multiple_of(BUFFER_ALIGNMENT)
}

fn sum_footprints<'a>(arrays: impl Iterator<Item = &'a dyn Array>) -> (usize, usize) {
    arrays
        .map(footprint)
        .fold((0, 0), |(h, n), (a, b)| (h + a, n + b))
}

fn bytes_footprint<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    values: &Buffer,
) -> (usize, usize) {
    let used = offsets.last().map_or(0, |last| last.as_usize())
        - offsets.first().map_or(0, |first| first.as_usize());
    (
        offsets.inner().inner().capacity() + values.capacity(),
        padded(offsets.inner().inner().len()) + padded(used),
    )
}

fn list_footprint<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    values: &ArrayRef,
) -> (usize, usize) {
    let start = offsets.first().map_or(0, |first| first.as_usize());
    let end = offsets.last().map_or(0, |last| last.as_usize());
    let (h, n) = footprint(values.slice(start, end - start).as_ref());
    (
        offsets.inner().inner().capacity() + h,
        padded(offsets.inner().inner().len()) + n,
    )
}

fn list_view_footprint<O: OffsetSizeTrait>(lists: &GenericListViewArray<O>) -> (usize, usize) {
    let offsets = lists.offsets().inner();
    let sizes = lists.sizes().inner();
    (
        offsets.capacity() + sizes.capacity(),
        padded(offsets.len()) + padded(sizes.len()),
    )
}

fn run_footprint<R: RunEndIndexType>(array: &dyn Array) -> (usize, usize) {
    let runs = array.as_run::<R>();
    let run_ends = runs.run_ends();
    let held = run_ends.inner().inner().capacity();
    if runs.is_empty() {
        return (held + footprint(runs.values().as_ref()).0, 0);
    }
    let start = runs.get_start_physical_index();
    let count = runs.get_end_physical_index() + 1 - start;
    let (h, n) = footprint(runs.values().slice(start, count).as_ref());
    (held + h, padded(count * size_of::<R::Native>()) + n)
}

/// Returns whether a type or any type nested in it matches.
fn contains(data_type: &DataType, matches: &dyn Fn(&DataType) -> bool) -> bool {
    matches(data_type)
        || child_types(data_type)
            .into_iter()
            .any(|child| contains(child, matches))
}

/// Returns whether a type has list views, which `MutableArrayData` would copy value by value.
fn contains_list_view(data_type: &DataType) -> bool {
    contains(data_type, &|t| {
        matches!(t, DataType::ListView(_) | DataType::LargeListView(_))
    })
}

/// Returns whether a type has dictionaries, which can hold values no key uses.
pub(super) fn contains_dictionary(data_type: &DataType) -> bool {
    contains(data_type, &|t| matches!(t, DataType::Dictionary(_, _)))
}

/// Returns the types of an array's children, in the order of its child data.
fn child_types(data_type: &DataType) -> Vec<&DataType> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field.data_type()],
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| field.data_type()).collect(),
        DataType::Dictionary(_, values) => vec![values],
        DataType::RunEndEncoded(run_ends, values) => {
            vec![run_ends.data_type(), values.data_type()]
        }
        _ => vec![],
    }
}

/// Returns how many values each dictionary in the schema can hold before its keys
/// overflow, in the order that [`dictionary_sizes`] lists them.
pub(super) fn dictionary_capacities(schema: &Schema) -> Vec<usize> {
    fn add(data_type: &DataType, capacities: &mut Vec<usize>) {
        if let DataType::Dictionary(key, _) = data_type {
            capacities.push(match key.as_ref() {
                DataType::Int8 => 1 << 7,
                DataType::UInt8 => 1 << 8,
                DataType::Int16 => 1 << 15,
                DataType::UInt16 => 1 << 16,
                DataType::Int32 => 1 << 31,
                DataType::UInt32 => 1 << 32,
                _ => usize::MAX,
            });
        }
        for child in child_types(data_type) {
            add(child, capacities);
        }
    }
    let mut capacities = vec![];
    for field in schema.fields() {
        add(field.data_type(), &mut capacities);
    }
    capacities
}

/// Returns the number of values of each dictionary in the batch.
pub(super) fn dictionary_sizes(batch: &RecordBatch) -> Vec<usize> {
    fn add(data: &ArrayData, sizes: &mut Vec<usize>) {
        if let (DataType::Dictionary(_, _), Some(values)) =
            (data.data_type(), data.child_data().first())
        {
            sizes.push(values.len());
        }
        for child in data.child_data() {
            add(child, sizes);
        }
    }
    let mut sizes = vec![];
    for column in batch.columns() {
        add(&column.to_data(), &mut sizes);
    }
    sizes
}
