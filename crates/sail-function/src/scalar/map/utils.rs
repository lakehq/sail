use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Int32Array, MapArray, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{filter, take};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use sail_common::spec::{SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

/// Helper function to get element [`DataType`]
/// from [`List`](DataType::List)/[`LargeList`](DataType::LargeList)/[`FixedSizeList`](DataType::FixedSizeList)<br>
/// [`Null`](DataType::Null) can be coerced to `ListType`([`Null`](DataType::Null)), so [`Null`](DataType::Null) is returned<br>
/// For all other types [`exec_err`] is raised
pub fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::Null => Ok(data_type),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "map_from_arrays expects 2 listarrays for keys and values as arguments, got {data_type:?}"
        ),
    }
}

/// Helper function to get [`values`](arrow::array::ListArray::values)
/// from [`ListArray`](arrow::array::ListArray)/[`LargeListArray`](arrow::array::LargeListArray)/[`FixedSizeListArray`](arrow::array::FixedSizeListArray)<br>
/// [`NullArray`](arrow::array::NullArray) can be coerced to `ListType`([`Null`](DataType::Null)), so [`NullArray`](arrow::array::NullArray) is returned<br>
/// For all other types [`exec_err`] is raised
pub fn get_list_values(array: &ArrayRef) -> Result<&ArrayRef> {
    match array.data_type() {
        DataType::Null => Ok(array),
        DataType::List(_) => Ok(array.as_list::<i32>().values()),
        DataType::LargeList(_) => Ok(array.as_list::<i64>().values()),
        DataType::FixedSizeList(..) => Ok(array.as_fixed_size_list().values()),
        wrong_type => internal_err!(
            "get_list_values expects List/LargeList/FixedSizeList as argument, got {wrong_type:?}"
        ),
    }
}

/// Helper function to get [`offsets`](arrow::array::ListArray::offsets)
/// from [`ListArray`](arrow::array::ListArray)/[`LargeListArray`](arrow::array::LargeListArray)/[`FixedSizeListArray`](arrow::array::FixedSizeListArray)<br>
/// For all other types [`exec_err`] is raised
pub fn get_list_offsets(array: &ArrayRef) -> Result<Cow<'_, [i32]>> {
    match array.data_type() {
        DataType::List(_) => Ok(Cow::Borrowed(array.as_list::<i32>().offsets().as_ref())),
        DataType::LargeList(_) => Ok(Cow::Owned(
            array
                .as_list::<i64>()
                .offsets()
                .iter()
                .map(|i| *i as i32)
                .collect::<Vec<_>>(),
        )),
        DataType::FixedSizeList(_, size) => Ok(Cow::Owned(
            (0..=array.len() as i32).map(|i| size * i).collect(),
        )),
        wrong_type => internal_err!(
            "map_from_arrays expects List/LargeList/FixedSizeList as first argument, got {wrong_type:?}"
        ),
    }
}

/// Helper function to construct [`MapType<K, V>`](DataType::Map) given K and V DataTypes for keys and values
/// - Map keys are unsorted
/// - Map keys are non-nullable
/// - Map entries are non-nullable
/// - Map values can be null
pub fn map_type_from_key_value_types(key_type: &DataType, value_type: &DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            SAIL_MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![
                // the key must not be nullable
                Field::new(SAIL_MAP_KEY_FIELD_NAME, key_type.clone(), false),
                Field::new(SAIL_MAP_VALUE_FIELD_NAME, value_type.clone(), true),
            ])),
            false, // the entry is not nullable
        )),
        false, // the keys are not sorted
    )
}

/// Helper function to construct MapArray from flattened ListArrays and OffsetBuffer
///
/// Logic is close to `datafusion_functions_nested::map::make_map_array_internal`<br>
/// But there are some core differences:
/// 1. Input arrays are not [`ListArrays`](arrow::array::ListArray) itself, but their flattened [`values`](arrow::array::ListArray::values)<br>
///    So the inputs can be [`ListArray`](`arrow::array::ListArray`)/[`LargeListArray`](`arrow::array::LargeListArray`)/[`FixedSizeListArray`](`arrow::array::FixedSizeListArray`)<br>
///    To preserve the row info, [`offsets`](arrow::array::ListArray::offsets) and [`nulls`](arrow::array::ListArray::nulls) for both keys and values need to be provided<br>
///    [`FixedSizeListArray`](`arrow::array::FixedSizeListArray`) has no `offsets`, so they can be generated as a cumulative sum of it's `Size`
/// 2. Duplicate-key handling follows Spark's `spark.sql.mapKeyDedupPolicy`:
///    `false` raises an error and `true` keeps the last value at the first key position.
pub fn map_from_keys_values_offsets_nulls(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
    last_value_wins: bool,
) -> Result<ArrayRef> {
    let (keys, values, offsets) = map_deduplicate_keys(
        flat_keys,
        flat_values,
        keys_offsets,
        values_offsets,
        keys_nulls,
        values_nulls,
        last_value_wins,
    )?;
    let nulls = NullBuffer::union(keys_nulls, values_nulls);

    let fields = Fields::from(vec![
        Field::new(
            SAIL_MAP_KEY_FIELD_NAME,
            flat_keys.data_type().clone(),
            false,
        ),
        Field::new(
            SAIL_MAP_VALUE_FIELD_NAME,
            flat_values.data_type().clone(),
            true,
        ),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new(
        SAIL_MAP_FIELD_NAME,
        DataType::Struct(fields),
        false,
    ));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, nulls, false,
    )?))
}

#[allow(clippy::allow_attributes, clippy::mutable_key_type)]
fn map_deduplicate_keys(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
    last_value_wins: bool,
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>)> {
    let offsets_len = keys_offsets.len();
    let mut new_offsets = Vec::with_capacity(offsets_len);

    let mut cur_keys_offset = keys_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);
    let mut cur_values_offset = values_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);

    let mut new_last_offset = 0;
    new_offsets.push(new_last_offset);

    let mut keys_mask_builder = BooleanBuilder::new();
    let mut value_indices = Vec::new();
    let mut key_to_output_index: HashMap<ScalarValue, usize> = HashMap::new();
    for (row_idx, (next_keys_offset, next_values_offset)) in keys_offsets
        .iter()
        .zip(values_offsets.iter())
        .skip(1)
        .enumerate()
    {
        let num_keys_entries = *next_keys_offset as usize - cur_keys_offset;
        let num_values_entries = *next_values_offset as usize - cur_values_offset;

        let key_is_valid = keys_nulls.is_none_or(|buf| buf.is_valid(row_idx));
        let value_is_valid = values_nulls.is_none_or(|buf| buf.is_valid(row_idx));

        if key_is_valid && value_is_valid {
            if num_keys_entries != num_values_entries {
                return exec_err!(
                    "map_deduplicate_keys: keys and values lists in the same row must have equal lengths"
                );
            }
            key_to_output_index.clear();
            for entry_index in 0..num_keys_entries {
                let key = ScalarValue::try_from_array(flat_keys, cur_keys_offset + entry_index)?
                    .compacted();
                let value_index = (cur_values_offset + entry_index) as i32;

                if let Some(&output_index) = key_to_output_index.get(&key) {
                    if last_value_wins {
                        value_indices[output_index] = value_index;
                        keys_mask_builder.append_value(false);
                        continue;
                    }
                    return exec_err!(
                        "[DUPLICATED_MAP_KEY] Duplicate map key {key} was found. To allow duplicate keys, set `spark.sql.mapKeyDedupPolicy` to `LAST_WIN`."
                    );
                }
                keys_mask_builder.append_value(true);
                key_to_output_index.insert(key, value_indices.len());
                value_indices.push(value_index);
                new_last_offset += 1;
            }
        } else {
            keys_mask_builder.append_n(num_keys_entries, false);
        }
        new_offsets.push(new_last_offset);
        cur_keys_offset += num_keys_entries;
        cur_values_offset += num_values_entries;
    }
    let keys_mask = keys_mask_builder.finish();
    let needed_keys = filter(flat_keys, &keys_mask)?;
    let needed_values = take(flat_values, &Int32Array::from(value_indices), None)?;
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}
