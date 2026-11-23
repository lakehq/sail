// SPDX-License-Identifier: Apache-2.0

//! The rows of a partitioned write are distributed and sorted without their
//! dictionaries, and the files are written with them.
//!
//! The operators that distribute and sort rows combine the batches that they
//! receive. Batches with dictionaries of their own cannot always be combined,
//! since the dictionary of the combined batch can have more values than
//! the type of the keys can refer to, and a batch that is divided has
//! the whole dictionary in each of its parts. So the plan of the write replaces
//! each dictionary with its values before the rows are distributed, and
//! the writer task returns the rows to the data types of the files.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, FixedSizeListArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions, StructArray,
    UInt64Array, make_array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{CastOptions, can_cast_types, cast, cast_with_options};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, cast as cast_expr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::Result;
use sail_common_datafusion::data_type::{contains_dictionary, without_dictionaries};

/// Returns the data type with the type of the values in place of each
/// dictionary at any depth, if the data type has a dictionary and Arrow
/// converts its arrays to that data type.
fn plain_type(data_type: &DataType) -> Option<DataType> {
    if !contains_dictionary(data_type) {
        return None;
    }
    let plain = without_dictionaries(data_type);
    can_cast_types(data_type, &plain).then_some(plain)
}

/// Returns the plan whose columns have the values of dictionaries in place of
/// the dictionaries, or the plan itself if none of its columns is converted.
/// The columns keep their names and their order.
pub(in crate::formats::parquet) fn without_dictionary_columns(
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let fields = schema.fields();
    if !fields.iter().any(|x| plain_type(x.data_type()).is_some()) {
        return Ok(input);
    }
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
            let column = match plain_type(field.data_type()) {
                Some(data_type) => cast_expr(column, &schema, data_type)?,
                None => column,
            };
            Ok((column, field.name().clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(columns, input)?))
}

/// Returns the rows with the data types of the schema, which differ from
/// the data types of the rows in their dictionaries only.
///
/// The rows are returned in several batches when one dictionary cannot refer
/// to the values of all rows. One row always has a dictionary, since the row
/// had one in the input.
pub(super) fn with_dictionaries(rows: RecordBatch, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    // The file reports rows that do not have the columns of its schema.
    let converted = rows.num_columns() == schema.fields().len()
        && rows
            .columns()
            .iter()
            .zip(schema.fields())
            .any(|(column, field)| column.data_type() != field.data_type());
    if !converted {
        return Ok(vec![rows]);
    }
    let mut batches = vec![];
    // The rows that are not converted, with the last rows first.
    let mut rest = vec![rows];
    while let Some(rows) = rest.pop() {
        let columns = rows
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| with_dictionary(column, field.data_type()))
            .collect::<Result<Vec<_>, _>>();
        match columns {
            Ok(columns) => {
                let options = RecordBatchOptions::new().with_row_count(Some(rows.num_rows()));
                let schema = Arc::clone(schema);
                batches.push(RecordBatch::try_new_with_options(
                    schema, columns, &options,
                )?);
            }
            Err(ArrowError::DictionaryKeyOverflowError) if rows.num_rows() > 1 => {
                let half = rows.num_rows() / 2;
                rest.push(rows.slice(half, rows.num_rows() - half));
                rest.push(rows.slice(0, half));
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(batches)
}

/// Returns the array with the data type, which differs from the data type
/// of the array in its dictionaries only.
///
/// A dictionary is created for the values that the rows of the array refer to,
/// and not for all values of an array that the array is a slice of. The values
/// of an array of runs are not divided, so all its values are in the dictionary.
fn with_dictionary(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    match data_type {
        DataType::List(field) => list_with_dictionary::<i32>(array, field),
        DataType::LargeList(field) => list_with_dictionary::<i64>(array, field),
        DataType::ListView(field) => list_view_with_dictionary::<i32>(array, field),
        DataType::LargeListView(field) => list_view_with_dictionary::<i64>(array, field),
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let values = with_dictionary(list.values(), field.data_type())?;
            let list = FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                *size,
                values,
                list.nulls().cloned(),
                list.len(),
            )?;
            Ok(Arc::new(list))
        }
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let (start, offsets) = rebased_offsets(map.offsets());
            let length = offsets.last().map_or(0, |x| x.as_usize());
            let entries: ArrayRef = Arc::new(map.entries().slice(start, length));
            let entries = with_dictionary(&entries, field.data_type())?;
            let map = MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *sorted,
            )?;
            Ok(Arc::new(map))
        }
        DataType::Struct(fields) => {
            let values = array.as_struct();
            let columns = values
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| with_dictionary(column, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            let values = StructArray::try_new_with_length(
                fields.clone(),
                columns,
                values.nulls().cloned(),
                values.len(),
            )?;
            Ok(Arc::new(values))
        }
        DataType::Dictionary(keys, _) => match cast(array, data_type) {
            // Arrow does not create dictionaries for the values of some data types.
            Err(ArrowError::CastError(_) | ArrowError::NotYetImplemented(_)) => {
                dictionary_of_rows(array, keys)
            }
            result => result,
        },
        _ => cast(array, data_type),
    }
}

/// Returns the dictionary with keys of the data type that has each row
/// of the array as a value, also when the rows have equal values.
fn dictionary_of_rows(array: &ArrayRef, keys: &DataType) -> Result<ArrayRef, ArrowError> {
    let rows: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..array.len() as u64));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let rows = cast_with_options(&rows, keys, &options)
        .map_err(|_| ArrowError::DictionaryKeyOverflowError)?
        .to_data();
    let data_type =
        DataType::Dictionary(Box::new(keys.clone()), Box::new(array.data_type().clone()));
    let data = ArrayData::builder(data_type)
        .len(array.len())
        .buffers(rows.buffers().to_vec())
        .nulls(array.logical_nulls())
        .add_child_data(array.to_data())
        .build()?;
    Ok(make_array(data))
}

/// Returns the offset of the first value of a list array in the array of its
/// values, and the offsets of the lists from that value on.
fn rebased_offsets<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> (usize, OffsetBuffer<O>) {
    let start = offsets.first().map_or(0, |x| x.as_usize());
    (start, OffsetBuffer::from_lengths(offsets.lengths()))
}

fn list_with_dictionary<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let (start, offsets) = rebased_offsets(list.offsets());
    let length = offsets.last().map_or(0, |x| x.as_usize());
    let values = list.values().slice(start, length);
    let values = with_dictionary(&values, field.data_type())?;
    let list =
        GenericListArray::try_new(Arc::clone(field), offsets, values, list.nulls().cloned())?;
    Ok(Arc::new(list))
}

fn list_view_with_dictionary<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    // The lists can be anywhere in the array of their values.
    let ranges = || list.offsets().iter().zip(list.sizes().iter());
    let start = ranges().map(|(x, _)| x.as_usize()).min().unwrap_or(0);
    let end = ranges()
        .map(|(x, size)| x.as_usize() + size.as_usize())
        .max()
        .unwrap_or(0);
    let values = list.values().slice(start, end - start);
    let values = with_dictionary(&values, field.data_type())?;
    let offsets: ScalarBuffer<O> = list
        .offsets()
        .iter()
        .map(|x| O::usize_as(x.as_usize() - start))
        .collect();
    let list = GenericListViewArray::try_new(
        Arc::clone(field),
        offsets,
        list.sizes().clone(),
        values,
        list.nulls().cloned(),
    )?;
    Ok(Arc::new(list))
}
