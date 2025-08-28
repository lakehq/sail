//! Utilities to extract columns from a record batch or nested / complex arrays.

use std::sync::Arc;

use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::array::{
    Array, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use deltalake::{DeltaResult, DeltaTableError};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/arrow/extract.rs>

/// Trait to extract a column by name from a record batch or nested / complex array.
pub(crate) trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

/// Extracts a column by name and casts it to the given type array type `T`.
///
/// Returns an error if the column does not exist or if the column is not of type `T`.
pub(crate) fn extract_and_cast<'a, T: Array + 'static>(
    arr: &'a dyn ProvidesColumnByName,
    name: &'a str,
) -> DeltaResult<&'a T> {
    extract_and_cast_opt::<T>(arr, name)
        .ok_or(DeltaTableError::Generic(format!("missing-column: {name}")))
}

/// Extracts a column by name and casts it to the given type array type `T`.
///
/// Returns `None` if the column does not exist or if the column is not of type `T`.
pub(crate) fn extract_and_cast_opt<'a, T: Array + 'static>(
    array: &'a dyn ProvidesColumnByName,
    name: &'a str,
) -> Option<&'a T> {
    let mut path_steps = name.split('.');
    let first = path_steps.next()?;
    extract_column(array, first, &mut path_steps)
        .ok()?
        .as_any()
        .downcast_ref::<T>()
}

pub(crate) fn extract_column<'a>(
    array: &'a dyn ProvidesColumnByName,
    path_step: &str,
    remaining_path_steps: &mut impl Iterator<Item = &'a str>,
) -> Result<&'a Arc<dyn Array>, ArrowError> {
    let child = array
        .column_by_name(path_step)
        .ok_or(ArrowError::SchemaError(format!(
            "No such field: {path_step}",
        )))?;

    if let Some(next_path_step) = remaining_path_steps.next() {
        match child.data_type() {
            DataType::Map(_, _) => {
                // NOTE a map has exactly one child, but we want to be agnostic of its name.
                // so we case the current array as map, and use the entries accessor.
                let maparr = cast_column_as::<MapArray>(path_step, &Some(child))?;
                if let Some(next_path) = remaining_path_steps.next() {
                    extract_column(maparr.entries(), next_path, remaining_path_steps)
                } else {
                    Ok(child)
                    // if maparr.entries().num_columns() != 2 {
                    //     return Err(ArrowError::SchemaError(format!(
                    //         "Map {path_step} has {} columns, expected 2",
                    //         maparr.entries().num_columns()
                    //     )));
                    // }
                    // if next_path_step == *maparr.entries().column_names().first().unwrap() {
                    //     Ok(maparr.entries().column(0))
                    // } else {
                    //     Ok(maparr.entries().column(1))
                    // }
                }
            }
            DataType::List(_) => {
                let listarr = cast_column_as::<ListArray>(path_step, &Some(child))?;
                if let Some(next_path) = remaining_path_steps.next() {
                    extract_column(
                        cast_column_as::<StructArray>(next_path_step, &Some(listarr.values()))?,
                        next_path,
                        remaining_path_steps,
                    )
                } else {
                    Ok(listarr.values())
                }
            }
            _ => extract_column(
                cast_column_as::<StructArray>(path_step, &Some(child))?,
                next_path_step,
                remaining_path_steps,
            ),
        }
    } else {
        Ok(child)
    }
}

fn cast_column_as<'a, T: Array + 'static>(
    name: &str,
    column: &Option<&'a Arc<dyn Array>>,
) -> Result<&'a T, ArrowError> {
    column
        .ok_or(ArrowError::SchemaError(format!("No such column: {name}")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or(ArrowError::SchemaError(format!(
            "{name} is not of expected type."
        )))
}

#[inline]
pub(crate) fn read_str(arr: &StringArray, idx: usize) -> DeltaResult<&str> {
    read_str_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(crate) fn read_str_opt(arr: &StringArray, idx: usize) -> Option<&str> {
    arr.is_valid(idx).then(|| arr.value(idx))
}

#[inline]
pub(crate) fn read_primitive<T>(arr: &PrimitiveArray<T>, idx: usize) -> DeltaResult<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    read_primitive_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(crate) fn read_primitive_opt<T>(arr: &PrimitiveArray<T>, idx: usize) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    arr.is_valid(idx).then(|| arr.value(idx))
}

#[inline]
pub(crate) fn read_bool(arr: &BooleanArray, idx: usize) -> DeltaResult<bool> {
    read_bool_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(crate) fn read_bool_opt(arr: &BooleanArray, idx: usize) -> Option<bool> {
    arr.is_valid(idx).then(|| arr.value(idx))
}
