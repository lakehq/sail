use std::collections::HashMap;

use datafusion::arrow::array::{Array, DictionaryArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::UInt16Type;
use itertools::Either;

use crate::kernel::models::Add;
use crate::kernel::{DeltaResult, DeltaTableError};

pub(crate) fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<impl Iterator<Item = Option<&'a str>>> {
    let err = || DeltaTableError::generic("Unable to obtain internal file path column".to_string());
    let dict_array = batch
        .column_by_name(path_column)
        .ok_or_else(err)?
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .ok_or_else(err)?;

    let values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(err)?;

    Ok(dict_array
        .keys()
        .iter()
        .map(move |key| key.and_then(|k| values.value(k as usize).into())))
}

pub(crate) fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err =
            || DeltaTableError::generic("Unable to obtain internal file path column".to_string());

        let iter = if dict_array {
            Either::Left(get_path_column(&batch, path_column)?)
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Either::Right(array.iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::generic(format!(
                "{path_column} cannot be null"
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::generic(
                        "Unable to map internal file path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}
