//! Utilities for converting Arrow arrays into Delta data structures.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/snapshot/parse.rs>

use datafusion::arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, MapArray, StringArray, StructArray,
};
use deltalake::kernel::{DeletionVectorDescriptor, Remove};
use deltalake::{DeltaResult, DeltaTableError};
use percent_encoding::percent_decode_str;

use crate::kernel::arrow::extract::{self as ex, ProvidesColumnByName};

pub(super) fn read_removes(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Remove>> {
    let mut result = Vec::new();

    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(array, "remove") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(result);
        }

        let path = ex::extract_and_cast::<StringArray>(arr, "path")?;
        let data_change = ex::extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let deletion_timestamp = ex::extract_and_cast::<Int64Array>(arr, "deletionTimestamp")?;

        let extended_file_metadata =
            ex::extract_and_cast_opt::<BooleanArray>(arr, "extendedFileMetadata");
        let pvs = ex::extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = ex::extract_and_cast_opt::<Int64Array>(arr, "size");
        let tags = ex::extract_and_cast_opt::<MapArray>(arr, "tags");
        let dv = ex::extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        let get_dv: Box<dyn Fn(usize) -> Option<DeletionVectorDescriptor>> = if let Some(d) = dv {
            let storage_type = ex::extract_and_cast::<StringArray>(d, "storageType")?;
            let path_or_inline_dv = ex::extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
            let offset = ex::extract_and_cast::<Int32Array>(d, "offset")?;
            let size_in_bytes = ex::extract_and_cast::<Int32Array>(d, "sizeInBytes")?;
            let cardinality = ex::extract_and_cast::<Int64Array>(d, "cardinality")?;

            // Column might exist but have nullability set for the whole array, so we just return Nones
            if d.null_count() == d.len() {
                Box::new(|_| None)
            } else {
                Box::new(|idx: usize| {
                    d.is_valid(idx)
                        .then(|| {
                            if ex::read_str(storage_type, idx).is_ok() {
                                Some(DeletionVectorDescriptor {
                                    storage_type: std::str::FromStr::from_str(
                                        ex::read_str(storage_type, idx).ok()?,
                                    )
                                    .ok()?,
                                    path_or_inline_dv: ex::read_str(path_or_inline_dv, idx)
                                        .ok()?
                                        .to_string(),
                                    offset: ex::read_primitive_opt(offset, idx),
                                    size_in_bytes: ex::read_primitive(size_in_bytes, idx).ok()?,
                                    cardinality: ex::read_primitive(cardinality, idx).ok()?,
                                })
                            } else {
                                None
                            }
                        })
                        .flatten()
                })
            }
        } else {
            Box::new(|_| None)
        };

        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let path_ = ex::read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(Remove {
                    path: path_,
                    data_change: ex::read_bool(data_change, i)?,
                    deletion_timestamp: ex::read_primitive_opt(deletion_timestamp, i),
                    extended_file_metadata: extended_file_metadata
                        .and_then(|e| ex::read_bool_opt(e, i)),
                    size: size.and_then(|s| ex::read_primitive_opt(s, i)),
                    partition_values: pvs
                        .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect())),
                    tags: tags.and_then(|t| collect_map(&t.value(i)).map(|m| m.collect())),
                    deletion_vector: get_dv(i),
                    base_row_id: None,
                    default_row_commit_version: None,
                });
            }
        }
    }

    Ok(result)
}

pub(crate) fn collect_map(
    val: &StructArray,
) -> Option<impl Iterator<Item = (String, Option<String>)> + '_> {
    let keys = val
        .column(0)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = val
        .column(1)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    Some(
        keys.iter()
            .zip(values.iter())
            .filter_map(|(k, v)| k.map(|kv| (kv.to_string(), v.map(|vv| vv.to_string())))),
    )
}
