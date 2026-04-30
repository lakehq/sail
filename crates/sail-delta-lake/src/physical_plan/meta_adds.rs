use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, MapArray, RecordBatch, StringArray,
    StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use percent_encoding::percent_decode_str;

use crate::datasource::{COMMIT_TIMESTAMP_COLUMN, COMMIT_VERSION_COLUMN, PATH_COLUMN};
use crate::spec::fields::FIELD_NAME_STATS_PARSED;
use crate::spec::{Add, DeletionVectorDescriptor, StorageType};

const COL_SIZE_BYTES: &str = "size_bytes";
const COL_MODIFICATION_TIME: &str = "modification_time";
const COL_STATS_JSON: &str = "stats_json";
const COL_TAGS: &str = "tags";
const COL_BASE_ROW_ID: &str = "baseRowId";
const COL_DEFAULT_ROW_COMMIT_VERSION: &str = "defaultRowCommitVersion";
const COL_CLUSTERING_PROVIDER: &str = "clusteringProvider";
const COL_PARTITION_SCAN: &str = "partition_scan";
const COL_DELETION_VECTOR: &str = "deletionVector";

const RESERVED_META_COLUMNS: [&str; 13] = [
    PATH_COLUMN,
    COL_SIZE_BYTES,
    COL_MODIFICATION_TIME,
    COL_STATS_JSON,
    COL_TAGS,
    COL_BASE_ROW_ID,
    COL_DEFAULT_ROW_COMMIT_VERSION,
    COL_CLUSTERING_PROVIDER,
    FIELD_NAME_STATS_PARSED,
    COL_PARTITION_SCAN,
    COMMIT_VERSION_COLUMN,
    COMMIT_TIMESTAMP_COLUMN,
    COL_DELETION_VECTOR,
];

/// Infer partition column names from a metadata batch schema by excluding known reserved columns.
pub fn infer_partition_columns_from_schema(schema: &SchemaRef) -> Vec<String> {
    let reserved: HashSet<&str> = RESERVED_META_COLUMNS.into_iter().collect();
    schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| !reserved.contains(n))
        .map(|s| s.to_string())
        .collect()
}

/// Decode a "metadata table" batch (path/size/modification_time/stats_json + partition columns)
/// into Delta kernel `Add` actions.
///
/// - If `partition_columns` is `Some`, we will only read those columns (when present).
/// - If `partition_columns` is `None`, we will infer partition columns from the schema by
///   excluding reserved meta columns.
pub fn decode_adds_from_meta_batch(
    batch: &RecordBatch,
    partition_columns: Option<&[String]>,
) -> Result<Vec<Add>> {
    let path_arr = batch
        .column_by_name(PATH_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "metadata batch must have Utf8 column '{PATH_COLUMN}'"
            ))
        })?;

    let size_arr: Option<&Int64Array> = batch
        .column_by_name(COL_SIZE_BYTES)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let mod_time_arr: Option<&Int64Array> = batch
        .column_by_name(COL_MODIFICATION_TIME)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let commit_version_arr: Option<&Int64Array> = batch
        .column_by_name(COMMIT_VERSION_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let commit_timestamp_arr: Option<&Int64Array> = batch
        .column_by_name(COMMIT_TIMESTAMP_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    // stats_json may arrive as LargeUtf8 depending on upstream casts.
    let stats_arr: Option<ArrayRef> = batch
        .column_by_name(COL_STATS_JSON)
        .map(|c| cast(c, &DataType::Utf8).unwrap_or_else(|_| Arc::clone(c)));
    let stats_arr: Option<&StringArray> = stats_arr
        .as_ref()
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let tags_arr: Option<&MapArray> = batch
        .column_by_name(COL_TAGS)
        .and_then(|c| c.as_any().downcast_ref::<MapArray>());
    let base_row_id_arr: Option<&Int64Array> = batch
        .column_by_name(COL_BASE_ROW_ID)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let default_row_commit_version_arr: Option<&Int64Array> = batch
        .column_by_name(COL_DEFAULT_ROW_COMMIT_VERSION)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let clustering_provider_arr: Option<&StringArray> = batch
        .column_by_name(COL_CLUSTERING_PROVIDER)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());

    let partition_columns: Vec<String> = match partition_columns {
        Some(cols) => cols.to_vec(),
        None => infer_partition_columns_from_schema(&batch.schema()),
    };

    let part_arrays: Vec<(String, Arc<dyn Array>)> = partition_columns
        .iter()
        .filter_map(|name| {
            batch.column_by_name(name).map(|a| {
                let a = cast(a, &DataType::Utf8).unwrap_or_else(|_| Arc::clone(a));
                (name.clone(), a)
            })
        })
        .collect();

    // Extract deletion vector struct column if present.
    let dv_arr: Option<&StructArray> = batch.column_by_name(COL_DELETION_VECTOR).and_then(|c| {
        if matches!(c.data_type(), DataType::Struct(_)) {
            Some(c.as_struct())
        } else {
            None
        }
    });

    let mut adds = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if path_arr.is_null(row) {
            return Err(DataFusionError::Plan(format!(
                "metadata batch '{PATH_COLUMN}' cannot be null"
            )));
        }

        let raw_path = path_arr.value(row);
        let path = percent_decode_str(raw_path)
            .decode_utf8()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .to_string();

        let size = size_arr
            .map(|a| if a.is_null(row) { 0 } else { a.value(row) })
            .unwrap_or_default();
        let modification_time = mod_time_arr
            .map(|a| if a.is_null(row) { 0 } else { a.value(row) })
            .unwrap_or_default();
        let commit_version = commit_version_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row))
            }
        });
        let commit_timestamp = commit_timestamp_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row))
            }
        });

        let stats = stats_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row).to_string())
            }
        });
        let tags = extract_string_map(tags_arr, row)?;
        let base_row_id = base_row_id_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row))
            }
        });
        let default_row_commit_version = default_row_commit_version_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row))
            }
        });
        let clustering_provider = clustering_provider_arr.and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row).to_string())
            }
        });

        let mut partition_values: HashMap<String, Option<String>> =
            HashMap::with_capacity(part_arrays.len());
        for (name, arr) in &part_arrays {
            let v = if arr.is_null(row) {
                None
            } else if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                Some(s.value(row).to_string())
            } else {
                datafusion::arrow::util::display::array_value_to_string(arr.as_ref(), row).ok()
            };
            partition_values.insert(name.clone(), v);
        }

        adds.push(Add {
            path,
            partition_values,
            size,
            modification_time,
            data_change: true,
            stats,
            tags,
            deletion_vector: extract_dv_from_struct(dv_arr, row),
            base_row_id,
            default_row_commit_version,
            clustering_provider,
            commit_version,
            commit_timestamp,
        });
    }

    Ok(adds)
}

fn extract_string_map(
    map_arr: Option<&MapArray>,
    row: usize,
) -> Result<Option<HashMap<String, Option<String>>>> {
    let Some(map_arr) = map_arr else {
        return Ok(None);
    };
    if map_arr.is_null(row) {
        return Ok(None);
    }

    let keys = map_arr
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan("metadata map key column must be Utf8".to_string()))?;
    let values = map_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("metadata map value column must be Utf8".to_string())
        })?;

    let offsets = map_arr.value_offsets();
    let start =
        usize::try_from(offsets[row]).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let end =
        usize::try_from(offsets[row + 1]).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut out = HashMap::with_capacity(end.saturating_sub(start));
    for idx in start..end {
        if keys.is_null(idx) {
            continue;
        }
        let value = if values.is_null(idx) {
            None
        } else {
            Some(values.value(idx).to_string())
        };
        out.insert(keys.value(idx).to_string(), value);
    }
    Ok(Some(out))
}

/// Extract a `DeletionVectorDescriptor` from a struct array at the given row.
fn extract_dv_from_struct(
    dv_arr: Option<&StructArray>,
    row: usize,
) -> Option<DeletionVectorDescriptor> {
    let dv_arr = dv_arr?;
    if dv_arr.is_null(row) {
        return None;
    }

    let storage_type_str = dv_arr
        .column_by_name("storageType")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .filter(|a| !a.is_null(row))
        .map(|a| a.value(row))?;

    let storage_type = match storage_type_str {
        "u" => StorageType::UuidRelativePath,
        "i" => StorageType::Inline,
        "p" => StorageType::AbsolutePath,
        _ => return None,
    };

    let path_or_inline_dv = dv_arr
        .column_by_name("pathOrInlineDv")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .filter(|a| !a.is_null(row))
        .map(|a| a.value(row).to_string())?;

    let offset = dv_arr
        .column_by_name("offset")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .and_then(|a| {
            if a.is_null(row) {
                None
            } else {
                Some(a.value(row))
            }
        });

    let size_in_bytes = dv_arr
        .column_by_name("sizeInBytes")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .filter(|a| !a.is_null(row))
        .map(|a| a.value(row))?;

    let cardinality = dv_arr
        .column_by_name("cardinality")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .filter(|a| !a.is_null(row))
        .map(|a| a.value(row))?;

    Some(DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv,
        offset,
        size_in_bytes,
        cardinality,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn decode_adds_extracts_commit_metadata() -> Result<()> {
        let tags = Arc::new(
            MapArray::new_from_strings(
                vec!["purpose"].into_iter(),
                &StringArray::from(vec![Some("merge")]),
                &[0, 1],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            Field::new(PATH_COLUMN, DataType::Utf8, false),
            Field::new("size_bytes", DataType::Int64, true),
            Field::new("modification_time", DataType::Int64, true),
            Field::new(COL_STATS_JSON, DataType::Utf8, true),
            Field::new(COL_TAGS, tags.data_type().clone(), true),
            Field::new(COL_BASE_ROW_ID, DataType::Int64, true),
            Field::new(COL_DEFAULT_ROW_COMMIT_VERSION, DataType::Int64, true),
            Field::new(COL_CLUSTERING_PROVIDER, DataType::Utf8, true),
            Field::new(COMMIT_VERSION_COLUMN, DataType::Int64, true),
            Field::new(COMMIT_TIMESTAMP_COLUMN, DataType::Int64, true),
            Field::new("p", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("file.parquet")])),
                Arc::new(Int64Array::from(vec![Some(10)])),
                Arc::new(Int64Array::from(vec![Some(20)])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                tags,
                Arc::new(Int64Array::from(vec![Some(100)])),
                Arc::new(Int64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec![Some("liquid")])),
                Arc::new(Int64Array::from(vec![Some(7)])),
                Arc::new(Int64Array::from(vec![Some(42)])),
                Arc::new(StringArray::from(vec![Some("p1")])),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let adds = decode_adds_from_meta_batch(&batch, None)?;
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].commit_version, Some(7));
        assert_eq!(adds[0].commit_timestamp, Some(42));
        assert_eq!(
            adds[0].tags.as_ref().and_then(|tags| tags.get("purpose")),
            Some(&Some("merge".to_string()))
        );
        assert_eq!(adds[0].base_row_id, Some(100));
        assert_eq!(adds[0].default_row_commit_version, Some(7));
        assert_eq!(adds[0].clustering_provider.as_deref(), Some("liquid"));
        Ok(())
    }
}
