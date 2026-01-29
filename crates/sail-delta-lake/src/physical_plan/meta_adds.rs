use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::{DataFusionError, Result};

use crate::datasource::{COMMIT_TIMESTAMP_COLUMN, COMMIT_VERSION_COLUMN, PATH_COLUMN};
use crate::kernel::models::Add;

const COL_SIZE_BYTES: &str = "size_bytes";
const COL_MODIFICATION_TIME: &str = "modification_time";
const COL_STATS_JSON: &str = "stats_json";
const COL_PARTITION_SCAN: &str = "partition_scan";

const RESERVED_META_COLUMNS: [&str; 7] = [
    PATH_COLUMN,
    COL_SIZE_BYTES,
    COL_MODIFICATION_TIME,
    COL_STATS_JSON,
    COL_PARTITION_SCAN,
    COMMIT_VERSION_COLUMN,
    COMMIT_TIMESTAMP_COLUMN,
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

    let mut adds = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if path_arr.is_null(row) {
            return Err(DataFusionError::Plan(format!(
                "metadata batch '{PATH_COLUMN}' cannot be null"
            )));
        }

        let path = path_arr.value(row).to_string();

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
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version,
            commit_timestamp,
        });
    }

    Ok(adds)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn decode_adds_extracts_commit_metadata() -> Result<()> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            Field::new(PATH_COLUMN, DataType::Utf8, false),
            Field::new("size_bytes", DataType::Int64, true),
            Field::new("modification_time", DataType::Int64, true),
            Field::new(COL_STATS_JSON, DataType::Utf8, true),
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
        Ok(())
    }
}
