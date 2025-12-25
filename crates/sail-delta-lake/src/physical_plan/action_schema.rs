// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Arrow-native row-per-action schema for Delta DML.
//!
//! This replaces the previous "CommitInfo JSON in a single Utf8 column" dataflow between
//! physical nodes. The schema is intentionally minimal but join-friendly (notably `path`).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, MapArray, StringArray, StructArray, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result};
use serde_json::Value;

use crate::kernel::models::{Action, Add, Metadata, Protocol, Remove};
use crate::kernel::DeltaOperation;

pub const ACTION_TYPE_ADD: u8 = 0;
pub const ACTION_TYPE_REMOVE: u8 = 1;
pub const ACTION_TYPE_PROTOCOL: u8 = 2;
pub const ACTION_TYPE_METADATA: u8 = 3;
pub const ACTION_TYPE_COMMIT_META: u8 = 255;

pub const COL_ACTION_TYPE: &str = "action_type";
pub const COL_PATH: &str = "path";
pub const COL_PARTITION_VALUES: &str = "partition_values";
pub const COL_SIZE: &str = "size";
pub const COL_MODIFICATION_TIME: &str = "modification_time";
pub const COL_DATA_CHANGE: &str = "data_change";
pub const COL_STATS_JSON: &str = "stats_json";
pub const COL_DELETION_TIMESTAMP: &str = "deletion_timestamp";
pub const COL_EXTENDED_FILE_METADATA: &str = "extended_file_metadata";
pub const COL_PROTOCOL_JSON: &str = "protocol_json";
pub const COL_METADATA_JSON: &str = "metadata_json";
pub const COL_COMMIT_ROW_COUNT: &str = "commit_row_count";
pub const COL_OPERATION_JSON: &str = "operation_json";
pub const COL_OPERATION_METRICS_JSON: &str = "operation_metrics_json";

#[derive(Debug, Clone, Default)]
pub struct CommitMeta {
    pub row_count: u64,
    pub operation: Option<DeltaOperation>,
    pub operation_metrics: HashMap<String, Value>,
}

pub fn delta_action_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_ACTION_TYPE, DataType::UInt8, false),
        Field::new(COL_PATH, DataType::Utf8, true),
        Field::new(COL_PARTITION_VALUES, partition_values_type(), true),
        Field::new(COL_SIZE, DataType::Int64, true),
        Field::new(COL_MODIFICATION_TIME, DataType::Int64, true),
        Field::new(COL_DATA_CHANGE, DataType::Boolean, true),
        Field::new(COL_STATS_JSON, DataType::Utf8, true),
        Field::new(COL_DELETION_TIMESTAMP, DataType::Int64, true),
        Field::new(COL_EXTENDED_FILE_METADATA, DataType::Boolean, true),
        // Protocol / Metadata are relatively rare (typically 0-2 rows) so we keep them as JSON.
        Field::new(COL_PROTOCOL_JSON, DataType::Utf8, true),
        Field::new(COL_METADATA_JSON, DataType::Utf8, true),
        // Commit meta (exactly one row for most writers/removers).
        Field::new(COL_COMMIT_ROW_COUNT, DataType::UInt64, true),
        Field::new(COL_OPERATION_JSON, DataType::Utf8, true),
        Field::new(COL_OPERATION_METRICS_JSON, DataType::Utf8, true),
    ]))
}

fn partition_values_type() -> DataType {
    // Arrow Map is represented as `Map<entries: Struct<keys: Utf8, values: Utf8?>>`.
    let entries_struct = DataType::Struct(
        vec![
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ]
        .into(),
    );
    let entries_field = Arc::new(Field::new("entries", entries_struct, false));
    DataType::Map(entries_field, false)
}

fn empty_scalar_columns(num_rows: usize) -> Result<Vec<ArrayRef>> {
    let schema = delta_action_schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        cols.push(datafusion::arrow::array::new_null_array(
            f.data_type(),
            num_rows,
        ));
    }
    Ok(cols)
}

fn build_partition_values_array(values: &[HashMap<String, Option<String>>]) -> Result<ArrayRef> {
    let mut all_keys: Vec<String> = Vec::new();
    let mut all_values: Vec<Option<String>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);

    for m in values {
        let mut keys: Vec<&String> = m.keys().collect();
        keys.sort_unstable();

        for k in keys {
            all_keys.push(k.clone());
            all_values.push(m.get(k).cloned().unwrap_or(None));
        }
        let next = i32::try_from(all_keys.len()).map_err(|_| {
            DataFusionError::Internal("partition_values map too large for i32 offsets".to_string())
        })?;
        offsets.push(next);
    }

    let DataType::Map(entries_field, sorted) = partition_values_type() else {
        return internal_err!("partition_values_type must be Map");
    };

    let DataType::Struct(entry_struct_fields) = entries_field.data_type() else {
        return internal_err!("partition_values entries must be a Struct");
    };

    // Build entries struct using the *schema-provided* fields so nullability matches exactly
    // (Spark Connect is strict about MapArray entry field nullability).
    let keys_arr = Arc::new(StringArray::from(all_keys)) as ArrayRef;
    let values_arr = Arc::new(StringArray::from(all_values)) as ArrayRef;
    let entries_struct = StructArray::try_new(
        entry_struct_fields.clone(),
        vec![keys_arr, values_arr],
        None,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let map = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries_struct,
        None,
        sorted,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(map) as ArrayRef)
}

pub fn encode_add_actions(adds: Vec<Add>) -> Result<RecordBatch> {
    if adds.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    let num_rows = adds.len();
    let mut cols = empty_scalar_columns(num_rows)?;

    let action_type = UInt8Array::from(vec![ACTION_TYPE_ADD; num_rows]);
    let path = StringArray::from(
        adds.iter()
            .map(|a| Some(a.path.as_str()))
            .collect::<Vec<_>>(),
    );
    let partition_values_maps: Vec<HashMap<String, Option<String>>> =
        adds.iter().map(|a| a.partition_values.clone()).collect();
    let partition_values = build_partition_values_array(&partition_values_maps)?;
    let size = Int64Array::from(adds.iter().map(|a| Some(a.size)).collect::<Vec<_>>());
    let modification_time = Int64Array::from(
        adds.iter()
            .map(|a| Some(a.modification_time))
            .collect::<Vec<_>>(),
    );
    let data_change =
        BooleanArray::from(adds.iter().map(|a| Some(a.data_change)).collect::<Vec<_>>());
    let stats_json = StringArray::from(adds.iter().map(|a| a.stats.as_deref()).collect::<Vec<_>>());

    set_col(&mut cols, COL_ACTION_TYPE, Arc::new(action_type));
    set_col(&mut cols, COL_PATH, Arc::new(path));
    set_col(&mut cols, COL_PARTITION_VALUES, partition_values);
    set_col(&mut cols, COL_SIZE, Arc::new(size));
    set_col(
        &mut cols,
        COL_MODIFICATION_TIME,
        Arc::new(modification_time),
    );
    set_col(&mut cols, COL_DATA_CHANGE, Arc::new(data_change));
    set_col(&mut cols, COL_STATS_JSON, Arc::new(stats_json));

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_remove_actions(removes: Vec<Remove>) -> Result<RecordBatch> {
    if removes.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    let num_rows = removes.len();
    let mut cols = empty_scalar_columns(num_rows)?;

    let action_type = UInt8Array::from(vec![ACTION_TYPE_REMOVE; num_rows]);
    let path = StringArray::from(
        removes
            .iter()
            .map(|r| Some(r.path.as_str()))
            .collect::<Vec<_>>(),
    );
    let partition_values_maps: Vec<HashMap<String, Option<String>>> = removes
        .iter()
        .map(|r| r.partition_values.clone().unwrap_or_default())
        .collect();
    let partition_values = build_partition_values_array(&partition_values_maps)?;
    let size = Int64Array::from(removes.iter().map(|r| r.size).collect::<Vec<Option<i64>>>());
    let deletion_timestamp = Int64Array::from(
        removes
            .iter()
            .map(|r| r.deletion_timestamp)
            .collect::<Vec<Option<i64>>>(),
    );
    let extended_file_metadata = BooleanArray::from(
        removes
            .iter()
            .map(|r| r.extended_file_metadata)
            .collect::<Vec<Option<bool>>>(),
    );
    let data_change = BooleanArray::from(
        removes
            .iter()
            .map(|r| Some(r.data_change))
            .collect::<Vec<_>>(),
    );

    set_col(&mut cols, COL_ACTION_TYPE, Arc::new(action_type));
    set_col(&mut cols, COL_PATH, Arc::new(path));
    set_col(&mut cols, COL_PARTITION_VALUES, partition_values);
    set_col(&mut cols, COL_SIZE, Arc::new(size));
    set_col(
        &mut cols,
        COL_DELETION_TIMESTAMP,
        Arc::new(deletion_timestamp),
    );
    set_col(
        &mut cols,
        COL_EXTENDED_FILE_METADATA,
        Arc::new(extended_file_metadata),
    );
    set_col(&mut cols, COL_DATA_CHANGE, Arc::new(data_change));

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_protocol_action(protocol: Protocol) -> Result<RecordBatch> {
    let mut cols = empty_scalar_columns(1)?;
    let action_type = UInt8Array::from(vec![ACTION_TYPE_PROTOCOL]);
    let protocol_json =
        serde_json::to_string(&protocol).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let protocol_json = StringArray::from(vec![Some(protocol_json.as_str())]);

    set_col(&mut cols, COL_ACTION_TYPE, Arc::new(action_type));
    set_col(&mut cols, COL_PROTOCOL_JSON, Arc::new(protocol_json));

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_metadata_action(metadata: Metadata) -> Result<RecordBatch> {
    let mut cols = empty_scalar_columns(1)?;
    let action_type = UInt8Array::from(vec![ACTION_TYPE_METADATA]);
    let metadata_json =
        serde_json::to_string(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let metadata_json = StringArray::from(vec![Some(metadata_json.as_str())]);

    set_col(&mut cols, COL_ACTION_TYPE, Arc::new(action_type));
    set_col(&mut cols, COL_METADATA_JSON, Arc::new(metadata_json));

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_commit_meta(meta: CommitMeta) -> Result<RecordBatch> {
    let mut cols = empty_scalar_columns(1)?;
    let action_type = UInt8Array::from(vec![ACTION_TYPE_COMMIT_META]);
    let row_count = UInt64Array::from(vec![Some(meta.row_count)]);
    let operation_json = meta
        .operation
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let operation_json = StringArray::from(vec![operation_json.as_deref()]);
    let metrics_json = serde_json::to_string(&meta.operation_metrics)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let metrics_json = StringArray::from(vec![Some(metrics_json.as_str())]);

    set_col(&mut cols, COL_ACTION_TYPE, Arc::new(action_type));
    set_col(&mut cols, COL_COMMIT_ROW_COUNT, Arc::new(row_count));
    set_col(&mut cols, COL_OPERATION_JSON, Arc::new(operation_json));
    set_col(
        &mut cols,
        COL_OPERATION_METRICS_JSON,
        Arc::new(metrics_json),
    );

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn decode_adds_from_batch(batch: &RecordBatch) -> Result<Vec<Add>> {
    let (actions, _meta) = decode_actions_and_meta_from_batch(batch)?;
    Ok(actions
        .into_iter()
        .filter_map(|a| match a {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .collect())
}

pub fn decode_actions_and_meta_from_batch(
    batch: &RecordBatch,
) -> Result<(Vec<Action>, Option<CommitMeta>)> {
    let action_type = col_as::<UInt8Array>(batch, COL_ACTION_TYPE)?;
    let path = col_as_opt::<StringArray>(batch, COL_PATH)?;
    let partition_values = col_as_opt::<MapArray>(batch, COL_PARTITION_VALUES)?;
    let size = col_as_opt::<Int64Array>(batch, COL_SIZE)?;
    let modification_time = col_as_opt::<Int64Array>(batch, COL_MODIFICATION_TIME)?;
    let data_change = col_as_opt::<BooleanArray>(batch, COL_DATA_CHANGE)?;
    let stats_json = col_as_opt::<StringArray>(batch, COL_STATS_JSON)?;
    let deletion_timestamp = col_as_opt::<Int64Array>(batch, COL_DELETION_TIMESTAMP)?;
    let extended_file_metadata = col_as_opt::<BooleanArray>(batch, COL_EXTENDED_FILE_METADATA)?;
    let protocol_json = col_as_opt::<StringArray>(batch, COL_PROTOCOL_JSON)?;
    let metadata_json = col_as_opt::<StringArray>(batch, COL_METADATA_JSON)?;
    let commit_row_count = col_as_opt::<UInt64Array>(batch, COL_COMMIT_ROW_COUNT)?;
    let operation_json = col_as_opt::<StringArray>(batch, COL_OPERATION_JSON)?;
    let operation_metrics_json = col_as_opt::<StringArray>(batch, COL_OPERATION_METRICS_JSON)?;

    let mut out_actions: Vec<Action> = Vec::new();
    let mut out_meta: Option<CommitMeta> = None;

    for row in 0..batch.num_rows() {
        let t = action_type.value(row);
        match t {
            ACTION_TYPE_ADD => {
                let Some(path) = path.and_then(|a| a.iter().nth(row).flatten()) else {
                    return internal_err!("Add action row missing path");
                };
                let pv = partition_values
                    .map(|m| decode_string_map_row(m, row))
                    .transpose()?
                    .unwrap_or_default();
                let sz = size
                    .and_then(|a| a.is_valid(row).then(|| a.value(row)))
                    .ok_or_else(|| {
                        DataFusionError::Internal("Add action row missing size".to_string())
                    })?;
                let mt = modification_time
                    .and_then(|a| a.is_valid(row).then(|| a.value(row)))
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Add action row missing modification_time".to_string(),
                        )
                    })?;
                let dc = data_change
                    .and_then(|a| a.is_valid(row).then(|| a.value(row)))
                    .unwrap_or(true);
                let stats = stats_json
                    .and_then(|a| a.iter().nth(row).flatten())
                    .map(|s| s.to_string());
                out_actions.push(Action::Add(Add {
                    path: path.to_string(),
                    partition_values: pv,
                    size: sz,
                    modification_time: mt,
                    data_change: dc,
                    stats,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                }));
            }
            ACTION_TYPE_REMOVE => {
                let Some(path) = path.and_then(|a| a.iter().nth(row).flatten()) else {
                    return internal_err!("Remove action row missing path");
                };
                let pv = partition_values
                    .map(|m| decode_string_map_row(m, row))
                    .transpose()?
                    .unwrap_or_default();
                let dc = data_change
                    .and_then(|a| a.is_valid(row).then(|| a.value(row)))
                    .unwrap_or(true);
                let del_ts = deletion_timestamp.and_then(|a| a.is_valid(row).then(|| a.value(row)));
                let ext =
                    extended_file_metadata.and_then(|a| a.is_valid(row).then(|| a.value(row)));
                let sz = size.and_then(|a| a.is_valid(row).then(|| a.value(row)));
                out_actions.push(Action::Remove(Remove {
                    path: path.to_string(),
                    data_change: dc,
                    deletion_timestamp: del_ts,
                    extended_file_metadata: ext,
                    partition_values: Some(pv),
                    size: sz,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                }));
            }
            ACTION_TYPE_PROTOCOL => {
                let Some(s) = protocol_json.and_then(|a| a.iter().nth(row).flatten()) else {
                    return internal_err!("Protocol action row missing protocol_json");
                };
                let p: Protocol =
                    serde_json::from_str(s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Protocol(p));
            }
            ACTION_TYPE_METADATA => {
                let Some(s) = metadata_json.and_then(|a| a.iter().nth(row).flatten()) else {
                    return internal_err!("Metadata action row missing metadata_json");
                };
                let m: Metadata =
                    serde_json::from_str(s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Metadata(m));
            }
            ACTION_TYPE_COMMIT_META => {
                let row_count = commit_row_count
                    .and_then(|a| a.is_valid(row).then(|| a.value(row)))
                    .unwrap_or(0);
                let operation: Option<DeltaOperation> = operation_json
                    .and_then(|a| a.iter().nth(row).flatten())
                    .map(|s| serde_json::from_str::<DeltaOperation>(s))
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let operation_metrics: HashMap<String, Value> = operation_metrics_json
                    .and_then(|a| a.iter().nth(row).flatten())
                    .map(|s| serde_json::from_str::<HashMap<String, Value>>(s))
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .unwrap_or_default();
                out_meta = Some(CommitMeta {
                    row_count,
                    operation,
                    operation_metrics,
                });
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Unknown delta action_type: {other}"
                )));
            }
        }
    }

    Ok((out_actions, out_meta))
}

fn decode_string_map_row(map: &MapArray, row: usize) -> Result<HashMap<String, Option<String>>> {
    if map.is_null(row) {
        return Ok(HashMap::new());
    }
    let entries = map.value(row);
    let st = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Internal("Map entry is not a StructArray".to_string()))?;

    let keys = st
        .column_by_name("keys")
        .ok_or_else(|| DataFusionError::Internal("Map missing keys column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("Map keys is not StringArray".to_string()))?;
    let values = st
        .column_by_name("values")
        .ok_or_else(|| DataFusionError::Internal("Map missing values column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("Map values is not StringArray".to_string()))?;

    let mut out: HashMap<String, Option<String>> = HashMap::with_capacity(keys.len());
    for i in 0..keys.len() {
        let k = keys.value(i).to_string();
        let v = if values.is_null(i) {
            None
        } else {
            Some(values.value(i).to_string())
        };
        out.insert(k, v);
    }
    Ok(out)
}

fn col_as<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Internal(format!("Missing column '{name}'")))?;
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| DataFusionError::Internal(format!("Column '{name}' has unexpected type")))
}

fn col_as_opt<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a T>> {
    Ok(match batch.column_by_name(name) {
        None => None,
        Some(col) => Some(col.as_any().downcast_ref::<T>().ok_or_else(|| {
            DataFusionError::Internal(format!("Column '{name}' has unexpected type"))
        })?),
    })
}

fn set_col(cols: &mut [ArrayRef], name: &str, value: ArrayRef) {
    let schema = delta_action_schema();
    #[allow(clippy::unwrap_used)]
    let idx = schema.index_of(name).unwrap();
    cols[idx] = value;
}
