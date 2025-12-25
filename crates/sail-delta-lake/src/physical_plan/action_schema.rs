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
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result};
use serde::{Deserialize, Serialize};
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

pub fn encode_add_actions(adds: Vec<Add>) -> Result<RecordBatch> {
    encode_add_actions_serde_arrow(adds)
}

fn encode_add_actions_serde_arrow(adds: Vec<Add>) -> Result<RecordBatch> {
    use std::collections::BTreeMap;

    use datafusion::arrow::datatypes::FieldRef;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    #[derive(Debug, Serialize, Deserialize)]
    struct AddRow {
        action_type: u8,
        path: String,
        partition_values: BTreeMap<String, Option<String>>,
        size: i64,
        modification_time: i64,
        data_change: bool,
        stats_json: Option<String>,
    }

    if adds.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    // Ensure schema matches `delta_action_schema`:
    // - strings are Utf8 (not LargeUtf8)
    // - maps are MapArray (not Struct)
    // - partition_values entry fields are named "keys"/"values" (Spark Connect + our decoder expect this)
    let options = TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        .overwrite(
            "partition_values",
            Field::new(COL_PARTITION_VALUES, partition_values_type(), true),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows: Vec<AddRow> = adds
        .into_iter()
        .map(|a| AddRow {
            action_type: ACTION_TYPE_ADD,
            path: a.path,
            partition_values: a.partition_values.into_iter().collect(),
            size: a.size,
            modification_time: a.modification_time,
            data_change: a.data_change,
            stats_json: a.stats,
        })
        .collect();

    let fields = Vec::<FieldRef>::from_type::<AddRow>(options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let narrow_batch = serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let num_rows = narrow_batch.num_rows();
    let mut cols = empty_scalar_columns(num_rows)?;

    for (name, col_name) in [
        ("action_type", COL_ACTION_TYPE),
        ("path", COL_PATH),
        ("partition_values", COL_PARTITION_VALUES),
        ("size", COL_SIZE),
        ("modification_time", COL_MODIFICATION_TIME),
        ("data_change", COL_DATA_CHANGE),
        ("stats_json", COL_STATS_JSON),
    ] {
        let arr = narrow_batch
            .column_by_name(name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "serde_arrow AddRow batch missing expected column '{name}'"
                ))
            })?
            .clone();
        set_col(&mut cols, col_name, arr);
    }

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_remove_actions(removes: Vec<Remove>) -> Result<RecordBatch> {
    encode_remove_actions_serde_arrow(removes)
}

fn encode_remove_actions_serde_arrow(removes: Vec<Remove>) -> Result<RecordBatch> {
    use std::collections::BTreeMap;

    use datafusion::arrow::datatypes::FieldRef;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    #[derive(Debug, Serialize, Deserialize)]
    struct RemoveRow {
        action_type: u8,
        path: String,
        // NOTE: Match existing semantics: `partition_values = None` is encoded as an empty map, not NULL.
        partition_values: BTreeMap<String, Option<String>>,
        size: Option<i64>,
        deletion_timestamp: Option<i64>,
        extended_file_metadata: Option<bool>,
        data_change: bool,
    }

    if removes.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    let options = TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        .overwrite(
            "partition_values",
            Field::new(COL_PARTITION_VALUES, partition_values_type(), true),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows: Vec<RemoveRow> = removes
        .into_iter()
        .map(|r| RemoveRow {
            action_type: ACTION_TYPE_REMOVE,
            path: r.path,
            partition_values: r.partition_values.unwrap_or_default().into_iter().collect(),
            size: r.size,
            deletion_timestamp: r.deletion_timestamp,
            extended_file_metadata: r.extended_file_metadata,
            data_change: r.data_change,
        })
        .collect();

    let fields = Vec::<FieldRef>::from_type::<RemoveRow>(options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let narrow_batch = serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let num_rows = narrow_batch.num_rows();
    let mut cols = empty_scalar_columns(num_rows)?;

    for (name, col_name) in [
        ("action_type", COL_ACTION_TYPE),
        ("path", COL_PATH),
        ("partition_values", COL_PARTITION_VALUES),
        ("size", COL_SIZE),
        ("deletion_timestamp", COL_DELETION_TIMESTAMP),
        ("extended_file_metadata", COL_EXTENDED_FILE_METADATA),
        ("data_change", COL_DATA_CHANGE),
    ] {
        let arr = narrow_batch
            .column_by_name(name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "serde_arrow RemoveRow batch missing expected column '{name}'"
                ))
            })?
            .clone();
        set_col(&mut cols, col_name, arr);
    }

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_protocol_action(protocol: Protocol) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::FieldRef;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    #[derive(Debug, Serialize, Deserialize)]
    struct ProtocolRow {
        action_type: u8,
        protocol_json: String,
    }

    let protocol_json =
        serde_json::to_string(&protocol).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows = vec![ProtocolRow {
        action_type: ACTION_TYPE_PROTOCOL,
        protocol_json,
    }];

    let options = TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false);

    let fields = Vec::<FieldRef>::from_type::<ProtocolRow>(options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let narrow_batch = serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut cols = empty_scalar_columns(1)?;
    for (name, col_name) in [
        ("action_type", COL_ACTION_TYPE),
        ("protocol_json", COL_PROTOCOL_JSON),
    ] {
        let arr = narrow_batch
            .column_by_name(name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "serde_arrow ProtocolRow batch missing expected column '{name}'"
                ))
            })?
            .clone();
        set_col(&mut cols, col_name, arr);
    }

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_metadata_action(metadata: Metadata) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::FieldRef;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    #[derive(Debug, Serialize, Deserialize)]
    struct MetadataRow {
        action_type: u8,
        metadata_json: String,
    }

    let metadata_json =
        serde_json::to_string(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows = vec![MetadataRow {
        action_type: ACTION_TYPE_METADATA,
        metadata_json,
    }];

    let options = TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false);

    let fields = Vec::<FieldRef>::from_type::<MetadataRow>(options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let narrow_batch = serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut cols = empty_scalar_columns(1)?;
    for (name, col_name) in [
        ("action_type", COL_ACTION_TYPE),
        ("metadata_json", COL_METADATA_JSON),
    ] {
        let arr = narrow_batch
            .column_by_name(name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "serde_arrow MetadataRow batch missing expected column '{name}'"
                ))
            })?
            .clone();
        set_col(&mut cols, col_name, arr);
    }

    RecordBatch::try_new(delta_action_schema(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn encode_commit_meta(meta: CommitMeta) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::FieldRef;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    #[derive(Debug, Serialize, Deserialize)]
    struct CommitMetaRow {
        action_type: u8,
        commit_row_count: u64,
        operation_json: Option<String>,
        operation_metrics_json: String,
    }

    let operation_json = meta
        .operation
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let operation_metrics_json = serde_json::to_string(&meta.operation_metrics)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows = vec![CommitMetaRow {
        action_type: ACTION_TYPE_COMMIT_META,
        commit_row_count: meta.row_count,
        operation_json,
        operation_metrics_json,
    }];

    let options = TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false);

    let fields = Vec::<FieldRef>::from_type::<CommitMetaRow>(options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let narrow_batch = serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut cols = empty_scalar_columns(1)?;
    for (name, col_name) in [
        ("action_type", COL_ACTION_TYPE),
        ("commit_row_count", COL_COMMIT_ROW_COUNT),
        ("operation_json", COL_OPERATION_JSON),
        ("operation_metrics_json", COL_OPERATION_METRICS_JSON),
    ] {
        let arr = narrow_batch
            .column_by_name(name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "serde_arrow CommitMetaRow batch missing expected column '{name}'"
                ))
            })?
            .clone();
        set_col(&mut cols, col_name, arr);
    }

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
    #[derive(Debug, Deserialize)]
    struct ActionRow {
        action_type: u8,
        path: Option<String>,
        partition_values: Option<HashMap<String, Option<String>>>,
        size: Option<i64>,
        modification_time: Option<i64>,
        data_change: Option<bool>,
        stats_json: Option<String>,
        deletion_timestamp: Option<i64>,
        extended_file_metadata: Option<bool>,
        protocol_json: Option<String>,
        metadata_json: Option<String>,
        commit_row_count: Option<u64>,
        operation_json: Option<String>,
        operation_metrics_json: Option<String>,
    }

    let mut out_actions: Vec<Action> = Vec::new();
    let mut out_meta: Option<CommitMeta> = None;

    let rows: Vec<ActionRow> = serde_arrow::from_record_batch(batch)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    for row in rows {
        let t = row.action_type;
        match t {
            ACTION_TYPE_ADD => {
                let Some(path) = row.path else {
                    return internal_err!("Add action row missing path");
                };
                let pv = row.partition_values.unwrap_or_default();
                let sz = row.size.ok_or_else(|| {
                    DataFusionError::Internal("Add action row missing size".to_string())
                })?;
                let mt = row.modification_time.ok_or_else(|| {
                    DataFusionError::Internal(
                        "Add action row missing modification_time".to_string(),
                    )
                })?;
                let dc = row.data_change.unwrap_or(true);
                let stats = row.stats_json;
                out_actions.push(Action::Add(Add {
                    path,
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
                let Some(path) = row.path else {
                    return internal_err!("Remove action row missing path");
                };
                let pv = row.partition_values.unwrap_or_default();
                let dc = row.data_change.unwrap_or(true);
                let del_ts = row.deletion_timestamp;
                let ext = row.extended_file_metadata;
                let sz = row.size;
                out_actions.push(Action::Remove(Remove {
                    path,
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
                let Some(s) = row.protocol_json else {
                    return internal_err!("Protocol action row missing protocol_json");
                };
                let p: Protocol =
                    serde_json::from_str(&s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Protocol(p));
            }
            ACTION_TYPE_METADATA => {
                let Some(s) = row.metadata_json else {
                    return internal_err!("Metadata action row missing metadata_json");
                };
                let m: Metadata =
                    serde_json::from_str(&s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Metadata(m));
            }
            ACTION_TYPE_COMMIT_META => {
                let row_count = row.commit_row_count.unwrap_or(0);
                let operation: Option<DeltaOperation> = row
                    .operation_json
                    .as_deref()
                    .map(serde_json::from_str::<DeltaOperation>)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let operation_metrics: HashMap<String, Value> = row
                    .operation_metrics_json
                    .as_deref()
                    .map(serde_json::from_str::<HashMap<String, Value>>)
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

fn set_col(cols: &mut [ArrayRef], name: &str, value: ArrayRef) {
    let schema = delta_action_schema();
    #[allow(clippy::unwrap_used)]
    let idx = schema.index_of(name).unwrap();
    cols[idx] = value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::array::{Array, MapArray, StringArray, StructArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::record_batch::RecordBatch;

    #[test]
    fn encode_add_actions_serde_arrow_matches_schema_and_map_entry_names() -> Result<()> {
        let adds = vec![
            Add {
                path: "a.parquet".to_string(),
                partition_values: HashMap::from([
                    ("b".to_string(), Some("2".to_string())),
                    ("a".to_string(), None),
                ]),
                size: 123,
                modification_time: 456,
                data_change: true,
                stats: Some(r#"{"numRecords": 1}"#.to_string()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            Add {
                path: "b.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 7,
                modification_time: 8,
                data_change: false,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
        ];

        let serde_rb = encode_add_actions_serde_arrow(adds)?;

        // Schema contract must stay wide schema.
        assert_eq!(serde_rb.schema(), delta_action_schema());
        assert_eq!(serde_rb.num_rows(), 2);

        // Verify map entry field names match our schema ("keys"/"values").
        let pv = serde_rb
            .column_by_name(COL_PARTITION_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let entries = pv.value(0);
        let st = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(st.column_by_name("keys").is_some());
        assert!(st.column_by_name("values").is_some());

        // Spot-check core columns.
        let path_s = serde_rb
            .column_by_name(COL_PATH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_s.value(0), "a.parquet");
        assert_eq!(path_s.value(1), "b.parquet");
        let size_s = serde_rb
            .column_by_name(COL_SIZE)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(size_s.value(0), 123);
        assert_eq!(size_s.value(1), 7);

        Ok(())
    }

    #[test]
    fn encode_remove_actions_serde_arrow_partition_values_none_is_empty_map_not_null() -> Result<()>
    {
        let removes = vec![Remove {
            path: "x.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(1),
            extended_file_metadata: Some(true),
            partition_values: None,
            size: Some(10),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }];

        let rb = encode_remove_actions(removes)?;
        assert_eq!(rb.schema(), delta_action_schema());
        assert_eq!(rb.num_rows(), 1);

        let pv = rb
            .column_by_name(COL_PARTITION_VALUES)
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert!(!pv.is_null(0), "partition_values should not be NULL");
        let entries = pv.value(0);
        assert_eq!(entries.len(), 0, "partition_values should be an empty map");
        Ok(())
    }

    #[test]
    fn decode_actions_and_meta_serde_arrow_mixed_batch() -> Result<()> {
        let adds = vec![Add {
            path: "a.parquet".to_string(),
            partition_values: HashMap::from([("p".to_string(), Some("1".to_string()))]),
            size: 1,
            modification_time: 2,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }];
        let removes = vec![Remove {
            path: "a.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(3),
            extended_file_metadata: Some(true),
            partition_values: None,
            size: Some(1),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }];
        let meta = CommitMeta {
            row_count: 10,
            operation: None,
            operation_metrics: HashMap::new(),
        };

        let schema = delta_action_schema();
        let out_batches: Vec<RecordBatch> = vec![
            encode_add_actions(adds)?,
            encode_remove_actions(removes)?,
            encode_commit_meta(meta.clone())?,
        ];
        let batch = concat_batches(&schema, &out_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::Add(_)));
        assert!(matches!(actions[1], Action::Remove(_)));

        let decoded_meta = decoded_meta.ok_or_else(|| {
            DataFusionError::Internal("expected CommitMeta to be present".to_string())
        })?;
        assert_eq!(decoded_meta.row_count, 10);
        Ok(())
    }
}
