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
//
//! Arrow-native row-per-action schema for Delta DML.
//!
//! This module migrates the internal Sail delta physical-plan action stream from a
//! "wide table + action_type" representation to an "Action enum -> UnionArray" representation.
//!
//! Note: MERGE joins on file path. With a union-only schema, the join key is derived using
//! DataFusion scalar functions `union_extract + get_field` (see `planner/op_merge.rs`).
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::models::{Action, Add, Metadata, Protocol, Remove};
use crate::kernel::DeltaOperation;

pub const COL_ACTION: &str = "action";
const COL_PARTITION_VALUES: &str = "partition_values";

#[derive(Debug, Clone, Default)]
pub struct CommitMeta {
    pub row_count: u64,
    pub operation: Option<DeltaOperation>,
    pub operation_metrics: HashMap<String, Value>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddAction {
    path: String,
    partition_values: BTreeMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    stats_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoveAction {
    path: String,
    data_change: bool,
    deletion_timestamp: Option<i64>,
    extended_file_metadata: Option<bool>,
    // Keep non-null in Arrow: Remove.partition_values=None is encoded as empty map (not NULL)
    partition_values: BTreeMap<String, Option<String>>,
    size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitMetaAction {
    commit_row_count: u64,
    operation_json: Option<String>,
    operation_metrics_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ExecAction {
    #[serde(rename = "add")]
    Add(AddAction),
    #[serde(rename = "remove")]
    Remove(RemoveAction),
    // Protocol / Metadata are relatively rare; we keep them as JSON strings for now.
    #[serde(rename = "protocol")]
    Protocol(String),
    #[serde(rename = "metadata")]
    Metadata(String),
    #[serde(rename = "commit_meta")]
    CommitMeta(CommitMetaAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActionRow {
    action: ExecAction,
}

fn delta_action_tracing_options() -> serde_arrow::schema::TracingOptions {
    use serde_arrow::schema::TracingOptions;

    TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        // Force MapArray strategy + stable entry field names ("keys"/"values") inside the union variants.
        .overwrite(
            "action.add.partition_values",
            Field::new(COL_PARTITION_VALUES, partition_values_type(), false),
        )
        .expect("overwrite action.add.partition_values")
        .overwrite(
            "action.remove.partition_values",
            Field::new(COL_PARTITION_VALUES, partition_values_type(), false),
        )
        .expect("overwrite action.remove.partition_values")
}

fn delta_action_fields() -> &'static Vec<datafusion::arrow::datatypes::FieldRef> {
    use serde_arrow::schema::SchemaLike;

    static FIELDS: OnceLock<Vec<datafusion::arrow::datatypes::FieldRef>> = OnceLock::new();
    FIELDS.get_or_init(|| {
        Vec::<datafusion::arrow::datatypes::FieldRef>::from_type::<ActionRow>(
            delta_action_tracing_options(),
        )
        .expect("ActionRow schema tracing should succeed")
    })
}

pub fn delta_action_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let schema_fields = delta_action_fields().clone();
            Arc::new(Schema::new(schema_fields))
        })
        .clone()
}

pub fn encode_add_actions(adds: Vec<Add>) -> Result<RecordBatch> {
    if adds.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    let rows: Vec<ActionRow> = adds
        .into_iter()
        .map(|a| ActionRow {
            action: ExecAction::Add(AddAction {
                path: a.path,
                partition_values: a.partition_values.into_iter().collect(),
                size: a.size,
                modification_time: a.modification_time,
                data_change: a.data_change,
                stats_json: a.stats,
            }),
        })
        .collect();

    serde_arrow::to_record_batch(delta_action_fields(), &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_remove_actions(removes: Vec<Remove>) -> Result<RecordBatch> {
    if removes.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()));
    }

    let rows: Vec<ActionRow> = removes
        .into_iter()
        .map(|r| ActionRow {
            action: ExecAction::Remove(RemoveAction {
                path: r.path,
                data_change: r.data_change,
                deletion_timestamp: r.deletion_timestamp,
                extended_file_metadata: r.extended_file_metadata,
                partition_values: r.partition_values.unwrap_or_default().into_iter().collect(),
                size: r.size,
            }),
        })
        .collect();

    serde_arrow::to_record_batch(delta_action_fields(), &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_protocol_action(protocol: Protocol) -> Result<RecordBatch> {
    let protocol_json =
        serde_json::to_string(&protocol).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let rows = vec![ActionRow {
        action: ExecAction::Protocol(protocol_json),
    }];
    serde_arrow::to_record_batch(delta_action_fields(), &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_metadata_action(metadata: Metadata) -> Result<RecordBatch> {
    let metadata_json =
        serde_json::to_string(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let rows = vec![ActionRow {
        action: ExecAction::Metadata(metadata_json),
    }];
    serde_arrow::to_record_batch(delta_action_fields(), &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_commit_meta(meta: CommitMeta) -> Result<RecordBatch> {
    let operation_json = meta
        .operation
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let operation_metrics_json = serde_json::to_string(&meta.operation_metrics)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows = vec![ActionRow {
        action: ExecAction::CommitMeta(CommitMetaAction {
            commit_row_count: meta.row_count,
            operation_json,
            operation_metrics_json,
        }),
    }];
    serde_arrow::to_record_batch(delta_action_fields(), &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
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
    let mut out_actions: Vec<Action> = Vec::new();
    let mut out_meta: Option<CommitMeta> = None;

    let rows: Vec<ActionRow> = serde_arrow::from_record_batch(batch)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    for row in rows {
        match row.action {
            ExecAction::Add(a) => {
                out_actions.push(Action::Add(Add {
                    path: a.path,
                    partition_values: a.partition_values.into_iter().collect(),
                    size: a.size,
                    modification_time: a.modification_time,
                    data_change: a.data_change,
                    stats: a.stats_json,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                }));
            }
            ExecAction::Remove(r) => {
                out_actions.push(Action::Remove(Remove {
                    path: r.path,
                    data_change: r.data_change,
                    deletion_timestamp: r.deletion_timestamp,
                    extended_file_metadata: r.extended_file_metadata,
                    partition_values: Some(r.partition_values.into_iter().collect()),
                    size: r.size,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                }));
            }
            ExecAction::Protocol(s) => {
                let p: Protocol =
                    serde_json::from_str(&s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Protocol(p));
            }
            ExecAction::Metadata(s) => {
                let m: Metadata =
                    serde_json::from_str(&s).map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_actions.push(Action::Metadata(m));
            }
            ExecAction::CommitMeta(cm) => {
                let operation: Option<DeltaOperation> = cm
                    .operation_json
                    .as_deref()
                    .map(serde_json::from_str::<DeltaOperation>)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let operation_metrics: HashMap<String, Value> =
                    serde_json::from_str::<HashMap<String, Value>>(&cm.operation_metrics_json)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                out_meta = Some(CommitMeta {
                    row_count: cm.commit_row_count,
                    operation,
                    operation_metrics,
                });
            }
        }
    }

    Ok((out_actions, out_meta))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn encode_add_actions_produces_action_column() -> Result<()> {
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

        let rb = encode_add_actions(adds)?;
        assert_eq!(rb.schema(), delta_action_schema());
        assert_eq!(rb.num_rows(), 1);
        assert!(rb.column_by_name(COL_ACTION).is_some());
        Ok(())
    }

    #[test]
    fn decode_actions_and_meta_roundtrip_mixed_batch() -> Result<()> {
        let adds = vec![Add {
            path: "a.parquet".to_string(),
            partition_values: HashMap::new(),
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
            encode_commit_meta(meta)?,
        ];
        let batch = concat_batches(&schema, &out_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::Add(_)));
        assert!(matches!(actions[1], Action::Remove(_)));
        assert_eq!(decoded_meta.unwrap().row_count, 10);
        Ok(())
    }
}
