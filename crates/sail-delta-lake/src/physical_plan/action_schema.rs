use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::models::{Action, Add, Metadata, Protocol, Remove};
use crate::kernel::DeltaOperation;

pub const COL_ACTION: &str = "action";
const COL_PARTITION_VALUES: &str = "partition_values";

static ACTION_FIELDS: LazyLock<Vec<datafusion::arrow::datatypes::FieldRef>> = LazyLock::new(|| {
    #[expect(
        clippy::unwrap_used,
        reason = "ACTION_FIELDS is a process-global constant."
    )]
    let fields = delta_action_fields_build()
        .map_err(|msg| format!("delta action fields initialization failed: {msg}"))
        .unwrap();
    fields
});
static ACTION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new((*ACTION_FIELDS).clone())));

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
pub struct AddAction {
    path: String,
    partition_values: BTreeMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    stats_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveAction {
    path: String,
    data_change: bool,
    deletion_timestamp: Option<i64>,
    extended_file_metadata: Option<bool>,
    // Keep non-null in Arrow: Remove.partition_values=None is encoded as empty map (not NULL)
    partition_values: BTreeMap<String, Option<String>>,
    size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitMetaAction {
    commit_row_count: u64,
    operation_json: Option<String>,
    operation_metrics_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecAction {
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

fn delta_action_tracing_options() -> std::result::Result<serde_arrow::schema::TracingOptions, String>
{
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
        .and_then(|opts| {
            opts.overwrite(
                "action.remove.partition_values",
                Field::new(COL_PARTITION_VALUES, partition_values_type(), false),
            )
        })
        .map_err(|e| format!("failed to overwrite partition_values field: {e}"))
}

fn delta_action_fields_build(
) -> std::result::Result<Vec<datafusion::arrow::datatypes::FieldRef>, String> {
    use serde_arrow::schema::SchemaLike;

    Vec::<datafusion::arrow::datatypes::FieldRef>::from_type::<ActionRow>(
        delta_action_tracing_options()?,
    )
    .map_err(|e| format!("ActionRow schema tracing failed: {e}"))
}

fn delta_action_fields() -> Result<&'static Vec<datafusion::arrow::datatypes::FieldRef>> {
    Ok(&*ACTION_FIELDS)
}

pub fn delta_action_schema() -> Result<SchemaRef> {
    Ok(Arc::clone(&*ACTION_SCHEMA))
}

pub fn encode_actions(actions: Vec<ExecAction>) -> Result<RecordBatch> {
    if actions.is_empty() {
        return Ok(RecordBatch::new_empty(delta_action_schema()?));
    }

    let rows: Vec<ActionRow> = actions
        .into_iter()
        .map(|action| ActionRow { action })
        .collect();

    serde_arrow::to_record_batch(delta_action_fields()?, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_add_actions(adds: Vec<Add>) -> Result<RecordBatch> {
    let actions: Vec<ExecAction> = adds.into_iter().map(ExecAction::from).collect();
    encode_actions(actions)
}

impl From<Add> for ExecAction {
    fn from(add: Add) -> Self {
        ExecAction::Add(AddAction {
            path: add.path,
            partition_values: add.partition_values.into_iter().collect(),
            size: add.size,
            modification_time: add.modification_time,
            data_change: add.data_change,
            stats_json: add.stats,
        })
    }
}

impl From<Remove> for ExecAction {
    fn from(remove: Remove) -> Self {
        ExecAction::Remove(RemoveAction {
            path: remove.path,
            data_change: remove.data_change,
            deletion_timestamp: remove.deletion_timestamp,
            extended_file_metadata: remove.extended_file_metadata,
            partition_values: remove
                .partition_values
                .unwrap_or_default()
                .into_iter()
                .collect(),
            size: remove.size,
        })
    }
}

impl TryFrom<Protocol> for ExecAction {
    type Error = DataFusionError;

    fn try_from(protocol: Protocol) -> Result<Self> {
        let protocol_json =
            serde_json::to_string(&protocol).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ExecAction::Protocol(protocol_json))
    }
}

impl TryFrom<Metadata> for ExecAction {
    type Error = DataFusionError;

    fn try_from(metadata: Metadata) -> Result<Self> {
        let metadata_json =
            serde_json::to_string(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ExecAction::Metadata(metadata_json))
    }
}

impl TryFrom<CommitMeta> for ExecAction {
    type Error = DataFusionError;

    fn try_from(meta: CommitMeta) -> Result<Self> {
        let operation_json = meta
            .operation
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let operation_metrics_json = serde_json::to_string(&meta.operation_metrics)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ExecAction::CommitMeta(CommitMetaAction {
            commit_row_count: meta.row_count,
            operation_json,
            operation_metrics_json,
        }))
    }
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
                    commit_version: None,
                    commit_timestamp: None,
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
    use super::*;

    #[test]
    fn encode_actions_produces_action_column() -> Result<()> {
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
            commit_version: None,
            commit_timestamp: None,
        }];

        let exec_actions: Vec<ExecAction> = adds.into_iter().map(|add| add.into()).collect();
        let rb = encode_actions(exec_actions)?;
        assert_eq!(rb.schema(), delta_action_schema()?);
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
            commit_version: None,
            commit_timestamp: None,
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

        let mut exec_actions: Vec<ExecAction> = Vec::new();
        for add in adds {
            exec_actions.push(add.into());
        }
        for remove in removes {
            exec_actions.push(remove.into());
        }
        exec_actions.push(meta.try_into()?);

        let batch = encode_actions(exec_actions)?;

        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::Add(_)));
        assert!(matches!(actions[1], Action::Remove(_)));
        let decoded_meta = decoded_meta.ok_or_else(|| {
            DataFusionError::Internal("expected CommitMeta to be present in roundtrip batch".into())
        })?;
        assert_eq!(decoded_meta.row_count, 10);
        Ok(())
    }
}
