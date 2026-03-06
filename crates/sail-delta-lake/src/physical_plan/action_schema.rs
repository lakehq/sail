use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

use crate::kernel::transaction::OperationMetrics;
use crate::kernel::DeltaOperation;
use crate::spec::{Action, Add, Metadata, Protocol, Remove};

pub const COL_ACTION: &str = "action";
const COL_PARTITION_VALUES: &str = "partition_values";

static ACTION_FIELDS: LazyLock<Vec<datafusion::arrow::datatypes::FieldRef>> =
    LazyLock::new(delta_action_fields_build);
static ACTION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new((*ACTION_FIELDS).clone())));

#[derive(Debug, Clone, Default)]
pub struct CommitMeta {
    pub row_count: u64,
    pub operation: Option<DeltaOperation>,
    pub operation_metrics: OperationMetrics,
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

fn action_field(name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new(name, data_type, nullable))
}

fn action_union_type() -> DataType {
    let protocol_type = DataType::Struct(
        vec![
            action_field("minReaderVersion", DataType::Int32, false),
            action_field("minWriterVersion", DataType::Int32, false),
            action_field(
                "readerFeatures",
                DataType::List(action_field("element", DataType::Utf8, true)),
                true,
            ),
            action_field(
                "writerFeatures",
                DataType::List(action_field("element", DataType::Utf8, true)),
                true,
            ),
        ]
        .into(),
    );
    let metadata_format_type = DataType::Struct(
        vec![
            action_field("provider", DataType::Utf8, false),
            action_field("options", partition_values_type(), false),
        ]
        .into(),
    );
    let metadata_type = DataType::Struct(
        vec![
            action_field("id", DataType::Utf8, false),
            action_field("name", DataType::Utf8, true),
            action_field("description", DataType::Utf8, true),
            action_field("format", metadata_format_type, false),
            action_field("schemaString", DataType::Utf8, false),
            action_field(
                "partitionColumns",
                DataType::List(action_field("element", DataType::Utf8, true)),
                false,
            ),
            action_field("createdTime", DataType::Int64, true),
            action_field("configuration", partition_values_type(), false),
        ]
        .into(),
    );
    let add_type = DataType::Struct(
        vec![
            action_field("path", DataType::Utf8, false),
            action_field(COL_PARTITION_VALUES, partition_values_type(), false),
            action_field("size", DataType::Int64, false),
            action_field("modification_time", DataType::Int64, false),
            action_field("data_change", DataType::Boolean, false),
            action_field("stats_json", DataType::Utf8, true),
        ]
        .into(),
    );
    let remove_type = DataType::Struct(
        vec![
            action_field("path", DataType::Utf8, false),
            action_field("data_change", DataType::Boolean, false),
            action_field("deletion_timestamp", DataType::Int64, true),
            action_field("extended_file_metadata", DataType::Boolean, true),
            action_field(COL_PARTITION_VALUES, partition_values_type(), false),
            action_field("size", DataType::Int64, true),
        ]
        .into(),
    );
    let commit_meta_type = DataType::Struct(
        vec![
            action_field("commit_row_count", DataType::UInt64, false),
            action_field("operation_json", DataType::Utf8, true),
            action_field("num_files", DataType::UInt64, true),
            action_field("num_output_rows", DataType::UInt64, true),
            action_field("num_output_bytes", DataType::UInt64, true),
            action_field("execution_time_ms", DataType::UInt64, true),
            action_field("num_removed_files", DataType::UInt64, true),
            action_field("num_added_files", DataType::UInt64, true),
            action_field("num_output_files", DataType::UInt64, true),
            action_field("num_added_bytes", DataType::UInt64, true),
            action_field("num_removed_bytes", DataType::UInt64, true),
            action_field("write_time_ms", DataType::UInt64, true),
            action_field("operation_metrics_extra_json", DataType::Utf8, true),
        ]
        .into(),
    );

    DataType::Union(
        vec![
            (0, action_field("add", add_type, false)),
            (1, action_field("remove", remove_type, false)),
            (2, action_field("protocol", protocol_type, false)),
            (3, action_field("metadata", metadata_type, false)),
            (4, action_field("commit_meta", commit_meta_type, false)),
        ]
        .into_iter()
        .collect(),
        datafusion::arrow::datatypes::UnionMode::Dense,
    )
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
    num_files: Option<u64>,
    num_output_rows: Option<u64>,
    num_output_bytes: Option<u64>,
    execution_time_ms: Option<u64>,
    num_removed_files: Option<u64>,
    num_added_files: Option<u64>,
    num_output_files: Option<u64>,
    num_added_bytes: Option<u64>,
    num_removed_bytes: Option<u64>,
    write_time_ms: Option<u64>,
    operation_metrics_extra_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalExecAction {
    #[serde(rename = "add")]
    Add(AddAction),
    #[serde(rename = "remove")]
    Remove(RemoveAction),
    // Protocol / Metadata are relatively rare; we keep them as JSON strings for now.
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metadata")]
    Metadata(Metadata),
    #[serde(rename = "commit_meta")]
    CommitMeta(CommitMetaAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActionRow {
    action: PhysicalExecAction,
}

fn delta_action_fields_build() -> Vec<datafusion::arrow::datatypes::FieldRef> {
    vec![action_field(COL_ACTION, action_union_type(), false)]
}

fn delta_action_fields() -> Result<&'static Vec<datafusion::arrow::datatypes::FieldRef>> {
    Ok(&*ACTION_FIELDS)
}

pub fn delta_action_schema() -> Result<SchemaRef> {
    Ok(Arc::clone(&*ACTION_SCHEMA))
}

pub fn encode_actions(actions: Vec<PhysicalExecAction>) -> Result<RecordBatch> {
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
    let actions: Vec<PhysicalExecAction> = adds.into_iter().map(PhysicalExecAction::from).collect();
    encode_actions(actions)
}

impl From<Add> for PhysicalExecAction {
    fn from(add: Add) -> Self {
        PhysicalExecAction::Add(AddAction {
            path: add.path,
            partition_values: add.partition_values.into_iter().collect(),
            size: add.size,
            modification_time: add.modification_time,
            data_change: add.data_change,
            stats_json: add.stats,
        })
    }
}

impl From<Remove> for PhysicalExecAction {
    fn from(remove: Remove) -> Self {
        PhysicalExecAction::Remove(RemoveAction {
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

impl TryFrom<Protocol> for PhysicalExecAction {
    type Error = DataFusionError;

    fn try_from(protocol: Protocol) -> Result<Self> {
        Ok(PhysicalExecAction::Protocol(protocol))
    }
}

impl TryFrom<Metadata> for PhysicalExecAction {
    type Error = DataFusionError;

    fn try_from(metadata: Metadata) -> Result<Self> {
        Ok(PhysicalExecAction::Metadata(metadata))
    }
}

impl TryFrom<CommitMeta> for PhysicalExecAction {
    type Error = DataFusionError;

    fn try_from(meta: CommitMeta) -> Result<Self> {
        let operation_json = meta
            .operation
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let operation_metrics_extra_json = (!meta.operation_metrics.extra.is_empty())
            .then(|| serde_json::to_string(&meta.operation_metrics.extra))
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(PhysicalExecAction::CommitMeta(CommitMetaAction {
            commit_row_count: meta.row_count,
            operation_json,
            num_files: meta.operation_metrics.num_files,
            num_output_rows: meta.operation_metrics.num_output_rows,
            num_output_bytes: meta.operation_metrics.num_output_bytes,
            execution_time_ms: meta.operation_metrics.execution_time_ms,
            num_removed_files: meta.operation_metrics.num_removed_files,
            num_added_files: meta.operation_metrics.num_added_files,
            num_output_files: meta.operation_metrics.num_output_files,
            num_added_bytes: meta.operation_metrics.num_added_bytes,
            num_removed_bytes: meta.operation_metrics.num_removed_bytes,
            write_time_ms: meta.operation_metrics.write_time_ms,
            operation_metrics_extra_json,
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
            PhysicalExecAction::Add(a) => {
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
            PhysicalExecAction::Remove(r) => {
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
            PhysicalExecAction::Protocol(protocol) => out_actions.push(Action::Protocol(protocol)),
            PhysicalExecAction::Metadata(metadata) => out_actions.push(Action::Metadata(metadata)),
            PhysicalExecAction::CommitMeta(cm) => {
                let operation: Option<DeltaOperation> = cm
                    .operation_json
                    .as_deref()
                    .map(serde_json::from_str::<DeltaOperation>)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let extra = cm
                    .operation_metrics_extra_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .unwrap_or_default();
                out_meta = Some(CommitMeta {
                    row_count: cm.commit_row_count,
                    operation,
                    operation_metrics: OperationMetrics {
                        num_files: cm.num_files,
                        num_output_rows: cm.num_output_rows,
                        num_output_bytes: cm.num_output_bytes,
                        execution_time_ms: cm.execution_time_ms,
                        num_removed_files: cm.num_removed_files,
                        num_added_files: cm.num_added_files,
                        num_output_files: cm.num_output_files,
                        num_added_bytes: cm.num_added_bytes,
                        num_removed_bytes: cm.num_removed_bytes,
                        write_time_ms: cm.write_time_ms,
                        extra,
                    },
                });
            }
        }
    }

    Ok((out_actions, out_meta))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::kernel::transaction::OperationMetrics;
    use crate::spec::StructType;

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

        let exec_actions: Vec<PhysicalExecAction> =
            adds.into_iter().map(|add| add.into()).collect();
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
            operation_metrics: OperationMetrics::default(),
        };

        let mut exec_actions: Vec<PhysicalExecAction> = Vec::new();
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

    #[test]
    fn protocol_and_metadata_roundtrip_as_typed_actions() -> Result<()> {
        let protocol = Protocol::new(3, 7, None, None);
        let metadata = Metadata::try_new(
            Some("tbl".to_string()),
            Some("desc".to_string()),
            StructType::try_new([]).map_err(|e| DataFusionError::External(Box::new(e)))?,
            vec!["p".to_string()],
            0,
            HashMap::from([("k".to_string(), "v".to_string())]),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_table_id("table-id".to_string());

        let batch = encode_actions(vec![
            protocol.clone().try_into()?,
            metadata.clone().try_into()?,
        ])?;
        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;

        assert!(decoded_meta.is_none());
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], Action::Protocol(value) if value == &protocol));
        assert!(matches!(&actions[1], Action::Metadata(value) if value == &metadata));
        Ok(())
    }
}
