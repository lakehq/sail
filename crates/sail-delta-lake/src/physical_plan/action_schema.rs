use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

use crate::kernel::transaction::OperationMetrics;
use crate::kernel::DeltaOperation;
use crate::spec::{
    add_struct_type, metadata_struct_type, protocol_struct_type, remove_struct_type, Action, Add,
    Metadata, Protocol, Remove,
};

pub const COL_ACTION: &str = "action";

static ACTION_FIELDS: LazyLock<Vec<datafusion::arrow::datatypes::FieldRef>> =
    LazyLock::new(delta_action_fields_build);
static ACTION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new((*ACTION_FIELDS).clone())));

#[derive(Debug, Clone, Default)]
pub struct ExecCommitMeta {
    pub row_count: u64,
    pub operation: Option<DeltaOperation>,
    pub operation_metrics: OperationMetrics,
}

fn action_field(name: &str, data_type: ArrowDataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new(name, data_type, nullable))
}

fn action_struct_field(name: &str, schema: crate::spec::StructType, nullable: bool) -> Arc<Field> {
    #[expect(clippy::expect_used)]
    let data_type = ArrowDataType::try_from(&crate::spec::DataType::from(schema))
        .expect("action payload schema should convert to Arrow");
    action_field(name, data_type, nullable)
}

fn action_union_type() -> ArrowDataType {
    ArrowDataType::Union(
        vec![
            (0, action_struct_field("add", add_struct_type(), false)),
            (
                1,
                action_struct_field("remove", remove_struct_type(), false),
            ),
            (
                2,
                action_struct_field("protocol", protocol_struct_type(), false),
            ),
            (
                3,
                action_struct_field("metadata", metadata_struct_type(), false),
            ),
            (
                4,
                action_field("commit_meta", ExecCommitMetaTransport::data_type(), false),
            ),
        ]
        .into_iter()
        .collect(),
        datafusion::arrow::datatypes::UnionMode::Dense,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecCommitMetaTransport {
    commit_row_count: u64,
    operation_json: Option<String>,
    operation_metrics_json: Option<String>,
}

impl ExecCommitMetaTransport {
    fn data_type() -> ArrowDataType {
        ArrowDataType::Struct(
            vec![
                action_field("commit_row_count", ArrowDataType::UInt64, false),
                action_field("operation_json", ArrowDataType::Utf8, true),
                action_field("operation_metrics_json", ArrowDataType::Utf8, true),
            ]
            .into(),
        )
    }

    fn from_exec_meta(meta: ExecCommitMeta) -> Result<Self> {
        let operation_json = meta
            .operation
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let operation_metrics_json = if meta.operation_metrics == OperationMetrics::default() {
            None
        } else {
            Some(
                serde_json::to_string(&meta.operation_metrics)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        };

        Ok(Self {
            commit_row_count: meta.row_count,
            operation_json,
            operation_metrics_json,
        })
    }

    fn into_exec_meta(self) -> Result<ExecCommitMeta> {
        let operation: Option<DeltaOperation> = self
            .operation_json
            .as_deref()
            .map(serde_json::from_str::<DeltaOperation>)
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let operation_metrics = self
            .operation_metrics_json
            .as_deref()
            .map(serde_json::from_str::<OperationMetrics>)
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .unwrap_or_default();

        Ok(ExecCommitMeta {
            row_count: self.commit_row_count,
            operation,
            operation_metrics,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PhysicalExecAction {
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "remove")]
    Remove(Remove),
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metadata")]
    Metadata(Metadata),
    #[serde(rename = "commit_meta")]
    CommitMeta(ExecCommitMetaTransport),
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

fn encode_transport_actions(actions: Vec<PhysicalExecAction>) -> Result<RecordBatch> {
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

impl TryFrom<Action> for PhysicalExecAction {
    type Error = DataFusionError;

    fn try_from(action: Action) -> Result<Self> {
        match action {
            Action::Add(add) => Ok(Self::Add(add)),
            Action::Remove(remove) => Ok(Self::Remove(remove)),
            Action::Protocol(protocol) => Ok(Self::Protocol(protocol)),
            Action::Metadata(metadata) => Ok(Self::Metadata(metadata)),
            unsupported => Err(DataFusionError::Plan(format!(
                "unsupported physical action transport variant: {unsupported:?}"
            ))),
        }
    }
}

pub fn encode_actions(
    actions: Vec<Action>,
    exec_meta: Option<ExecCommitMeta>,
) -> Result<RecordBatch> {
    let mut transport_actions = actions
        .into_iter()
        .map(PhysicalExecAction::try_from)
        .collect::<Result<Vec<_>>>()?;

    if let Some(exec_meta) = exec_meta {
        transport_actions.push(PhysicalExecAction::CommitMeta(
            ExecCommitMetaTransport::from_exec_meta(exec_meta)?,
        ));
    }

    encode_transport_actions(transport_actions)
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
) -> Result<(Vec<Action>, Option<ExecCommitMeta>)> {
    let mut out_actions: Vec<Action> = Vec::new();
    let mut out_meta: Option<ExecCommitMeta> = None;

    let rows: Vec<ActionRow> = serde_arrow::from_record_batch(batch)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    for row in rows {
        match row.action {
            PhysicalExecAction::Add(add) => out_actions.push(Action::Add(add)),
            PhysicalExecAction::Remove(remove) => out_actions.push(Action::Remove(remove)),
            PhysicalExecAction::Protocol(protocol) => out_actions.push(Action::Protocol(protocol)),
            PhysicalExecAction::Metadata(metadata) => out_actions.push(Action::Metadata(metadata)),
            PhysicalExecAction::CommitMeta(cm) => {
                out_meta = Some(cm.into_exec_meta()?);
            }
        }
    }

    Ok((out_actions, out_meta))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::DataType as ArrowDataType;

    use super::*;
    use crate::kernel::transaction::OperationMetrics;
    use crate::spec::{DeletionVectorDescriptor, StorageType, StructType};

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

        let rb = encode_actions(adds.into_iter().map(Action::Add).collect(), None)?;
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
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }];
        let meta = ExecCommitMeta {
            row_count: 10,
            operation: None,
            operation_metrics: OperationMetrics::default(),
        };
        let batch = encode_actions(
            adds.into_iter()
                .map(Action::Add)
                .chain(removes.into_iter().map(Action::Remove))
                .collect(),
            Some(meta),
        )?;

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
    fn exec_commit_meta_transport_uses_compact_operation_metrics_json() -> Result<()> {
        let meta = ExecCommitMeta {
            row_count: 10,
            operation: None,
            operation_metrics: OperationMetrics {
                num_removed_files: Some(2),
                num_touched_rows: Some(5),
                ..Default::default()
            },
        };

        let transport = ExecCommitMetaTransport::from_exec_meta(meta)?;
        let metrics_json = transport
            .operation_metrics_json
            .as_deref()
            .ok_or_else(|| DataFusionError::Internal("expected operation metrics json".into()))?;
        let metrics_value = serde_json::from_str::<serde_json::Value>(metrics_json)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        assert_eq!(
            metrics_value.get("numRemovedFiles"),
            Some(&serde_json::Value::from(2))
        );
        assert_eq!(
            metrics_value.get("numTouchedRows"),
            Some(&serde_json::Value::from(5))
        );
        assert!(metrics_value.get("num_removed_files").is_none());
        assert!(!metrics_json.contains("null"));

        let decoded = transport.into_exec_meta()?;
        assert_eq!(decoded.operation_metrics.num_removed_files, Some(2));
        assert_eq!(decoded.operation_metrics.num_touched_rows, Some(5));
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

        let batch = encode_actions(
            vec![
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
            None,
        )?;
        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;

        assert!(decoded_meta.is_none());
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], Action::Protocol(value) if value == &protocol));
        assert!(matches!(&actions[1], Action::Metadata(value) if value == &metadata));
        Ok(())
    }

    #[test]
    fn add_and_remove_roundtrip_preserve_extended_fields() -> Result<()> {
        let add = Add {
            path: "part-000.parquet".to_string(),
            partition_values: HashMap::from([("p".to_string(), Some("1".to_string()))]),
            size: 10,
            modification_time: 20,
            data_change: true,
            stats: Some("{\"numRecords\":1}".to_string()),
            tags: Some(HashMap::from([("k".to_string(), Some("v".to_string()))])),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: StorageType::Inline,
                path_or_inline_dv: "encoded-dv".to_string(),
                offset: Some(12),
                size_in_bytes: 34,
                cardinality: 56,
            }),
            base_row_id: Some(1),
            default_row_commit_version: Some(2),
            clustering_provider: Some("liquid".to_string()),
            commit_version: None,
            commit_timestamp: None,
        };
        let remove = Remove {
            path: "part-000.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(30),
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::from([("p".to_string(), Some("1".to_string()))])),
            size: Some(10),
            stats: Some("{\"numRecords\":1}".to_string()),
            tags: Some(HashMap::from([("k".to_string(), Some("v".to_string()))])),
            deletion_vector: add.deletion_vector.clone(),
            base_row_id: Some(1),
            default_row_commit_version: Some(2),
        };

        let batch = encode_actions(
            vec![Action::Add(add.clone()), Action::Remove(remove.clone())],
            None,
        )?;
        let (actions, decoded_meta) = decode_actions_and_meta_from_batch(&batch)?;

        assert!(decoded_meta.is_none());
        assert_eq!(actions, vec![Action::Add(add), Action::Remove(remove)]);
        Ok(())
    }

    #[test]
    fn action_union_reuses_shared_payload_types() -> Result<()> {
        let schema = delta_action_schema()?;
        let action_field = schema
            .field_with_name(COL_ACTION)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ArrowDataType::Union(fields, _) = action_field.data_type() else {
            return Err(DataFusionError::Internal(
                "action column should be a dense union".into(),
            ));
        };

        let add_type = fields
            .iter()
            .find(|(_, field)| field.name() == "add")
            .map(|(_, field)| field.data_type().clone())
            .ok_or_else(|| DataFusionError::Internal("missing add union member".into()))?;
        let remove_type = fields
            .iter()
            .find(|(_, field)| field.name() == "remove")
            .map(|(_, field)| field.data_type().clone())
            .ok_or_else(|| DataFusionError::Internal("missing remove union member".into()))?;

        let expected_add =
            ArrowDataType::try_from(&crate::spec::DataType::from(crate::spec::add_struct_type()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let expected_remove = ArrowDataType::try_from(&crate::spec::DataType::from(
            crate::spec::remove_struct_type(),
        ))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        assert_eq!(add_type, expected_add);
        assert_eq!(remove_type, expected_remove);
        Ok(())
    }
}
