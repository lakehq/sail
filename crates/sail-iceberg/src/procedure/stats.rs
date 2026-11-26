// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::TaskContext;
use sail_common_datafusion::lakeprocedure::{LakeProcedureInvocation, LakeProcedureValue};
use url::Url;

use super::arguments::optional_i64;
use super::descriptor::IcebergProcedureType;
use super::snapshot::operation::PreparedSnapshotOperation;
use crate::io::StoreContext;
use crate::spec::{
    SnapshotReference, SnapshotRetention, TableMetadata, TableRequirement, TableUpdate,
};
use crate::table::metadata_loader::table_metadata_location;

mod alpha;
mod avro;
mod partition;
mod table;

pub(crate) enum ComputeStats {
    Table {
        snapshot_id: Option<i64>,
        columns: Option<Vec<String>>,
    },
    Partition {
        snapshot_id: Option<i64>,
    },
}

impl ComputeStats {
    pub(super) fn new(
        kind: IcebergProcedureType,
        invocation: &LakeProcedureInvocation,
    ) -> Result<Self> {
        let snapshot_id = optional_i64(invocation, "snapshot_id")?;
        if kind == IcebergProcedureType::ComputePartitionStats {
            return Ok(Self::Partition { snapshot_id });
        }
        let columns = match invocation.argument("columns") {
            None | Some(LakeProcedureValue::Null) => None,
            Some(LakeProcedureValue::StringArray(names)) => {
                if names.is_empty() {
                    return plan_err!("Columns cannot be null/empty");
                }
                Some(
                    names
                        .iter()
                        .cloned()
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| plan_datafusion_err!("Column names cannot be null"))?,
                )
            }
            _ => return plan_err!("Expected an array of column names"),
        };
        Ok(Self::Table {
            snapshot_id,
            columns,
        })
    }

    pub(crate) fn guard_catalog_commit(
        &self,
        metadata: &TableMetadata,
        requirements: &mut Vec<TableRequirement>,
        updates: &mut Vec<TableUpdate>,
    ) {
        let (Self::Table {
            snapshot_id: Some(id),
            ..
        }
        | Self::Partition {
            snapshot_id: Some(id),
        }) = self
        else {
            return;
        };
        if let Some((name, _)) = metadata.refs.iter().find(|(_, r)| r.snapshot_id == *id) {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: Some(*id),
            });
        } else {
            // REST has no snapshot-exists requirement. Setting a temporary tag validates the
            // historical snapshot inside the same atomic commit, including for v1 tables.
            let name = format!("sail-stats-{}", uuid::Uuid::new_v4());
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: None,
            });
            updates.push(TableUpdate::SetSnapshotRef {
                ref_name: name.clone(),
                reference: SnapshotReference {
                    snapshot_id: *id,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            });
            updates.push(TableUpdate::RemoveSnapshotRef { ref_name: name });
        }
    }

    pub(crate) async fn prepare(
        &mut self,
        ctx: &TaskContext,
        store: &StoreContext,
        table_url: &Url,
        metadata: &TableMetadata,
        schema: SchemaRef,
    ) -> Result<PreparedSnapshotOperation> {
        let snapshot_id = match self {
            Self::Table { snapshot_id, .. } | Self::Partition { snapshot_id } => snapshot_id,
        };
        let mut prepared = PreparedSnapshotOperation {
            changed: false,
            requirements: vec![],
            updates: vec![],
            staged_files: vec![],
            output: RecordBatch::new_empty(schema.clone()),
        };
        let Some(id) = snapshot_id.or_else(|| metadata.current_snapshot().map(|s| s.snapshot_id()))
        else {
            return Ok(prepared);
        };
        // Keep the selected snapshot across optimistic commit retries.
        *snapshot_id = Some(id);
        let snapshot = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id() == id)
            .ok_or_else(|| plan_datafusion_err!("Snapshot not found: {id}"))?;
        let metadata_path = |filename: &str| -> Result<String> {
            match metadata.properties.get("write.metadata.path") {
                Some(location) => {
                    let path = format!("{}/{}", location.trim_end_matches('/'), filename);
                    if path.starts_with('/') {
                        return Ok(Url::from_file_path(&path)
                            .map_err(|()| plan_datafusion_err!("Invalid metadata path {path}"))?
                            .to_string());
                    }
                    table_metadata_location(table_url, &path)
                }
                None => table_metadata_location(table_url, &format!("metadata/{filename}")),
            }
        };
        let (update, bytes, path) = match self {
            Self::Table { columns, .. } => {
                let filename = format!("{id}-{}.stats", uuid::Uuid::new_v4());
                let path = metadata_path(&filename)?;
                let (file, bytes) = table::compute(
                    ctx,
                    table_url,
                    metadata,
                    snapshot,
                    columns.as_deref(),
                    path.clone(),
                )
                .await?;
                (TableUpdate::SetStatistics { statistics: file }, bytes, path)
            }
            Self::Partition { .. } => {
                if let Some(file) = metadata
                    .partition_statistics
                    .iter()
                    .find(|file| file.snapshot_id == id)
                {
                    prepared.output = output(schema, &file.statistics_path)?;
                    return Ok(prepared);
                }
                let Some((bytes, format)) =
                    partition::compute(store, metadata, snapshot, ctx).await?
                else {
                    return Ok(prepared);
                };
                let filename = format!("partition-stats-{id}-{}.{format}", uuid::Uuid::new_v4());
                let path = metadata_path(&filename)?;
                let file = crate::spec::PartitionStatisticsFile {
                    snapshot_id: id,
                    statistics_path: path.clone(),
                    file_size_in_bytes: bytes.len() as i64,
                };
                (
                    TableUpdate::SetPartitionStatistics {
                        partition_statistics: file,
                    },
                    bytes,
                    path,
                )
            }
        };
        prepared.changed = true;
        prepared.updates.push(update);
        prepared.staged_files.push((path.clone(), bytes));
        prepared.output = output(schema, &path)?;
        Ok(prepared)
    }
}

fn output(schema: SchemaRef, path: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![path]))],
    )?)
}
