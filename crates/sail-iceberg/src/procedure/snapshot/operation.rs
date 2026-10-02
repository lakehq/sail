use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::common::{DataFusionError, Result, plan_err};
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;

use super::super::arguments::optional_i64;
use crate::spec::snapshots::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use crate::spec::{Snapshot, TableMetadata, TableRequirement, TableUpdate};

pub(in crate::procedure) fn ancestors_output(
    metadata: &TableMetadata,
    invocation: &LakeProcedureInvocation,
) -> Result<RecordBatch> {
    let start = optional_i64(invocation, "snapshot_id")?.or_else(|| main_snapshot_id(metadata));
    let Some(start) = start else {
        return Ok(RecordBatch::new_empty(invocation.procedure.schema()));
    };
    let ancestors = ancestor_chain(metadata, start)?;
    let snapshot_ids = ancestors
        .iter()
        .map(|snapshot| Some(snapshot.snapshot_id()))
        .collect::<Vec<_>>();
    let timestamps = ancestors
        .iter()
        .map(|snapshot| Some(snapshot.timestamp_ms()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        invocation.procedure.schema(),
        vec![
            Arc::new(Int64Array::from(snapshot_ids)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[derive(Clone)]
pub(in crate::procedure) enum SnapshotOperation {
    RollbackToSnapshot(i64),
    RollbackToTimestamp(i64),
    SetCurrentSnapshot {
        snapshot_id: Option<i64>,
        reference: Option<String>,
    },
    FastForward {
        branch: String,
        to: String,
    },
}

pub(super) struct PreparedSnapshotOperation {
    pub(super) requirement: TableRequirement,
    pub(super) update: TableUpdate,
    pub(super) output: RecordBatch,
    pub(super) changed: bool,
}

impl SnapshotOperation {
    pub(super) fn prepare(
        &self,
        metadata: &TableMetadata,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<PreparedSnapshotOperation> {
        match self {
            Self::RollbackToSnapshot(snapshot_id) => {
                let previous = required_current_snapshot(metadata)?;
                snapshot(metadata, *snapshot_id)?;
                if !is_ancestor(metadata, *snapshot_id, previous)? {
                    return plan_err!(
                        "Cannot roll back to snapshot {snapshot_id}: it is not an ancestor of the current snapshot {previous}"
                    );
                }
                prepare_main_update(metadata, previous, *snapshot_id, schema, false)
            }
            Self::RollbackToTimestamp(timestamp_ms) => {
                let previous = required_current_snapshot(metadata)?;
                let target = ancestor_chain(metadata, previous)?
                    .into_iter()
                    .filter(|snapshot| snapshot.timestamp_ms() < *timestamp_ms)
                    .max_by_key(|snapshot| snapshot.timestamp_ms())
                    .map(Snapshot::snapshot_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Cannot roll back: no ancestor snapshot is older than timestamp {timestamp_ms}"
                        ))
                    })?;
                prepare_main_update(metadata, previous, target, schema, false)
            }
            Self::SetCurrentSnapshot {
                snapshot_id,
                reference,
            } => {
                let previous = main_snapshot_id(metadata);
                let target = match (snapshot_id, reference) {
                    (Some(snapshot_id), None) => *snapshot_id,
                    (None, Some(reference)) => reference_snapshot_id(metadata, reference)?,
                    _ => {
                        return plan_err!(
                            "Exactly one of snapshot_id or ref must be provided to set_current_snapshot"
                        );
                    }
                };
                snapshot(metadata, target)?;
                prepare_main_update_nullable(metadata, previous, target, schema)
            }
            Self::FastForward { branch, to } => {
                let target = reference_snapshot_id(metadata, to)?;
                snapshot(metadata, target)?;
                let previous_reference = metadata.refs.get(branch);
                if let Some(reference) = previous_reference {
                    if !reference.is_branch() {
                        return plan_err!("Ref {branch} is a tag, not a branch");
                    }
                    if !is_ancestor(metadata, reference.snapshot_id, target)? {
                        return plan_err!(
                            "Cannot fast-forward: {branch} is not an ancestor of {to}"
                        );
                    }
                }
                let previous = previous_reference
                    .map(|reference| reference.snapshot_id)
                    .or_else(|| {
                        (branch == MAIN_BRANCH)
                            .then_some(main_snapshot_id(metadata))
                            .flatten()
                    });
                if previous_reference.is_none()
                    && let Some(previous) = previous
                    && !is_ancestor(metadata, previous, target)?
                {
                    return plan_err!("Cannot fast-forward: {branch} is not an ancestor of {to}");
                }
                let retention = previous_reference
                    .map(|reference| reference.retention.clone())
                    .unwrap_or_else(default_branch_retention);
                let requirement = TableRequirement::RefSnapshotIdMatch {
                    r#ref: branch.clone(),
                    snapshot_id: previous,
                };
                let update = TableUpdate::SetSnapshotRef {
                    ref_name: branch.clone(),
                    reference: SnapshotReference {
                        snapshot_id: target,
                        retention,
                    },
                };
                let output = fast_forward_output(schema, branch, previous, target)?;
                Ok(PreparedSnapshotOperation {
                    requirement,
                    update,
                    output,
                    changed: previous != Some(target),
                })
            }
        }
    }
}

fn prepare_main_update(
    metadata: &TableMetadata,
    previous: i64,
    target: i64,
    schema: datafusion::arrow::datatypes::SchemaRef,
    previous_nullable: bool,
) -> Result<PreparedSnapshotOperation> {
    let output = snapshot_change_batch(schema, Some(previous), target, previous_nullable)?;
    prepare_main_reference_update(metadata, Some(previous), target, output)
}

fn prepare_main_update_nullable(
    metadata: &TableMetadata,
    previous: Option<i64>,
    target: i64,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<PreparedSnapshotOperation> {
    let output = snapshot_change_batch(schema, previous, target, true)?;
    prepare_main_reference_update(metadata, previous, target, output)
}

fn prepare_main_reference_update(
    metadata: &TableMetadata,
    previous: Option<i64>,
    target: i64,
    output: RecordBatch,
) -> Result<PreparedSnapshotOperation> {
    if let Some(reference) = metadata.refs.get(MAIN_BRANCH)
        && !reference.is_branch()
    {
        return plan_err!("Ref {MAIN_BRANCH} is a tag, not a branch");
    }
    let retention = metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|reference| reference.retention.clone())
        .unwrap_or_else(default_branch_retention);
    Ok(PreparedSnapshotOperation {
        requirement: TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: previous,
        },
        update: TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference {
                snapshot_id: target,
                retention,
            },
        },
        output,
        changed: previous != Some(target),
    })
}

fn snapshot_change_batch(
    schema: datafusion::arrow::datatypes::SchemaRef,
    previous: Option<i64>,
    current: i64,
    previous_nullable: bool,
) -> Result<RecordBatch> {
    if !previous_nullable && previous.is_none() {
        return plan_err!("Iceberg table has no current snapshot");
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![current])),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn fast_forward_output(
    schema: datafusion::arrow::datatypes::SchemaRef,
    branch: &str,
    previous: Option<i64>,
    current: i64,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![branch])),
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![current])),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn default_branch_retention() -> SnapshotRetention {
    SnapshotRetention::Branch {
        min_snapshots_to_keep: None,
        max_snapshot_age_ms: None,
        max_ref_age_ms: None,
    }
}

fn required_current_snapshot(metadata: &TableMetadata) -> Result<i64> {
    main_snapshot_id(metadata)
        .ok_or_else(|| DataFusionError::Plan("Iceberg table has no current snapshot".to_string()))
}

fn main_snapshot_id(metadata: &TableMetadata) -> Option<i64> {
    metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|reference| reference.snapshot_id)
        .or(metadata.current_snapshot_id)
        .filter(|snapshot_id| *snapshot_id >= 0)
}

fn snapshot(metadata: &TableMetadata, snapshot_id: i64) -> Result<&Snapshot> {
    metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id() == snapshot_id)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Cannot find Iceberg snapshot with id {snapshot_id}"
            ))
        })
}

fn reference_snapshot_id(metadata: &TableMetadata, reference: &str) -> Result<i64> {
    if reference == MAIN_BRANCH {
        return required_current_snapshot(metadata);
    }
    metadata
        .refs
        .get(reference)
        .map(|reference| reference.snapshot_id)
        .ok_or_else(|| DataFusionError::Plan(format!("Ref does not exist: {reference}")))
}

fn ancestor_chain(metadata: &TableMetadata, start: i64) -> Result<Vec<&Snapshot>> {
    let mut current = Some(start);
    let mut visited = HashSet::new();
    let mut ancestors = Vec::new();
    while let Some(snapshot_id) = current {
        if !visited.insert(snapshot_id) {
            return plan_err!("Cycle detected in Iceberg snapshot ancestry at {snapshot_id}");
        }
        let current_snapshot = snapshot(metadata, snapshot_id)?;
        current = current_snapshot.parent_snapshot_id();
        ancestors.push(current_snapshot);
    }
    Ok(ancestors)
}

fn is_ancestor(metadata: &TableMetadata, ancestor: i64, descendant: i64) -> Result<bool> {
    Ok(ancestor_chain(metadata, descendant)?
        .iter()
        .any(|snapshot| snapshot.snapshot_id() == ancestor))
}

#[cfg(test)]
mod tests {
    use datafusion::common::{Result, plan_err};

    use super::*;
    use crate::procedure::descriptor::IcebergProcedureType;
    use crate::spec::{FormatVersion, Operation, Summary};

    fn snapshot_with_parent(id: i64, parent: Option<i64>, timestamp_ms: i64) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            parent_snapshot_id: parent,
            sequence_number: id,
            timestamp_ms,
            manifest_list: String::new(),
            manifests: None,
            summary: Summary::new(Operation::Append),
            schema_id: None,
            first_row_id: None,
            added_rows: None,
            key_id: None,
        }
    }

    fn metadata_with_snapshots() -> TableMetadata {
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "file:///tmp/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 30,
            last_column_id: 0,
            schemas: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: Default::default(),
            current_snapshot_id: Some(3),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![
                snapshot_with_parent(1, None, 10),
                snapshot_with_parent(2, Some(1), 20),
                snapshot_with_parent(3, Some(2), 30),
            ],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: Default::default(),
            statistics: vec![],
            partition_statistics: vec![],
        }
    }

    #[test]
    fn snapshot_ancestry_is_ordered_from_start_to_root() -> Result<()> {
        let metadata = metadata_with_snapshots();
        let ids = ancestor_chain(&metadata, 3)?
            .into_iter()
            .map(Snapshot::snapshot_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![3, 2, 1]);
        assert!(is_ancestor(&metadata, 1, 3)?);
        assert!(!is_ancestor(&metadata, 3, 1)?);
        Ok(())
    }

    #[test]
    fn rollback_requires_ancestry_but_set_current_does_not() -> Result<()> {
        let mut metadata = metadata_with_snapshots();
        metadata
            .snapshots
            .push(snapshot_with_parent(4, Some(1), 40));
        let Some(rollback_procedure) = IcebergProcedureType::RollbackToSnapshot.descriptor() else {
            return plan_err!("rollback_to_snapshot should be supported");
        };
        let Err(error) = SnapshotOperation::RollbackToSnapshot(4)
            .prepare(&metadata, rollback_procedure.schema())
        else {
            return plan_err!("sibling snapshot cannot be a rollback target");
        };
        assert!(error.to_string().contains("not an ancestor"));

        let Some(set_current_procedure) = IcebergProcedureType::SetCurrentSnapshot.descriptor()
        else {
            return plan_err!("set_current_snapshot should be supported");
        };
        let prepared = SnapshotOperation::SetCurrentSnapshot {
            snapshot_id: Some(4),
            reference: None,
        }
        .prepare(&metadata, set_current_procedure.schema())?;
        assert!(matches!(
            prepared.update,
            TableUpdate::SetSnapshotRef { reference, .. } if reference.snapshot_id == 4
        ));
        Ok(())
    }

    #[test]
    fn fast_forward_preserves_branch_retention_and_checks_ancestry() -> Result<()> {
        let mut metadata = metadata_with_snapshots();
        let retention = SnapshotRetention::Branch {
            min_snapshots_to_keep: Some(2),
            max_snapshot_age_ms: Some(10_000),
            max_ref_age_ms: Some(20_000),
        };
        metadata.refs.insert(
            "audit".to_string(),
            SnapshotReference {
                snapshot_id: 1,
                retention: retention.clone(),
            },
        );
        metadata.refs.insert(
            "tip".to_string(),
            SnapshotReference {
                snapshot_id: 3,
                retention: default_branch_retention(),
            },
        );
        let Some(procedure) = IcebergProcedureType::FastForward.descriptor() else {
            return plan_err!("fast_forward should be supported");
        };
        let schema = procedure.schema();
        let prepared = SnapshotOperation::FastForward {
            branch: "audit".to_string(),
            to: "tip".to_string(),
        }
        .prepare(&metadata, schema.clone())?;
        assert!(matches!(
            prepared.update,
            TableUpdate::SetSnapshotRef { reference, .. }
                if reference.snapshot_id == 3 && reference.retention == retention
        ));

        let Err(error) = SnapshotOperation::FastForward {
            branch: "tip".to_string(),
            to: "audit".to_string(),
        }
        .prepare(&metadata, schema) else {
            return plan_err!("a branch cannot be moved backward by fast-forward");
        };
        assert!(error.to_string().contains("not an ancestor"));
        Ok(())
    }
}
