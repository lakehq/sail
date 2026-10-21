use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result, plan_err};
use sail_common::spec::{
    SnapshotReferenceCreateMode as Mode, SnapshotReferenceOperation, SnapshotReferenceType as Kind,
};

use super::operation::PreparedSnapshotOperation;
use crate::spec::manifest_list::ManifestListWriter;
use crate::spec::{
    FormatVersion, MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
    TableMetadata, TableRequirement, TableUpdate,
};

pub(super) fn prepare_reference(
    metadata: &TableMetadata,
    operation: &SnapshotReferenceOperation,
    schema: SchemaRef,
) -> Result<PreparedSnapshotOperation> {
    let mut prepared = PreparedSnapshotOperation {
        requirements: vec![],
        updates: vec![],
        manifests: vec![],
        output: RecordBatch::new_empty(schema),
        changed: false,
    };
    let (name, kind) = match operation {
        SnapshotReferenceOperation::Create { name, kind, .. }
        | SnapshotReferenceOperation::Drop { name, kind, .. } => (name, *kind),
    };
    let current = metadata.current_snapshot_id.filter(|id| *id >= 0);
    let implicit_main = current.map(|snapshot_id| SnapshotReference {
        snapshot_id,
        retention: empty_retention(Kind::Branch),
    });
    let existing = metadata.refs.get(name).or_else(|| {
        (name == MAIN_BRANCH)
            .then_some(implicit_main.as_ref())
            .flatten()
    });
    match operation {
        SnapshotReferenceOperation::Drop { if_exists, .. } => {
            if existing.is_none() && *if_exists {
                return Ok(prepared);
            }
            if name == MAIN_BRANCH && kind == Kind::Branch {
                return plan_err!("Cannot remove main branch");
            }
            let Some(existing) = existing else {
                return plan_err!("Snapshot reference does not exist: {name}");
            };
            validate_kind(name, kind, existing)?;
            prepared.updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: name.clone(),
            });
        }
        SnapshotReferenceOperation::Create {
            mode,
            snapshot_id,
            max_ref_age_ms,
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            ..
        } => {
            // Spark checks the tag's snapshot before applying IF NOT EXISTS.
            let selected = snapshot_id.or(current);
            if kind == Kind::Tag && selected.is_none() {
                return plan_err!("Cannot create or replace tag: main has no snapshot");
            }
            if existing.is_some() && *mode == Mode::CreateIfNotExists {
                return Ok(prepared);
            }
            if existing.is_some() && *mode == Mode::Create {
                return plan_err!("Ref {name} already exists");
            }
            if existing.is_none() && *mode == Mode::Replace {
                return plan_err!("Snapshot reference does not exist: {name}");
            }
            if let Some(existing) = existing {
                validate_kind(name, kind, existing)?;
                if selected.is_none() {
                    return plan_err!("Cannot replace branch: main has no snapshot");
                }
            }
            if name == MAIN_BRANCH && kind == Kind::Tag {
                return plan_err!("Ref main must be a branch");
            }
            if min_snapshots_to_keep.is_some_and(|value| value <= 0)
                || max_snapshot_age_ms.is_some_and(|value| value <= 0)
                || max_ref_age_ms.is_some_and(|value| value <= 0)
            {
                return plan_err!("Snapshot retention values must be greater than 0");
            }
            if kind == Kind::Tag
                && (min_snapshots_to_keep.is_some() || max_snapshot_age_ms.is_some())
            {
                return plan_err!("Snapshot retention is only supported for branches");
            }
            let snapshot_id = match selected {
                Some(id) => {
                    if !metadata
                        .snapshots
                        .iter()
                        .any(|snapshot| snapshot.snapshot_id() == id)
                    {
                        return plan_err!("Cannot find Iceberg snapshot with id {id}");
                    }
                    id
                }
                None => {
                    let snapshot = empty_snapshot(metadata, &mut prepared)?;
                    let id = snapshot.snapshot_id;
                    prepared.updates.push(TableUpdate::AddSnapshot { snapshot });
                    if name != MAIN_BRANCH {
                        prepared
                            .requirements
                            .push(TableRequirement::RefSnapshotIdMatch {
                                r#ref: MAIN_BRANCH.to_string(),
                                snapshot_id: None,
                            });
                    }
                    id
                }
            };
            let mut retention = existing
                .map(|reference| reference.retention.clone())
                .unwrap_or_else(|| empty_retention(kind));
            match &mut retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: count,
                    max_snapshot_age_ms: age,
                    max_ref_age_ms: ref_age,
                } => {
                    *count = min_snapshots_to_keep.or(*count);
                    *age = max_snapshot_age_ms.or(*age);
                    *ref_age = max_ref_age_ms.or(*ref_age);
                }
                SnapshotRetention::Tag {
                    max_ref_age_ms: ref_age,
                } => {
                    *ref_age = max_ref_age_ms.or(*ref_age);
                }
            }
            let reference = SnapshotReference {
                snapshot_id,
                retention,
            };
            if existing == Some(&reference) {
                return Ok(prepared);
            }
            prepared.updates.push(TableUpdate::SetSnapshotRef {
                ref_name: name.clone(),
                reference,
            });
        }
    }
    prepared
        .requirements
        .push(TableRequirement::RefSnapshotIdMatch {
            r#ref: name.clone(),
            snapshot_id: existing.map(|reference| reference.snapshot_id),
        });
    prepared.changed = true;
    Ok(prepared)
}

fn validate_kind(name: &str, kind: Kind, reference: &SnapshotReference) -> Result<()> {
    if reference.is_branch() != (kind == Kind::Branch) {
        return plan_err!("Snapshot reference {name} has a different type");
    }
    Ok(())
}

fn empty_retention(kind: Kind) -> SnapshotRetention {
    match kind {
        Kind::Branch => SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        },
        Kind::Tag => SnapshotRetention::Tag {
            max_ref_age_ms: None,
        },
    }
}

fn empty_snapshot(
    metadata: &TableMetadata,
    prepared: &mut PreparedSnapshotOperation,
) -> Result<Snapshot> {
    let id = crate::utils::snapshot_id::generate_unique_snapshot_id(metadata);
    let path = format!("metadata/snap-{id}-{}.avro", uuid::Uuid::new_v4());
    let bytes = ManifestListWriter::new()
        .to_bytes(metadata.format_version)
        .map_err(DataFusionError::Execution)?;
    prepared.manifests.push((path.clone(), bytes));
    let mut summary = Summary::new(Operation::Append);
    for key in [
        "total-records",
        "total-files-size",
        "total-data-files",
        "total-delete-files",
        "total-position-deletes",
        "total-equality-deletes",
    ] {
        summary.additional_properties.insert(key.into(), "0".into());
    }
    Ok(Snapshot {
        snapshot_id: id,
        parent_snapshot_id: None,
        sequence_number: if metadata.format_version == FormatVersion::V1 {
            0
        } else {
            metadata.last_sequence_number + 1
        },
        timestamp_ms: crate::utils::timestamp::monotonic_timestamp_ms(),
        manifest_list: format!("{}/{path}", metadata.location.trim_end_matches('/')),
        manifests: None,
        summary,
        schema_id: Some(metadata.current_schema_id),
        first_row_id: (metadata.format_version >= FormatVersion::V3)
            .then_some(metadata.next_row_id.unwrap_or(0)),
        added_rows: (metadata.format_version >= FormatVersion::V3).then_some(0),
        key_id: None,
    })
}
