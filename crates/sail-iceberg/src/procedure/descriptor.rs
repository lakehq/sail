use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureAccess, LakeProcedureDataType, LakeProcedureField,
    LakeProcedureParameter, LakeProcedureRetryPolicy, LakeProcedureTarget,
};

/// Procedures in the Iceberg catalog's `system` namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IcebergProcedureType {
    /// Moves the current table state back to an ancestor snapshot ID.
    RollbackToSnapshot,
    /// Uses snapshot-log history to restore the state that was current at a timestamp.
    RollbackToTimestamp,
    /// Sets the current snapshot from an arbitrary snapshot ID or reference without requiring it to
    /// be an ancestor.
    SetCurrentSnapshot,
    /// Applies an append or dynamic-overwrite snapshot as a new current snapshot while
    /// retaining the source snapshot.
    CherrypickSnapshot,
    /// Reorganizes data files, commonly compacting small files or sorting table data.
    RewriteDataFiles,
    /// Rewrites and clusters manifests to reduce metadata and scan-planning cost.
    RewriteManifests,
    /// Finds and optionally deletes files that are not referenced by table metadata.
    RemoveOrphanFiles,
    /// Expires snapshots and deletes files that are no longer required by any retained
    /// snapshot or reference.
    ExpireSnapshots,
    /// Replaces a source table with an Iceberg table that references its existing data files.
    Migrate,
    /// Creates a lightweight Iceberg table that initially shares another table's data files.
    Snapshot,
    /// Imports existing data files into an Iceberg table without moving them.
    AddFiles,
    /// Returns the parent lineage of a selected snapshot, defaulting to the current snapshot.
    AncestorsOf,
    /// Creates a catalog entry for an existing Iceberg metadata JSON file.
    RegisterTable,
    /// Publishes the uniquely identified staged WAP snapshot into the current table state.
    PublishChanges,
    /// Creates a view exposing row changes between selected snapshots or timestamps.
    CreateChangelogView,
    /// Compacts positional-delete files and removes dangling positional-delete records.
    RewritePositionDeleteFiles,
    /// Advances one branch head to the descendant snapshot referenced by another branch.
    FastForward,
    /// Computes table-level NDV statistics and writes a registered statistics file.
    ComputeTableStats,
    /// Computes and registers partition statistics, incrementally when prior statistics exist.
    ComputePartitionStats,
    /// Stages metadata with rewritten path prefixes and a copy manifest; it does not copy the
    /// table's files.
    RewriteTablePath,
}

impl IcebergProcedureType {
    /// All Iceberg procedure types.
    pub(super) const ALL: &'static [Self] = &[
        Self::RollbackToSnapshot,
        Self::RollbackToTimestamp,
        Self::SetCurrentSnapshot,
        Self::CherrypickSnapshot,
        Self::RewriteDataFiles,
        Self::RewriteManifests,
        Self::RemoveOrphanFiles,
        Self::ExpireSnapshots,
        Self::Migrate,
        Self::Snapshot,
        Self::AddFiles,
        Self::AncestorsOf,
        Self::RegisterTable,
        Self::PublishChanges,
        Self::CreateChangelogView,
        Self::RewritePositionDeleteFiles,
        Self::FastForward,
        Self::ComputeTableStats,
        Self::ComputePartitionStats,
        Self::RewriteTablePath,
    ];

    pub(super) fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|procedure_type| procedure_type.name().eq_ignore_ascii_case(name))
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::RollbackToSnapshot => "rollback_to_snapshot",
            Self::RollbackToTimestamp => "rollback_to_timestamp",
            Self::SetCurrentSnapshot => "set_current_snapshot",
            Self::CherrypickSnapshot => "cherrypick_snapshot",
            Self::RewriteDataFiles => "rewrite_data_files",
            Self::RewriteManifests => "rewrite_manifests",
            Self::RemoveOrphanFiles => "remove_orphan_files",
            Self::ExpireSnapshots => "expire_snapshots",
            Self::Migrate => "migrate",
            Self::Snapshot => "snapshot",
            Self::AddFiles => "add_files",
            Self::AncestorsOf => "ancestors_of",
            Self::RegisterTable => "register_table",
            Self::PublishChanges => "publish_changes",
            Self::CreateChangelogView => "create_changelog_view",
            Self::RewritePositionDeleteFiles => "rewrite_position_delete_files",
            Self::FastForward => "fast_forward",
            Self::ComputeTableStats => "compute_table_stats",
            Self::ComputePartitionStats => "compute_partition_stats",
            Self::RewriteTablePath => "rewrite_table_path",
        }
    }

    pub(super) fn descriptor(self) -> Option<LakeProcedure> {
        let string = LakeProcedureDataType::Utf8;
        let long = LakeProcedureDataType::Int64;
        let timestamp = LakeProcedureDataType::TimestampMicros;
        let (parameters, output, access) = match self {
            Self::AncestorsOf => (
                vec![
                    LakeProcedureParameter::required("table", string),
                    LakeProcedureParameter::optional("snapshot_id", long),
                ],
                vec![
                    LakeProcedureField::new("snapshot_id", long, true),
                    LakeProcedureField::new("timestamp", long, true),
                ],
                LakeProcedureAccess::MetadataRead,
            ),
            Self::RollbackToSnapshot => (
                vec![
                    LakeProcedureParameter::required("table", string),
                    LakeProcedureParameter::required("snapshot_id", long),
                ],
                snapshot_change_output(false),
                LakeProcedureAccess::MetadataCommit,
            ),
            Self::RollbackToTimestamp => (
                vec![
                    LakeProcedureParameter::required("table", string),
                    LakeProcedureParameter::required("timestamp", timestamp),
                ],
                snapshot_change_output(false),
                LakeProcedureAccess::MetadataCommit,
            ),
            Self::SetCurrentSnapshot => (
                vec![
                    LakeProcedureParameter::required("table", string),
                    LakeProcedureParameter::optional("snapshot_id", long),
                    LakeProcedureParameter::optional("ref", string),
                ],
                snapshot_change_output(true),
                LakeProcedureAccess::MetadataCommit,
            ),
            Self::FastForward => (
                vec![
                    LakeProcedureParameter::required("table", string),
                    LakeProcedureParameter::required("branch", string),
                    LakeProcedureParameter::required("to", string),
                ],
                vec![
                    LakeProcedureField::new("branch_updated", string, false),
                    LakeProcedureField::new("previous_ref", long, true),
                    LakeProcedureField::new("updated_ref", long, false),
                ],
                LakeProcedureAccess::MetadataCommit,
            ),
            _ => return None,
        };
        Some(LakeProcedure {
            name: self.name().to_string(),
            parameters,
            output,
            access,
            target: LakeProcedureTarget::table("table"),
            retry_policy: match access {
                LakeProcedureAccess::MetadataRead => LakeProcedureRetryPolicy::Safe,
                LakeProcedureAccess::MetadataCommit => LakeProcedureRetryPolicy::Forbidden,
            },
        })
    }

    pub(super) fn unsupported_reason(self) -> String {
        format!(
            "Iceberg system procedure '{}' is recognized but not implemented",
            self.name()
        )
    }
}

fn snapshot_change_output(previous_nullable: bool) -> Vec<LakeProcedureField> {
    vec![
        LakeProcedureField::new(
            "previous_snapshot_id",
            LakeProcedureDataType::Int64,
            previous_nullable,
        ),
        LakeProcedureField::new("current_snapshot_id", LakeProcedureDataType::Int64, false),
    ]
}
