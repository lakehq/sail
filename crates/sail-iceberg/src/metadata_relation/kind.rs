use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::common::Result;

use super::{history, metadata_log_entries, refs, snapshots};
use crate::table::Table;

/// Metadata relations owned by an Iceberg table rather than by its catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergMetadataRelationType {
    /// Manifest entries in the current snapshot, including their add/delete status and sequence
    /// lineage.
    Entries,
    /// Live data and delete content files referenced by the current snapshot.
    Files,
    /// The data-file subset of [`Self::Files`].
    DataFiles,
    /// The position- and equality-delete-file subset of [`Self::Files`].
    DeleteFiles,
    /// The snapshot log of states that were current, annotated with current ancestry.
    History,
    /// The retained metadata JSON log, followed by the current metadata JSON file.
    MetadataLogEntries,
    /// All valid snapshots retained in the current table metadata.
    Snapshots,
    /// Named snapshot branches and tags together with their retention policies.
    Refs,
    /// Manifest-list entries referenced by the current snapshot.
    Manifests,
    /// Current-snapshot content-file metrics aggregated by partition spec and partition tuple.
    Partitions,
    /// Data files reachable across all retained snapshots; a file may appear more than once.
    AllDataFiles,
    /// Delete files reachable across all retained snapshots; a file may appear more than once.
    AllDeleteFiles,
    /// Data and delete files reachable across all retained snapshots.
    AllFiles,
    /// Manifest-list entries across all retained snapshots, including the referencing snapshot.
    AllManifests,
    /// Manifest entries across all retained snapshots.
    AllEntries,
    /// Individual positional-delete records applicable to the current snapshot, not merely
    /// delete-file descriptors.
    PositionDeletes,
}

impl IcebergMetadataRelationType {
    /// All metadata relation types.
    pub(crate) const ALL: &'static [Self] = &[
        Self::Entries,
        Self::Files,
        Self::DataFiles,
        Self::DeleteFiles,
        Self::History,
        Self::MetadataLogEntries,
        Self::Snapshots,
        Self::Refs,
        Self::Manifests,
        Self::Partitions,
        Self::AllDataFiles,
        Self::AllDeleteFiles,
        Self::AllFiles,
        Self::AllManifests,
        Self::AllEntries,
        Self::PositionDeletes,
    ];

    pub(crate) fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|relation_type| relation_type.name().eq_ignore_ascii_case(name))
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Entries => "entries",
            Self::Files => "files",
            Self::DataFiles => "data_files",
            Self::DeleteFiles => "delete_files",
            Self::History => "history",
            Self::MetadataLogEntries => "metadata_log_entries",
            Self::Snapshots => "snapshots",
            Self::Refs => "refs",
            Self::Manifests => "manifests",
            Self::Partitions => "partitions",
            Self::AllDataFiles => "all_data_files",
            Self::AllDeleteFiles => "all_delete_files",
            Self::AllFiles => "all_files",
            Self::AllManifests => "all_manifests",
            Self::AllEntries => "all_entries",
            Self::PositionDeletes => "position_deletes",
        }
    }

    pub(crate) fn is_supported(self) -> bool {
        matches!(
            self,
            Self::History | Self::MetadataLogEntries | Self::Snapshots | Self::Refs
        )
    }

    pub(crate) fn unsupported_reason(self) -> String {
        format!(
            "Iceberg metadata table '{}' is recognized but not implemented",
            self.name()
        )
    }

    pub(super) fn schema(self) -> Arc<ArrowSchema> {
        match self {
            Self::History => history::schema(),
            Self::MetadataLogEntries => metadata_log_entries::schema(),
            Self::Snapshots => snapshots::schema(),
            Self::Refs => refs::schema(),
            unsupported => Arc::new(ArrowSchema::new_with_metadata(
                Vec::<Field>::new(),
                HashMap::from([("unsupported".to_string(), unsupported.name().to_string())]),
            )),
        }
    }

    pub(super) fn record_batch(self, table: &Table) -> Result<RecordBatch> {
        match self {
            Self::History => history::batch(table.metadata()),
            Self::MetadataLogEntries => {
                metadata_log_entries::batch(table.metadata(), table.metadata_location())
            }
            Self::Refs => refs::batch(table.metadata()),
            Self::Snapshots => snapshots::batch(table.metadata()),
            unsupported => Err(datafusion::common::DataFusionError::NotImplemented(
                unsupported.unsupported_reason(),
            )),
        }
    }
}
