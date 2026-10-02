use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use datafusion::common::{Result, exec_err, not_impl_err};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::io::{StoreContext, load_manifest, load_manifest_list_with_version};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, TableMetadata,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum SnapshotScope {
    Current,
    All,
}

/// Selection before manifest I/O. All snapshots scan each immutable manifest only once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManifestScope {
    pub snapshots: SnapshotScope,
    pub content: Option<ManifestContentType>,
}

/// Entry selection is applied after inheritance, including for entries that will be discarded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EntrySelection {
    pub statuses: Vec<ManifestStatus>,
    pub content: Option<DataContentType>,
}

impl EntrySelection {
    pub(crate) fn live() -> Self {
        Self {
            statuses: vec![ManifestStatus::Added, ManifestStatus::Existing],
            content: None,
        }
    }

    pub(crate) fn matches(&self, entry: &ManifestEntry) -> bool {
        self.statuses.contains(&entry.status)
            && self
                .content
                .is_none_or(|content| content == entry.data_file.content)
    }
}

/// Manifest-list descriptors from one immutable table version.
pub(crate) async fn scan_manifests(
    store: &StoreContext,
    metadata: &TableMetadata,
    scope: &ManifestScope,
) -> Result<Vec<ManifestFile>> {
    let snapshots = match scope.snapshots {
        SnapshotScope::Current => metadata.current_snapshot().into_iter().collect::<Vec<_>>(),
        SnapshotScope::All => metadata.snapshots.iter().collect(),
    };
    let mut paths = HashSet::new();
    let mut manifests = vec![];
    for snapshot in snapshots {
        if snapshot.manifest_list().is_empty() {
            return not_impl_err!("Iceberg metadata scans require a snapshot manifest list");
        }
        let list = load_manifest_list_with_version(
            store,
            snapshot.manifest_list(),
            metadata.format_version,
        )
        .await?;
        for manifest in list.entries() {
            if scope
                .content
                .as_ref()
                .is_none_or(|content| &manifest.content == content)
                && paths.insert(manifest.manifest_path.clone())
            {
                manifests.push(manifest.clone());
            }
        }
    }
    Ok(manifests)
}

/// Read entries without losing status or lineage when constructing a live-file view.
pub(crate) async fn manifest_entries(
    store: &StoreContext,
    descriptor: &ManifestFile,
) -> Result<Vec<ManifestEntry>> {
    let manifest = load_manifest(store, &descriptor.manifest_path).await?;
    let (entries, metadata) = manifest.into_parts();
    let mut next_row_id = descriptor.first_row_id;
    entries
        .into_iter()
        .map(|entry| {
            let mut entry = Arc::unwrap_or_clone(entry);
            entry
                .snapshot_id
                .get_or_insert(descriptor.added_snapshot_id);
            if metadata.format_version == FormatVersion::V1 {
                entry.sequence_number.get_or_insert(0);
                entry.file_sequence_number.get_or_insert(0);
            } else if entry.status == ManifestStatus::Added {
                entry
                    .sequence_number
                    .get_or_insert(descriptor.sequence_number);
                entry
                    .file_sequence_number
                    .get_or_insert(descriptor.sequence_number);
            } else if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
                return exec_err!(
                    "Iceberg existing/deleted manifest entries require sequence numbers"
                );
            }
            let file = &mut entry.data_file;
            file.partition_spec_id = descriptor.partition_spec_id;
            if file.content == DataContentType::Data
                && file.first_row_id.is_none()
                && let Some(first_row_id) = next_row_id
            {
                file.first_row_id = Some(first_row_id);
                next_row_id = Some(
                    i64::try_from(file.record_count)
                        .ok()
                        .and_then(|count| first_row_id.checked_add(count))
                        .filter(|id| *id >= 0)
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!("Iceberg row ID overflow")
                        })?,
                );
            }
            Ok(entry)
        })
        .collect()
}

pub(crate) async fn collect_live_files<T: Send>(
    store: &StoreContext,
    metadata: &TableMetadata,
    concurrency: usize,
    select: impl Fn(DataFile) -> Result<Option<T>> + Sync,
) -> Result<Vec<T>> {
    let manifests = scan_manifests(
        store,
        metadata,
        &ManifestScope {
            snapshots: SnapshotScope::Current,
            content: None,
        },
    )
    .await?;
    let selection = EntrySelection::live();
    let batches = stream::iter(manifests)
        .map(|manifest| {
            let select = &select;
            let selection = &selection;
            async move {
                manifest_entries(store, &manifest)
                    .await?
                    .into_iter()
                    .filter(|entry| selection.matches(entry))
                    .filter_map(|entry| select(entry.data_file).transpose())
                    .collect::<Result<Vec<_>>>()
            }
        })
        .buffered(concurrency.max(1));
    batches
        .try_fold(Vec::new(), |mut files, batch| async move {
            files.extend(batch);
            Ok(files)
        })
        .await
}

/// Balance immutable work descriptions without moving the file contents through the driver.
pub(crate) fn balance_by_size<T>(
    mut items: Vec<T>,
    partitions: usize,
    size: impl Fn(&T) -> u64,
) -> Vec<Vec<T>> {
    let count = partitions.max(1).min(items.len().max(1));
    items.sort_by_key(|item| Reverse(size(item)));
    let mut groups: Vec<Vec<T>> = (0..count).map(|_| vec![]).collect();
    let mut loads: BinaryHeap<_> = (0..count).map(|index| Reverse((0u64, index))).collect();
    for item in items {
        if let Some(Reverse((bytes, index))) = loads.pop() {
            loads.push(Reverse((bytes.saturating_add(size(&item).max(1)), index)));
            groups[index].push(item);
        }
    }
    groups
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::spec::{ManifestMetadata, ManifestWriter, PartitionSpec, Schema};

    fn entry(
        status: ManifestStatus,
        sequence: Option<i64>,
        first_row_id: Option<i64>,
    ) -> ManifestEntry {
        let file = serde_json::from_value(serde_json::json!({
            "content": "DATA", "file_path": "data/file.parquet", "file_format": "PARQUET",
            "partition": [], "record_count": 10, "file_size_in_bytes": 100,
            "partition_spec_id": 0, "first_row_id": first_row_id
        }))
        .expect("file");
        ManifestEntry::new(status, None, sequence, sequence, file)
    }

    async fn read_entries(
        version: FormatVersion,
        entries: Vec<ManifestEntry>,
        first_row_id: Option<i64>,
    ) -> Result<Vec<ManifestEntry>> {
        use bytes::Bytes;
        use object_store::ObjectStoreExt;
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use url::Url;

        let store = StoreContext::new(
            Arc::new(InMemory::new()),
            &Url::parse("memory://bucket/table").expect("URL"),
        )?;
        let mut writer = ManifestWriter::new(
            Some(20),
            None,
            ManifestMetadata::new(
                Arc::new(Schema::builder().build().expect("schema")),
                0,
                PartitionSpec::unpartitioned_spec(),
                version,
                ManifestContentType::Data,
            ),
        );
        for mut entry in entries {
            if version == FormatVersion::V1 {
                entry.snapshot_id = Some(10);
            }
            writer.add_entry(entry);
        }
        let mut descriptor = writer
            .clone()
            .into_manifest_file("metadata/manifest.avro".into(), 7, 20)
            .expect("descriptor");
        descriptor.partition_spec_id = 3;
        descriptor.first_row_id = first_row_id;
        store
            .prefixed
            .put(
                &Path::from("metadata/manifest.avro"),
                Bytes::from(writer.to_avro_bytes_v2().expect("manifest bytes")).into(),
            )
            .await?;
        manifest_entries(&store, &descriptor).await
    }

    #[tokio::test]
    async fn entries_retain_deleted_status_and_inherit_only_unassigned_lineage() -> Result<()> {
        let entries = read_entries(
            FormatVersion::V2,
            vec![
                entry(ManifestStatus::Added, None, None),
                entry(ManifestStatus::Existing, Some(2), None),
                entry(ManifestStatus::Deleted, Some(3), None),
            ],
            None,
        )
        .await?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| (
                    entry.status,
                    entry.snapshot_id,
                    entry.sequence_number,
                    entry.file_sequence_number,
                    entry.data_file.partition_spec_id
                ))
                .collect::<Vec<_>>(),
            vec![
                (ManifestStatus::Added, Some(20), Some(7), Some(7), 3),
                (ManifestStatus::Existing, Some(20), Some(2), Some(2), 3),
                (ManifestStatus::Deleted, Some(20), Some(3), Some(3), 3),
            ]
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| EntrySelection::live().matches(entry))
                .count(),
            2
        );
        for status in [ManifestStatus::Existing, ManifestStatus::Deleted] {
            assert!(
                read_entries(FormatVersion::V2, vec![entry(status, None, None)], None)
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn old_manifests_use_zero_sequence_and_row_ids_advance_before_filtering() -> Result<()> {
        let entries = read_entries(
            FormatVersion::V1,
            vec![entry(ManifestStatus::Existing, None, None)],
            None,
        )
        .await?;
        assert_eq!(
            (entries[0].sequence_number, entries[0].file_sequence_number),
            (Some(0), Some(0))
        );
        let entries = read_entries(
            FormatVersion::V3,
            vec![
                entry(ManifestStatus::Deleted, Some(1), None),
                entry(ManifestStatus::Existing, Some(2), Some(500)),
                entry(ManifestStatus::Added, None, None),
            ],
            Some(100),
        )
        .await?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.data_file.first_row_id)
                .collect::<Vec<_>>(),
            vec![Some(100), Some(500), Some(110)]
        );
        assert!(
            read_entries(
                FormatVersion::V3,
                vec![entry(ManifestStatus::Added, None, None)],
                Some(i64::MAX)
            )
            .await
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn balances_skewed_work_and_preserves_empty_partition() {
        let groups = balance_by_size(vec![100, 40, 30, 20, 10], 2, |value| *value);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.iter().sum::<u64>())
                .collect::<Vec<_>>(),
            vec![100, 100]
        );
        assert_eq!(
            balance_by_size(Vec::<u64>::new(), 4, |value| *value),
            vec![Vec::<u64>::new()]
        );
        assert_eq!(
            balance_by_size(vec![0, 0, 0], 3, |value| *value),
            vec![vec![0]; 3]
        );
    }
}
