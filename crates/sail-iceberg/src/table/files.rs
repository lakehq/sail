use std::cmp::Reverse;
use std::collections::BinaryHeap;

use datafusion::common::{Result, not_impl_err};
use futures::{StreamExt, TryStreamExt, stream};

use crate::io::{StoreContext, load_manifest, load_manifest_list_with_version};
use crate::spec::{DataFile, ManifestFile, ManifestStatus, TableMetadata};

/// Manifest-list descriptors from one immutable table version.
pub(crate) async fn current_manifests(
    store: &StoreContext,
    metadata: &TableMetadata,
) -> Result<Vec<ManifestFile>> {
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(vec![]);
    };
    if snapshot.manifest_list().is_empty() {
        return not_impl_err!("Iceberg metadata scans require a snapshot manifest list");
    }
    let manifests =
        load_manifest_list_with_version(store, snapshot.manifest_list(), metadata.format_version)
            .await?;
    Ok(manifests.entries().to_vec())
}

pub(crate) async fn live_files(
    store: &StoreContext,
    descriptor: &ManifestFile,
) -> Result<Vec<DataFile>> {
    let manifest = load_manifest(store, &descriptor.manifest_path).await?;
    Ok(manifest
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.status,
                ManifestStatus::Added | ManifestStatus::Existing
            )
        })
        .map(|entry| {
            let mut file = entry.data_file.clone();
            // The manifest-list spec ID also applies to older file encodings.
            file.partition_spec_id = descriptor.partition_spec_id;
            file
        })
        .collect())
}

pub(crate) async fn collect_live_files<T: Send>(
    store: &StoreContext,
    metadata: &TableMetadata,
    concurrency: usize,
    select: impl Fn(DataFile) -> Result<Option<T>> + Sync,
) -> Result<Vec<T>> {
    let manifests = current_manifests(store, metadata).await?;
    let batches = stream::iter(manifests)
        .map(|manifest| {
            let select = &select;
            async move {
                live_files(store, &manifest)
                    .await?
                    .into_iter()
                    .filter_map(|file| select(file).transpose())
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
mod tests {
    use super::*;

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
