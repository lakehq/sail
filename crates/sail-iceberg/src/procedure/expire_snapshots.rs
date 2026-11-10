//! Snapshot retention and physical reachability cleanup. No candidate is a staged artifact.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, plan_err};
use futures::{StreamExt, stream};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_common_datafusion::lakeprocedure::{LakeProcedureInvocation, LakeProcedureValue};
use url::Url;

use super::arguments::{optional_bool, optional_i32};
use crate::io::StoreContext;
use crate::spec::snapshots::{SnapshotReference, SnapshotRetention};
use crate::spec::{
    DataContentType, Manifest, ManifestList, ManifestStatus, TableMetadata, TableUpdate,
};

// The coordinator holds path inventories, not file contents. Bound both inventory and fan-out.
#[cfg(test)]
mod tests;

const MAX_PATHS: usize = 1_000_000;
const MAX_DELETE_CONCURRENCY: usize = 256;
const DEFAULT_MAX_AGE_MS: i64 = 5 * 24 * 60 * 60 * 1000;

pub(crate) struct ExpireSnapshots {
    now_ms: i64,
    older_than_ms: Option<i64>,
    retain_last: Option<i32>,
    snapshot_ids: HashSet<i64>,
    clean_metadata: bool,
    concurrency: usize,
}

pub(crate) struct Expiration {
    candidates: Inventory,
    concurrency: usize,
}

#[derive(Default)]
struct Inventory {
    paths: HashMap<Path, usize>,
    specs: HashSet<i32>,
}

pub(crate) struct PreparedExpiration {
    pub(crate) updates: Vec<TableUpdate>,
    pub(crate) expiration: Expiration,
}

impl ExpireSnapshots {
    pub(crate) fn from_invocation(invocation: &LakeProcedureInvocation) -> Result<Self> {
        let older_than_ms = match invocation.argument("older_than") {
            Some(LakeProcedureValue::TimestampMicros(value)) => Some(value.div_euclid(1000)),
            Some(LakeProcedureValue::Null) | None => None,
            _ => return plan_err!("older_than must be a timestamp"),
        };
        let retain_last = optional_i32(invocation, "retain_last")?;
        if retain_last.is_some_and(|value| value < 1) {
            return plan_err!("Number of snapshots to retain must be at least 1");
        }
        let concurrency = optional_i32(invocation, "max_concurrent_deletes")?.unwrap_or(1);
        if concurrency < 1 || concurrency as usize > MAX_DELETE_CONCURRENCY {
            return plan_err!(
                "max_concurrent_deletes must be between 1 and {MAX_DELETE_CONCURRENCY}"
            );
        }
        // Both modes iterate the bounded coordinator inventory without collecting file contents.
        let _stream_results = optional_bool(invocation, "stream_results")?;
        let snapshot_ids = match invocation.argument("snapshot_ids") {
            Some(LakeProcedureValue::Int64Array(values)) => {
                values.iter().flatten().copied().collect()
            }
            Some(LakeProcedureValue::Null) | None => HashSet::new(),
            _ => return plan_err!("snapshot_ids must be an array of longs"),
        };
        Ok(Self {
            now_ms: chrono::Utc::now().timestamp_millis(),
            older_than_ms,
            retain_last,
            snapshot_ids,
            clean_metadata: optional_bool(invocation, "clean_expired_metadata")?.unwrap_or(false),
            concurrency: concurrency as usize,
        })
    }

    // [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/RemoveSnapshots.java#L213-L365
    fn select(&self, metadata: &TableMetadata) -> Result<(HashSet<i64>, Vec<String>)> {
        if metadata
            .properties
            .get("gc.enabled")
            .is_some_and(|value| !value.eq_ignore_ascii_case("true"))
        {
            return plan_err!(
                "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
            );
        }
        let property = |name: &str, default: i64| -> Result<i64> {
            metadata.properties.get(name).map_or(Ok(default), |value| {
                value.parse::<i64>().map_err(|_| {
                    DataFusionError::Plan(format!("Invalid Iceberg property {name}: {value}"))
                })
            })
        };
        let cutoff = self
            .older_than_ms
            .unwrap_or(self.now_ms.saturating_sub(property(
                "history.expire.max-snapshot-age-ms",
                DEFAULT_MAX_AGE_MS,
            )?));
        let min_count = i64::from(
            self.retain_last.unwrap_or(
                i32::try_from(property("history.expire.min-snapshots-to-keep", 1)?)
                    .map_err(|_| DataFusionError::Plan("Invalid minimum snapshot count".into()))?,
            ),
        );
        let max_ref_age = property("history.expire.max-ref-age-ms", i64::MAX)?;
        if min_count < 1 {
            return plan_err!("Number of snapshots to retain must be at least 1");
        }
        let snapshots: HashMap<_, _> = metadata
            .snapshots
            .iter()
            .map(|s| (s.snapshot_id, s))
            .collect();
        let mut refs = metadata.refs.clone();
        if let Some(id) = metadata.current_snapshot_id.filter(|id| *id != -1) {
            refs.entry("main".into()).or_insert(SnapshotReference {
                snapshot_id: id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            });
        }
        let mut removed_refs = vec![];
        refs.retain(|name, reference| {
            let age = match reference.retention {
                SnapshotRetention::Branch { max_ref_age_ms, .. }
                | SnapshotRetention::Tag { max_ref_age_ms } => {
                    max_ref_age_ms.unwrap_or(max_ref_age)
                }
            };
            let retain = name == "main"
                || snapshots
                    .get(&reference.snapshot_id)
                    .is_some_and(|s| self.now_ms.saturating_sub(s.timestamp_ms) <= age);
            if !retain {
                removed_refs.push(name.clone());
            }
            retain
        });
        let mut retained: HashSet<_> = refs.values().map(|r| r.snapshot_id).collect();
        // Protect current even if malformed metadata has an inconsistent main reference.
        retained.extend(metadata.current_snapshot_id.filter(|id| *id != -1));
        if let Some(id) = self.snapshot_ids.intersection(&retained).next() {
            return plan_err!(
                "Cannot expire {id}. Still referenced by refs or the current snapshot"
            );
        }
        let mut referenced = retained.clone();
        for reference in refs.values() {
            if let SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                ..
            } = reference.retention
            {
                let min = i64::from(min_snapshots_to_keep.unwrap_or(min_count as i32));
                let cutoff =
                    max_snapshot_age_ms.map_or(cutoff, |age| self.now_ms.saturating_sub(age));
                let mut id = Some(reference.snapshot_id);
                let mut seen = HashSet::new();
                let mut retaining = true;
                while let Some(snapshot) = id.and_then(|id| snapshots.get(&id)) {
                    if !seen.insert(snapshot.snapshot_id) {
                        return plan_err!("Cyclic Iceberg snapshot ancestry");
                    }
                    referenced.insert(snapshot.snapshot_id);
                    if retaining && (seen.len() as i64 <= min || snapshot.timestamp_ms >= cutoff) {
                        retained.insert(snapshot.snapshot_id);
                    } else {
                        retaining = false;
                    }
                    id = snapshot.parent_snapshot_id;
                }
            }
        }
        for snapshot in &metadata.snapshots {
            if !referenced.contains(&snapshot.snapshot_id) && snapshot.timestamp_ms >= cutoff {
                retained.insert(snapshot.snapshot_id);
            }
        }
        let expired = metadata
            .snapshots
            .iter()
            .filter(|s| {
                !retained.contains(&s.snapshot_id) || self.snapshot_ids.contains(&s.snapshot_id)
            })
            .map(|s| s.snapshot_id)
            .collect();
        Ok((expired, removed_refs))
    }

    pub(crate) async fn prepare(
        &self,
        store: &StoreContext,
        url: &Url,
        metadata: &TableMetadata,
    ) -> Result<PreparedExpiration> {
        let (expired, removed_refs) = self.select(metadata)?;
        // Validate every snapshot, including retained/staged branches, before publication.
        let all = inventory(store, url, metadata, None).await?;
        let mut candidates = inventory(store, url, metadata, Some(&expired)).await?;
        let retained_ids = metadata
            .snapshots
            .iter()
            .map(|s| s.snapshot_id)
            .filter(|id| !expired.contains(id))
            .collect();
        let retained = inventory(store, url, metadata, Some(&retained_ids)).await?;
        candidates
            .paths
            .retain(|path, _| !retained.paths.contains_key(path));
        drop(all);
        let mut updates: Vec<_> = removed_refs
            .into_iter()
            .map(|ref_name| TableUpdate::RemoveSnapshotRef { ref_name })
            .collect();
        if !expired.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: expired.iter().copied().collect(),
            });
        }
        if self.clean_metadata {
            let mut schemas: HashSet<_> = metadata
                .snapshots
                .iter()
                .filter(|s| !expired.contains(&s.snapshot_id))
                .filter_map(|s| s.schema_id)
                .collect();
            schemas.insert(metadata.current_schema_id);
            // Old snapshots without schema IDs may use any historical schema.
            if !metadata
                .snapshots
                .iter()
                .any(|s| !expired.contains(&s.snapshot_id) && s.schema_id.is_none())
            {
                let schema_ids: Vec<_> = metadata
                    .schemas
                    .iter()
                    .map(|s| s.schema_id())
                    .filter(|id| !schemas.contains(id))
                    .collect();
                if !schema_ids.is_empty() {
                    updates.push(TableUpdate::RemoveSchemas { schema_ids });
                }
            }
            let spec_ids: Vec<_> = metadata
                .partition_specs
                .iter()
                .map(|s| s.spec_id())
                .filter(|id| *id != metadata.default_spec_id && !retained.specs.contains(id))
                .collect();
            if !spec_ids.is_empty() {
                updates.push(TableUpdate::RemovePartitionSpecs { spec_ids });
            }
        }
        Ok(PreparedExpiration {
            updates,
            expiration: Expiration {
                candidates,
                concurrency: self.concurrency,
            },
        })
    }
}

pub(crate) fn checked_path(store: &StoreContext, table: &Url, raw: &str) -> Result<Path> {
    // Drive paths are not relative object keys, even when validating on Unix. Reject
    // until their identity with file URIs is supported on every platform.
    if raw.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && raw.as_bytes().get(1) == Some(&b':')
    {
        return plan_err!("expire_snapshots does not support Windows drive paths; use file URIs");
    }
    if let Some(url) = crate::utils::parse_absolute_url(raw) {
        if url.scheme() != table.scheme()
            || url.host_str() != table.host_str()
            || url.port() != table.port()
            || if matches!(table.scheme(), "abfs" | "abfss") {
                url.username() != table.username()
            } else {
                !url.username().is_empty()
            }
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return plan_err!(
                "expire_snapshots cannot access a path outside the table's object store: {raw}"
            );
        }
        return crate::utils::url_to_object_path(&url);
    } else if raw.contains("://") || raw.starts_with("//") {
        return plan_err!("Invalid Iceberg expiration path: {raw}");
    }
    if raw.is_empty() {
        return plan_err!("Empty Iceberg expiration path");
    }
    // Path::from/join encode '%' again. Preserve literal partition escapes exactly,
    // as url_to_object_path does; use this same key for reads, reachability and deletion.
    let key = if raw.starts_with('/') {
        raw.trim_start_matches('/').to_string()
    } else {
        format!("{}/{raw}", store.prefix_path)
    };
    Path::parse(key).map_err(|error| DataFusionError::External(Box::new(error)))
}

impl Inventory {
    fn insert(&mut self, store: &StoreContext, url: &Url, raw: &str, kind: usize) -> Result<Path> {
        let path = checked_path(store, url, raw)?;
        if self.paths.len() >= MAX_PATHS && !self.paths.contains_key(&path) {
            return plan_err!(
                "expire_snapshots exceeds the coordinator inventory limit of {MAX_PATHS} physical paths"
            );
        }
        self.paths.entry(path.clone()).or_insert(kind);
        Ok(path)
    }
}

async fn inventory(
    store: &StoreContext,
    url: &Url,
    metadata: &TableMetadata,
    ids: Option<&HashSet<i64>>,
) -> Result<Inventory> {
    if !metadata.encryption_keys.is_empty() {
        return plan_err!("expire_snapshots does not support encrypted tables");
    }
    let mut result = Inventory::default();
    let mut manifests = HashSet::new();
    for snapshot in metadata
        .snapshots
        .iter()
        .filter(|s| ids.is_none_or(|ids| ids.contains(&s.snapshot_id)))
    {
        if snapshot.key_id.is_some()
            || snapshot.manifest_list.is_empty()
            || snapshot.manifests.is_some()
        {
            return plan_err!(
                "expire_snapshots requires unencrypted manifest lists; legacy inline manifests are unsupported"
            );
        }
        let path = result.insert(store, url, &snapshot.manifest_list, 4)?;
        let bytes = store.base.get(&path).await?.bytes().await?;
        let list = ManifestList::parse_with_version(&bytes, metadata.format_version)
            .map_err(DataFusionError::Execution)?;
        for descriptor in list.entries() {
            if descriptor.key_metadata.is_some() {
                return plan_err!("expire_snapshots does not support encrypted manifests");
            }
            result.specs.insert(descriptor.partition_spec_id);
            let path = result.insert(store, url, &descriptor.manifest_path, 3)?;
            if !manifests.insert(path.clone()) {
                continue;
            }
            let bytes = store.base.get(&path).await?.bytes().await?;
            let manifest = Manifest::parse_avro(&bytes).map_err(DataFusionError::Execution)?;
            for entry in manifest.entries() {
                if entry.data_file.key_metadata.is_some() {
                    return plan_err!("expire_snapshots does not support encrypted files");
                }
                if matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                ) {
                    let kind = match entry.data_file.content {
                        DataContentType::Data => 0,
                        DataContentType::PositionDeletes => 1,
                        DataContentType::EqualityDeletes => 2,
                    };
                    // Deletion vectors share physical Puffin containers: offsets are not identities.
                    result.insert(store, url, &entry.data_file.file_path, kind)?;
                }
            }
        }
    }
    for file in metadata
        .statistics
        .iter()
        .filter(|s| ids.is_none_or(|ids| ids.contains(&s.snapshot_id)))
    {
        if file.key_metadata.is_some() {
            return plan_err!("expire_snapshots does not support encrypted statistics");
        }
        result.insert(store, url, &file.statistics_path, 5)?;
    }
    for file in metadata
        .partition_statistics
        .iter()
        .filter(|s| ids.is_none_or(|ids| ids.contains(&s.snapshot_id)))
    {
        result.insert(store, url, &file.statistics_path, 5)?;
    }
    Ok(result)
}

pub(crate) fn output(schema: SchemaRef, counts: [i64; 6]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema,
        counts
            .into_iter()
            .map(|count| Arc::new(Int64Array::from(vec![count])) as _)
            .collect(),
    )?)
}

impl Expiration {
    /// Called only after confirmed publication, with freshly loaded authoritative metadata.
    pub(crate) async fn finish(
        self,
        store: &StoreContext,
        url: &Url,
        current: Result<TableMetadata>,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let reachable = match current {
            Ok(metadata) => inventory(store, url, &metadata, None).await,
            Err(error) => Err(error),
        };
        let reachable = match reachable {
            Ok(inventory) => inventory,
            Err(error) => {
                log::warn!(
                    "Snapshot expiration committed, but refresh failed; preserving all candidate files: {error}"
                );
                return output(schema, [0; 6]);
            }
        };
        let mut counts = [0; 6];
        let mut deletes = stream::iter(
            self.candidates
                .paths
                .into_iter()
                .filter(|(path, _)| !reachable.paths.contains_key(path)),
        )
        .map(|(path, kind)| async move {
            for attempt in 0..3 {
                match store.base.delete(&path).await {
                    Ok(()) => return Some(kind),
                    Err(object_store::Error::NotFound { .. }) => return Some(kind),
                    Err(error) if attempt == 2 => {
                        log::warn!("Snapshot expiration could not delete {path}: {error}")
                    }
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10 * (1 << attempt)))
                            .await
                    }
                }
            }
            None
        })
        .buffer_unordered(self.concurrency);
        while let Some(kind) = deletes.next().await {
            if let Some(kind) = kind {
                counts[kind] += 1;
            }
        }
        output(schema, counts)
    }
}
