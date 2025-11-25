// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::datasource::TableProvider;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::options::TableIcebergOptions;
use crate::spec::{PartitionSpec, Schema, Snapshot, TableMetadata};

enum SelectionKind {
    SnapshotId,
    Ref,
    Timestamp,
    Current,
}

/// Create an Iceberg table provider for reading
pub async fn create_iceberg_provider(
    ctx: &dyn Session,
    table_url: Url,
    options: TableIcebergOptions,
) -> Result<Arc<dyn TableProvider>> {
    let (schema, snapshot, partition_specs) =
        load_table_metadata_with_options(ctx, &table_url, options).await?;

    let provider =
        IcebergTableProvider::new(table_url.to_string(), schema, snapshot, partition_specs)?;
    Ok(Arc::new(provider))
}

/// Parse a table URL from a path string
pub async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
    if paths.len() != 1 {
        return plan_err!(
            "Iceberg table requires exactly one path, got {}",
            paths.len()
        );
    }

    let path = &paths[0];
    let mut table_url = Url::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))?;

    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }

    // Validate that we can access the object store
    let _object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(table_url)
}

/// Load Iceberg table metadata from the table location
#[allow(dead_code)]
pub(crate) async fn load_table_metadata(
    ctx: &dyn Session,
    table_url: &Url,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!("Loading table metadata from: {}", table_url);
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = find_latest_metadata_file(&object_store, table_url).await?;
    log::trace!("Found metadata file: {}", metadata_location);

    let metadata_path = object_store::path::Path::from(metadata_location.as_str());
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log::trace!("Read {} bytes from metadata file", metadata_data.len());

    let table_metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
        log::trace!("Failed to parse table metadata: {:?}", e);
        DataFusionError::External(Box::new(e))
    })?;

    log::trace!("Loaded metadata file: {}", &metadata_location);
    log::trace!(
        "  current_snapshot_id: {:?}",
        &table_metadata.current_snapshot_id
    );
    log::trace!("  refs: {:?}", &table_metadata.refs);
    log::trace!("  snapshots count: {}", &table_metadata.snapshots.len());

    // Get the current schema
    let schema = table_metadata
        .current_schema()
        .ok_or_else(|| {
            DataFusionError::Plan("No current schema found in table metadata".to_string())
        })?
        .clone();

    // Get the current snapshot
    let snapshot = table_metadata
        .current_snapshot()
        .ok_or_else(|| {
            DataFusionError::Plan("No current snapshot found in table metadata".to_string())
        })?
        .clone();

    log::trace!(
        "load_table_metadata: loaded snapshot id={} manifest_list={}",
        snapshot.snapshot_id(),
        snapshot.manifest_list()
    );

    let partition_specs = table_metadata.partition_specs.clone();
    Ok((schema, snapshot, partition_specs))
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: TableIcebergOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = find_latest_metadata_file(&object_store, table_url).await?;
    let metadata_path = object_store::path::Path::from(metadata_location.as_str());
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table_metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
        log::trace!("Failed to parse table metadata: {:?}", e);
        DataFusionError::External(Box::new(e))
    })?;

    // Choose snapshot according to precedence
    let (chosen_snapshot, selection_kind) = if let Some(id) = options.snapshot_id {
        (
            table_metadata
                .snapshots
                .iter()
                .find(|s| s.snapshot_id() == id)
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Snapshot with id {} not found", id))
                })?,
            SelectionKind::SnapshotId,
        )
    } else if let Some(ref name) = options.use_ref {
        let sid = table_metadata
            .refs
            .get(name)
            .map(|r| r.snapshot_id)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown Iceberg ref: {}", name)))?;
        (
            table_metadata
                .snapshots
                .iter()
                .find(|s| s.snapshot_id() == sid)
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Snapshot for ref {} (id={}) not found",
                        name, sid
                    ))
                })?,
            SelectionKind::Ref,
        )
    } else if let Some(ts_str) = options.timestamp_as_of {
        let ts_ms =
            parse_timestamp_to_ms(&ts_str).map_err(|e| DataFusionError::Plan(e.to_string()))?;
        match find_snapshot_by_ts(&table_metadata, ts_ms).cloned() {
            Some(s) => (s, SelectionKind::Timestamp),
            None => {
                log::error!(
                    "No Iceberg snapshot exists at or before timestamp {} (snapshot_log length: {})",
                    ts_str,
                    table_metadata.snapshot_log.len()
                );
                return Err(DataFusionError::Plan(format!(
                    "No Iceberg snapshot exists at or before timestamp {}",
                    ts_str
                )));
            }
        }
    } else {
        (
            table_metadata.current_snapshot().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current snapshot found in table metadata".to_string())
            })?,
            SelectionKind::Current,
        )
    };

    // Pick schema associated with snapshot, or current schema as fallback
    let schema = match selection_kind {
        // For branch/tag refs, prefer table schema rather than snapshot schema
        SelectionKind::Ref => table_metadata.current_schema().cloned().ok_or_else(|| {
            DataFusionError::Plan("No current schema found in table metadata".to_string())
        })?,
        // For point-in-time (snapshot id or timestamp) or current, use the snapshot schema when available
        _ => {
            if let Some(schema_id) = chosen_snapshot.schema_id() {
                table_metadata
                    .schemas
                    .iter()
                    .find(|s| s.schema_id() == schema_id)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Schema with id {} not found for chosen snapshot",
                            schema_id
                        ))
                    })?
            } else {
                table_metadata.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema found in table metadata".to_string())
                })?
            }
        }
    };

    let partition_specs = table_metadata.partition_specs.clone();
    Ok((schema, chosen_snapshot, partition_specs))
}

/// Find the latest metadata file in the table location
pub async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    log::trace!("Finding latest metadata file");
    let version_hint_path =
        ObjectPath::parse(format!("{}metadata/version-hint.text", table_url.path()).as_str())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut hinted_version: Option<i32> = None;
    let mut hinted_filename: Option<String> = None;
    if let Ok(version_hint_data) = object_store.get(&version_hint_path).await {
        if let Ok(version_hint_bytes) = version_hint_data.bytes().await {
            if let Ok(version_hint) = String::from_utf8(version_hint_bytes.to_vec()) {
                let content = version_hint.trim();
                if let Ok(version) = content.parse::<i32>() {
                    log::trace!("Using numeric version hint: {}", version);
                    hinted_version = Some(version);
                } else {
                    // If the hint already contains the full metadata filename, use it as-is,
                    // otherwise append .metadata.json
                    let fname = if content.ends_with(".metadata.json") {
                        content.to_string()
                    } else {
                        format!("{}.metadata.json", content)
                    };
                    log::trace!("Using filename version hint: {}", fname);
                    hinted_filename = Some(fname);
                }
            }
        }
    }

    log::trace!("Listing metadata directory");
    let metadata_prefix = ObjectPath::parse(format!("{}metadata/", table_url.path()).as_str())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let objects = object_store.list(Some(&metadata_prefix));

    let metadata_files: Result<Vec<_>, _> = objects
        .try_filter_map(|obj| async move {
            let path_str = obj.location.to_string();
            if path_str.ends_with(".metadata.json") {
                if let Some(filename) = path_str.split('/').next_back() {
                    // Try new format first: 00001-uuid.metadata.json
                    if let Some(version_part) = filename.split('-').next() {
                        if let Ok(version) = version_part.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                    // Try old format: v123.metadata.json
                    if let Some(version_str) = filename
                        .strip_prefix('v')
                        .and_then(|s| s.strip_suffix(".metadata.json"))
                    {
                        if let Ok(version) = version_str.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                }
            }
            Ok(None)
        })
        .try_collect()
        .await;

    match metadata_files {
        Ok(mut files) => {
            log::trace!("find_latest_metadata_file: found files: {:?}", &files);
            files.sort_by_key(|(version, _, _)| *version);

            if let Some(fname) = hinted_filename {
                if let Some((version, path, _)) =
                    files.iter().rev().find(|(_, p, _)| p.ends_with(&fname))
                {
                    log::trace!(
                        "find_latest_metadata_file: selected by filename hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            } else if let Some(hint) = hinted_version {
                if let Some((version, path, _)) = files.iter().rev().find(|(v, _, _)| *v == hint) {
                    log::trace!(
                        "find_latest_metadata_file: selected by numeric hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            }

            if let Some((version, latest_file, _)) = files.last() {
                log::trace!(
                    "find_latest_metadata_file: selected version {} path={}",
                    version,
                    &latest_file
                );
                Ok(latest_file.clone())
            } else {
                plan_err!("No metadata files found in table location: {}", table_url)
            }
        }
        Err(e) => {
            plan_err!("Failed to list metadata directory: {}", e)
        }
    }
}

fn parse_timestamp_to_ms(s: &str) -> std::result::Result<i64, String> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    // Try RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).timestamp_millis());
    }

    // Fallback format "yyyy-MM-dd HH:mm:ss.SSS"
    let fmt = "%Y-%m-%d %H:%M:%S%.3f";
    let naive = NaiveDateTime::parse_from_str(s, fmt)
        .map_err(|e| format!("Invalid timestamp '{}': {}", s, e))?;
    Ok(Utc.from_utc_datetime(&naive).timestamp_millis())
}

fn find_snapshot_by_ts(meta: &TableMetadata, ts_ms: i64) -> Option<&Snapshot> {
    if meta.snapshot_log.is_empty() {
        log::error!("Iceberg timestamp time-travel requires snapshot_log; no history available");
        return None;
    }
    if let Some(entry) = meta
        .snapshot_log
        .iter()
        .rev()
        .find(|e| e.timestamp_ms <= ts_ms)
    {
        return meta
            .snapshots
            .iter()
            .find(|s| s.snapshot_id() == entry.snapshot_id);
    }
    None
}
