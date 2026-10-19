use std::collections::BTreeMap;

use datafusion::arrow::array::RecordBatch;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStoreExt};
use serde::Serialize;
use serde_json::{Value, json};

use super::{MetadataState, timestamp_micros};
use crate::delta_log::{
    list_delta_log_entries_from, parse_commit_version_from_location,
    version_uses_in_commit_timestamps,
};
use crate::snapshot::catalog_managed_commit_path;
use crate::spec::inspection::history_schema;
use crate::spec::{DeltaError, DeltaResult, commit_path};

#[derive(Serialize)]
struct HistoryRow {
    timestamp: i64,
    #[serde(flatten)]
    fields: serde_json::Map<String, Value>,
}

/// Prefer a published commit, then a catalog-ratified staged commit. Never discover staged
/// commits by directory listing: unpublished files may not have been ratified.
pub(super) fn staged_commit(state: &MetadataState, version: i64) -> Option<Path> {
    state
        .snapshot
        .load_config()
        .catalog_managed_commits
        .as_ref()?
        .commits
        .iter()
        .find(|commit| commit.version == version)
        .map(|commit| catalog_managed_commit_path(&commit.file_name))
}

pub(super) async fn last_modified(state: &MetadataState) -> DeltaResult<i64> {
    let version = state.snapshot.version();
    let store = state.log_store.object_store(None);
    let meta = match store.head(&commit_path(version)).await {
        Ok(meta) => meta,
        Err(ObjectStoreError::NotFound { .. }) if staged_commit(state, version).is_some() => {
            let path = staged_commit(state, version)
                .ok_or_else(|| DeltaError::generic("Missing catalog-ratified commit"))?;
            store.head(&path).await?
        }
        Err(error) => return Err(error.into()),
    };
    timestamp_micros(meta.last_modified.timestamp_millis())
}

pub(super) async fn batch(state: &MetadataState, limit: Option<usize>) -> DeltaResult<RecordBatch> {
    let schema = history_schema();
    if limit == Some(0) {
        return Ok(RecordBatch::new_empty(schema));
    }
    let end = state.snapshot.version();
    let start = limit
        .map(|limit| end.saturating_sub(limit as i64 - 1).max(0))
        .unwrap_or(0);
    let store = state.log_store.object_store(None);
    let mut commits = list_delta_log_entries_from(store.clone(), start)
        .await?
        .into_iter()
        .filter_map(|meta| {
            let version = parse_commit_version_from_location(&meta.location)?;
            (version >= start && version <= end).then_some((version, meta.location))
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(ratified) = &state.snapshot.load_config().catalog_managed_commits {
        for commit in &ratified.commits {
            if commit.version >= start && commit.version <= end {
                commits
                    .entry(commit.version)
                    .or_insert_with(|| catalog_managed_commit_path(&commit.file_name));
            }
        }
    }
    let mut rows = stream::iter(commits).map(|(version, path)| {
        let store = store.clone();
        async move {
            let result = match store.get(&path).await {
                Ok(result) => result,
                // Log cleanup can remove commits while history is being read.
                Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(DeltaError::from(error)),
            };
            let modified = result.meta.last_modified.timestamp_millis();
            let bytes = result.bytes().await?;
            let mut info = json!({});
            for action in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
                if let Some(commit_info) = action?.get("commitInfo") {
                    info = commit_info.clone();
                    break;
                }
            }
            let uses_ict = version_uses_in_commit_timestamps(
                version, state.snapshot.protocol(), state.snapshot.metadata());
            let timestamp = if uses_ict {
                info.get("inCommitTimestamp").and_then(Value::as_i64).ok_or_else(||
                    DeltaError::generic(format!("commit {version} is missing inCommitTimestamp and object metadata fallback is disallowed")))?
            } else {
                modified
            };
            let mut row = history_schema().fields().iter().map(|field| {
                (field.name().clone(), info.get(field.name()).cloned().unwrap_or(Value::Null))
            }).collect::<serde_json::Map<_, _>>();
            row.insert("version".to_string(), json!(version));
            for field in ["operationParameters", "operationMetrics"] {
                if let Some(Value::Object(values)) = row.get_mut(field) {
                    for value in values.values_mut() {
                        if !value.is_string() && !value.is_null() {
                            *value = Value::String(value.to_string());
                        }
                    }
                }
            }
            Ok(Some((row, timestamp, uses_ict)))
        }
    }).buffered(state.snapshot.load_config().log_buffer_size.max(1))
        .try_filter_map(|row| async { Ok(row) }).try_collect::<Vec<_>>().await?;

    // Ensure filesystem timestamps increase within the requested non-ICT range.
    let mut previous: Option<i64> = None;
    for (row, timestamp, uses_ict) in &mut rows {
        if !*uses_ict {
            if let Some(previous) = previous {
                *timestamp = (*timestamp).max(
                    previous
                        .checked_add(1)
                        .ok_or_else(|| DeltaError::generic("Delta history timestamp overflow"))?,
                );
            }
            previous = Some(*timestamp);
        }
        row.remove("timestamp");
    }
    let rows = rows
        .into_iter()
        .rev()
        .map(|(fields, timestamp, _)| {
            Ok(HistoryRow {
                timestamp: timestamp_micros(timestamp)?,
                fields,
            })
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    serde_arrow::to_record_batch(schema.fields(), &rows).map_err(DeltaError::generic_err)
}
