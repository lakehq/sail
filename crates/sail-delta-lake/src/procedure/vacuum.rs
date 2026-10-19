use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{Result, exec_err, not_impl_err, plan_err};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;
use serde_json::json;
use url::Url;

use super::{optional_bool, optional_string};
use crate::deletion_vector::resolve_dv_absolute_path;
use crate::delta_log::LogStoreRef;
use crate::schema::arrow_field_physical_name;
use crate::snapshot::DeltaSnapshot;
use crate::spec::utils::encode_path;
use crate::spec::{DeletionVectorDescriptor, DeltaError, DeltaOperation, TableFeature};
use crate::transaction::{CommitBuilder, CommitProperties, PROTOCOL};

pub(super) async fn execute(
    _session: &dyn Session,
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    invocation: &LakeProcedureInvocation,
    options: &BTreeMap<String, String>,
) -> Result<RecordBatch> {
    if optional_string(invocation, "mode")?.is_some_and(|mode| !mode.eq_ignore_ascii_case("FULL")) {
        return not_impl_err!("VACUUM currently supports FULL mode only");
    }
    let dry_run = optional_bool(invocation, "dry_run")?.unwrap_or(false);
    let ignore_window = options
        .get("spark.databricks.delta.vacuum.retentionWindowIgnore.enabled")
        .map(|value| value.to_ascii_lowercase().parse::<bool>())
        .transpose()
        .map_err(|e| DeltaError::generic(e.to_string()))?
        .unwrap_or(true);
    let safety = options
        .get("spark.databricks.delta.retentionDurationCheck.enabled")
        .map(|value| value.to_ascii_lowercase().parse::<bool>())
        .transpose()
        .map_err(|e| DeltaError::generic(e.to_string()))?
        .unwrap_or(true);
    let default_retention = i64::try_from(
        snapshot
            .table_properties()
            .deleted_file_retention_duration()
            .as_millis(),
    )
    .map_err(|_| DeltaError::generic("Delta retention exceeds i64 milliseconds"))?;
    let specified = optional_string(invocation, "retention_hours")?
        .map(|hours| {
            let hours = hours
                .parse::<f64>()
                .map_err(|e| DeltaError::generic(e.to_string()))?;
            if !hours.is_finite() || hours < 0.0 || hours.round() > i64::MAX as f64 / 3_600_000.0 {
                return Err(DeltaError::generic(
                    "VACUUM retention must be finite and non-negative",
                ));
            }
            Ok((hours.round() as i64) * 3_600_000)
        })
        .transpose()?
        .filter(|millis| !ignore_window || *millis == 0);
    let retention = specified.unwrap_or(default_retention);
    if safety && retention < default_retention {
        return plan_err!(
            "VACUUM retention is shorter than delta.deletedFileRetentionDuration; set spark.databricks.delta.retentionDurationCheck.enabled=false to explicitly disable the retention check"
        );
    }
    let cutoff = Utc::now()
        .timestamp_millis()
        .checked_sub(retention)
        .ok_or_else(|| DeltaError::generic("VACUUM retention timestamp overflow"))?;
    let protected = protected_files(&snapshot, cutoff)?;
    let partition_names = snapshot
        .metadata()
        .partition_columns()
        .iter()
        .map(|name| {
            let field = snapshot.schema().field_with_name(name)?;
            Ok(
                arrow_field_physical_name(field, snapshot.effective_column_mapping_mode())
                    .to_string(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let store = log_store.object_store(None);
    let candidates = store
        .list(None)
        .try_filter(|meta| {
            let candidate = meta.last_modified.timestamp_millis() < cutoff
                && !protected.contains(&meta.location)
                && !hidden(&meta.location, &partition_names);
            futures::future::ready(candidate)
        })
        .try_collect::<Vec<_>>()
        .await?;
    let root = snapshot.table_url();
    if dry_run {
        let mut paths = candidates
            .iter()
            .map(|file| absolute_path(root, &file.location))
            .collect::<Result<Vec<_>>>()?;
        paths.sort();
        paths.truncate(1000);
        return result(invocation, paths);
    }
    let bytes = candidates.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.size)
            .ok_or_else(|| DeltaError::generic("VACUUM byte count overflow"))
    })?;
    let start_metrics = HashMap::from([
        ("numFilesToDelete".to_string(), json!(candidates.len())),
        ("sizeOfDataToDelete".to_string(), json!(bytes)),
    ]);
    CommitBuilder::from(CommitProperties::default().with_operation_metrics(start_metrics))
        .build(
            Some(snapshot.clone()),
            log_store.clone(),
            DeltaOperation::VacuumStart {
                retention_check_enabled: safety,
                specified_retention_millis: specified,
                default_retention_millis: default_retention,
            },
        )
        .await?;
    // Protect references committed while discovery was running, and recheck object identity.
    let latest = Arc::new(
        DeltaSnapshot::try_new(
            log_store.as_ref(),
            snapshot.load_config().clone(),
            None,
            None,
        )
        .await?,
    );
    PROTOCOL
        .can_write_to_protocol(latest.protocol())
        .map_err(DeltaError::from)?;
    if latest.metadata() != snapshot.metadata() || latest.protocol() != snapshot.protocol() {
        return exec_err!(
            "Delta metadata or protocol changed during VACUUM discovery; retry the operation"
        );
    }
    if latest
        .protocol()
        .has_writer_feature(&TableFeature::CatalogManaged)
    {
        return not_impl_err!("VACUUM cannot delete files after the table becomes catalog-managed");
    }
    let protected = protected_files(&latest, cutoff)?;
    let mut deleted = 0_u64;
    for candidate in &candidates {
        if protected.contains(&candidate.location) {
            continue;
        }
        let current = match store.head(&candidate.location).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(error) => return Err(error.into()),
        };
        if current.last_modified != candidate.last_modified
            || current.size != candidate.size
            || current.e_tag != candidate.e_tag
            || current.version != candidate.version
        {
            continue;
        }
        match store.delete(&candidate.location).await {
            Ok(()) => deleted += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let end_metrics = HashMap::from([
        ("numDeletedFiles".to_string(), json!(deleted)),
        ("numVacuumedDirectories".to_string(), json!(0)),
    ]);
    CommitBuilder::from(CommitProperties::default().with_operation_metrics(end_metrics))
        .build(Some(latest), log_store, DeltaOperation::VacuumEnd {})
        .await?;
    result(
        invocation,
        vec![root.as_str().trim_end_matches('/').to_string()],
    )
}

fn result(invocation: &LakeProcedureInvocation, paths: Vec<String>) -> Result<RecordBatch> {
    let rows = paths
        .into_iter()
        .map(|path| json!({"path": path}))
        .collect::<Vec<_>>();
    serde_arrow::to_record_batch(invocation.procedure.schema().fields(), &rows)
        .map_err(|e| DeltaError::generic_err(e).into())
}

fn protected_files(snapshot: &DeltaSnapshot, cutoff: i64) -> Result<HashSet<Path>> {
    let mut files = HashSet::new();
    for add in snapshot.adds() {
        protect(
            &mut files,
            snapshot.table_url(),
            &add.path,
            add.deletion_vector.as_ref(),
        )?;
    }
    for remove in snapshot.removes() {
        if remove.deletion_timestamp.unwrap_or(0) >= cutoff {
            protect(
                &mut files,
                snapshot.table_url(),
                &remove.path,
                remove.deletion_vector.as_ref(),
            )?;
        }
    }
    Ok(files)
}

fn protect(
    files: &mut HashSet<Path>,
    root: &Url,
    path: &str,
    dv: Option<&DeletionVectorDescriptor>,
) -> Result<()> {
    // Action paths have already been URI-decoded by the protocol deserializer.
    let url = root
        .join(&encode_path(path))
        .map_err(DeltaError::generic_err)?;
    if let Some(path) = relative_path(root, &url)? {
        files.insert(path);
    }
    if let Some(dv) = dv
        && let Some(url) = resolve_dv_absolute_path(root, dv)?
        && let Some(path) = relative_path(root, &url)?
    {
        files.insert(path);
    }
    Ok(())
}

fn relative_path(root: &Url, url: &Url) -> Result<Option<Path>> {
    if root.scheme() != url.scheme()
        || root.host_str() != url.host_str()
        || root.port() != url.port()
    {
        return Ok(None);
    }
    let base = Path::from_url_path(root.path())?;
    let path = Path::from_url_path(url.path())?;
    Ok(path.prefix_match(&base).map(Path::from_iter))
}

fn absolute_path(root: &Url, path: &Path) -> Result<String> {
    Ok(root
        .join(&encode_path(path.as_ref()))
        .map_err(DeltaError::generic_err)?
        .to_string())
}

fn hidden(path: &Path, partition_names: &[String]) -> bool {
    path.as_ref().split('/').any(|part| {
        part == "metadata"
            || ((part.starts_with('.') || part.starts_with('_'))
                && !matches!(part, "_change_data" | "_delta_index")
                && !partition_names
                    .iter()
                    .any(|name| part.starts_with(&format!("{name}="))))
    })
}
