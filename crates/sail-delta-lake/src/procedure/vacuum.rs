use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{Result, exec_err, not_impl_err, plan_err};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
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
    session: &dyn Session,
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
    let deleted = delete_candidates(
        store,
        &candidates,
        &protected,
        session.config().target_partitions(),
    )
    .await?;
    // TODO: Discover and remove eligible empty directories, including their result paths and metrics.
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

async fn delete_candidates(
    store: Arc<dyn ObjectStore>,
    candidates: &[ObjectMeta],
    protected: &HashSet<Path>,
    concurrency: usize,
) -> object_store::Result<u64> {
    let mut deleted = 0;
    // Bound the interval between identity checks and deletion while allowing backend batching.
    for batch in candidates.chunks(1000) {
        let checks = batch
            .iter()
            .filter(|candidate| !protected.contains(&candidate.location))
            .map(|candidate| {
                let store = store.clone();
                let candidate = candidate.clone();
                async move {
                    let current = match store.head(&candidate.location).await {
                        Ok(meta) => meta,
                        Err(object_store::Error::NotFound { .. }) => return Ok(None),
                        Err(error) => return Err(error),
                    };
                    Ok((current.last_modified == candidate.last_modified
                        && current.size == candidate.size
                        && current.e_tag == candidate.e_tag
                        && current.version == candidate.version)
                        .then(|| candidate.location.clone()))
                }
            })
            .collect::<Vec<_>>();
        let locations = stream::iter(checks)
            .buffer_unordered(concurrency.max(1))
            .try_filter_map(|path| futures::future::ready(Ok(path)))
            .try_collect::<Vec<_>>()
            .await?;
        let mut results = store.delete_stream(stream::iter(locations.into_iter().map(Ok)).boxed());
        while let Some(result) = results.next().await {
            match result {
                Ok(_) => deleted += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(deleted)
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
    // Hadoop absolute paths have an absolute path component, not merely a scheme.
    let absolute_uri = path.split_once(':').filter(|(scheme, rest)| {
        scheme
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            && rest.starts_with('/')
    });
    let encoded = encode_path(path);
    let mut url = if let Some((scheme, rest)) = absolute_uri {
        if scheme.eq_ignore_ascii_case(root.scheme())
            && (!rest.starts_with("//") || rest.starts_with("///"))
        {
            // Qualify before parsing: special URL schemes otherwise infer a host from the path.
            let mut qualified = root.clone();
            qualified.set_path(&encode_path(rest.strip_prefix("//").unwrap_or(rest)));
            Ok(qualified)
        } else {
            Url::parse(&encoded)
        }
    } else if path.starts_with('/') {
        root.join(&encoded)
    } else {
        root.join(&format!("./{encoded}"))
    }
    .map_err(DeltaError::generic_err)?;
    // Qualify a missing authority with the table's filesystem, never replace a host.
    if url.scheme() == root.scheme() && url.host_str().is_none() {
        let mut qualified = root.clone();
        qualified.set_path(url.path());
        url = qualified;
    }
    if let Some(path) = relative_path(root, &url)? {
        files.insert(path);
    }
    // Decoding erased the distinction between `scheme:/path` and `scheme%3A/path`.
    // Protect the literal relative object too; `scheme://` cannot name such an object.
    if absolute_uri.is_some_and(|(_, rest)| !rest.starts_with("//")) {
        let url = root
            .join(&format!("./{encoded}"))
            .map_err(DeltaError::generic_err)?;
        if let Some(path) = relative_path(root, &url)? {
            files.insert(path);
        }
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
        .join(&format!("./{}", encode_path(path.as_ref())))
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::fmt;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    use super::*;

    #[test]
    fn vacuum_resolves_relative_object_paths() {
        for root in ["file:///table%20%23/", "s3://bucket/table%20%23/"] {
            let root = Url::parse(root).unwrap();
            for (path, encoded) in [
                ("plain.parquet", "plain.parquet"),
                ("data:0001.parquet", "data:0001.parquet"),
                ("nested/data:0002.parquet", "nested/data:0002.parquet"),
                ("data:/nested.parquet", "data:/nested.parquet"),
                ("1scheme:/nested.parquet", "1scheme:/nested.parquet"),
                ("+scheme:/nested.parquet", "+scheme:/nested.parquet"),
                ("-scheme:/nested.parquet", "-scheme:/nested.parquet"),
                (".scheme:/nested.parquet", ".scheme:/nested.parquet"),
                ("space #/%3A?*.parquet", "space%20%23/%253A%3F%2A.parquet"),
            ] {
                let mut files = HashSet::new();
                protect(&mut files, &root, path, None).unwrap();
                let object = Path::parse(path).unwrap();
                assert_eq!(files, HashSet::from([object.clone()]), "{root} {path}");
                assert_eq!(
                    absolute_path(&root, &object).unwrap(),
                    format!("{root}{encoded}")
                );
            }
        }
    }

    #[test]
    fn vacuum_protects_only_in_table_absolute_paths() {
        for root in [
            "file:///table/",
            "s3://bucket/table/",
            "https://bucket.s3.amazonaws.com/table/",
        ] {
            let root = Url::parse(root).unwrap();
            for (path, expected) in [
                ("/table/data.parquet".to_string(), true),
                (format!("{root}data.parquet"), true),
                (format!("{}:///table/data.parquet", root.scheme()), true),
                ("foreign://bucket/table/data.parquet".to_string(), false),
                ("/table-other/data.parquet".to_string(), false),
                (
                    format!("{}://other/table/data.parquet", root.scheme()),
                    false,
                ),
                (
                    format!(
                        "{}/outside/data.parquet",
                        &root[..url::Position::BeforePath]
                    ),
                    false,
                ),
                ("//other/table/data.parquet".to_string(), false),
            ] {
                let mut files = HashSet::new();
                protect(&mut files, &root, &path, None).unwrap();
                let expected = if expected {
                    HashSet::from([Path::from("data.parquet")])
                } else {
                    HashSet::new()
                };
                assert_eq!(files, expected, "{root} {path}");
            }
        }
        let mut files = HashSet::new();
        let root = Url::parse("s3://bucket/table/").unwrap();
        protect(&mut files, &root, "//bucket/table/data.parquet", None).unwrap();
        assert_eq!(files, HashSet::from([Path::from("data.parquet")]));
    }

    #[test]
    fn vacuum_protects_both_interpretations_of_decoded_scheme_paths() {
        for root in [
            "file:///table/",
            "s3://bucket/table/",
            "https://bucket.s3.amazonaws.com/table/",
        ] {
            let root = Url::parse(root).unwrap();
            for (suffix, relative) in [
                ("/table/data.parquet", Some("data.parquet")),
                ("/table/space #/%3A?.parquet", Some("space #/%3A?.parquet")),
                ("/outside/data.parquet", None),
            ] {
                let path = format!("{}:{suffix}", root.scheme());
                let mut files = HashSet::new();
                protect(&mut files, &root, &path, None).unwrap();
                let object = Path::parse(&path).unwrap();
                let mut expected = HashSet::from([object.clone()]);
                if let Some(relative) = relative {
                    expected.insert(Path::parse(relative).unwrap());
                }
                assert_eq!(files, expected, "{root} {path}");
                assert_eq!(
                    absolute_path(&root, &object).unwrap(),
                    format!("{root}{}", encode_path(&path))
                );
            }
            let mut files = HashSet::new();
            protect(&mut files, &root, "foreign:/table/data.parquet", None).unwrap();
            assert_eq!(
                files,
                HashSet::from([Path::from("foreign:/table/data.parquet")])
            );
        }
    }

    #[test]
    fn vacuum_fails_closed_on_invalid_absolute_paths() {
        for root in ["file:///table/", "s3://bucket/table/"] {
            let root = Url::parse(root).unwrap();
            for path in [
                "s3://bucket:invalid/table/data.parquet",
                "https://bucket:invalid/table/data.parquet",
            ] {
                let mut files = HashSet::new();
                assert!(
                    protect(&mut files, &root, path, None).is_err(),
                    "{root} {path}"
                );
                assert!(files.is_empty());
            }
        }
    }

    #[derive(Debug, Default)]
    struct VacuumStore {
        inner: Arc<InMemory>,
        batches: Arc<Mutex<Vec<Vec<Path>>>>,
        fail_head: bool,
        fail_delete: bool,
    }

    impl fmt::Display for VacuumStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "VacuumStore")
        }
    }

    fn denied(path: &Path) -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: path.to_string(),
            source: "access denied".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for VacuumStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if self.fail_head && options.head {
                return Err(denied(location));
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let batches = self.batches.clone();
            let inner = self.inner.clone();
            let fail = self.fail_delete;
            stream::once(async move {
                let paths = locations.try_collect::<Vec<_>>().await?;
                batches.lock().unwrap().push(paths.clone());
                let results = stream::iter(
                    paths.into_iter().map(
                        move |path| {
                            if fail { Err(denied(&path)) } else { Ok(path) }
                        },
                    ),
                )
                .boxed();
                Ok::<_, object_store::Error>(inner.delete_stream(results))
            })
            .try_flatten()
            .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn vacuum_batches_deletes_and_rechecks_candidates() {
        let store = Arc::new(VacuumStore::default());
        for i in 0..1005 {
            store
                .put(&Path::from(format!("file-{i:04}")), "old".into())
                .await
                .unwrap();
        }
        let candidates = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let protected = HashSet::from([Path::from("file-0000")]);
        let changed = Path::from("file-0001");
        // Keep size and last-modified equal to exercise the object identity check.
        let mut candidates = candidates;
        store.put(&changed, "new".into()).await.unwrap();
        candidates[1].last_modified = store.head(&changed).await.unwrap().last_modified;
        store.inner.delete(&Path::from("file-0002")).await.unwrap();
        assert_eq!(
            delete_candidates(store.clone(), &candidates, &protected, 4)
                .await
                .unwrap(),
            1002
        );
        assert_eq!(
            store
                .batches
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![997, 5]
        );
        assert_eq!(
            store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store.get(&changed).await.unwrap().bytes().await.unwrap(),
            "new"
        );
    }

    #[tokio::test]
    async fn vacuum_propagates_head_and_delete_errors() {
        for fail_head in [true, false] {
            let store = Arc::new(VacuumStore {
                fail_head,
                fail_delete: !fail_head,
                ..Default::default()
            });
            store.put(&Path::from("file"), "old".into()).await.unwrap();
            let candidates = store.list(None).try_collect::<Vec<_>>().await.unwrap();
            let error = delete_candidates(store.clone(), &candidates, &HashSet::new(), 4)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                object_store::Error::PermissionDenied { .. }
            ));
            assert_eq!(store.batches.lock().unwrap().len(), usize::from(!fail_head));
            assert_eq!(
                store
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }
}
