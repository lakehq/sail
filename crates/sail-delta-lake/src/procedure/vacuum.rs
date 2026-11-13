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
use crate::spec::utils::{decode_path, encode_path};
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
    let logging = options
        .get("spark.databricks.delta.vacuum.logging.enabled")
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
    let files = store
        .list(None)
        .try_filter(|meta| futures::future::ready(!hidden(&meta.location, &partition_names)))
        .try_collect::<Vec<_>>()
        .await?;
    let root = snapshot.table_url();
    let directories = Directories::discover(
        &log_store.root_object_store(None),
        root,
        &partition_names,
        &files,
    )
    .await?;
    let mut occupied = directory_ancestors(
        files
            .iter()
            .filter(|file| file.last_modified.timestamp_millis() < cutoff)
            .map(|file| &file.location),
    );
    occupied.extend(directory_ancestors(protected.iter()));
    occupied.extend(protected.iter().cloned());
    let directory_candidates = directories.candidates(&occupied);
    let candidates = files
        .into_iter()
        .filter(|meta| {
            meta.last_modified.timestamp_millis() < cutoff && !protected.contains(&meta.location)
        })
        .collect::<Vec<_>>();
    if dry_run {
        let mut paths = candidates
            .iter()
            .map(|file| &file.location)
            .chain(directory_candidates.iter())
            .map(|path| absolute_path(root, path))
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
        (
            "numFilesToDelete".to_string(),
            json!(candidates.len() + directory_candidates.len()),
        ),
        ("sizeOfDataToDelete".to_string(), json!(bytes)),
    ]);
    if logging {
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
    }
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
    let mut protected_directories = directory_ancestors(protected.iter());
    protected_directories.extend(protected);
    let deleted_directories = directories
        .delete(directory_candidates, protected_directories)
        .await?;
    let end_metrics = HashMap::from([
        (
            "numDeletedFiles".to_string(),
            json!(deleted + deleted_directories),
        ),
        (
            "numVacuumedDirectories".to_string(),
            json!(directories.count()),
        ),
    ]);
    if logging {
        CommitBuilder::from(CommitProperties::default().with_operation_metrics(end_metrics))
            .build(Some(latest), log_store, DeltaOperation::VacuumEnd {})
            .await?;
    }
    result(
        invocation,
        vec![display_path(root)?.trim_end_matches('/').to_string()],
    )
}

fn directory_ancestors<'a>(paths: impl Iterator<Item = &'a Path>) -> HashSet<Path> {
    let mut directories = HashSet::new();
    for path in paths {
        let mut parent = path.parent();
        while let Some(path) = parent {
            if path == Path::ROOT {
                break;
            }
            parent = path.parent();
            directories.insert(path);
        }
    }
    directories
}

struct Directories {
    paths: HashSet<Path>,
    #[cfg(unix)]
    local: Option<Arc<local_directories::LocalDirectories>>,
}

impl Directories {
    async fn discover(
        store: &Arc<dyn ObjectStore>,
        root: &Url,
        partition_names: &[String],
        files: &[ObjectMeta],
    ) -> Result<Self> {
        #[cfg(unix)]
        if let Some(root) = sail_object_store::builtin_local_filesystem_path(
            store,
            &Path::from_url_path(root.path())?,
        )? {
            let partition_names = partition_names.to_vec();
            let local = tokio::task::spawn_blocking(move || {
                local_directories::LocalDirectories::discover(&root, &partition_names)
            })
            .await
            .map_err(DeltaError::generic_err)??;
            return Ok(Self {
                paths: local.paths().cloned().collect(),
                local: Some(Arc::new(local)),
            });
        }
        #[cfg(not(unix))]
        let _ = (store, root, partition_names);
        Ok(Self {
            paths: directory_ancestors(files.iter().map(|file| &file.location)),
            #[cfg(unix)]
            local: None,
        })
    }

    fn count(&self) -> usize {
        self.paths.len() + 1
    }

    fn candidates(&self, occupied: &HashSet<Path>) -> Vec<Path> {
        #[cfg(unix)]
        if self.local.is_some() {
            return self.paths.difference(occupied).cloned().collect();
        }
        let _ = occupied;
        Vec::new()
    }

    async fn delete(&self, candidates: Vec<Path>, protected: HashSet<Path>) -> Result<u64> {
        #[cfg(unix)]
        if let Some(local) = &self.local {
            let local = local.clone();
            return tokio::task::spawn_blocking(move || local.delete(&candidates, &protected))
                .await
                .map_err(DeltaError::generic_err)?;
        }
        let _ = (candidates, protected);
        Ok(0)
    }
}

#[cfg(unix)]
mod local_directories {
    use std::path::Path as FsPath;

    use rustix::fd::{AsFd, OwnedFd};
    use rustix::fs::{
        AtFlags, Dir, FileType, Mode, OFlags, Stat, fstat, open, openat, statat, unlinkat,
    };
    use rustix::io::Errno;

    use super::*;

    const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);

    pub(super) struct LocalDirectories {
        root: OwnedFd,
        identities: HashMap<Path, Stat>,
    }

    fn io_error(error: Errno) -> DeltaError {
        DeltaError::generic_err(std::io::Error::from(error))
    }

    fn changed(error: Errno) -> bool {
        matches!(error, Errno::NOENT | Errno::NOTDIR | Errno::LOOP)
    }

    impl LocalDirectories {
        pub(super) fn discover(root: &FsPath, partition_names: &[String]) -> Result<Self> {
            let root = root.canonicalize().map_err(DeltaError::generic_err)?;
            let root = open(root, DIRECTORY_FLAGS, Mode::empty()).map_err(io_error)?;
            let mut identities = HashMap::new();
            let mut pending = vec![Path::ROOT];
            while let Some(path) = pending.pop() {
                let directory = match Self::open_directory(&root, &path) {
                    Ok(directory) => directory,
                    Err(error) if changed(error) => continue,
                    Err(error) => return Err(io_error(error).into()),
                };
                let entries = Dir::read_from(&directory).map_err(io_error)?;
                for entry in entries {
                    let entry = entry.map_err(io_error)?;
                    let name = entry.file_name();
                    if matches!(name.to_bytes(), b"." | b"..") {
                        continue;
                    }
                    let name = name.to_str().map_err(DeltaError::generic_err)?;
                    let child = if path == Path::ROOT {
                        Path::parse(name)?
                    } else {
                        Path::parse(format!("{path}/{name}"))?
                    };
                    if hidden(&child, partition_names) {
                        continue;
                    }
                    let stat = match statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW) {
                        Ok(stat) => stat,
                        Err(error) if changed(error) => continue,
                        Err(error) => return Err(io_error(error).into()),
                    };
                    if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                        identities.insert(child.clone(), stat);
                        pending.push(child);
                    }
                }
            }
            Ok(Self { root, identities })
        }

        pub(super) fn paths(&self) -> impl Iterator<Item = &Path> {
            self.identities.keys()
        }

        fn open_directory(root: &OwnedFd, path: &Path) -> rustix::io::Result<OwnedFd> {
            let mut directory = openat(root, ".", DIRECTORY_FLAGS, Mode::empty())?;
            for part in path.parts() {
                directory = openat(
                    directory.as_fd(),
                    part.as_ref(),
                    DIRECTORY_FLAGS,
                    Mode::empty(),
                )?;
            }
            Ok(directory)
        }

        pub(super) fn delete(&self, candidates: &[Path], protected: &HashSet<Path>) -> Result<u64> {
            let mut deleted = 0;
            for path in candidates {
                if protected.contains(path) {
                    continue;
                }
                let Some(parent) = path.parent() else {
                    continue;
                };
                let Some(name) = path.filename() else {
                    continue;
                };
                let parent = match Self::open_directory(&self.root, &parent) {
                    Ok(parent) => parent,
                    Err(error) if changed(error) => continue,
                    Err(error) => return Err(io_error(error).into()),
                };
                let directory = match openat(&parent, name, DIRECTORY_FLAGS, Mode::empty()) {
                    Ok(directory) => directory,
                    Err(error) if changed(error) => continue,
                    Err(error) => return Err(io_error(error).into()),
                };
                let stat = fstat(&directory).map_err(io_error)?;
                if !self.identities.get(path).is_some_and(|original| {
                    original.st_dev == stat.st_dev && original.st_ino == stat.st_ino
                }) {
                    continue;
                }
                // Never recurse: a concurrent writer or a hidden child makes rmdir fail safely.
                // The parent descriptor and single-component name cannot traverse a symlink.
                match unlinkat(&parent, name, AtFlags::REMOVEDIR) {
                    Ok(()) => deleted += 1,
                    Err(error) if changed(error) || error == Errno::NOTEMPTY => {}
                    Err(error) => return Err(io_error(error).into()),
                }
            }
            Ok(deleted)
        }
    }
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
    // Protection compares physical objects, not log-action identities.
    let decoded = decode_path(path).map_err(DeltaError::generic_err)?;
    let path = decoded.as_str();
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
    let url = root
        .join(&format!("./{}", encode_path(path.as_ref())))
        .map_err(DeltaError::generic_err)?;
    display_path(&url)
}

fn display_path(url: &Url) -> Result<String> {
    // Hadoop Path strings expose the physical path, including literal percent signs.
    let path = decode_path(url.path()).map_err(DeltaError::generic_err)?;
    Ok(format!("{}{path}", &url[..url::Position::BeforePath]))
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
                protect(&mut files, &root, encoded, None).unwrap();
                let object = Path::parse(path).unwrap();
                assert_eq!(files, HashSet::from([object.clone()]), "{root} {path}");
                assert_eq!(
                    absolute_path(&root, &object).unwrap(),
                    format!("{}{path}", display_path(&root).unwrap())
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
                protect(&mut files, &root, &encode_path(&path), None).unwrap();
                let object = Path::parse(&path).unwrap();
                let mut expected = HashSet::from([object.clone()]);
                if let Some(relative) = relative {
                    expected.insert(Path::parse(relative).unwrap());
                }
                assert_eq!(files, expected, "{root} {path}");
                assert_eq!(
                    absolute_path(&root, &object).unwrap(),
                    format!("{}{path}", display_path(&root).unwrap())
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

    #[cfg(unix)]
    fn test_registry() -> sail_object_store::DynamicObjectStoreRegistry {
        let handle = tokio::runtime::Handle::current();
        sail_object_store::DynamicObjectStoreRegistry::new(
            sail_common::runtime::RuntimeHandle::new(handle.clone(), handle),
        )
    }

    #[cfg(unix)]
    async fn create_empty_table(store: Arc<dyn ObjectStore>, url: Url) -> LogStoreRef {
        use crate::delta_log::StorageConfig;
        use crate::spec::{
            CommitAction, DataType, Metadata, Protocol, SaveMode, StructField, StructType,
        };

        let log_store =
            crate::table::create_logstore_with_object_store(store, url.clone(), StorageConfig)
                .unwrap();
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)]).unwrap(),
            vec![],
            0,
            HashMap::new(),
        )
        .unwrap();
        CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store.clone(),
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: url.to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await
            .unwrap();
        log_store
    }

    #[cfg(unix)]
    async fn execute_test_vacuum(log_store: &LogStoreRef, dry_run: bool) -> RecordBatch {
        use datafusion::prelude::SessionContext;
        use sail_common_datafusion::lakeprocedure::LakeProcedureValue;

        use crate::snapshot::DeltaSnapshotConfig;
        use crate::spec::inspection::DeltaCommandType;

        let snapshot = Arc::new(
            DeltaSnapshot::try_new(
                log_store.as_ref(),
                DeltaSnapshotConfig::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );
        let procedure = super::super::descriptor(DeltaCommandType::Vacuum);
        let arguments = procedure
            .parameters
            .iter()
            .map(|parameter| match parameter.name.as_str() {
                "table" => LakeProcedureValue::Utf8(log_store.config().location.to_string()),
                "dry_run" => LakeProcedureValue::Boolean(dry_run),
                _ => LakeProcedureValue::Null,
            })
            .collect();
        execute(
            &SessionContext::new().state(),
            snapshot,
            log_store.clone(),
            &LakeProcedureInvocation {
                procedure,
                arguments,
            },
            &BTreeMap::new(),
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    struct DirectoryTestRoot(std::path::PathBuf);

    #[cfg(unix)]
    impl DirectoryTestRoot {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("sail-vacuum-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn url(&self) -> Url {
            Url::from_directory_path(&self.0).unwrap()
        }

        async fn directories(&self, partition_names: &[String]) -> Directories {
            use datafusion::execution::object_store::ObjectStoreRegistry;
            let store = test_registry().get_store(&self.url()).unwrap();
            Directories::discover(&store, &self.url(), partition_names, &[])
                .await
                .unwrap()
        }
    }

    #[cfg(unix)]
    impl Drop for DirectoryTestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_respects_registered_memory_store_with_shadow_or_absent_host_path() {
        use datafusion::execution::object_store::ObjectStoreRegistry;

        for session_specific in [false, true] {
            for shadow_exists in [false, true] {
                let host = DirectoryTestRoot::new();
                let table_path = host.0.join("table");
                let url = Url::from_directory_path(&table_path).unwrap();
                if shadow_exists {
                    std::fs::create_dir_all(table_path.join("keep")).unwrap();
                }
                let registry = test_registry();
                let builtin = registry.get_store(&url).unwrap();
                let replacement: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let resolved = if session_specific {
                    registry.register_session_store(&url, "custom-session", replacement.clone());
                    assert!(Arc::ptr_eq(&registry.get_store(&url).unwrap(), &builtin));
                    registry
                        .get_store_with_session(&url, Some("custom-session"))
                        .unwrap()
                } else {
                    registry.register_store(&url, replacement.clone());
                    registry.get_store(&url).unwrap()
                };
                assert!(Arc::ptr_eq(&resolved, &replacement));
                let log_store = create_empty_table(resolved, url).await;
                let recent = Path::from("recent-file");
                log_store
                    .object_store(None)
                    .put(&recent, "keep bytes".into())
                    .await
                    .unwrap();
                assert_eq!(execute_test_vacuum(&log_store, true).await.num_rows(), 0);
                assert_eq!(log_store.get_latest_version(0).await.unwrap(), 0);
                assert_eq!(execute_test_vacuum(&log_store, false).await.num_rows(), 1);
                assert_eq!(log_store.get_latest_version(0).await.unwrap(), 2);
                assert_eq!(
                    log_store
                        .object_store(None)
                        .get(&recent)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                    "keep bytes"
                );
                assert_eq!(table_path.exists(), shadow_exists);
                if shadow_exists {
                    assert!(table_path.join("keep").is_dir());
                    assert!(!table_path.join("_delta_log").exists());
                }
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_does_not_bypass_remapped_or_wrapped_local_stores() {
        use datafusion::execution::object_store::ObjectStoreRegistry;
        use object_store::local::LocalFileSystem;
        use object_store::prefix::PrefixStore;

        for prefix_wrapper in [false, true] {
            let host = DirectoryTestRoot::new();
            let backing = DirectoryTestRoot::new();
            let url = host.url();
            std::fs::create_dir(host.0.join("keep")).unwrap();
            let registry = test_registry();
            let mapped = LocalFileSystem::new_with_prefix(&backing.0).unwrap();
            let physical_table = mapped
                .path_to_filesystem(&Path::from_url_path(url.path()).unwrap())
                .unwrap();
            let replacement: Arc<dyn ObjectStore> = if prefix_wrapper {
                Arc::new(PrefixStore::new(
                    registry.get_store(&url).unwrap(),
                    Path::from_url_path(backing.url().path()).unwrap(),
                ))
            } else {
                Arc::new(mapped)
            };
            registry.register_store(&url, replacement.clone());
            let resolved = registry.get_store(&url).unwrap();
            assert!(Arc::ptr_eq(&resolved, &replacement));
            let log_store = create_empty_table(resolved, url).await;
            std::fs::create_dir(physical_table.join("mapped-empty")).unwrap();
            assert_eq!(execute_test_vacuum(&log_store, true).await.num_rows(), 0);
            assert_eq!(log_store.get_latest_version(0).await.unwrap(), 0);
            execute_test_vacuum(&log_store, false).await;
            assert_eq!(log_store.get_latest_version(0).await.unwrap(), 2);
            assert!(physical_table.join("mapped-empty").is_dir());
            assert!(physical_table.join("_delta_log").is_dir());
            assert!(host.0.join("keep").is_dir());
            assert!(!host.0.join("_delta_log").exists());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_uses_registered_builtin_local_store_for_directory_cleanup() {
        use datafusion::arrow::array::StringArray;
        use datafusion::execution::object_store::ObjectStoreRegistry;

        let root = DirectoryTestRoot::new();
        let registry = test_registry();
        for name in [
            "table",
            "table#1",
            "table#123",
            "table?x",
            "table%23",
            "table%2F",
            "table é",
        ] {
            let physical = root.0.join(name);
            let url = Url::from_directory_path(&physical).unwrap();
            let log_store =
                create_empty_table(registry.get_store(&url).unwrap(), url.clone()).await;
            std::fs::create_dir(physical.join("empty")).unwrap();
            let dry_run = execute_test_vacuum(&log_store, true).await;
            assert_eq!(dry_run.num_rows(), 1);
            assert_eq!(
                dry_run
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                format!("file://{}", physical.join("empty").display())
            );
            assert_eq!(log_store.get_latest_version(0).await.unwrap(), 0);
            assert!(physical.join("empty").is_dir());
            execute_test_vacuum(&log_store, false).await;
            assert_eq!(log_store.get_latest_version(0).await.unwrap(), 2);
            assert!(!physical.join("empty").exists());
            assert!(physical.join("_delta_log").is_dir());
            assert!(physical.is_dir());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_directory_candidate_preserves_recent_untracked_contents() {
        use datafusion::arrow::array::StringArray;
        use datafusion::execution::object_store::ObjectStoreRegistry;

        let root = DirectoryTestRoot::new();
        let registry = test_registry();
        let log_store =
            create_empty_table(registry.get_store(&root.url()).unwrap(), root.url()).await;
        let directory = root.0.join("_change_data");
        std::fs::create_dir(&directory).unwrap();
        let recent = directory.join("recent.parquet");
        std::fs::write(&recent, b"retained").unwrap();
        let dry_run = execute_test_vacuum(&log_store, true).await;
        assert_eq!(dry_run.num_rows(), 1);
        assert_eq!(
            dry_run
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            Url::from_file_path(&directory).unwrap().as_str()
        );
        assert_eq!(log_store.get_latest_version(0).await.unwrap(), 0);
        execute_test_vacuum(&log_store, false).await;
        assert_eq!(std::fs::read(recent).unwrap(), b"retained");
        assert!(directory.is_dir());
        assert_eq!(log_store.get_latest_version(0).await.unwrap(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_discovers_directories_without_following_symlinks() {
        let root = DirectoryTestRoot::new();
        let outside = DirectoryTestRoot::new();
        for name in [
            "_delta_log",
            ".hidden",
            "_hidden",
            "metadata",
            "kept",
            "dv",
            "old/child",
            "empty space% #",
            "_part=x",
        ] {
            std::fs::create_dir_all(root.0.join(name)).unwrap();
        }
        std::fs::create_dir(outside.0.join("empty")).unwrap();
        std::os::unix::fs::symlink(&outside.0, root.0.join("link")).unwrap();
        let directories = root.directories(&["_part".to_string()]).await;
        assert_eq!(directories.count(), 7);
        // Missing active files and DVs still protect their directories.
        let references = [
            Path::from("kept/missing.parquet"),
            Path::from("dv/missing.bin"),
            Path::from("old/child/orphan"),
        ];
        let protected = directory_ancestors(references.iter());
        let candidates = directories.candidates(&protected);
        assert_eq!(
            candidates.iter().cloned().collect::<HashSet<_>>(),
            HashSet::from([
                Path::parse("empty space% #").unwrap(),
                Path::from("_part=x")
            ])
        );
        assert_eq!(directories.delete(candidates, protected).await.unwrap(), 2);
        assert!(root.0.is_dir());
        assert!(root.0.join("_delta_log").is_dir());
        assert!(root.0.join("kept").is_dir());
        assert!(root.0.join("dv").is_dir());
        assert!(outside.0.join("empty").is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_preserves_populated_replaced_and_newly_protected_directories() {
        let root = DirectoryTestRoot::new();
        let outside = DirectoryTestRoot::new();
        for name in ["populated", "replaced", "ancestor/child", "protected"] {
            std::fs::create_dir_all(root.0.join(name)).unwrap();
        }
        std::fs::create_dir(outside.0.join("child")).unwrap();
        let directories = root.directories(&[]).await;
        let candidates = directories.candidates(&HashSet::new());
        std::fs::write(root.0.join("populated/new-file"), b"new data").unwrap();
        std::fs::rename(root.0.join("replaced"), root.0.join("original")).unwrap();
        std::fs::create_dir(root.0.join("replaced")).unwrap();
        std::fs::rename(root.0.join("ancestor"), root.0.join("moved")).unwrap();
        std::os::unix::fs::symlink(&outside.0, root.0.join("ancestor")).unwrap();
        let protected = directory_ancestors([Path::from("protected/live.parquet")].iter());
        assert_eq!(directories.delete(candidates, protected).await.unwrap(), 0);
        assert_eq!(
            std::fs::read(root.0.join("populated/new-file")).unwrap(),
            b"new data"
        );
        assert!(root.0.join("replaced").is_dir());
        assert!(root.0.join("original").is_dir());
        assert!(root.0.join("protected").is_dir());
        assert!(root.0.join("moved/child").is_dir());
        assert!(outside.0.join("child").is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn vacuum_removes_empty_ancestry_over_successive_passes() {
        let root = DirectoryTestRoot::new();
        std::fs::create_dir_all(root.0.join("old/child")).unwrap();
        std::fs::create_dir(root.0.join("_delta_log")).unwrap();
        std::fs::write(root.0.join("old/child/orphan"), b"old").unwrap();
        let directories = root.directories(&[]).await;
        let occupied = directory_ancestors([Path::from("old/child/orphan")].iter());
        assert!(directories.candidates(&occupied).is_empty());
        std::fs::remove_file(root.0.join("old/child/orphan")).unwrap();
        for _ in 0..2 {
            let directories = root.directories(&[]).await;
            directories
                .delete(directories.candidates(&HashSet::new()), HashSet::new())
                .await
                .unwrap();
        }
        assert!(!root.0.join("old").exists());
        assert!(root.0.join("_delta_log").is_dir());
        assert!(root.0.is_dir());
    }

    #[tokio::test]
    async fn vacuum_counts_object_prefixes_without_deleting_directories() {
        let store = InMemory::new();
        for path in ["part=a/one", "part=a/two", "part=b/nested/three"] {
            store.put(&Path::from(path), "data".into()).await.unwrap();
        }
        let files = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let directories = Directories::discover(
            &store,
            &Url::parse("s3://bucket/table/").unwrap(),
            &[],
            &files,
        )
        .await
        .unwrap();
        assert_eq!(directories.count(), 4);
        assert!(directories.candidates(&HashSet::new()).is_empty());
        assert_eq!(
            directories
                .delete(Vec::new(), HashSet::new())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            3
        );
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
