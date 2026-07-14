use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{ObjectStoreExt, ObjectStoreScheme, PutPayload};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_python_udf::config::{PySparkArtifactKind, PySparkPythonArtifact};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tonic::Status;
use tonic::codegen::tokio_stream::Stream;
use url::Url;
use uuid::Uuid;

use crate::artifact::{
    SparkArtifactRegistry, artifact_cleanup_confirmation_window, schedule_artifact_cleanup,
    schedule_unsettled_artifact_cleanup,
};
use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::add_artifacts_request::{ArtifactChunk, Payload};
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

const PYFILES_PREFIX: &str = "pyfiles/";
const FILES_PREFIX: &str = "files/";
const ARCHIVES_PREFIX: &str = "archives/";
const JARS_PREFIX: &str = "jars/";
const CLASSES_PREFIX: &str = "classes/";
const FORWARD_TO_FS_PREFIX: &str = "forward_to_fs/";
const CACHE_PREFIX: &str = "cache/";
const STAGING_DIR: &str = ".staging";
const MAX_SINGLE_PUT_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CONCURRENT_ARTIFACT_BLOCKING_IO: usize = 16;
const MAX_CONCURRENT_ARTIFACT_UPLOADS: usize = 8;
const ARTIFACT_IO_DRAIN_MARGIN: Duration = Duration::from_secs(1);
const ARTIFACT_CLEANUP_JOURNAL_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ARTIFACT_NAME_BYTES: usize = 4096;
const MAX_ARTIFACT_PATH_COMPONENT_BYTES: usize = 255;
const MAX_ARTIFACT_PATH_COMPONENTS: usize = 128;

struct ArtifactIoLimits {
    blocking: Arc<tokio::sync::Semaphore>,
    uploads: Arc<tokio::sync::Semaphore>,
}

fn artifact_io_limits() -> &'static ArtifactIoLimits {
    static LIMITS: OnceLock<ArtifactIoLimits> = OnceLock::new();
    LIMITS.get_or_init(|| ArtifactIoLimits {
        blocking: Arc::new(tokio::sync::Semaphore::new(
            MAX_CONCURRENT_ARTIFACT_BLOCKING_IO,
        )),
        uploads: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_ARTIFACT_UPLOADS)),
    })
}

struct ArtifactIoTracker {
    active: AtomicUsize,
    drained: tokio::sync::Notify,
}

fn artifact_io_tracker() -> &'static ArtifactIoTracker {
    static TRACKER: OnceLock<ArtifactIoTracker> = OnceLock::new();
    TRACKER.get_or_init(|| ArtifactIoTracker {
        active: AtomicUsize::new(0),
        drained: tokio::sync::Notify::new(),
    })
}

struct ArtifactIoLease {
    tracker: &'static ArtifactIoTracker,
}

impl ArtifactIoLease {
    fn acquire() -> Self {
        let tracker = artifact_io_tracker();
        tracker.active.fetch_add(1, Ordering::SeqCst);
        Self { tracker }
    }
}

impl Drop for ArtifactIoLease {
    fn drop(&mut self) {
        let previous = self.tracker.active.fetch_sub(1, Ordering::SeqCst);
        if previous == 0 {
            self.tracker.active.store(0, Ordering::SeqCst);
            log::error!("Spark artifact I/O tracker underflow");
        }
        self.tracker.drained.notify_waiters();
    }
}

struct TrackedArtifactIoResult<T> {
    // This field must drop before the lease so cleanup ownership is enqueued first.
    result: Option<SparkResult<T>>,
    _lease: ArtifactIoLease,
}

impl<T> TrackedArtifactIoResult<T> {
    fn new(result: SparkResult<T>, lease: ArtifactIoLease) -> Self {
        Self {
            result: Some(result),
            _lease: lease,
        }
    }

    fn into_result(mut self) -> SparkResult<T> {
        self.result
            .take()
            .ok_or_else(|| SparkError::internal("tracked artifact I/O result was missing"))?
    }
}

pub(crate) async fn drain_artifact_io(timeout: Duration) -> SparkResult<()> {
    let tracker = artifact_io_tracker();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let drained = tracker.drained.notified();
        tokio::pin!(drained);
        drained.as_mut().enable();
        let active = tracker.active.load(Ordering::SeqCst);
        if active == 0 {
            return Ok(());
        }
        if tokio::time::timeout_at(deadline, drained).await.is_err() {
            return Err(SparkError::deadline_exceeded(format!(
                "timed out waiting for {active} active Spark artifact I/O operations"
            )));
        }
    }
}

pub(crate) fn artifact_io_drain_timeout(transfer_timeout: Duration) -> Duration {
    transfer_timeout
        .saturating_mul(2)
        .saturating_add(ARTIFACT_IO_DRAIN_MARGIN)
}

fn ensure_single_put_artifact_size(size: u64) -> SparkResult<()> {
    if size > MAX_SINGLE_PUT_ARTIFACT_BYTES {
        return Err(SparkError::invalid(format!(
            "artifact size {size} bytes exceeds the object-store single-PUT limit of {MAX_SINGLE_PUT_ARTIFACT_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn artifact_cleanup_journal_flush_timeout() -> Duration {
    ARTIFACT_CLEANUP_JOURNAL_FLUSH_TIMEOUT
}

fn validate_crc(data: &[u8], expected_crc: i64) -> bool {
    if !(0..=u32::MAX as i64).contains(&expected_crc) {
        return false;
    }
    let computed = crc32fast::hash(data) as i64;
    computed == expected_crc
}

fn normalize_artifact_name(name: &str) -> SparkResult<String> {
    if name.is_empty() {
        return Err(SparkError::invalid("artifact name must not be empty"));
    }
    if name.len() > MAX_ARTIFACT_NAME_BYTES {
        return Err(SparkError::invalid(format!(
            "artifact name exceeded the limit of {MAX_ARTIFACT_NAME_BYTES} bytes"
        )));
    }
    Ok(name.replace('\\', "/"))
}

fn validate_relative_path(path: &str, description: &str) -> SparkResult<PathBuf> {
    if path.contains(':') || path.contains('\0') {
        return Err(SparkError::invalid(format!(
            "{description} must use a platform-neutral relative path: {path:?}"
        )));
    }
    let path = Path::new(path);
    let mut out = PathBuf::new();
    let mut component_count = 0;
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                component_count += 1;
                if component_count > MAX_ARTIFACT_PATH_COMPONENTS {
                    return Err(SparkError::invalid(format!(
                        "{description} exceeded the limit of {MAX_ARTIFACT_PATH_COMPONENTS} path components"
                    )));
                }
                if part.as_encoded_bytes().len() > MAX_ARTIFACT_PATH_COMPONENT_BYTES {
                    return Err(SparkError::invalid(format!(
                        "{description} contains a path component longer than {MAX_ARTIFACT_PATH_COMPONENT_BYTES} bytes"
                    )));
                }
                let part = part.to_str().ok_or_else(|| {
                    SparkError::invalid(format!(
                        "{description} must contain valid UTF-8 path components"
                    ))
                })?;
                if part.trim_end_matches([' ', '.']) != part || is_windows_reserved_name(part) {
                    return Err(SparkError::invalid(format!(
                        "{description} contains a platform-reserved path component: {part:?}"
                    )));
                }
                out.push(part);
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(SparkError::invalid(format!(
                    "{description} must be a relative path without '.' or '..': {}",
                    path.display()
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(SparkError::invalid(format!(
            "{description} must not be empty"
        )));
    }
    let components = out
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => part
                .to_str()
                .map(|part| part.trim_end_matches([' ', '.']).to_ascii_lowercase()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.iter().any(|component| {
        matches!(
            component.as_str(),
            ".archives" | ".sail-artifact.sha256" | ".sail-locks" | ".staging"
        )
    }) {
        return Err(SparkError::invalid(format!(
            "{description} uses a reserved path: {}",
            path.display()
        )));
    }
    Ok(out)
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component
        .split_once('.')
        .map_or(component, |(stem, _extension)| stem)
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|number| {
                matches!(number, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
            })
}

fn artifact_storage_name(name: &str) -> SparkResult<String> {
    let normalized = normalize_artifact_name(name)?;
    if let Some(rest) = normalized.strip_prefix(ARCHIVES_PREFIX) {
        if rest.matches('#').count() > 1 {
            return Err(SparkError::invalid(format!(
                "'#' in the path is not supported for archive artifact: {name}"
            )));
        }
        if let Some((path, fragment)) = rest.split_once('#') {
            if path.is_empty() {
                return Err(SparkError::invalid(format!(
                    "archive artifact path must not be empty: {name}"
                )));
            }
            validate_relative_path(fragment, "archive destination fragment")?;
            return Ok(format!("{ARCHIVES_PREFIX}{path}"));
        }
    }
    Ok(normalized)
}

fn artifact_target_path(name: &str, artifact_dir: &Path) -> SparkResult<PathBuf> {
    let storage_name = artifact_storage_name(name)?;
    let relative_path = validate_relative_path(&storage_name, "artifact name")?;
    Ok(artifact_dir.join(relative_path))
}

fn artifact_kind(normalized_name: &str) -> SparkResult<Option<PySparkArtifactKind>> {
    if normalized_name.starts_with(JARS_PREFIX) || normalized_name.starts_with(CLASSES_PREFIX) {
        return Err(SparkError::unsupported(format!(
            "JVM artifact is not supported: {normalized_name}"
        )));
    }

    if let Some(file_name) = normalized_name.strip_prefix(PYFILES_PREFIX) {
        if file_name.ends_with(".py")
            || file_name.ends_with(".zip")
            || file_name.ends_with(".egg")
            || file_name.ends_with(".jar")
        {
            validate_relative_path(file_name, "Python artifact name")?;
            return Ok(Some(PySparkArtifactKind::PyFile));
        }
        return Err(SparkError::invalid(format!(
            "unsupported Python artifact type: {file_name}"
        )));
    }

    if let Some(file_name) = normalized_name.strip_prefix(FILES_PREFIX) {
        validate_relative_path(file_name, "file artifact name")?;
        return Ok(Some(PySparkArtifactKind::File));
    }

    if let Some(rest) = normalized_name.strip_prefix(ARCHIVES_PREFIX) {
        let storage_name = artifact_storage_name(normalized_name)?;
        let archive_name = storage_name
            .strip_prefix(ARCHIVES_PREFIX)
            .ok_or_else(|| SparkError::internal("archive prefix was not preserved"))?;
        validate_relative_path(archive_name, "archive artifact name")?;
        if !is_supported_archive(archive_name) {
            return Err(SparkError::invalid(format!(
                "unsupported archive artifact type: {rest}"
            )));
        }
        return Ok(Some(PySparkArtifactKind::Archive));
    }

    Ok(None)
}

fn cache_artifact_hash(normalized_name: &str) -> SparkResult<Option<String>> {
    let Some(hash) = normalized_name.strip_prefix(CACHE_PREFIX) else {
        return Ok(None);
    };
    let relative = validate_relative_path(hash, "cache artifact name")?;
    let hash = relative.to_string_lossy().into_owned();
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SparkError::invalid(format!(
            "cache artifact name must be a SHA-256 hex digest: {hash}"
        )));
    }
    Ok(Some(hash))
}

fn cache_artifact_status_hash(normalized_name: &str) -> SparkResult<Option<String>> {
    let Some(hash) = normalized_name.strip_prefix(CACHE_PREFIX) else {
        return Ok(None);
    };
    let relative = validate_relative_path(hash, "cache artifact name")?;
    let hash = relative.to_string_lossy().into_owned();
    if hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(Some(hash))
    } else {
        Ok(None)
    }
}

fn is_supported_archive(path: &str) -> bool {
    path.ends_with(".zip")
        || path.ends_with(".jar")
        || path.ends_with(".tar.gz")
        || path.ends_with(".tgz")
        || path.ends_with(".tar")
}

struct StoredArtifact {
    normalized_name: String,
    local_path: Option<String>,
    transport_source_path: Option<PathBuf>,
    local_publish: Option<PendingLocalPublish>,
    kind: Option<PySparkArtifactKind>,
    cache_hash: Option<String>,
}

struct PendingArtifactUpdate {
    normalized_name: String,
    size: usize,
    cache_hash: Option<String>,
    local_publish: Option<PendingLocalPublish>,
    python_artifact: Option<PySparkPythonArtifact>,
    object_upload: Option<PendingObjectUpload>,
}

impl PendingArtifactUpdate {
    async fn commit(mut self, artifacts: &SparkArtifactRegistry) -> SparkResult<()> {
        let mut published = match self.local_publish.take() {
            Some(local_publish) => Some(local_publish.publish().await?),
            None => None,
        };
        artifacts.register_artifact(
            self.normalized_name.clone(),
            self.size,
            self.cache_hash.clone(),
            self.python_artifact.clone(),
        )?;
        if let Some(mut upload) = self.object_upload.take() {
            upload.disarm();
        }
        if let Some(local_artifact) = &mut published {
            local_artifact.disarm();
        }
        Ok(())
    }
}

struct LocalArtifactTargetGuard {
    target: PathBuf,
    lock: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for LocalArtifactTargetGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        let locks = local_artifact_target_locks();
        let mut locks = match locks.lock() {
            Ok(locks) => locks,
            Err(error) => error.into_inner(),
        };
        if Arc::strong_count(&self.lock) == 1
            && locks
                .get(&self.target)
                .is_some_and(|lock| Weak::ptr_eq(lock, &Arc::downgrade(&self.lock)))
        {
            locks.remove(&self.target);
        }
    }
}

fn local_artifact_target_locks() -> &'static Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn lock_local_artifact_target(target: PathBuf) -> SparkResult<LocalArtifactTargetGuard> {
    let lock = {
        let mut locks = local_artifact_target_locks().lock()?;
        match locks.get(&target).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(target.clone(), Arc::downgrade(&lock));
                lock
            }
        }
    };
    let guard = Arc::clone(&lock).lock_owned().await;
    Ok(LocalArtifactTargetGuard {
        target,
        lock,
        guard: Some(guard),
    })
}

struct PendingLocalPublish {
    staging: Arc<PendingLocalArtifact>,
    target: PathBuf,
}

impl PendingLocalPublish {
    async fn publish(self) -> SparkResult<PublishedLocalArtifact> {
        let staging = Arc::try_unwrap(self.staging).map_err(|_| {
            SparkError::internal("artifact staging file still has an active upload reader")
        })?;
        let target = self.target;
        let runtime = staging.runtime.clone();
        spawn_blocking_artifact("publish local artifact", move || {
            let staging_file = staging.take_file()?;
            let target_guard = staging.take_target_guard()?;
            let persisted = staging_file.persist_noclobber(&target).map_err(|error| {
                SparkError::internal(format!(
                    "failed to atomically persist artifact file {}: {}",
                    target.display(),
                    error.error
                ))
            })?;
            drop(persisted);
            Ok(PublishedLocalArtifact {
                path: Some(target),
                runtime,
                target_guard: Some(target_guard),
            })
        })
        .await
    }
}

struct PendingLocalArtifact {
    file: Mutex<Option<NamedTempFile>>,
    runtime: RuntimeHandle,
    target_guard: Mutex<Option<LocalArtifactTargetGuard>>,
}

impl PendingLocalArtifact {
    fn new(
        file: NamedTempFile,
        runtime: RuntimeHandle,
        target_guard: LocalArtifactTargetGuard,
    ) -> Self {
        Self {
            file: Mutex::new(Some(file)),
            runtime,
            target_guard: Mutex::new(Some(target_guard)),
        }
    }

    fn path(&self) -> SparkResult<PathBuf> {
        let file = self.file.lock()?;
        Ok(file
            .as_ref()
            .ok_or_else(|| SparkError::internal("artifact staging file was missing"))?
            .path()
            .to_path_buf())
    }

    fn take_file(&self) -> SparkResult<NamedTempFile> {
        self.file
            .lock()?
            .take()
            .ok_or_else(|| SparkError::internal("artifact staging file was missing"))
    }

    fn take_target_guard(&self) -> SparkResult<LocalArtifactTargetGuard> {
        self.target_guard
            .lock()?
            .take()
            .ok_or_else(|| SparkError::internal("artifact target guard was missing"))
    }
}

impl Drop for PendingLocalArtifact {
    fn drop(&mut self) {
        let file = match self.file.get_mut() {
            Ok(file) => file.take(),
            Err(error) => error.into_inner().take(),
        };
        let target_guard = match self.target_guard.get_mut() {
            Ok(target_guard) => target_guard.take(),
            Err(error) => error.into_inner().take(),
        };
        if file.is_some() || target_guard.is_some() {
            schedule_blocking_artifact_cleanup(
                &self.runtime,
                "clean local artifact staging file",
                move || {
                    drop(file);
                    drop(target_guard);
                },
            );
        }
    }
}

struct PublishedLocalArtifact {
    path: Option<PathBuf>,
    runtime: RuntimeHandle,
    target_guard: Option<LocalArtifactTargetGuard>,
}

impl PublishedLocalArtifact {
    fn disarm(&mut self) {
        self.path.take();
        self.target_guard.take();
    }
}

impl Drop for PublishedLocalArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            schedule_local_file_cleanup(&self.runtime, None, path, self.target_guard.take());
        }
    }
}

struct PendingSourceArtifact {
    path: Option<PathBuf>,
    runtime: RuntimeHandle,
}

impl Drop for PendingSourceArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            schedule_local_file_cleanup(&self.runtime, None, path, None);
        }
    }
}

enum ArtifactPayload {
    Bytes(Vec<u8>),
    File {
        source: PathBuf,
        source_cleanup: Option<PendingSourceArtifact>,
    },
}

impl ArtifactPayload {
    fn copy_to_staging(
        &self,
        parent: &Path,
        target: &Path,
        runtime: RuntimeHandle,
        target_guard: LocalArtifactTargetGuard,
    ) -> SparkResult<Arc<PendingLocalArtifact>> {
        let mut staging = NamedTempFile::new_in(parent).map_err(|e| {
            SparkError::internal(format!(
                "failed to create staging file for {}: {e}",
                target.display()
            ))
        })?;
        match self {
            Self::Bytes(data) => {
                staging.write_all(data).map_err(|e| {
                    SparkError::internal(format!("failed to write file {}: {e}", target.display()))
                })?;
            }
            Self::File {
                source,
                source_cleanup,
            } => {
                let _source_cleanup = source_cleanup;
                let mut source_file = File::open(source).map_err(|e| {
                    SparkError::internal(format!(
                        "failed to open artifact file {}: {e}",
                        source.display()
                    ))
                })?;
                std::io::copy(&mut source_file, &mut staging).map_err(|e| {
                    SparkError::internal(format!(
                        "failed to copy artifact file {} to {}: {e}",
                        source.display(),
                        target.display()
                    ))
                })?;
            }
        }
        staging.flush().map_err(|e| {
            SparkError::internal(format!(
                "failed to flush staging file for {}: {e}",
                target.display()
            ))
        })?;
        Ok(Arc::new(PendingLocalArtifact::new(
            staging,
            runtime,
            target_guard,
        )))
    }

    fn content_equals(&self, target: &Path) -> SparkResult<bool> {
        match self {
            Self::Bytes(data) => file_content_equals_bytes(target, data),
            Self::File {
                source,
                source_cleanup,
            } => {
                let _source_cleanup = source_cleanup;
                file_content_equals_file(source, target)
            }
        }
    }
}

async fn spawn_blocking_artifact<T, F>(description: &'static str, operation: F) -> SparkResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> SparkResult<T> + Send + 'static,
{
    let permit = Arc::clone(&artifact_io_limits().blocking)
        .acquire_owned()
        .await
        .map_err(|_| SparkError::internal("artifact blocking I/O limit was closed"))?;
    let lease = ArtifactIoLease::acquire();
    let tracked = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        TrackedArtifactIoResult::new(operation(), lease)
    })
    .await
    .map_err(|error| {
        SparkError::internal(format!("{description} task stopped unexpectedly: {error}"))
    })?;
    tracked.into_result()
}

fn schedule_blocking_artifact_cleanup<F>(
    runtime: &RuntimeHandle,
    description: &'static str,
    cleanup: F,
) where
    F: FnOnce() + Send + 'static,
{
    spawn_tracked_artifact_io(runtime, async move {
        if let Err(error) = spawn_blocking_artifact(description, move || {
            cleanup();
            Ok(())
        })
        .await
        {
            log::debug!("{description} failed: {error}");
        }
    });
}

fn spawn_tracked_artifact_io<F>(runtime: &RuntimeHandle, operation: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let lease = ArtifactIoLease::acquire();
    runtime.io().spawn(async move {
        let _lease = lease;
        operation.await;
    });
}

fn schedule_local_file_cleanup(
    runtime: &RuntimeHandle,
    file: Option<File>,
    path: PathBuf,
    target_guard: Option<LocalArtifactTargetGuard>,
) {
    schedule_blocking_artifact_cleanup(runtime, "clean local artifact file", move || {
        drop(file);
        if let Err(error) = std::fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::debug!(
                "failed to remove uncommitted artifact file {}: {error}",
                path.display()
            );
        }
        drop(target_guard);
    });
}

fn file_content_equals_bytes(path: &Path, expected: &[u8]) -> SparkResult<bool> {
    let metadata = std::fs::metadata(path).map_err(|e| {
        SparkError::internal(format!(
            "failed to inspect existing artifact file {}: {e}",
            path.display()
        ))
    })?;
    if metadata.len() != expected.len() as u64 {
        return Ok(false);
    }
    let mut file = File::open(path).map_err(|e| {
        SparkError::internal(format!(
            "failed to open existing artifact file {}: {e}",
            path.display()
        ))
    })?;
    let mut offset = 0;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|e| {
            SparkError::internal(format!(
                "failed to read existing artifact file {}: {e}",
                path.display()
            ))
        })?;
        if read == 0 {
            return Ok(true);
        }
        if buffer[..read] != expected[offset..offset + read] {
            return Ok(false);
        }
        offset += read;
    }
}

fn file_content_equals_file(left: &Path, right: &Path) -> SparkResult<bool> {
    let left_metadata = std::fs::metadata(left).map_err(|e| {
        SparkError::internal(format!(
            "failed to inspect artifact file {}: {e}",
            left.display()
        ))
    })?;
    let right_metadata = std::fs::metadata(right).map_err(|e| {
        SparkError::internal(format!(
            "failed to inspect existing artifact file {}: {e}",
            right.display()
        ))
    })?;
    if left_metadata.len() != right_metadata.len() {
        return Ok(false);
    }
    let mut left = File::open(left).map_err(|e| {
        SparkError::internal(format!(
            "failed to open artifact file {}: {e}",
            left.display()
        ))
    })?;
    let mut right = File::open(right).map_err(|e| {
        SparkError::internal(format!(
            "failed to open existing artifact file {}: {e}",
            right.display()
        ))
    })?;
    let mut left_buffer = [0_u8; 8192];
    let mut right_buffer = [0_u8; 8192];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|e| SparkError::internal(format!("failed to read artifact file: {e}")))?;
        let right_read = right.read(&mut right_buffer).map_err(|e| {
            SparkError::internal(format!("failed to read existing artifact file: {e}"))
        })?;
        if left_read != right_read {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
        if left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
    }
}

/// Processes a complete artifact (name + assembled data) and stores it appropriately.
async fn store_artifact(
    name: String,
    payload: ArtifactPayload,
    artifact_dir: PathBuf,
    runtime: RuntimeHandle,
) -> SparkResult<StoredArtifact> {
    let target_path = artifact_target_path(&name, &artifact_dir)?;
    let target_guard = lock_local_artifact_target(target_path).await?;
    spawn_blocking_artifact("store local artifact", move || {
        store_artifact_blocking(&name, &payload, &artifact_dir, runtime, target_guard)
    })
    .await
}

fn store_artifact_blocking(
    name: &str,
    payload: &ArtifactPayload,
    artifact_dir: &Path,
    runtime: RuntimeHandle,
    target_guard: LocalArtifactTargetGuard,
) -> SparkResult<StoredArtifact> {
    let normalized_name = normalize_artifact_name(name)?;
    if normalized_name.starts_with(FORWARD_TO_FS_PREFIX) {
        return Err(SparkError::unsupported(
            "copyFromLocalToFs to the server local filesystem is not supported",
        ));
    }

    let target_path = artifact_target_path(name, artifact_dir)?;
    let kind = artifact_kind(&normalized_name)?;
    let cache_hash = cache_artifact_hash(&normalized_name)?;
    if target_path.exists() {
        if payload.content_equals(&target_path)? {
            return Ok(StoredArtifact {
                normalized_name,
                local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
                transport_source_path: Some(target_path),
                local_publish: None,
                kind,
                cache_hash,
            });
        }
        return Err(SparkError::invalid(format!(
            "artifact already exists with different content: {name}"
        )));
    }

    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            SparkError::internal(format!(
                "failed to create artifact subdirectory {}: {e}",
                parent.display()
            ))
        })?;
    }
    let staging = payload.copy_to_staging(
        target_path.parent().ok_or_else(|| {
            SparkError::internal(format!(
                "artifact target does not have a parent directory: {}",
                target_path.display()
            ))
        })?,
        &target_path,
        runtime,
        target_guard,
    )?;
    let transport_source_path = staging.path()?;

    Ok(StoredArtifact {
        normalized_name,
        local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
        transport_source_path: Some(transport_source_path),
        local_publish: Some(PendingLocalPublish {
            staging,
            target: target_path,
        }),
        kind,
        cache_hash,
    })
}

async fn artifact_transport(
    artifacts: &Arc<SparkArtifactRegistry>,
    name: &str,
    sha256: &str,
    size: usize,
    source_path: PathBuf,
    staging: Option<Arc<PendingLocalArtifact>>,
) -> SparkResult<ArtifactTransport> {
    if let Some(existing) = artifacts
        .artifacts()?
        .into_iter()
        .find(|artifact| artifact.name == name)
    {
        if existing.sha256 != sha256 || existing.size != size as u64 {
            return Err(SparkError::invalid(format!(
                "registered artifact {name} has different content"
            )));
        }
        if existing.data.is_none() && existing.uri.is_none() {
            return Err(SparkError::internal(format!(
                "registered artifact {name} does not have a transport"
            )));
        }
        if let Some(uri) = &existing.uri
            && !artifacts.has_artifact_uri(uri)?
        {
            return Err(SparkError::internal(format!(
                "registered artifact {name} lost ownership of its object-store URI"
            )));
        }
        return Ok(ArtifactTransport {
            data: existing.data,
            uri: existing.uri,
            object_upload: None,
        });
    }

    if let Some(existing) = artifacts.artifacts()?.into_iter().find(|artifact| {
        artifact.sha256 == sha256 && artifact.size == size as u64 && artifact.uri.is_some()
    }) {
        let uri = existing.uri.ok_or_else(|| {
            SparkError::internal("committed artifact content did not have an object-store URI")
        })?;
        if !artifacts.has_artifact_uri(&uri)? {
            return Err(SparkError::internal(
                "committed artifact content lost ownership of its object-store URI",
            ));
        }
        return Ok(ArtifactTransport {
            data: None,
            uri: Some(uri),
            object_upload: None,
        });
    }

    let options = artifacts.options();
    if size <= options.inline_max_bytes && artifacts.can_inline_artifact(name, size)? {
        let data = spawn_blocking_artifact("read inline artifact", move || {
            let data = std::fs::read(&source_path).map_err(|error| {
                SparkError::internal(format!(
                    "failed to read artifact file {}: {error}",
                    source_path.display()
                ))
            })?;
            drop(staging);
            Ok(Arc::from(data))
        })
        .await?;
        return Ok(ArtifactTransport {
            data: Some(data),
            uri: None,
            object_upload: None,
        });
    }

    let Some(base_uri) = &options.store_uri else {
        return Err(SparkError::invalid(format!(
            "artifact {name} cannot fit within the per-artifact inline limit of {} bytes and aggregate inline task budget; spark.artifact_store_uri is not configured",
            options.inline_max_bytes
        )));
    };
    let upload_id = Uuid::new_v4();
    let uri = artifact_object_uri(
        base_uri,
        &options.store_namespace,
        artifacts.session_scope_id(),
        sha256,
        upload_id,
    )?;
    let object_upload = upload_artifact(
        uri.clone(),
        ArtifactUploadSource {
            path: source_path,
            staging,
        },
        Arc::clone(artifacts),
        options.runtime.clone(),
        options.transfer_timeout,
    )
    .await?;
    Ok(ArtifactTransport {
        data: None,
        uri: Some(uri),
        object_upload: Some(object_upload),
    })
}

struct ArtifactTransport {
    data: Option<Arc<[u8]>>,
    uri: Option<String>,
    object_upload: Option<PendingObjectUpload>,
}

struct ArtifactUploadSource {
    path: PathBuf,
    staging: Option<Arc<PendingLocalArtifact>>,
}

struct PendingObjectUpload {
    runtime: RuntimeHandle,
    cleanup_uris: Vec<String>,
    may_commit_late: bool,
    not_found_confirmation_window: Duration,
    // Keeps durable session cleanup ownership alive through cleanup handoff.
    _registry_cleanup_lease: Arc<SparkArtifactRegistry>,
}

impl PendingObjectUpload {
    fn disarm(&mut self) {
        self.cleanup_uris.clear();
    }

    fn mark_in_flight(&mut self) {
        self.may_commit_late = true;
    }

    fn mark_settled(&mut self) {
        self.may_commit_late = false;
    }
}

impl Drop for PendingObjectUpload {
    fn drop(&mut self) {
        let uris = std::mem::take(&mut self.cleanup_uris);
        if self.may_commit_late {
            schedule_unsettled_artifact_cleanup(
                &self.runtime,
                uris,
                self.not_found_confirmation_window,
            );
        } else {
            schedule_artifact_cleanup(&self.runtime, None, uris);
        }
    }
}

async fn upload_artifact(
    uri: String,
    source: ArtifactUploadSource,
    registry_cleanup_lease: Arc<SparkArtifactRegistry>,
    runtime: RuntimeHandle,
    transfer_timeout: std::time::Duration,
) -> SparkResult<PendingObjectUpload> {
    let upload_runtime = runtime.clone();
    await_owned_upload(runtime, transfer_timeout, async move {
        upload_artifact_task(
            uri,
            source,
            registry_cleanup_lease,
            upload_runtime,
            transfer_timeout,
        )
        .await
    })
    .await
}

async fn await_owned_upload<T, F>(
    runtime: RuntimeHandle,
    transfer_timeout: std::time::Duration,
    upload: F,
) -> SparkResult<T>
where
    T: Send + 'static,
    F: Future<Output = SparkResult<T>> + Send + 'static,
{
    let permit = Arc::clone(&artifact_io_limits().uploads)
        .acquire_owned()
        .await
        .map_err(|_| SparkError::internal("artifact upload limit was closed"))?;
    let lease = ArtifactIoLease::acquire();
    let upload = runtime.io().spawn(async move {
        let _permit = permit;
        let result = match tokio::time::timeout(transfer_timeout, upload).await {
            Ok(result) => result,
            Err(_) => Err(SparkError::deadline_exceeded(format!(
                "artifact transfer timed out after {transfer_timeout:?}"
            ))),
        };
        TrackedArtifactIoResult::new(result, lease)
    });
    let tracked = upload.await.map_err(|error| {
        SparkError::internal(format!(
            "artifact upload task stopped unexpectedly: {error}"
        ))
    })?;
    tracked.into_result()
}

async fn upload_artifact_task(
    uri: String,
    source: ArtifactUploadSource,
    registry_cleanup_lease: Arc<SparkArtifactRegistry>,
    runtime: RuntimeHandle,
    transfer_timeout: std::time::Duration,
) -> SparkResult<PendingObjectUpload> {
    let ArtifactUploadSource {
        path: source_path,
        staging: staging_lease,
    } = source;
    let url =
        Url::parse(&uri).map_err(|_| SparkError::invalid("invalid artifact object-store URI"))?;
    let (_scheme, final_path) = ObjectStoreScheme::parse(&url)
        .map_err(|_| SparkError::invalid("invalid artifact object-store path"))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|_| SparkError::internal("failed to create artifact object store"))?;
    let mut staging_url = url.clone();
    staging_url.set_path(&format!("{}.staging-{}", url.path(), Uuid::new_v4()));
    let (_scheme, staging_path) = ObjectStoreScheme::parse(&staging_url)
        .map_err(|_| SparkError::invalid("invalid artifact object-store staging path"))?;
    let mut upload = PendingObjectUpload {
        runtime: runtime.clone(),
        cleanup_uris: vec![staging_url.to_string(), uri.clone()],
        may_commit_late: false,
        not_found_confirmation_window: artifact_cleanup_confirmation_window(transfer_timeout),
        _registry_cleanup_lease: registry_cleanup_lease,
    };
    let metadata_path = source_path.clone();
    let metadata_staging = staging_lease.clone();
    let size = spawn_blocking_artifact("inspect artifact upload source", move || {
        let result = std::fs::metadata(&metadata_path)
            .map(|metadata| metadata.len())
            .map_err(|error| {
                SparkError::internal(format!(
                    "failed to read artifact file metadata {}: {error}",
                    metadata_path.display()
                ))
            });
        drop(metadata_staging);
        result
    })
    .await?;
    ensure_single_put_artifact_size(size)?;
    let read_path = source_path.clone();
    let read_staging = staging_lease.clone();
    let data = spawn_blocking_artifact("read artifact upload source", move || {
        let result = std::fs::read(&read_path).map_err(|error| {
            SparkError::internal(format!(
                "failed to read artifact file {}: {error}",
                read_path.display()
            ))
        });
        drop(read_staging);
        result
    })
    .await?;
    upload.mark_in_flight();
    store
        .put(&staging_path, PutPayload::from(data))
        .await
        .map_err(|error| {
            SparkError::internal(format!("failed to write artifact {uri}: {error}"))
        })?;
    drop(staging_lease);
    store
        .rename(&staging_path, &final_path)
        .await
        .map_err(|error| {
            SparkError::internal(format!("failed to publish artifact {uri}: {error}"))
        })?;
    upload.mark_settled();
    Ok(upload)
}

fn artifact_object_uri(
    base_uri: &str,
    store_namespace: &str,
    session_scope_id: &str,
    sha256: &str,
    upload_id: Uuid,
) -> SparkResult<String> {
    let mut url = Url::parse(base_uri)
        .map_err(|_| SparkError::invalid("invalid spark.artifact_store_uri value"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(SparkError::invalid(
            "spark.artifact_store_uri must not contain userinfo, query parameters, or a fragment",
        ));
    }
    if sha256.len() < 2 {
        return Err(SparkError::internal(format!(
            "artifact SHA-256 is unexpectedly short: {sha256}"
        )));
    }
    if session_scope_id.len() != 64
        || !session_scope_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(SparkError::internal(
            "artifact session scope is not a SHA-256 hex digest",
        ));
    }
    let prefix = url.path().trim_end_matches('/');
    let path = if prefix.is_empty() {
        format!(
            "/sail-artifacts/sessions/{store_namespace}/{session_scope_id}/{}/{sha256}/{upload_id}",
            &sha256[..2],
        )
    } else {
        format!(
            "{prefix}/sail-artifacts/sessions/{store_namespace}/{session_scope_id}/{}/{sha256}/{upload_id}",
            &sha256[..2],
        )
    };
    url.set_path(&path);
    Ok(url.to_string())
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct ChunkedArtifact {
    name: String,
    path: Option<PathBuf>,
    file: Option<File>,
    runtime: RuntimeHandle,
    sha256: Sha256,
    expected_chunks: usize,
    total_bytes: usize,
    bytes_seen: usize,
    chunks_seen: usize,
    is_crc_successful: bool,
}

impl ChunkedArtifact {
    fn try_new(
        name: String,
        total_bytes: i64,
        num_chunks: i64,
        initial_chunk: Option<ArtifactChunk>,
        artifact_dir: &Path,
        max_bytes: usize,
        max_chunks: usize,
        runtime: RuntimeHandle,
    ) -> SparkResult<Self> {
        let total_bytes = usize::try_from(total_bytes).map_err(|_| {
            SparkError::invalid(format!("artifact {name} has invalid total byte count"))
        })?;
        let expected_chunks = usize::try_from(num_chunks)
            .map_err(|_| SparkError::invalid(format!("artifact {name} has invalid chunk count")))?;
        if expected_chunks == 0 {
            return Err(SparkError::invalid(format!(
                "artifact {name} must have at least one chunk"
            )));
        }
        if total_bytes > max_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {name} exceeded the limit of {max_bytes} bytes"
            )));
        }
        if expected_chunks > max_chunks {
            return Err(SparkError::invalid(format!(
                "artifact {name} exceeded the limit of {max_chunks} chunks"
            )));
        }
        let path = create_staging_artifact_path(artifact_dir)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o600);
        }
        let file = options.open(&path).map_err(|e| {
            SparkError::internal(format!(
                "failed to create staging artifact file {}: {e}",
                path.display()
            ))
        })?;
        let mut out = Self {
            name,
            path: Some(path),
            file: Some(file),
            runtime,
            sha256: Sha256::new(),
            expected_chunks,
            total_bytes,
            bytes_seen: 0,
            chunks_seen: 0,
            is_crc_successful: true,
        };
        let initial_chunk = initial_chunk.required("initial artifact chunk")?;
        out.process_chunk(&initial_chunk)?;
        Ok(out)
    }

    fn process_chunk(&mut self, chunk: &ArtifactChunk) -> SparkResult<()> {
        if self.chunks_seen >= self.expected_chunks {
            return Err(SparkError::invalid(format!(
                "excessive data chunks for artifact: {}",
                self.name
            )));
        }
        let bytes_seen = self
            .bytes_seen
            .checked_add(chunk.data.len())
            .ok_or_else(|| SparkError::invalid("artifact byte count overflow"))?;
        if bytes_seen > self.total_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} exceeded expected byte count",
                self.name
            )));
        }
        let chunk_ok = validate_crc(&chunk.data, chunk.crc);
        if !chunk_ok {
            self.is_crc_successful = false;
        }
        self.file
            .as_mut()
            .ok_or_else(|| SparkError::internal("chunked artifact file was missing"))?
            .write_all(&chunk.data)
            .map_err(|e| {
                SparkError::internal(format!(
                    "failed to write chunked artifact {}: {e}",
                    self.name
                ))
            })?;
        self.sha256.update(&chunk.data);
        self.bytes_seen = bytes_seen;
        self.chunks_seen += 1;
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.chunks_seen == self.expected_chunks
    }

    fn validate_complete(&self) -> SparkResult<()> {
        if self.chunks_seen != self.expected_chunks || self.bytes_seen != self.total_bytes {
            return Err(SparkError::invalid(format!(
                "missing data chunks for artifact: {}; expected {} chunks and {} bytes, received {} chunks and {} bytes",
                self.name,
                self.expected_chunks,
                self.total_bytes,
                self.chunks_seen,
                self.bytes_seen
            )));
        }
        Ok(())
    }

    fn finish(mut self) -> SparkResult<CompletedChunkedArtifact> {
        self.validate_complete()?;
        self.file
            .as_mut()
            .ok_or_else(|| SparkError::internal("chunked artifact file was missing"))?
            .flush()
            .map_err(|e| {
                SparkError::internal(format!(
                    "failed to flush chunked artifact {}: {e}",
                    self.name
                ))
            })?;
        drop(self.file.take());
        let path = self
            .path
            .take()
            .ok_or_else(|| SparkError::internal("chunked artifact staging path was missing"))?;
        let sha256 = self
            .sha256
            .clone()
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(CompletedChunkedArtifact {
            name: std::mem::take(&mut self.name),
            path: Some(path),
            sha256,
            size: self.bytes_seen,
            is_crc_successful: self.is_crc_successful,
            runtime: self.runtime.clone(),
        })
    }
}

impl Drop for ChunkedArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            schedule_local_file_cleanup(&self.runtime, self.file.take(), path, None);
        }
    }
}

struct CompletedChunkedArtifact {
    name: String,
    path: Option<PathBuf>,
    sha256: String,
    size: usize,
    is_crc_successful: bool,
    runtime: RuntimeHandle,
}

impl CompletedChunkedArtifact {
    fn take_payload(&mut self) -> SparkResult<ArtifactPayload> {
        let source = self
            .path
            .take()
            .ok_or_else(|| SparkError::internal("completed artifact path was missing"))?;
        Ok(ArtifactPayload::File {
            source: source.clone(),
            source_cleanup: Some(PendingSourceArtifact {
                path: Some(source),
                runtime: self.runtime.clone(),
            }),
        })
    }
}

impl Drop for CompletedChunkedArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            schedule_local_file_cleanup(&self.runtime, None, path, None);
        }
    }
}

fn create_staging_artifact_path(artifact_dir: &Path) -> SparkResult<PathBuf> {
    let staging_dir = artifact_dir.join(STAGING_DIR);
    std::fs::create_dir_all(&staging_dir).map_err(|e| {
        SparkError::internal(format!(
            "failed to create staging artifact directory {}: {e}",
            staging_dir.display()
        ))
    })?;
    Ok(staging_dir.join(Uuid::new_v4().to_string()))
}

#[derive(Default)]
struct ArtifactRpcUsage {
    artifacts: usize,
    chunks: usize,
    bytes: usize,
}

impl ArtifactRpcUsage {
    fn ensure_artifact_capacity(
        &self,
        artifacts: &SparkArtifactRegistry,
        additional: usize,
    ) -> SparkResult<()> {
        let total = self
            .artifacts
            .checked_add(additional)
            .ok_or_else(|| SparkError::invalid("artifact RPC count overflow"))?;
        if total > artifacts.options().rpc_max_artifacts {
            return Err(SparkError::invalid(format!(
                "AddArtifacts RPC exceeded the limit of {} artifacts",
                artifacts.options().rpc_max_artifacts
            )));
        }
        Ok(())
    }

    fn start_artifact(
        &mut self,
        artifacts: &SparkArtifactRegistry,
        name: &str,
        declared_bytes: usize,
        declared_chunks: usize,
    ) -> SparkResult<()> {
        let limits = artifacts.options();
        if declared_bytes > limits.max_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {name} exceeded the limit of {} bytes",
                limits.max_bytes
            )));
        }
        if declared_chunks == 0 || declared_chunks > limits.max_chunks {
            return Err(SparkError::invalid(format!(
                "artifact {name} must contain between 1 and {} chunks",
                limits.max_chunks
            )));
        }
        self.artifacts = self
            .artifacts
            .checked_add(1)
            .ok_or_else(|| SparkError::invalid("artifact RPC count overflow"))?;
        if self.artifacts > limits.rpc_max_artifacts {
            return Err(SparkError::invalid(format!(
                "AddArtifacts RPC exceeded the limit of {} artifacts",
                limits.rpc_max_artifacts
            )));
        }
        Ok(())
    }

    fn observe_chunk(
        &mut self,
        artifacts: &SparkArtifactRegistry,
        chunk_bytes: usize,
    ) -> SparkResult<()> {
        let limits = artifacts.options();
        self.chunks = self
            .chunks
            .checked_add(1)
            .ok_or_else(|| SparkError::invalid("artifact RPC chunk count overflow"))?;
        if self.chunks > limits.rpc_max_chunks {
            return Err(SparkError::invalid(format!(
                "AddArtifacts RPC exceeded the limit of {} chunks",
                limits.rpc_max_chunks
            )));
        }
        self.bytes = self
            .bytes
            .checked_add(chunk_bytes)
            .ok_or_else(|| SparkError::invalid("artifact RPC byte count overflow"))?;
        if self.bytes > limits.rpc_max_bytes {
            return Err(SparkError::invalid(format!(
                "AddArtifacts RPC exceeded the limit of {} bytes",
                limits.rpc_max_bytes
            )));
        }
        Ok(())
    }
}

async fn add_artifact_summary(
    name: String,
    payload: ArtifactPayload,
    sha256: &str,
    size: usize,
    is_crc_successful: bool,
    artifact_dir: &Path,
    artifacts: &Arc<SparkArtifactRegistry>,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let normalized_name = normalize_artifact_name(&name)?;
    artifact_target_path(&name, artifact_dir)?;
    let _ = artifact_kind(&normalized_name)?;
    let declared_cache_hash = cache_artifact_hash(&normalized_name)?;
    let mut pending_update = None;
    if is_crc_successful {
        if let Some(cache_hash) = declared_cache_hash
            && cache_hash != sha256
        {
            return Err(SparkError::invalid(format!(
                "cache artifact {normalized_name} content hash does not match its name"
            )));
        }
        artifacts.validate_artifact_registration(&normalized_name, size)?;
        let stored = store_artifact(
            name.clone(),
            payload,
            artifact_dir.to_path_buf(),
            artifacts.options().runtime.clone(),
        )
        .await?;
        let StoredArtifact {
            normalized_name,
            local_path,
            transport_source_path,
            local_publish,
            kind,
            cache_hash,
        } = stored;
        let transport_staging = local_publish
            .as_ref()
            .map(|publish| Arc::clone(&publish.staging));
        let mut update = PendingArtifactUpdate {
            normalized_name: normalized_name.clone(),
            size,
            cache_hash,
            local_publish,
            python_artifact: None,
            object_upload: None,
        };
        if let Some(kind) = kind {
            let transport_source_path = transport_source_path.ok_or_else(|| {
                SparkError::internal(format!(
                    "stored Python artifact {normalized_name} did not have a transport source"
                ))
            })?;
            let python_path = local_path.ok_or_else(|| {
                SparkError::internal(format!(
                    "stored Python artifact {normalized_name} did not have a local path"
                ))
            })?;
            let transport = artifact_transport(
                artifacts,
                &normalized_name,
                sha256,
                size,
                transport_source_path,
                transport_staging,
            )
            .await?;
            update.object_upload = transport.object_upload;
            update.python_artifact = Some(PySparkPythonArtifact {
                scope_id: artifacts.session_scope_id().to_string(),
                name: normalized_name,
                python_path,
                data: transport.data,
                uri: transport.uri,
                sha256: sha256.to_string(),
                size: size as u64,
                kind,
            });
        }
        pending_update = Some(update);
    }
    summaries.push(ArtifactSummary {
        name,
        is_crc_successful,
    });
    Ok(pending_update)
}

async fn add_single_chunk_artifact(
    name: String,
    chunk: ArtifactChunk,
    artifact_dir: &Path,
    artifacts: &Arc<SparkArtifactRegistry>,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let (chunk, chunk_ok, sha256, size) =
        spawn_blocking_artifact("validate single-chunk artifact", move || {
            let chunk_ok = validate_crc(&chunk.data, chunk.crc);
            let sha256 = sha256_hex(&chunk.data);
            let size = chunk.data.len();
            Ok((chunk, chunk_ok, sha256, size))
        })
        .await?;
    let payload = ArtifactPayload::Bytes(chunk.data);
    add_artifact_summary(
        name,
        payload,
        &sha256,
        size,
        chunk_ok,
        artifact_dir,
        artifacts,
        summaries,
    )
    .await
}

async fn finalize_chunked_artifact(
    chunked: ChunkedArtifact,
    artifact_dir: &Path,
    artifacts: &Arc<SparkArtifactRegistry>,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let mut completed =
        spawn_blocking_artifact("finish chunked artifact", move || chunked.finish()).await?;
    let payload = completed.take_payload()?;
    add_artifact_summary(
        completed.name.clone(),
        payload,
        &completed.sha256,
        completed.size,
        completed.is_crc_successful,
        artifact_dir,
        artifacts,
        summaries,
    )
    .await
}

pub(crate) async fn handle_add_artifacts(
    ctx: &SessionContext,
    stream: impl Stream<Item = Result<Payload, Status>>,
) -> SparkResult<Vec<ArtifactSummary>> {
    let artifacts = ctx.extension::<SparkArtifactRegistry>()?;
    let _guard = artifacts.lock_add_artifacts().await;
    artifacts.ensure_durable_cleanup_lease().await?;
    let artifact_registry = Arc::clone(&artifacts);
    let artifact_dir = spawn_blocking_artifact("initialize artifact directory", move || {
        artifact_registry.artifact_dir()
    })
    .await?;
    let chunk_timeout = artifacts.options().chunk_timeout;

    let mut summaries: Vec<ArtifactSummary> = Vec::new();
    let mut current_chunked: Option<ChunkedArtifact> = None;
    let mut usage = ArtifactRpcUsage::default();

    tokio::pin!(stream);
    loop {
        let item = tokio::time::timeout(chunk_timeout, stream.next())
            .await
            .map_err(|_| {
                SparkError::deadline_exceeded(format!(
                    "timed out waiting {:?} for the next artifact chunk",
                    chunk_timeout
                ))
            })?;
        let Some(item) = item else {
            break;
        };
        let payload = item.map_err(|e| SparkError::internal(e.to_string()))?;
        match payload {
            Payload::Batch(batch) => {
                if let Some(chunked) = current_chunked.take() {
                    return Err(SparkError::invalid(format!(
                        "received artifact batch before chunked artifact {} was complete",
                        chunked.name
                    )));
                }
                usage.ensure_artifact_capacity(&artifacts, batch.artifacts.len())?;
                let mut errors = vec![];
                for artifact in batch.artifacts {
                    let name = artifact.name;
                    let result = async {
                        let declared_bytes =
                            artifact.data.as_ref().map_or(0, |chunk| chunk.data.len());
                        usage.start_artifact(&artifacts, &name, declared_bytes, 1)?;
                        let chunk = artifact.data.required("artifact data")?;
                        usage.observe_chunk(&artifacts, chunk.data.len())?;
                        if let Some(update) = add_single_chunk_artifact(
                            name,
                            chunk,
                            &artifact_dir,
                            &artifacts,
                            &mut summaries,
                        )
                        .await?
                        {
                            update.commit(&artifacts).await?;
                        }
                        Ok::<(), SparkError>(())
                    }
                    .await;
                    if let Err(error) = result {
                        errors.push(error.to_string());
                    }
                }
                if !errors.is_empty() {
                    return Err(SparkError::invalid(format!(
                        "one or more artifacts failed: {}",
                        errors.join("; ")
                    )));
                }
            }
            Payload::BeginChunk(begin) => {
                if let Some(chunked) = current_chunked.take() {
                    return Err(SparkError::invalid(format!(
                        "received new chunked artifact before chunked artifact {} was complete",
                        chunked.name
                    )));
                }
                let declared_bytes = usize::try_from(begin.total_bytes).map_err(|_| {
                    SparkError::invalid(format!(
                        "artifact {} has invalid total byte count",
                        begin.name
                    ))
                })?;
                let declared_chunks = usize::try_from(begin.num_chunks).map_err(|_| {
                    SparkError::invalid(format!("artifact {} has invalid chunk count", begin.name))
                })?;
                usage.start_artifact(&artifacts, &begin.name, declared_bytes, declared_chunks)?;
                if let Some(initial_chunk) = &begin.initial_chunk {
                    usage.observe_chunk(&artifacts, initial_chunk.data.len())?;
                }
                let chunk_artifact_dir = artifact_dir.clone();
                let max_bytes = artifacts.options().max_bytes;
                let max_chunks = artifacts.options().max_chunks;
                let runtime = artifacts.options().runtime.clone();
                let chunked = spawn_blocking_artifact("start chunked artifact", move || {
                    ChunkedArtifact::try_new(
                        begin.name,
                        begin.total_bytes,
                        begin.num_chunks,
                        begin.initial_chunk,
                        &chunk_artifact_dir,
                        max_bytes,
                        max_chunks,
                        runtime,
                    )
                })
                .await?;
                if chunked.is_complete() {
                    if let Some(update) = finalize_chunked_artifact(
                        chunked,
                        &artifact_dir,
                        &artifacts,
                        &mut summaries,
                    )
                    .await?
                    {
                        update.commit(&artifacts).await?;
                    }
                } else {
                    current_chunked = Some(chunked);
                }
            }
            Payload::Chunk(chunk) => {
                usage.observe_chunk(&artifacts, chunk.data.len())?;
                let chunked = current_chunked.take().ok_or_else(|| {
                    SparkError::invalid(
                        "received artifact chunk without an active chunked artifact",
                    )
                })?;
                let chunked = spawn_blocking_artifact("write artifact chunk", move || {
                    let mut chunked = chunked;
                    chunked.process_chunk(&chunk)?;
                    Ok(chunked)
                })
                .await?;
                if chunked.is_complete() {
                    if let Some(update) = finalize_chunked_artifact(
                        chunked,
                        &artifact_dir,
                        &artifacts,
                        &mut summaries,
                    )
                    .await?
                    {
                        update.commit(&artifacts).await?;
                    }
                } else {
                    current_chunked = Some(chunked);
                }
            }
        }
    }

    // Finalize any remaining chunked artifact
    if let Some(chunked) = current_chunked.take()
        && let Some(update) =
            finalize_chunked_artifact(chunked, &artifact_dir, &artifacts, &mut summaries).await?
    {
        update.commit(&artifacts).await?;
    }

    if usage.artifacts == 0 {
        return Err(SparkError::invalid(
            "at least one artifact payload is required",
        ));
    }
    Ok(summaries)
}

pub(crate) async fn handle_artifact_statuses(
    ctx: &SessionContext,
    names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    let artifacts = ctx.extension::<SparkArtifactRegistry>()?;
    let mut statuses = HashMap::new();
    for name in names {
        let normalized = normalize_artifact_name(&name)?;
        let exists = if let Some(hash) = cache_artifact_status_hash(&normalized)? {
            artifacts.has_cache_artifact(&hash)?
        } else {
            false
        };
        statuses.insert(name, ArtifactStatus { exists });
    }
    Ok(statuses)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::join_all;
    use sail_common::runtime::RuntimeHandle;
    use tokio::runtime::Handle;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::{
        ArtifactPayload, MAX_CONCURRENT_ARTIFACT_BLOCKING_IO, MAX_CONCURRENT_ARTIFACT_UPLOADS,
        MAX_SINGLE_PUT_ARTIFACT_BYTES, PendingObjectUpload, PublishedLocalArtifact,
        artifact_cleanup_confirmation_window, artifact_cleanup_journal_flush_timeout,
        artifact_io_drain_timeout, artifact_object_uri, await_owned_upload, drain_artifact_io,
        ensure_single_put_artifact_size, lock_local_artifact_target, spawn_blocking_artifact,
        store_artifact,
    };
    use crate::artifact::{
        SparkArtifactOptions, SparkArtifactProcessBudget, SparkArtifactRegistry,
    };
    use crate::error::SparkError;

    #[derive(Debug)]
    struct UploadResultDrop(Option<oneshot::Sender<()>>);

    impl Drop for UploadResultDrop {
        fn drop(&mut self) {
            if let Some(dropped) = self.0.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[test]
    fn retry_upload_uses_a_distinct_owned_object_key() {
        let session_scope = "01".repeat(32);
        let content_hash = "ab".repeat(32);
        let first_upload = artifact_object_uri(
            "s3://bucket/prefix",
            "server-instance",
            &session_scope,
            &content_hash,
            Uuid::from_u128(1),
        )
        .unwrap();
        let retry_upload = artifact_object_uri(
            "s3://bucket/prefix",
            "server-instance",
            &session_scope,
            &content_hash,
            Uuid::from_u128(2),
        )
        .unwrap();

        assert_ne!(first_upload, retry_upload);
        assert!(first_upload.ends_with("/00000000-0000-0000-0000-000000000001"));
        assert!(retry_upload.ends_with("/00000000-0000-0000-0000-000000000002"));
    }

    #[test]
    fn shutdown_waits_for_a_bounded_cleanup_journal_flush() {
        let transfer_timeout = Duration::from_millis(20);
        assert_eq!(
            artifact_cleanup_confirmation_window(transfer_timeout),
            transfer_timeout
        );
        assert_eq!(
            artifact_cleanup_journal_flush_timeout(),
            Duration::from_secs(5)
        );
        assert_eq!(
            artifact_io_drain_timeout(transfer_timeout),
            Duration::from_millis(1040)
        );
    }

    #[test]
    fn cleanup_confirmation_covers_the_full_transfer_timeout() {
        let transfer_timeout = Duration::from_secs(300);
        assert_eq!(
            artifact_cleanup_confirmation_window(transfer_timeout),
            transfer_timeout
        );
    }

    #[test]
    fn single_put_artifact_size_limit_is_inclusive() {
        ensure_single_put_artifact_size(MAX_SINGLE_PUT_ARTIFACT_BYTES).unwrap();
        assert!(matches!(
            ensure_single_put_artifact_size(MAX_SINGLE_PUT_ARTIFACT_BYTES + 1),
            Err(SparkError::InvalidArgument(message))
                if message.contains("single-PUT limit")
                    && message.contains("67108864")
        ));
    }

    #[tokio::test]
    async fn pending_object_upload_keeps_registry_alive_until_guard_drop() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let registry = Arc::new(SparkArtifactRegistry::new(
            "session".to_string(),
            "user".to_string(),
            SparkArtifactOptions {
                root: None,
                inline_max_bytes: 1,
                max_bytes: 1,
                max_chunks: 1,
                rpc_max_artifacts: 1,
                rpc_max_chunks: 1,
                rpc_max_bytes: 1,
                session_max_artifacts: 1,
                session_max_bytes: 1,
                chunk_timeout: Duration::from_secs(1),
                transfer_timeout: Duration::from_secs(1),
                store_uri: None,
                store_namespace: "test".to_string(),
                runtime: runtime.clone(),
            },
            Arc::new(SparkArtifactProcessBudget::new(1, 1, 1)),
        ));
        let registry_weak = Arc::downgrade(&registry);
        let upload = PendingObjectUpload {
            runtime,
            cleanup_uris: Vec::new(),
            may_commit_late: false,
            not_found_confirmation_window: Duration::from_secs(1),
            _registry_cleanup_lease: Arc::clone(&registry),
        };

        drop(registry);
        assert!(registry_weak.upgrade().is_some());
        drop(upload);
        assert!(registry_weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn owned_upload_finishes_after_waiter_cancellation() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let (started, wait_for_start) = oneshot::channel();
        let (finish, wait_for_finish) = oneshot::channel();
        let (dropped, mut wait_for_drop) = oneshot::channel();
        let waiter = tokio::spawn(await_owned_upload(
            runtime,
            Duration::from_secs(1),
            async move {
                started
                    .send(())
                    .map_err(|_| SparkError::internal("failed to signal upload start"))?;
                wait_for_finish
                    .await
                    .map_err(|_| SparkError::internal("failed to finish upload"))?;
                Ok::<_, SparkError>(UploadResultDrop(Some(dropped)))
            },
        ));

        wait_for_start.await.unwrap();
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        let mut drain = tokio::spawn(drain_artifact_io(Duration::from_secs(2)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut drain)
                .await
                .is_err()
        );
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(wait_for_drop.try_recv(), Ok(())));
    }

    #[tokio::test]
    async fn owned_upload_stops_at_transfer_timeout() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let (dropped, wait_for_drop) = oneshot::channel();
        let result = await_owned_upload(runtime, Duration::from_millis(20), async move {
            let _drop = UploadResultDrop(Some(dropped));
            std::future::pending::<()>().await;
            Ok::<(), SparkError>(())
        })
        .await;

        assert!(matches!(result, Err(SparkError::DeadlineExceeded(_))));
        tokio::time::timeout(Duration::from_secs(1), wait_for_drop)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn artifact_io_concurrency_is_bounded() {
        let blocking_active = Arc::new(AtomicUsize::new(0));
        let blocking_peak = Arc::new(AtomicUsize::new(0));
        let blocking_tasks = (0..MAX_CONCURRENT_ARTIFACT_BLOCKING_IO * 2).map(|_| {
            let active = Arc::clone(&blocking_active);
            let peak = Arc::clone(&blocking_peak);
            tokio::spawn(async move {
                spawn_blocking_artifact("test bounded artifact I/O", move || {
                    let active_count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(active_count, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(10));
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            })
        });
        for result in join_all(blocking_tasks).await {
            result.unwrap().unwrap();
        }
        assert!(blocking_peak.load(Ordering::SeqCst) <= MAX_CONCURRENT_ARTIFACT_BLOCKING_IO);

        let upload_active = Arc::new(AtomicUsize::new(0));
        let upload_peak = Arc::new(AtomicUsize::new(0));
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let upload_tasks = (0..MAX_CONCURRENT_ARTIFACT_UPLOADS * 2).map(|_| {
            let active = Arc::clone(&upload_active);
            let peak = Arc::clone(&upload_peak);
            let runtime = runtime.clone();
            tokio::spawn(async move {
                await_owned_upload(runtime, Duration::from_secs(1), async move {
                    let active_count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(active_count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            })
        });
        for result in join_all(upload_tasks).await {
            result.unwrap().unwrap();
        }
        assert!(upload_peak.load(Ordering::SeqCst) <= MAX_CONCURRENT_ARTIFACT_UPLOADS);
    }

    #[tokio::test]
    async fn cancelled_publish_releases_target_after_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("files/retry.txt");
        tokio::fs::create_dir_all(target.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&target, b"first").await.unwrap();
        let target_guard = lock_local_artifact_target(target.clone()).await.unwrap();
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let (started, wait_for_start) = oneshot::channel();
        let (finish, wait_for_finish) = std::sync::mpsc::channel();
        let published_target = target.clone();
        let publish_runtime = runtime.clone();
        let waiter = tokio::spawn(spawn_blocking_artifact(
            "test cancelled artifact publish",
            move || {
                started.send(()).unwrap();
                wait_for_finish.recv().unwrap();
                Ok(PublishedLocalArtifact {
                    path: Some(published_target),
                    runtime: publish_runtime,
                    target_guard: Some(target_guard),
                })
            },
        ));

        wait_for_start.await.unwrap();
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        let mut io_drain = tokio::spawn(drain_artifact_io(Duration::from_secs(1)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut io_drain)
                .await
                .is_err()
        );
        let retry_target = target.clone();
        let mut retry = tokio::spawn(async move { lock_local_artifact_target(retry_target).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut retry)
                .await
                .is_err()
        );

        finish.send(()).unwrap();
        let retry_guard = tokio::time::timeout(Duration::from_secs(1), retry)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        io_drain.await.unwrap().unwrap();
        assert!(!target.exists());
        drop(retry_guard);
    }

    #[tokio::test]
    async fn local_artifact_is_staged_until_publish() {
        let directory = tempfile::tempdir().unwrap();
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let target = directory.path().join("files/example.txt");
        let stored = store_artifact(
            "files/example.txt".to_string(),
            ArtifactPayload::Bytes(b"artifact".to_vec()),
            directory.path().to_path_buf(),
            runtime,
        )
        .await
        .unwrap();

        let staging_path = stored.transport_source_path.clone().unwrap();
        assert!(staging_path.exists());
        assert!(!target.exists());

        let mut published = stored.local_publish.unwrap().publish().await.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"artifact");
        published.disarm();
        drop(published);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(target.exists());
    }
}
