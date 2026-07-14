use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::prelude::SessionContext;
use fs2::FileExt;
use futures::{StreamExt, stream};
use object_store::{ObjectStoreExt, ObjectStoreScheme};
use prost::Message;
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_common::runtime::RuntimeHandle;
use sail_common::spec;
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_plan::config::{
    CachedLocalRelationData, LocalRelationCache, LocalRelationCacheFuture, PlanConfig,
};
use sail_plan::error::{PlanError, PlanResult};
use sail_python_udf::config::PySparkPythonArtifact;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::proto::data_type::{DEFAULT_FIELD_NAME, parse_spark_data_type};
use crate::session::SparkSession;
use crate::spark::connect::LocalRelation;

const MAX_SESSION_INLINE_ARTIFACT_BYTES: usize = GRPC_MAX_MESSAGE_LENGTH_DEFAULT / 2;
const MAX_CONCURRENT_LOCAL_RELATION_CACHE_IO: usize = 8;

fn local_relation_cache_io_semaphore() -> &'static Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_LOCAL_RELATION_CACHE_IO)))
}

async fn run_local_relation_cache_io<T, F>(
    timeout: Duration,
    description: &'static str,
    work: F,
) -> PlanResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> PlanResult<T> + Send + 'static,
{
    let blocking_task = async move {
        let permit = Arc::clone(local_relation_cache_io_semaphore())
            .acquire_owned()
            .await
            .map_err(|_| PlanError::internal("local relation cache I/O limit was closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|error| {
            PlanError::internal(format!("{description} task stopped unexpectedly: {error}"))
        })?
    };
    tokio::time::timeout(timeout, blocking_task)
        .await
        .map_err(|_| PlanError::internal(format!("{description} timed out")))?
}

#[derive(Debug, Clone)]
pub(crate) struct SparkArtifactOptions {
    pub root: Option<PathBuf>,
    pub inline_max_bytes: usize,
    pub max_bytes: usize,
    pub max_chunks: usize,
    pub rpc_max_artifacts: usize,
    pub rpc_max_chunks: usize,
    pub rpc_max_bytes: usize,
    pub session_max_artifacts: usize,
    pub session_max_bytes: usize,
    pub chunk_timeout: Duration,
    pub transfer_timeout: Duration,
    pub store_uri: Option<String>,
    pub store_namespace: String,
    pub runtime: RuntimeHandle,
}

pub(crate) struct SparkArtifactProcessBudget {
    max_artifacts: usize,
    max_bytes: usize,
    max_inline_bytes: usize,
    state: Mutex<SparkArtifactProcessUsage>,
}

#[derive(Default)]
struct SparkArtifactProcessUsage {
    artifacts: usize,
    bytes: usize,
    inline_bytes: usize,
}

#[derive(Default)]
struct SparkArtifactProcessReservation {
    artifacts: usize,
    bytes: usize,
    inline_bytes: usize,
}

impl SparkArtifactProcessBudget {
    pub(crate) fn new(max_artifacts: usize, max_bytes: usize, max_inline_bytes: usize) -> Self {
        Self {
            max_artifacts,
            max_bytes,
            max_inline_bytes,
            state: Mutex::new(SparkArtifactProcessUsage::default()),
        }
    }

    pub(crate) fn configured_limits(&self) -> (usize, usize, usize) {
        (self.max_artifacts, self.max_bytes, self.max_inline_bytes)
    }

    fn reserve_artifact(&self, size: usize, inline: bool) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        let artifacts = state
            .artifacts
            .checked_add(1)
            .ok_or_else(|| SparkError::invalid("artifact process count overflow"))?;
        if artifacts > self.max_artifacts {
            return Err(SparkError::invalid(format!(
                "Spark artifact process exceeded the limit of {} artifacts",
                self.max_artifacts
            )));
        }
        let bytes = state
            .bytes
            .checked_add(size)
            .ok_or_else(|| SparkError::invalid("artifact process byte count overflow"))?;
        if bytes > self.max_bytes {
            return Err(SparkError::invalid(format!(
                "Spark artifact process exceeded the limit of {} bytes",
                self.max_bytes
            )));
        }
        let inline_bytes = if inline {
            state
                .inline_bytes
                .checked_add(size)
                .ok_or_else(|| SparkError::invalid("inline artifact process byte count overflow"))?
        } else {
            state.inline_bytes
        };
        if inline_bytes > self.max_inline_bytes {
            return Err(SparkError::invalid(format!(
                "Spark artifact process exceeded the inline memory limit of {} bytes",
                self.max_inline_bytes
            )));
        }
        state.artifacts = artifacts;
        state.bytes = bytes;
        state.inline_bytes = inline_bytes;
        Ok(())
    }

    fn release(&self, reservation: SparkArtifactProcessReservation) {
        let mut state = self.state.lock().unwrap_or_else(|error| {
            log::warn!("recovering poisoned Spark artifact process budget: {error}");
            error.into_inner()
        });
        let Some(artifacts) = state.artifacts.checked_sub(reservation.artifacts) else {
            log::error!("Spark artifact process reservation count underflow");
            return;
        };
        let Some(bytes) = state.bytes.checked_sub(reservation.bytes) else {
            log::error!("Spark artifact process reservation byte underflow");
            return;
        };
        let Some(inline_bytes) = state.inline_bytes.checked_sub(reservation.inline_bytes) else {
            log::error!("Spark artifact process inline reservation byte underflow");
            return;
        };
        state.artifacts = artifacts;
        state.bytes = bytes;
        state.inline_bytes = inline_bytes;
    }
}

pub(crate) struct SparkArtifactRegistry {
    session_scope_id: String,
    options: SparkArtifactOptions,
    process_budget: Arc<SparkArtifactProcessBudget>,
    add_artifacts_lock: tokio::sync::Mutex<()>,
    cleanup_lease_lock: tokio::sync::Mutex<()>,
    state: Mutex<SparkArtifactState>,
}

impl Drop for SparkArtifactRegistry {
    fn drop(&mut self) {
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(error) => error.into_inner(),
        };
        let process_reservation = std::mem::take(&mut state.process_reservation);
        let artifact_dir = state.artifact_dir.take();
        let artifact_uris = state
            .artifacts
            .iter()
            .filter_map(|artifact| artifact.uri.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(lease_id) = state.cleanup_lease_id.take() {
            activate_artifact_cleanup_lease(
                lease_id,
                state.registered_artifacts.len(),
                state.registered_bytes,
                Arc::clone(&self.process_budget),
                process_reservation,
            );
        } else {
            self.process_budget.release(process_reservation);
            schedule_artifact_cleanup(&self.options.runtime, artifact_dir, artifact_uris);
        }
    }
}

impl SessionExtension for SparkArtifactRegistry {
    fn name() -> &'static str {
        "spark artifact registry"
    }
}

impl SparkArtifactRegistry {
    pub(crate) fn new(
        session_id: String,
        user_id: String,
        options: SparkArtifactOptions,
        process_budget: Arc<SparkArtifactProcessBudget>,
    ) -> Self {
        let session_scope_id = session_artifact_scope_id(&user_id, &session_id);
        Self {
            session_scope_id,
            options,
            process_budget,
            add_artifacts_lock: tokio::sync::Mutex::new(()),
            cleanup_lease_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(SparkArtifactState::new()),
        }
    }

    pub(crate) fn session_scope_id(&self) -> &str {
        &self.session_scope_id
    }

    pub(crate) fn options(&self) -> &SparkArtifactOptions {
        &self.options
    }

    pub(crate) async fn lock_add_artifacts(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.add_artifacts_lock.lock().await
    }

    pub(crate) async fn ensure_durable_cleanup_lease(&self) -> SparkResult<()> {
        let _guard = self.cleanup_lease_lock.lock().await;
        if self.state.lock()?.cleanup_lease_id.is_some() {
            return Ok(());
        }
        let lease_id = Uuid::new_v4().to_string();
        let artifact_dir = self.artifact_dir_path()?;
        let uri_prefixes = self.artifact_object_prefix_uri()?.into_iter().collect();
        {
            let mut state = self.state.lock()?;
            state.cleanup_lease_id = Some(lease_id.clone());
        }
        let result = persist_dormant_artifact_cleanup_lease(
            lease_id.clone(),
            artifact_dir,
            uri_prefixes,
            self.options.transfer_timeout,
        )
        .await;
        if result.is_err() {
            let mut state = self.state.lock()?;
            if state.cleanup_lease_id.as_deref() == Some(&lease_id) {
                state.cleanup_lease_id = None;
            }
        }
        result
    }

    fn artifact_dir_path(&self) -> SparkResult<PathBuf> {
        let root = match &self.options.root {
            Some(root) => root.clone(),
            None => default_artifact_root()?,
        };
        Ok(root
            .join(&self.options.store_namespace)
            .join(&self.session_scope_id))
    }

    fn artifact_object_prefix_uri(&self) -> SparkResult<Option<String>> {
        let Some(base_uri) = &self.options.store_uri else {
            return Ok(None);
        };
        let mut url = Url::parse(base_uri)
            .map_err(|_| SparkError::invalid("invalid spark.artifact_store_uri value"))?;
        let base_path = url.path().trim_end_matches('/');
        let suffix = format!(
            "sail-artifacts/sessions/{}/{}/",
            self.options.store_namespace, self.session_scope_id
        );
        let path = if base_path.is_empty() {
            format!("/{suffix}")
        } else {
            format!("{base_path}/{suffix}")
        };
        url.set_path(&path);
        Ok(Some(url.to_string()))
    }

    pub(crate) fn artifact_dir(&self) -> SparkResult<PathBuf> {
        let mut state = self.state.lock()?;
        if let Some(dir) = &state.artifact_dir {
            return Ok(dir.clone());
        }
        let dir = self.artifact_dir_path()?;
        std::fs::create_dir_all(&dir).map_err(|e| {
            SparkError::internal(format!("failed to create artifact directory: {e}"))
        })?;
        let metadata = std::fs::symlink_metadata(&dir).map_err(|error| {
            SparkError::internal(format!(
                "failed to inspect artifact directory {}: {error}",
                dir.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(SparkError::invalid(format!(
                "artifact directory is not a private directory: {}",
                dir.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).map_err(
                |error| {
                    SparkError::internal(format!(
                        "failed to secure artifact directory {}: {error}",
                        dir.display()
                    ))
                },
            )?;
        }
        state.artifact_dir = Some(dir.clone());
        Ok(dir)
    }

    pub(crate) fn register_artifact(
        &self,
        normalized_name: String,
        size: usize,
        cache_hash: Option<String>,
        python_artifact: Option<PySparkPythonArtifact>,
    ) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        let existing_size = state.registered_artifacts.get(&normalized_name).copied();
        if existing_size.is_some_and(|existing| existing != size) {
            return Err(SparkError::invalid(format!(
                "registered artifact {normalized_name} has a different size"
            )));
        }
        let is_new = existing_size.is_none();
        if is_new {
            let artifact_count = state
                .registered_artifacts
                .len()
                .checked_add(1)
                .ok_or_else(|| SparkError::invalid("artifact session count overflow"))?;
            if artifact_count > self.options.session_max_artifacts {
                return Err(SparkError::invalid(format!(
                    "Spark artifact session exceeded the limit of {} artifacts",
                    self.options.session_max_artifacts
                )));
            }
            let registered_bytes = state
                .registered_bytes
                .checked_add(size)
                .ok_or_else(|| SparkError::invalid("artifact session byte count overflow"))?;
            if registered_bytes > self.options.session_max_bytes {
                return Err(SparkError::invalid(format!(
                    "Spark artifact session exceeded the limit of {} bytes",
                    self.options.session_max_bytes
                )));
            }
            let inline_artifact_bytes = if python_artifact
                .as_ref()
                .is_some_and(|artifact| artifact.data.is_some())
            {
                let bytes = state
                    .inline_artifact_bytes
                    .checked_add(size)
                    .ok_or_else(|| SparkError::invalid("inline artifact byte count overflow"))?;
                if bytes > MAX_SESSION_INLINE_ARTIFACT_BYTES {
                    return Err(SparkError::invalid(format!(
                        "Spark artifact session exceeded the inline task budget of {MAX_SESSION_INLINE_ARTIFACT_BYTES} bytes"
                    )));
                }
                Some(bytes)
            } else {
                None
            };
            let process_artifacts = state
                .process_reservation
                .artifacts
                .checked_add(1)
                .ok_or_else(|| SparkError::invalid("artifact process reservation overflow"))?;
            let process_bytes = state
                .process_reservation
                .bytes
                .checked_add(size)
                .ok_or_else(|| SparkError::invalid("artifact process reservation byte overflow"))?;
            let inline = inline_artifact_bytes.is_some();
            let process_inline_bytes = state
                .process_reservation
                .inline_bytes
                .checked_add(if inline { size } else { 0 })
                .ok_or_else(|| {
                    SparkError::invalid("inline artifact process reservation byte overflow")
                })?;
            self.process_budget.reserve_artifact(size, inline)?;
            state.registered_artifacts.insert(normalized_name, size);
            state.registered_bytes = registered_bytes;
            if let Some(bytes) = inline_artifact_bytes {
                state.inline_artifact_bytes = bytes;
            }
            state.process_reservation = SparkArtifactProcessReservation {
                artifacts: process_artifacts,
                bytes: process_bytes,
                inline_bytes: process_inline_bytes,
            };
        }
        if let Some(hash) = cache_hash {
            state.cache_artifacts.insert(hash);
        }
        if let Some(artifact) = python_artifact {
            if let Some(existing) = state
                .artifacts
                .iter_mut()
                .find(|existing| existing.name == artifact.name)
            {
                *existing = artifact;
            } else {
                state.artifacts.push(artifact);
            }
        }
        Ok(())
    }

    pub(crate) fn validate_artifact_registration(
        &self,
        normalized_name: &str,
        size: usize,
    ) -> SparkResult<()> {
        let state = self.state.lock()?;
        if state.registered_artifacts.contains_key(normalized_name) {
            return Ok(());
        }
        let artifact_count = state
            .registered_artifacts
            .len()
            .checked_add(1)
            .ok_or_else(|| SparkError::invalid("artifact session count overflow"))?;
        if artifact_count > self.options.session_max_artifacts {
            return Err(SparkError::invalid(format!(
                "Spark artifact session exceeded the limit of {} artifacts",
                self.options.session_max_artifacts
            )));
        }
        let registered_bytes = state
            .registered_bytes
            .checked_add(size)
            .ok_or_else(|| SparkError::invalid("artifact session byte count overflow"))?;
        if registered_bytes > self.options.session_max_bytes {
            return Err(SparkError::invalid(format!(
                "Spark artifact session exceeded the limit of {} bytes",
                self.options.session_max_bytes
            )));
        }
        Ok(())
    }

    pub(crate) fn can_inline_artifact(
        &self,
        normalized_name: &str,
        size: usize,
    ) -> SparkResult<bool> {
        let state = self.state.lock()?;
        if state.registered_artifacts.contains_key(normalized_name) {
            return Ok(state
                .artifacts
                .iter()
                .find(|artifact| artifact.name == normalized_name)
                .is_some_and(|artifact| artifact.data.is_some()));
        }
        Ok(state
            .inline_artifact_bytes
            .checked_add(size)
            .is_some_and(|bytes| bytes <= MAX_SESSION_INLINE_ARTIFACT_BYTES))
    }

    pub(crate) fn has_cache_artifact(&self, hash: &str) -> SparkResult<bool> {
        let state = self.state.lock()?;
        Ok(state.cache_artifacts.contains(hash))
    }

    pub(crate) fn has_artifact_uri(&self, uri: &str) -> SparkResult<bool> {
        let state = self.state.lock()?;
        Ok(state
            .artifacts
            .iter()
            .any(|artifact| artifact.uri.as_deref() == Some(uri)))
    }

    fn cache_artifact_path(&self, hash: &str) -> SparkResult<PathBuf> {
        validate_cache_hash(hash)?;
        let state = self.state.lock()?;
        if !state.cache_artifacts.contains(hash) {
            return Err(SparkError::invalid(format!(
                "cached local relation block not found: {hash}"
            )));
        }
        Ok(state
            .artifact_dir
            .clone()
            .ok_or_else(|| SparkError::internal("artifact directory was not initialized"))?
            .join("cache")
            .join(hash))
    }

    pub(crate) fn artifacts(&self) -> SparkResult<Vec<PySparkPythonArtifact>> {
        let state = self.state.lock()?;
        Ok(state.artifacts.clone())
    }
}

fn inspect_cache_artifact_size(path: &Path) -> SparkResult<usize> {
    let size = std::fs::metadata(path)
        .map_err(|error| {
            SparkError::internal(format!(
                "failed to inspect cached local relation block {}: {error}",
                path.display()
            ))
        })?
        .len();
    usize::try_from(size).map_err(|_| {
        SparkError::invalid(format!(
            "cached local relation block {} is too large",
            path.display()
        ))
    })
}

fn read_cache_artifact_file(path: &Path, expected_hash: &str) -> SparkResult<Vec<u8>> {
    let data = std::fs::read(path).map_err(|error| {
        SparkError::internal(format!(
            "failed to read cached local relation block {}: {error}",
            path.display()
        ))
    })?;
    let actual_hash = sha256_hex(&data);
    if actual_hash != expected_hash {
        return Err(SparkError::invalid(format!(
            "cached local relation block hash mismatch: expected {expected_hash}, got {actual_hash}"
        )));
    }
    Ok(data)
}

fn default_artifact_root() -> SparkResult<PathBuf> {
    static DEFAULT_ARTIFACT_ROOT: OnceLock<Result<tempfile::TempDir, String>> = OnceLock::new();
    DEFAULT_ARTIFACT_ROOT
        .get_or_init(|| {
            let parent = std::env::temp_dir().join("sail-artifacts");
            std::fs::create_dir_all(&parent).map_err(|error| error.to_string())?;
            tempfile::Builder::new()
                .prefix("process-")
                .tempdir_in(parent)
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map(|root| root.path().to_path_buf())
        .map_err(|error| {
            SparkError::internal(format!("failed to create private artifact root: {error}"))
        })
}

fn validate_cache_hash(hash: &str) -> SparkResult<()> {
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SparkError::invalid(format!(
            "cached local relation hash must be a SHA-256 hex digest: {hash}"
        )));
    }
    Ok(())
}

fn session_artifact_scope_id(user_id: &str, session_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update((user_id.len() as u64).to_be_bytes());
    hasher.update(user_id.as_bytes());
    hasher.update((session_id.len() as u64).to_be_bytes());
    hasher.update(session_id.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct SparkLocalRelationCache {
    artifacts: Arc<SparkArtifactRegistry>,
}

impl fmt::Debug for SparkLocalRelationCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SparkLocalRelationCache")
            .finish_non_exhaustive()
    }
}

impl LocalRelationCache for SparkLocalRelationCache {
    fn read_cached_local_relation<'a>(
        &'a self,
        hash: &'a str,
    ) -> LocalRelationCacheFuture<'a, CachedLocalRelationData> {
        let artifacts = Arc::clone(&self.artifacts);
        let expected_hash = hash.to_owned();
        let timeout = self.artifacts.options.transfer_timeout;
        Box::pin(async move {
            run_local_relation_cache_io(timeout, "read cached local relation", move || {
                let path = artifacts
                    .cache_artifact_path(&expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                let data = read_cache_artifact_file(&path, &expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                let relation = LocalRelation::decode(data.as_slice()).map_err(|error| {
                    PlanError::invalid(format!(
                        "invalid cached local relation proto {expected_hash}: {error}"
                    ))
                })?;
                let LocalRelation { data, schema } = relation;
                let schema = parse_local_relation_schema(schema.as_deref())?;
                Ok(CachedLocalRelationData { data, schema })
            })
            .await
        })
    }

    fn cached_local_relation_block_size<'a>(
        &'a self,
        hash: &'a str,
    ) -> LocalRelationCacheFuture<'a, usize> {
        let artifacts = Arc::clone(&self.artifacts);
        let expected_hash = hash.to_owned();
        let timeout = self.artifacts.options.transfer_timeout;
        Box::pin(async move {
            run_local_relation_cache_io(timeout, "inspect cached local relation", move || {
                let path = artifacts
                    .cache_artifact_path(&expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                inspect_cache_artifact_size(&path).map_err(spark_error_to_plan_error)
            })
            .await
        })
    }

    fn read_chunked_cached_local_relation_data<'a>(
        &'a self,
        hash: &'a str,
    ) -> LocalRelationCacheFuture<'a, Vec<u8>> {
        let artifacts = Arc::clone(&self.artifacts);
        let expected_hash = hash.to_owned();
        let timeout = self.artifacts.options.transfer_timeout;
        Box::pin(async move {
            run_local_relation_cache_io(timeout, "read cached local relation data", move || {
                let path = artifacts
                    .cache_artifact_path(&expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                read_cache_artifact_file(&path, &expected_hash).map_err(spark_error_to_plan_error)
            })
            .await
        })
    }

    fn read_chunked_cached_local_relation_schema<'a>(
        &'a self,
        hash: &'a str,
    ) -> LocalRelationCacheFuture<'a, spec::Schema> {
        let artifacts = Arc::clone(&self.artifacts);
        let expected_hash = hash.to_owned();
        let timeout = self.artifacts.options.transfer_timeout;
        Box::pin(async move {
            run_local_relation_cache_io(timeout, "read cached local relation schema", move || {
                let path = artifacts
                    .cache_artifact_path(&expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                let data = read_cache_artifact_file(&path, &expected_hash)
                    .map_err(spark_error_to_plan_error)?;
                let schema = std::str::from_utf8(&data).map_err(|error| {
                    PlanError::invalid(format!(
                        "invalid chunked cached local relation schema block {expected_hash}: {error}"
                    ))
                })?;
                parse_required_local_relation_schema(schema)
            })
            .await
        })
    }
}

fn parse_local_relation_schema(schema: Option<&str>) -> PlanResult<Option<spec::Schema>> {
    schema
        .filter(|s| !s.is_empty())
        .map(parse_required_local_relation_schema)
        .transpose()
}

fn parse_required_local_relation_schema(schema: &str) -> PlanResult<spec::Schema> {
    parse_spark_data_type(schema)
        .map(|dt| dt.into_schema(DEFAULT_FIELD_NAME, true))
        .map_err(spark_error_to_plan_error)
}

fn spark_error_to_plan_error(error: SparkError) -> PlanError {
    match error {
        SparkError::DataFusionError(e) => PlanError::DataFusionError(e),
        SparkError::IoError(e) => {
            PlanError::DataFusionError(datafusion::error::DataFusionError::IoError(e))
        }
        SparkError::ArrowError(e) => PlanError::ArrowError(e),
        SparkError::MissingArgument(message) => PlanError::MissingArgument(message),
        SparkError::InvalidArgument(message) => PlanError::InvalidArgument(message),
        SparkError::NotImplemented(message) => PlanError::NotImplemented(message),
        SparkError::NotSupported(message) => PlanError::NotSupported(message),
        SparkError::InternalError(message) => PlanError::InternalError(message),
        SparkError::AnalysisError(message) => PlanError::AnalysisError(message),
        error => PlanError::InternalError(error.to_string()),
    }
}

struct SparkArtifactState {
    artifacts: Vec<PySparkPythonArtifact>,
    cache_artifacts: HashSet<String>,
    registered_artifacts: HashMap<String, usize>,
    registered_bytes: usize,
    inline_artifact_bytes: usize,
    process_reservation: SparkArtifactProcessReservation,
    artifact_dir: Option<PathBuf>,
    cleanup_lease_id: Option<String>,
}

impl SparkArtifactState {
    fn new() -> Self {
        Self {
            artifacts: vec![],
            cache_artifacts: HashSet::new(),
            registered_artifacts: HashMap::new(),
            registered_bytes: 0,
            inline_artifact_bytes: 0,
            process_reservation: SparkArtifactProcessReservation::default(),
            artifact_dir: None,
            cleanup_lease_id: None,
        }
    }
}

pub(crate) fn resolve_plan_config(ctx: &SessionContext) -> SparkResult<Arc<PlanConfig>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut config = (*spark.plan_config()?).clone();
    let artifact_registry = ctx.extension::<SparkArtifactRegistry>()?;
    let artifacts = artifact_registry.artifacts()?;
    let mut pyspark_udf_config = (*config.pyspark_udf_config).clone();
    pyspark_udf_config.python_artifacts = artifacts.into();
    config.pyspark_udf_config = Arc::new(pyspark_udf_config);
    config.local_relation_cache = Arc::new(SparkLocalRelationCache {
        artifacts: artifact_registry,
    });
    Ok(Arc::new(config))
}

pub(crate) fn schedule_artifact_cleanup(
    _runtime: &RuntimeHandle,
    artifact_dir: Option<PathBuf>,
    uris: Vec<String>,
) {
    schedule_artifact_cleanup_request(artifact_dir, uris, 0);
}

pub(crate) fn schedule_unsettled_artifact_cleanup(
    _runtime: &RuntimeHandle,
    uris: Vec<String>,
    not_found_confirmation_window: Duration,
) {
    let window_millis =
        u64::try_from(not_found_confirmation_window.as_millis()).unwrap_or(u64::MAX);
    schedule_artifact_cleanup_request(None, uris, unix_time_millis().saturating_add(window_millis));
}

pub(crate) fn artifact_cleanup_confirmation_window(transfer_timeout: Duration) -> Duration {
    transfer_timeout
}

fn schedule_artifact_cleanup_request(
    artifact_dir: Option<PathBuf>,
    uris: Vec<String>,
    not_found_confirmation_until_unix_millis: u64,
) {
    if artifact_dir.is_none() && uris.is_empty() {
        return;
    }
    let artifact_dirs = artifact_dir.into_iter().collect::<Vec<_>>();
    let ownerships = artifact_cleanup_work_units(&artifact_dirs, &uris, &[], 0, 0);
    let request = ArtifactCleanupRequest {
        artifact_dirs,
        uris,
        retry_attempt: 0,
        retry_after_unix_millis: 0,
        not_found_confirmation_until_unix_millis,
        ownerships,
        owner_process_id: String::new(),
        lease_id: None,
        dormant: false,
        uri_prefixes: Vec::new(),
        confirmation_window_millis: 0,
        registered_artifacts: 0,
        registered_bytes: 0,
    };
    match ARTIFACT_CLEANUP_SERVICE.get() {
        Some(Ok(service)) => service.enqueue(request),
        Some(Err(error)) => {
            log::warn!("Spark artifact cleanup service is unavailable: {error}");
        }
        None => {
            log::warn!("Spark artifact cleanup service was not initialized");
        }
    }
}

fn artifact_cleanup_work_units(
    artifact_dirs: &[PathBuf],
    uris: &[String],
    uri_prefixes: &[String],
    registered_artifacts: usize,
    registered_bytes: usize,
) -> usize {
    const BYTE_WORK_UNIT: usize = 1024 * 1024;

    let byte_units = registered_bytes.saturating_add(BYTE_WORK_UNIT - 1) / BYTE_WORK_UNIT;
    artifact_dirs
        .len()
        .saturating_add(uris.len())
        .saturating_add(uri_prefixes.len())
        .saturating_add(registered_artifacts)
        .saturating_add(byte_units)
        .max(1)
}

#[derive(Serialize, Deserialize)]
struct ArtifactCleanupRequest {
    artifact_dirs: Vec<PathBuf>,
    uris: Vec<String>,
    #[serde(default)]
    retry_attempt: u32,
    #[serde(default)]
    retry_after_unix_millis: u64,
    #[serde(default)]
    not_found_confirmation_until_unix_millis: u64,
    #[serde(default = "default_artifact_cleanup_ownerships")]
    ownerships: usize,
    #[serde(default)]
    owner_process_id: String,
    #[serde(default)]
    lease_id: Option<String>,
    #[serde(default)]
    dormant: bool,
    #[serde(default)]
    uri_prefixes: Vec<String>,
    #[serde(default)]
    confirmation_window_millis: u64,
    #[serde(default)]
    registered_artifacts: usize,
    #[serde(default)]
    registered_bytes: usize,
}

fn default_artifact_cleanup_ownerships() -> usize {
    1
}

impl ArtifactCleanupRequest {
    fn is_ready(&self) -> bool {
        unix_time_millis() >= self.retry_after_unix_millis
    }

    fn defer_retry(&mut self) {
        const RETRY_BASE: Duration = Duration::from_millis(100);
        const RETRY_MAX: Duration = Duration::from_secs(30);

        self.retry_attempt = self.retry_attempt.saturating_add(1);
        let shift = self.retry_attempt.saturating_sub(1).min(9);
        let delay = RETRY_BASE.saturating_mul(1_u32 << shift).min(RETRY_MAX);
        let delay_millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        self.retry_after_unix_millis = unix_time_millis().saturating_add(delay_millis);
    }

    fn ownerships(&self) -> usize {
        self.ownerships.max(1)
    }
}

fn unix_time_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

enum ArtifactCleanupJournalCommand {
    Persist(ArtifactCleanupRequest),
    PersistDormant {
        request: ArtifactCleanupRequest,
        result: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Activate {
        lease_id: String,
        registered_artifacts: usize,
        registered_bytes: usize,
    },
    Flush {
        result: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

const ARTIFACT_CLEANUP_BACKLOG_HEALTH_LIMIT: usize = 4096;

struct ArtifactCleanupProcessReservation {
    budget: Arc<SparkArtifactProcessBudget>,
    reservation: SparkArtifactProcessReservation,
}

struct ArtifactCleanupState {
    pending_ownerships: AtomicUsize,
    activation_slots: AtomicUsize,
    journal_failures: AtomicUsize,
    workers_healthy: AtomicBool,
    process_reservations: Mutex<HashMap<String, ArtifactCleanupProcessReservation>>,
}

impl ArtifactCleanupState {
    fn new(pending_ownerships: usize) -> Self {
        Self {
            pending_ownerships: AtomicUsize::new(pending_ownerships),
            activation_slots: AtomicUsize::new(0),
            journal_failures: AtomicUsize::new(0),
            workers_healthy: AtomicBool::new(true),
            process_reservations: Mutex::new(HashMap::new()),
        }
    }

    fn reserve(&self, ownerships: usize) {
        let ownerships = ownerships.max(1);
        let previous = self
            .pending_ownerships
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                Some(pending.saturating_add(ownerships))
            })
            .unwrap_or_else(|pending| pending);
        if previous.checked_add(ownerships).is_none() {
            log::error!("Spark artifact cleanup pending ownership count overflow");
        }
    }

    fn complete(&self, ownerships: usize) {
        let ownerships = ownerships.max(1);
        let previous = self
            .pending_ownerships
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                Some(pending.saturating_sub(ownerships))
            })
            .unwrap_or_else(|pending| pending);
        if previous < ownerships {
            log::error!(
                "Spark artifact cleanup pending ownership count underflow: pending={previous}, completed={ownerships}"
            );
        }
    }

    fn reserve_activation_slot(&self) -> SparkResult<()> {
        self.activation_slots
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |slots| {
                (slots < ARTIFACT_CLEANUP_BACKLOG_HEALTH_LIMIT).then_some(slots + 1)
            })
            .map(|_| ())
            .map_err(|slots| {
                SparkError::internal(format!(
                    "Spark artifact cleanup activation capacity is exhausted: {slots} reserved slots"
                ))
            })
    }

    fn release_activation_slot(&self) {
        let previous = self
            .activation_slots
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |slots| {
                Some(slots.saturating_sub(1))
            })
            .unwrap_or_else(|slots| slots);
        if previous == 0 {
            log::error!("Spark artifact cleanup activation slot underflow");
        }
    }

    fn note_journal_failure(&self) {
        self.journal_failures.fetch_add(1, Ordering::SeqCst);
    }

    fn resolve_journal_failure(&self) {
        let previous = self
            .journal_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |failures| {
                Some(failures.saturating_sub(1))
            })
            .unwrap_or_else(|failures| failures);
        if previous == 0 {
            log::error!("Spark artifact cleanup journal failure count underflow");
        }
    }

    fn mark_workers_unhealthy(&self) {
        self.workers_healthy.store(false, Ordering::SeqCst);
    }

    fn ensure_capacity(&self) -> SparkResult<()> {
        if !self.workers_healthy.load(Ordering::SeqCst) {
            return Err(SparkError::internal(
                "Spark artifact cleanup workers are unavailable",
            ));
        }
        if self.journal_failures.load(Ordering::SeqCst) != 0 {
            return Err(SparkError::internal(
                "Spark artifact cleanup journal is unavailable",
            ));
        }
        let pending = self.pending_ownerships.load(Ordering::SeqCst);
        if pending >= ARTIFACT_CLEANUP_BACKLOG_HEALTH_LIMIT {
            return Err(SparkError::internal(format!(
                "Spark artifact cleanup backlog is unhealthy: {pending} pending ownerships"
            )));
        }
        Ok(())
    }

    fn hold_process_reservation(
        &self,
        lease_id: String,
        budget: Arc<SparkArtifactProcessBudget>,
        reservation: SparkArtifactProcessReservation,
    ) {
        let mut reservations = self.process_reservations.lock().unwrap_or_else(|error| {
            log::warn!("recovering poisoned Spark artifact cleanup reservation state: {error}");
            error.into_inner()
        });
        reservations.insert(
            lease_id,
            ArtifactCleanupProcessReservation {
                budget,
                reservation,
            },
        );
    }

    fn release_process_reservation(&self, lease_id: &str) {
        let reservation = self
            .process_reservations
            .lock()
            .unwrap_or_else(|error| {
                log::warn!("recovering poisoned Spark artifact cleanup reservation state: {error}");
                error.into_inner()
            })
            .remove(lease_id);
        if let Some(ArtifactCleanupProcessReservation {
            budget,
            reservation,
        }) = reservation
        {
            budget.release(reservation);
        }
    }
}

struct ArtifactCleanupService {
    cleanup_sender: SyncSender<ArtifactCleanupRequest>,
    journal_sender: Sender<ArtifactCleanupJournalCommand>,
    state: Arc<ArtifactCleanupState>,
    process_id: String,
    _owner_lock: std::fs::File,
}

impl ArtifactCleanupService {
    fn enqueue(&self, mut request: ArtifactCleanupRequest) {
        request.owner_process_id.clone_from(&self.process_id);
        self.state.reserve(request.ownerships());
        let pending = match self.cleanup_sender.try_send(request) {
            Ok(()) => return,
            Err(TrySendError::Full(pending)) => pending,
            Err(TrySendError::Disconnected(pending)) => {
                self.state.mark_workers_unhealthy();
                pending
            }
        };
        if let Err(error) = self
            .journal_sender
            .send(ArtifactCleanupJournalCommand::Persist(pending))
        {
            log::warn!("Spark artifact cleanup journal worker is unavailable");
            self.state.mark_workers_unhealthy();
            let _ = error;
        }
    }

    fn persist_dormant(
        &self,
        mut request: ArtifactCleanupRequest,
    ) -> SparkResult<tokio::sync::oneshot::Receiver<Result<(), String>>> {
        self.state.reserve_activation_slot()?;
        request.owner_process_id.clone_from(&self.process_id);
        let ownerships = request.ownerships();
        self.state.reserve(ownerships);
        let (result, output) = tokio::sync::oneshot::channel();
        if self
            .journal_sender
            .send(ArtifactCleanupJournalCommand::PersistDormant { request, result })
            .is_err()
        {
            self.state.complete(ownerships);
            self.state.release_activation_slot();
            self.state.mark_workers_unhealthy();
            return Err(SparkError::internal(
                "Spark artifact cleanup journal worker is unavailable",
            ));
        }
        Ok(output)
    }

    fn activate(&self, lease_id: String, registered_artifacts: usize, registered_bytes: usize) {
        if self
            .journal_sender
            .send(ArtifactCleanupJournalCommand::Activate {
                lease_id,
                registered_artifacts,
                registered_bytes,
            })
            .is_err()
        {
            self.state.mark_workers_unhealthy();
            log::error!("failed to activate durable Spark artifact cleanup lease");
        }
    }

    fn flush_journal(&self) -> SparkResult<tokio::sync::oneshot::Receiver<Result<(), String>>> {
        let (result, output) = tokio::sync::oneshot::channel();
        if self
            .journal_sender
            .send(ArtifactCleanupJournalCommand::Flush { result })
            .is_err()
        {
            self.state.mark_workers_unhealthy();
            return Err(SparkError::internal(
                "Spark artifact cleanup journal worker is unavailable",
            ));
        }
        Ok(output)
    }
}

static ARTIFACT_CLEANUP_SERVICE: OnceLock<Result<ArtifactCleanupService, String>> = OnceLock::new();
static ARTIFACT_CLEANUP_JOURNAL_PATH: OnceLock<PathBuf> = OnceLock::new();

pub(crate) fn initialize_artifact_cleanup(journal: &Path) -> SparkResult<()> {
    let configured = ARTIFACT_CLEANUP_JOURNAL_PATH.get_or_init(|| journal.to_path_buf());
    if configured != journal {
        return Err(SparkError::invalid(format!(
            "Spark artifact cleanup journal conflicts with the process-wide configuration: configured={}, requested={}",
            configured.display(),
            journal.display()
        )));
    }
    match ARTIFACT_CLEANUP_SERVICE.get_or_init(|| start_artifact_cleanup_service(configured)) {
        Ok(_) => Ok(()),
        Err(error) => Err(SparkError::internal(format!(
            "failed to start the Spark artifact cleanup service: {error}"
        ))),
    }
}

async fn persist_dormant_artifact_cleanup_lease(
    lease_id: String,
    artifact_dir: PathBuf,
    uri_prefixes: Vec<String>,
    transfer_timeout: Duration,
) -> SparkResult<()> {
    let service = match ARTIFACT_CLEANUP_SERVICE.get() {
        Some(Ok(service)) => service,
        Some(Err(error)) => {
            return Err(SparkError::internal(format!(
                "Spark artifact cleanup service is unavailable: {error}"
            )));
        }
        None => {
            return Err(SparkError::internal(
                "Spark artifact cleanup service was not initialized",
            ));
        }
    };
    let confirmation_window = artifact_cleanup_confirmation_window(transfer_timeout);
    let confirmation_window_millis =
        u64::try_from(confirmation_window.as_millis()).unwrap_or(u64::MAX);
    let artifact_dirs = vec![artifact_dir];
    let ownerships = artifact_cleanup_work_units(&artifact_dirs, &[], &uri_prefixes, 0, 0);
    let result = service.persist_dormant(ArtifactCleanupRequest {
        artifact_dirs,
        uris: Vec::new(),
        retry_attempt: 0,
        retry_after_unix_millis: 0,
        not_found_confirmation_until_unix_millis: 0,
        ownerships,
        owner_process_id: String::new(),
        lease_id: Some(lease_id),
        dormant: true,
        uri_prefixes,
        confirmation_window_millis,
        registered_artifacts: 0,
        registered_bytes: 0,
    })?;
    result
        .await
        .map_err(|error| {
            SparkError::internal(format!(
                "Spark artifact cleanup journal stopped before persisting the lease: {error}"
            ))
        })?
        .map_err(|error| {
            SparkError::internal(format!(
                "failed to persist Spark artifact cleanup lease: {error}"
            ))
        })
}

fn activate_artifact_cleanup_lease(
    lease_id: String,
    registered_artifacts: usize,
    registered_bytes: usize,
    budget: Arc<SparkArtifactProcessBudget>,
    reservation: SparkArtifactProcessReservation,
) {
    match ARTIFACT_CLEANUP_SERVICE.get() {
        Some(Ok(service)) => {
            service
                .state
                .hold_process_reservation(lease_id.clone(), budget, reservation);
            service.activate(lease_id, registered_artifacts, registered_bytes);
        }
        Some(Err(error)) => {
            log::error!("Spark artifact cleanup service is unavailable: {error}")
        }
        None => log::error!("Spark artifact cleanup service was not initialized"),
    }
}

pub(crate) fn ensure_artifact_cleanup_capacity() -> SparkResult<()> {
    match ARTIFACT_CLEANUP_SERVICE.get() {
        Some(Ok(service)) => service.state.ensure_capacity(),
        Some(Err(error)) => Err(SparkError::internal(format!(
            "Spark artifact cleanup service is unavailable: {error}"
        ))),
        None => Err(SparkError::internal(
            "Spark artifact cleanup service was not initialized",
        )),
    }
}

pub(crate) async fn flush_artifact_cleanup_journal(timeout: Duration) -> SparkResult<()> {
    let output = match ARTIFACT_CLEANUP_SERVICE.get() {
        Some(Ok(service)) => service.flush_journal()?,
        Some(Err(error)) => {
            return Err(SparkError::internal(format!(
                "Spark artifact cleanup service is unavailable: {error}"
            )));
        }
        None => {
            return Err(SparkError::internal(
                "Spark artifact cleanup service was not initialized",
            ));
        }
    };
    tokio::time::timeout(timeout, output)
        .await
        .map_err(|_| {
            SparkError::deadline_exceeded(format!(
                "timed out after {timeout:?} flushing the Spark artifact cleanup journal"
            ))
        })?
        .map_err(|error| {
            SparkError::internal(format!(
                "Spark artifact cleanup journal stopped before the flush barrier: {error}"
            ))
        })?
        .map_err(|error| {
            SparkError::internal(format!(
                "failed to flush Spark artifact cleanup journal: {error}"
            ))
        })
}

fn start_artifact_cleanup_service(journal: &Path) -> Result<ArtifactCleanupService, String> {
    const CLEANUP_QUEUE_CAPACITY: usize = 256;
    const CLEANUP_WORKERS: usize = 4;

    prepare_artifact_cleanup_journal(journal)?;
    let journal = journal.to_path_buf();
    let process_id = Uuid::new_v4().to_string();
    let owner_lock = acquire_artifact_cleanup_owner_lock(&journal, &process_id)?;
    let recovered_ownerships = recover_persisted_artifact_cleanup(&journal, &process_id, None)?;
    let state = Arc::new(ArtifactCleanupState::new(recovered_ownerships));
    let (cleanup_sender, receiver) = std::sync::mpsc::sync_channel(CLEANUP_QUEUE_CAPACITY);
    let (journal_sender, journal_receiver) = std::sync::mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => Arc::new(runtime),
        Err(error) => {
            return Err(format!(
                "failed to create Spark artifact cleanup runtime: {error}"
            ));
        }
    };
    let journal_path = journal.clone();
    let journal_state = Arc::clone(&state);
    std::thread::Builder::new()
        .name("sail-artifact-cleanup-journal".to_string())
        .spawn(move || {
            run_artifact_cleanup_journal(journal_receiver, &journal_path, &journal_state)
        })
        .map_err(|error| format!("failed to start Spark artifact cleanup journal: {error}"))?;
    let mut started = 0;
    for worker in 0..CLEANUP_WORKERS {
        let receiver = Arc::clone(&receiver);
        let runtime = Arc::clone(&runtime);
        let journal_sender = journal_sender.clone();
        let journal = journal.clone();
        let state = Arc::clone(&state);
        let process_id = process_id.clone();
        let recover_foreign_owners = worker == 0;
        match std::thread::Builder::new()
            .name(format!("sail-artifact-cleanup-{worker}"))
            .spawn(move || {
                let mut last_recovery = Instant::now();
                loop {
                    let received = {
                        let receiver = match receiver.lock() {
                            Ok(receiver) => receiver,
                            Err(error) => error.into_inner(),
                        };
                        receiver.recv_timeout(Duration::from_millis(100))
                    };
                    match received {
                        Ok(request) => {
                            run_queued_artifact_cleanup(&runtime, &journal_sender, &state, request)
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            state.mark_workers_unhealthy();
                            while let Some(claimed) =
                                take_persisted_artifact_cleanup(&journal, &process_id)
                            {
                                run_claimed_artifact_cleanup(&runtime, &journal, &state, claimed);
                            }
                            break;
                        }
                    }
                    if let Some(claimed) = take_persisted_artifact_cleanup(&journal, &process_id) {
                        run_claimed_artifact_cleanup(&runtime, &journal, &state, claimed);
                    }
                    if recover_foreign_owners && last_recovery.elapsed() >= Duration::from_secs(1) {
                        match recover_persisted_artifact_cleanup(
                            &journal,
                            &process_id,
                            Some(&state),
                        ) {
                            Ok(_) => {}
                            Err(error) => log::warn!(
                                "failed to recover Spark artifact cleanup ownership: {error}"
                            ),
                        }
                        last_recovery = Instant::now();
                    }
                }
            }) {
            Ok(_) => started += 1,
            Err(error) => {
                log::warn!("failed to start Spark artifact cleanup worker: {error}");
            }
        }
    }
    if started == 0 {
        return Err("failed to start any Spark artifact cleanup workers".to_string());
    }
    Ok(ArtifactCleanupService {
        cleanup_sender,
        journal_sender,
        state,
        process_id,
        _owner_lock: owner_lock,
    })
}

fn run_artifact_cleanup_journal(
    receiver: Receiver<ArtifactCleanupJournalCommand>,
    journal: &Path,
    state: &ArtifactCleanupState,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            ArtifactCleanupJournalCommand::Persist(request) => {
                persist_artifact_cleanup_until_success(Some(journal), state, &request);
            }
            ArtifactCleanupJournalCommand::PersistDormant { request, result } => {
                persist_dormant_cleanup_request_until_success(journal, state, &request);
                let _ = result.send(Ok(()));
            }
            ArtifactCleanupJournalCommand::Activate {
                lease_id,
                registered_artifacts,
                registered_bytes,
            } => {
                activate_persisted_cleanup_lease_until_success(
                    journal,
                    state,
                    &lease_id,
                    registered_artifacts,
                    registered_bytes,
                );
                state.release_activation_slot();
            }
            ArtifactCleanupJournalCommand::Flush { result } => {
                let _ = result.send(sync_artifact_cleanup_journal(journal));
            }
        }
    }
}

fn prepare_artifact_cleanup_journal(journal: &Path) -> Result<(), String> {
    if !journal.is_absolute() {
        return Err(format!(
            "Spark artifact cleanup journal path must be absolute: {}",
            journal.display()
        ));
    }
    match std::fs::create_dir(journal) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.to_string()),
    }
    let metadata = std::fs::symlink_metadata(journal).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "Spark artifact cleanup journal is not a private directory: {}",
            journal.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let owner_probe =
            tempfile::NamedTempFile::new_in(journal).map_err(|error| error.to_string())?;
        let process_uid = owner_probe
            .as_file()
            .metadata()
            .map_err(|error| error.to_string())?
            .uid();
        if metadata.uid() != process_uid {
            return Err(format!(
                "Spark artifact cleanup journal has an unexpected owner: {}",
                journal.display()
            ));
        }
        std::fs::set_permissions(journal, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn artifact_cleanup_owner_lock_path(journal: &Path, process_id: &str) -> PathBuf {
    journal.join(format!(".owner-{}.lock", sha256_hex(process_id.as_bytes())))
}

fn open_artifact_cleanup_owner_lock(
    journal: &Path,
    process_id: &str,
) -> Result<std::fs::File, String> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(artifact_cleanup_owner_lock_path(journal, process_id))
        .map_err(|error| error.to_string())
}

fn acquire_artifact_cleanup_owner_lock(
    journal: &Path,
    process_id: &str,
) -> Result<std::fs::File, String> {
    let owner_lock = open_artifact_cleanup_owner_lock(journal, process_id)?;
    FileExt::lock_exclusive(&owner_lock).map_err(|error| error.to_string())?;
    owner_lock.sync_all().map_err(|error| error.to_string())?;
    sync_artifact_cleanup_journal(journal)?;
    Ok(owner_lock)
}

fn try_acquire_artifact_cleanup_owner_lock(
    journal: &Path,
    process_id: &str,
) -> Result<Option<std::fs::File>, String> {
    let owner_lock = open_artifact_cleanup_owner_lock(journal, process_id)?;
    match FileExt::try_lock_exclusive(&owner_lock) {
        Ok(()) => Ok(Some(owner_lock)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn recover_persisted_artifact_cleanup(
    journal: &Path,
    process_id: &str,
    state: Option<&ArtifactCleanupState>,
) -> Result<usize, String> {
    let mut ownerships = 0_usize;
    let entries = std::fs::read_dir(journal)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    for entry in entries {
        let path = entry.path();
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name.starts_with(".pending-") {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("json")
            && !name.starts_with(".work-")
            && !name.starts_with(".recovery-")
        {
            continue;
        }
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.to_string()),
        };
        let mut request = match serde_json::from_reader::<_, ArtifactCleanupRequest>(file) {
            Ok(request) => request,
            Err(error) => {
                log::warn!(
                    "removing invalid Spark artifact cleanup journal entry {}: {error}",
                    path.display()
                );
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(remove_error) if remove_error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(remove_error) => return Err(remove_error.to_string()),
                }
                continue;
            }
        };
        if request.owner_process_id == process_id {
            continue;
        }
        let Some(_previous_owner_lock) =
            try_acquire_artifact_cleanup_owner_lock(journal, &request.owner_process_id)?
        else {
            continue;
        };
        let claimed = journal.join(format!(".recovery-{}", Uuid::new_v4()));
        match std::fs::rename(&path, &claimed) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.to_string()),
        }
        request.owner_process_id = process_id.to_string();
        request.dormant = false;
        request.not_found_confirmation_until_unix_millis =
            unix_time_millis().saturating_add(request.confirmation_window_millis);
        let request_ownerships = artifact_cleanup_work_units(
            &request.artifact_dirs,
            &request.uris,
            &request.uri_prefixes,
            request.registered_artifacts,
            request.registered_bytes,
        );
        request.ownerships = request_ownerships;
        if let Some(state) = state {
            state.reserve(request_ownerships);
            replace_claimed_artifact_cleanup_until_success(journal, state, &claimed, &request);
        } else {
            ownerships = ownerships.saturating_add(request_ownerships);
            replace_claimed_artifact_cleanup(journal, &claimed, &request)?;
        }
    }
    remove_unreferenced_artifact_cleanup_owner_locks(journal)?;
    sync_artifact_cleanup_journal(journal)?;
    Ok(ownerships)
}

fn remove_unreferenced_artifact_cleanup_owner_locks(journal: &Path) -> Result<(), String> {
    let mut referenced_locks = HashSet::new();
    let entries = std::fs::read_dir(journal)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    for entry in &entries {
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if path.extension().and_then(|extension| extension.to_str()) != Some("json")
            && !name.starts_with(".work-")
            && !name.starts_with(".recovery-")
        {
            continue;
        }
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let Ok(request) = serde_json::from_reader::<_, ArtifactCleanupRequest>(file) else {
            continue;
        };
        referenced_locks.insert(artifact_cleanup_owner_lock_path(
            journal,
            &request.owner_process_id,
        ));
    }

    let mut removed = false;
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(".owner-") || path.extension().and_then(|x| x.to_str()) != Some("lock")
        {
            continue;
        }
        if referenced_locks.contains(&path) {
            continue;
        }
        let owner_lock = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(owner_lock) => owner_lock,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.to_string()),
        };
        match FileExt::try_lock_exclusive(&owner_lock) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.to_string()),
        }
        drop(owner_lock);
        match std::fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    if removed {
        sync_artifact_cleanup_journal(journal)?;
    }
    Ok(())
}

fn persist_artifact_cleanup(
    journal: &Path,
    request: &ArtifactCleanupRequest,
) -> Result<(), String> {
    let mut pending = tempfile::Builder::new()
        .prefix(".pending-")
        .tempfile_in(journal)
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(pending.as_file_mut(), request).map_err(|error| error.to_string())?;
    pending
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    let path = journal.join(format!("{}.json", Uuid::new_v4()));
    pending
        .persist_noclobber(path)
        .map_err(|error| error.to_string())?;
    sync_artifact_cleanup_journal(journal)?;
    Ok(())
}

fn artifact_cleanup_lease_path(journal: &Path, lease_id: &str) -> PathBuf {
    journal.join(format!(".lease-{}.json", sha256_hex(lease_id.as_bytes())))
}

fn persist_dormant_cleanup_request(
    journal: &Path,
    request: &ArtifactCleanupRequest,
) -> Result<(), String> {
    let lease_id = request
        .lease_id
        .as_deref()
        .ok_or_else(|| "Spark artifact cleanup lease ID is missing".to_string())?;
    let target = artifact_cleanup_lease_path(journal, lease_id);
    let mut pending = tempfile::Builder::new()
        .prefix(".pending-")
        .tempfile_in(journal)
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(pending.as_file_mut(), request).map_err(|error| error.to_string())?;
    pending
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    pending
        .persist_noclobber(target)
        .map_err(|error| error.to_string())?;
    sync_artifact_cleanup_journal(journal)
}

fn persist_dormant_cleanup_request_until_success(
    journal: &Path,
    state: &ArtifactCleanupState,
    request: &ArtifactCleanupRequest,
) {
    let mut retry_delay = Duration::from_millis(10);
    let mut failure_recorded = false;
    loop {
        match persist_dormant_cleanup_request(journal, request) {
            Ok(()) => {
                if failure_recorded {
                    state.resolve_journal_failure();
                }
                return;
            }
            Err(error) => {
                if !failure_recorded {
                    state.note_journal_failure();
                    failure_recorded = true;
                }
                log::warn!("failed to persist dormant Spark artifact cleanup lease: {error}");
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

fn activate_persisted_cleanup_lease_until_success(
    journal: &Path,
    state: &ArtifactCleanupState,
    lease_id: &str,
    registered_artifacts: usize,
    registered_bytes: usize,
) {
    let path = artifact_cleanup_lease_path(journal, lease_id);
    let mut retry_delay = Duration::from_millis(10);
    let mut failure_recorded = false;
    let mut request = loop {
        let output: Result<ArtifactCleanupRequest, String> = std::fs::File::open(&path)
            .map_err(|error| error.to_string())
            .and_then(|file| serde_json::from_reader(file).map_err(|error| error.to_string()));
        match output {
            Ok(request) => break request,
            Err(error) => {
                if !failure_recorded {
                    state.note_journal_failure();
                    failure_recorded = true;
                }
                log::warn!("failed to read Spark artifact cleanup lease: {error}");
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    };
    request.registered_artifacts = registered_artifacts;
    request.registered_bytes = registered_bytes;
    let work_units = artifact_cleanup_work_units(
        &request.artifact_dirs,
        &request.uris,
        &request.uri_prefixes,
        registered_artifacts,
        registered_bytes,
    );
    if work_units > request.ownerships() {
        state.reserve(work_units - request.ownerships());
    }
    request.ownerships = work_units;
    request.dormant = false;
    request.not_found_confirmation_until_unix_millis =
        unix_time_millis().saturating_add(request.confirmation_window_millis);
    loop {
        match replace_cleanup_request_at_path(journal, &path, &request) {
            Ok(()) => {
                if failure_recorded {
                    state.resolve_journal_failure();
                }
                return;
            }
            Err(error) => {
                if !failure_recorded {
                    state.note_journal_failure();
                    failure_recorded = true;
                }
                log::warn!("failed to activate Spark artifact cleanup lease: {error}");
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

fn replace_cleanup_request_at_path(
    journal: &Path,
    path: &Path,
    request: &ArtifactCleanupRequest,
) -> Result<(), String> {
    let mut pending = tempfile::Builder::new()
        .prefix(".pending-")
        .tempfile_in(journal)
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(pending.as_file_mut(), request).map_err(|error| error.to_string())?;
    pending
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    pending.persist(path).map_err(|error| error.to_string())?;
    sync_artifact_cleanup_journal(journal)
}

fn sync_artifact_cleanup_journal(journal: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        std::fs::File::open(journal)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

struct ClaimedArtifactCleanup {
    request: ArtifactCleanupRequest,
    path: PathBuf,
}

fn take_persisted_artifact_cleanup(
    journal: &Path,
    process_id: &str,
) -> Option<ClaimedArtifactCleanup> {
    let entries = match std::fs::read_dir(journal) {
        Ok(entries) => entries,
        Err(error) => {
            log::warn!("failed to read Spark artifact cleanup journal: {error}");
            return None;
        }
    };
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(error) => {
                log::warn!("failed to inspect Spark artifact cleanup journal entry: {error}");
                continue;
            }
        };
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) => {
                log::warn!(
                    "failed to open Spark artifact cleanup journal entry {}: {error}",
                    path.display()
                );
                continue;
            }
        };
        let request: ArtifactCleanupRequest = match serde_json::from_reader(file) {
            Ok(request) => request,
            Err(error) => {
                log::warn!(
                    "failed to decode Spark artifact cleanup journal entry {}: {error}",
                    path.display()
                );
                if let Err(remove_error) = std::fs::remove_file(&path) {
                    log::warn!(
                        "failed to remove invalid Spark artifact cleanup journal entry {}: {remove_error}",
                        path.display()
                    );
                }
                continue;
            }
        };
        if request.owner_process_id != process_id {
            continue;
        }
        if request.dormant || !request.is_ready() {
            continue;
        }
        let claimed = journal.join(format!(".work-{}", Uuid::new_v4()));
        match std::fs::rename(&path, &claimed) {
            Ok(()) => {
                return Some(ClaimedArtifactCleanup {
                    request,
                    path: claimed,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                log::warn!(
                    "failed to claim Spark artifact cleanup journal entry {}: {error}",
                    path.display()
                );
                continue;
            }
        }
    }
    None
}

fn run_queued_artifact_cleanup(
    runtime: &tokio::runtime::Runtime,
    journal_sender: &Sender<ArtifactCleanupJournalCommand>,
    state: &ArtifactCleanupState,
    request: ArtifactCleanupRequest,
) {
    let ownerships = request.ownerships();
    let lease_id = request.lease_id.clone();
    let Some(mut failed) = run_artifact_cleanup(runtime, request) else {
        state.complete(ownerships);
        if let Some(lease_id) = lease_id {
            state.release_process_reservation(&lease_id);
        }
        return;
    };
    failed.defer_retry();
    if let Err(error) = journal_sender.send(ArtifactCleanupJournalCommand::Persist(failed)) {
        state.mark_workers_unhealthy();
        let _ = error;
        log::error!("Spark artifact cleanup journal worker is unavailable");
    }
}

fn run_claimed_artifact_cleanup(
    runtime: &tokio::runtime::Runtime,
    journal: &Path,
    state: &ArtifactCleanupState,
    claimed: ClaimedArtifactCleanup,
) {
    let ClaimedArtifactCleanup { request, path } = claimed;
    let ownerships = request.ownerships();
    let lease_id = request.lease_id.clone();
    if let Some(mut failed) = run_artifact_cleanup(runtime, request) {
        failed.defer_retry();
        replace_claimed_artifact_cleanup_until_success(journal, state, &path, &failed);
        return;
    }
    let mut retry_delay = Duration::from_millis(10);
    loop {
        match std::fs::remove_file(&path) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                log::warn!(
                    "failed to remove completed Spark artifact cleanup claim {}: {error}",
                    path.display()
                );
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
    if let Err(error) = sync_artifact_cleanup_journal(journal) {
        log::warn!("failed to sync completed Spark artifact cleanup claim: {error}");
    }
    state.complete(ownerships);
    if let Some(lease_id) = lease_id {
        state.release_process_reservation(&lease_id);
    }
}

fn persist_artifact_cleanup_until_success(
    journal: Option<&Path>,
    state: &ArtifactCleanupState,
    request: &ArtifactCleanupRequest,
) {
    let Some(journal) = journal else {
        state.note_journal_failure();
        log::error!("Spark artifact cleanup journal is unavailable");
        return;
    };
    let mut retry_delay = Duration::from_millis(10);
    let mut failure_recorded = false;
    loop {
        match persist_artifact_cleanup(journal, request) {
            Ok(()) => {
                if failure_recorded {
                    state.resolve_journal_failure();
                }
                return;
            }
            Err(error) => {
                if !failure_recorded {
                    state.note_journal_failure();
                    failure_recorded = true;
                }
                log::warn!("failed to persist Spark artifact cleanup request: {error}");
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

fn replace_claimed_artifact_cleanup_until_success(
    journal: &Path,
    state: &ArtifactCleanupState,
    claimed: &Path,
    request: &ArtifactCleanupRequest,
) {
    let mut retry_delay = Duration::from_millis(10);
    let mut failure_recorded = false;
    loop {
        match replace_claimed_artifact_cleanup(journal, claimed, request) {
            Ok(()) => {
                if failure_recorded {
                    state.resolve_journal_failure();
                }
                return;
            }
            Err(error) => {
                if !failure_recorded {
                    state.note_journal_failure();
                    failure_recorded = true;
                }
                log::warn!("failed to persist Spark artifact cleanup retry: {error}");
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

fn replace_claimed_artifact_cleanup(
    journal: &Path,
    claimed: &Path,
    request: &ArtifactCleanupRequest,
) -> Result<(), String> {
    let mut pending = tempfile::Builder::new()
        .prefix(".pending-")
        .tempfile_in(journal)
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(pending.as_file_mut(), request).map_err(|error| error.to_string())?;
    pending
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    pending
        .persist(claimed)
        .map_err(|error| error.to_string())?;
    let retry = journal.join(format!("{}.json", Uuid::new_v4()));
    std::fs::rename(claimed, retry).map_err(|error| error.to_string())?;
    sync_artifact_cleanup_journal(journal)
}

fn run_artifact_cleanup(
    runtime: &tokio::runtime::Runtime,
    request: ArtifactCleanupRequest,
) -> Option<ArtifactCleanupRequest> {
    let ArtifactCleanupRequest {
        artifact_dirs,
        uris,
        retry_attempt,
        retry_after_unix_millis,
        not_found_confirmation_until_unix_millis,
        ownerships,
        owner_process_id,
        lease_id,
        dormant,
        uri_prefixes,
        confirmation_window_millis,
        registered_artifacts,
        registered_bytes,
    } = request;
    let mut failed_dirs = Vec::new();
    for dir in artifact_dirs {
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                log::warn!(
                    "Failed to remove Spark session artifact directory {}: {error}",
                    dir.display()
                );
                failed_dirs.push(dir);
            }
        }
    }

    let failed_uris = if uris.is_empty() {
        Vec::new()
    } else {
        let retry_uris = uris.clone();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(cleanup_artifact_uris(
                uris,
                not_found_confirmation_until_unix_millis,
            ))
        })) {
            Ok(failed) => failed,
            Err(_) => {
                log::warn!("Spark artifact object cleanup runtime stopped unexpectedly");
                retry_uris
            }
        }
    };
    let failed_uri_prefixes = if uri_prefixes.is_empty() {
        Vec::new()
    } else {
        let retry_prefixes = uri_prefixes.clone();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(cleanup_artifact_prefixes(
                uri_prefixes,
                not_found_confirmation_until_unix_millis,
            ))
        })) {
            Ok(failed) => failed,
            Err(_) => {
                log::warn!("Spark artifact prefix cleanup runtime stopped unexpectedly");
                retry_prefixes
            }
        }
    };
    if failed_dirs.is_empty() && failed_uris.is_empty() && failed_uri_prefixes.is_empty() {
        None
    } else {
        Some(ArtifactCleanupRequest {
            artifact_dirs: failed_dirs,
            uris: failed_uris,
            retry_attempt,
            retry_after_unix_millis,
            not_found_confirmation_until_unix_millis,
            ownerships,
            owner_process_id,
            lease_id,
            dormant,
            uri_prefixes: failed_uri_prefixes,
            confirmation_window_millis,
            registered_artifacts,
            registered_bytes,
        })
    }
}

async fn cleanup_artifact_uris(
    uris: Vec<String>,
    not_found_confirmation_until_unix_millis: u64,
) -> Vec<String> {
    const CLEANUP_CONCURRENCY: usize = 16;
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

    stream::iter(uris)
        .map(|uri| async move {
            match tokio::time::timeout(
                CLEANUP_TIMEOUT,
                cleanup_artifact_uri(&uri, not_found_confirmation_until_unix_millis),
            )
            .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) => {
                    log::warn!("Failed to clean up Spark artifact: {error}");
                    Some(uri)
                }
                Err(_) => {
                    log::warn!("Spark artifact object cleanup timed out");
                    Some(uri)
                }
            }
        })
        .buffer_unordered(CLEANUP_CONCURRENCY)
        .filter_map(|failed| async move { failed })
        .collect()
        .await
}

async fn cleanup_artifact_prefixes(
    uri_prefixes: Vec<String>,
    confirmation_until_unix_millis: u64,
) -> Vec<String> {
    const CLEANUP_CONCURRENCY: usize = 4;
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

    stream::iter(uri_prefixes)
        .map(|uri| async move {
            match tokio::time::timeout(CLEANUP_TIMEOUT, cleanup_artifact_prefix(&uri)).await {
                Ok(Ok(())) if unix_time_millis() >= confirmation_until_unix_millis => None,
                Ok(Ok(())) => Some(uri),
                Ok(Err(error)) => {
                    log::warn!("Failed to clean up Spark artifact prefix: {error}");
                    Some(uri)
                }
                Err(_) => {
                    log::warn!("Spark artifact prefix cleanup timed out");
                    Some(uri)
                }
            }
        })
        .buffer_unordered(CLEANUP_CONCURRENCY)
        .filter_map(|failed| async move { failed })
        .collect()
        .await
}

async fn cleanup_artifact_prefix(uri: &str) -> Result<(), String> {
    let url = Url::parse(uri).map_err(|error| format!("invalid artifact prefix URI: {error}"))?;
    let (_scheme, prefix) = ObjectStoreScheme::parse(&url)
        .map_err(|error| format!("invalid artifact prefix path: {error}"))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|error| format!("failed to create artifact prefix object store: {error}"))?;
    let mut objects = store.list(Some(&prefix));
    while let Some(object) = objects.next().await {
        let object = object.map_err(|error| format!("failed to list artifact prefix: {error}"))?;
        match store.delete(&object.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(format!(
                    "failed to delete object under artifact prefix: {error}"
                ));
            }
        }
    }
    Ok(())
}

async fn cleanup_artifact_uri(
    uri: &str,
    not_found_confirmation_until_unix_millis: u64,
) -> Result<(), String> {
    let url =
        Url::parse(uri).map_err(|e| format!("invalid artifact object-store URI {uri}: {e}"))?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url)
        .map_err(|e| format!("invalid artifact object-store path {uri}: {e}"))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|e| format!("failed to create artifact object store {uri}: {e}"))?;
    match store.delete(&path).await {
        Ok(()) if unix_time_millis() < not_found_confirmation_until_unix_millis => {
            Err("artifact object deletion is awaiting late-commit confirmation".to_string())
        }
        Ok(()) => Ok(()),
        Err(object_store::Error::NotFound { .. })
            if unix_time_millis() < not_found_confirmation_until_unix_millis =>
        {
            Err(
                "artifact object is not visible during the late-commit confirmation window"
                    .to_string(),
            )
        }
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("failed to delete artifact {uri}: {e}")),
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;

    fn test_dormant_cleanup_request(
        lease_id: String,
        confirmation_window_millis: u64,
    ) -> ArtifactCleanupRequest {
        ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("missing-session-artifacts")],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: Some(lease_id),
            dormant: true,
            uri_prefixes: Vec::new(),
            confirmation_window_millis,
            registered_artifacts: 0,
            registered_bytes: 0,
        }
    }

    fn spawn_test_cleanup_journal(
        journal: PathBuf,
        state: Arc<ArtifactCleanupState>,
    ) -> (
        Sender<ArtifactCleanupJournalCommand>,
        std::thread::JoinHandle<()>,
    ) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            run_artifact_cleanup_journal(receiver, &journal, &state);
        });
        (sender, worker)
    }

    async fn await_test_cleanup_journal_result(
        output: tokio::sync::oneshot::Receiver<Result<(), String>>,
        description: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let result = tokio::time::timeout(Duration::from_secs(1), output)
            .await
            .map_err(|_| std::io::Error::other(format!("timed out {description}")))?
            .map_err(|_| std::io::Error::other(format!("journal stopped {description}")))?;
        result.map_err(|error| std::io::Error::other(format!("failed {description}: {error}")))?;
        Ok(())
    }

    fn test_runtime() -> RuntimeHandle {
        let handle = tokio::runtime::Handle::current();
        RuntimeHandle::new(handle.clone(), handle)
    }

    fn test_artifact_options(runtime: RuntimeHandle) -> SparkArtifactOptions {
        SparkArtifactOptions {
            root: None,
            inline_max_bytes: usize::MAX,
            max_bytes: usize::MAX,
            max_chunks: usize::MAX,
            rpc_max_artifacts: usize::MAX,
            rpc_max_chunks: usize::MAX,
            rpc_max_bytes: usize::MAX,
            session_max_artifacts: usize::MAX,
            session_max_bytes: usize::MAX,
            chunk_timeout: Duration::from_secs(1),
            transfer_timeout: Duration::from_secs(300),
            store_uri: None,
            store_namespace: "test".to_string(),
            runtime,
        }
    }

    fn test_artifact_registry(
        session_id: &str,
        process_budget: Arc<SparkArtifactProcessBudget>,
    ) -> SparkArtifactRegistry {
        SparkArtifactRegistry::new(
            session_id.to_string(),
            "user".to_string(),
            test_artifact_options(test_runtime()),
            process_budget,
        )
    }

    fn inline_python_artifact(name: &str, size: usize) -> PySparkPythonArtifact {
        PySparkPythonArtifact {
            scope_id: "01".repeat(32),
            name: name.to_string(),
            python_path: format!("/tmp/{name}"),
            data: Some(Arc::from(vec![0_u8; size])),
            uri: None,
            sha256: "02".repeat(32),
            size: size as u64,
            kind: sail_python_udf::config::PySparkArtifactKind::PyFile,
        }
    }

    fn process_usage(budget: &SparkArtifactProcessBudget) -> (usize, usize, usize) {
        let usage = budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (usage.artifacts, usage.bytes, usage.inline_bytes)
    }

    #[tokio::test]
    async fn local_relation_cache_reads_exact_size_and_validates_hash()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let mut options = test_artifact_options(test_runtime());
        options.root = Some(root.path().to_path_buf());
        let registry = Arc::new(SparkArtifactRegistry::new(
            "session".to_string(),
            "user".to_string(),
            options,
            Arc::new(SparkArtifactProcessBudget::new(
                usize::MAX,
                usize::MAX,
                usize::MAX,
            )),
        ));
        let cache_dir = registry.artifact_dir()?.join("cache");
        std::fs::create_dir(&cache_dir)?;
        let data = b"content";
        let hash = sha256_hex(data);
        let path = cache_dir.join(&hash);
        std::fs::write(&path, data)?;
        registry.register_artifact(
            format!("cache/{hash}"),
            data.len(),
            Some(hash.clone()),
            None,
        )?;
        let cache: Arc<dyn LocalRelationCache> = Arc::new(SparkLocalRelationCache {
            artifacts: registry,
        });

        assert_eq!(
            cache.cached_local_relation_block_size(&hash).await?,
            data.len()
        );
        assert_eq!(
            cache.read_chunked_cached_local_relation_data(&hash).await?,
            data
        );

        std::fs::write(&path, b"changed")?;
        assert_eq!(
            cache.cached_local_relation_block_size(&hash).await?,
            data.len()
        );
        let error = cache
            .read_chunked_cached_local_relation_data(&hash)
            .await
            .expect_err("changed cache content must fail hash validation");
        assert!(error.to_string().contains("hash mismatch"));
        Ok(())
    }

    #[tokio::test]
    async fn process_artifact_limit_is_shared_and_drop_releases_reservation() -> SparkResult<()> {
        let budget = Arc::new(SparkArtifactProcessBudget::new(1, 16, 16));
        let first = test_artifact_registry("first", Arc::clone(&budget));
        let second = test_artifact_registry("second", Arc::clone(&budget));

        first.register_artifact("files/value".to_string(), 4, None, None)?;
        first.register_artifact("files/value".to_string(), 4, None, None)?;
        first
            .register_artifact("files/value".to_string(), 5, None, None)
            .expect_err("a duplicate name must keep its original size");
        assert_eq!(process_usage(&budget), (1, 4, 0));

        let error = second
            .register_artifact("files/other".to_string(), 4, None, None)
            .expect_err("the shared artifact count limit must reject another session");
        assert!(error.to_string().contains("process exceeded the limit"));
        assert_eq!(process_usage(&budget), (1, 4, 0));

        drop(first);
        assert_eq!(process_usage(&budget), (0, 0, 0));
        second.register_artifact("files/other".to_string(), 4, None, None)?;
        assert_eq!(process_usage(&budget), (1, 4, 0));
        drop(second);
        assert_eq!(process_usage(&budget), (0, 0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn process_byte_limit_is_shared_and_drop_releases_bytes() -> SparkResult<()> {
        let budget = Arc::new(SparkArtifactProcessBudget::new(8, 4, 4));
        let first = test_artifact_registry("first", Arc::clone(&budget));
        let second = test_artifact_registry("second", Arc::clone(&budget));

        first.register_artifact("files/value".to_string(), 4, None, None)?;
        let error = second
            .register_artifact("files/other".to_string(), 1, None, None)
            .expect_err("the shared byte limit must reject another session");
        assert!(
            error
                .to_string()
                .contains("process exceeded the limit of 4 bytes")
        );

        drop(first);
        second.register_artifact("files/other".to_string(), 1, None, None)?;
        assert_eq!(process_usage(&budget), (1, 1, 0));
        drop(second);
        assert_eq!(process_usage(&budget), (0, 0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn process_inline_limit_only_counts_inline_payloads() -> SparkResult<()> {
        let budget = Arc::new(SparkArtifactProcessBudget::new(8, 16, 3));
        let first = test_artifact_registry("first", Arc::clone(&budget));
        let second = test_artifact_registry("second", Arc::clone(&budget));
        let first_name = "pyfiles/first.py";
        let second_name = "pyfiles/second.py";

        first.register_artifact(
            first_name.to_string(),
            2,
            None,
            Some(inline_python_artifact(first_name, 2)),
        )?;
        let error = second
            .register_artifact(
                second_name.to_string(),
                2,
                None,
                Some(inline_python_artifact(second_name, 2)),
            )
            .expect_err("the shared inline byte limit must reject another session");
        assert!(error.to_string().contains("inline memory limit of 3 bytes"));

        second.register_artifact("files/non-inline".to_string(), 2, None, None)?;
        assert_eq!(process_usage(&budget), (2, 4, 2));
        drop(first);
        second.register_artifact(
            second_name.to_string(),
            2,
            None,
            Some(inline_python_artifact(second_name, 2)),
        )?;
        assert_eq!(process_usage(&budget), (2, 4, 2));
        drop(second);
        assert_eq!(process_usage(&budget), (0, 0, 0));
        Ok(())
    }

    #[test]
    fn full_cleanup_queue_spills_to_in_memory_journal_channel()
    -> Result<(), Box<dyn std::error::Error>> {
        let (cleanup_sender, _cleanup_receiver) = std::sync::mpsc::sync_channel(0);
        let (journal_sender, journal_receiver) = std::sync::mpsc::channel();
        let state = Arc::new(ArtifactCleanupState::new(0));
        let service = ArtifactCleanupService {
            cleanup_sender,
            journal_sender,
            state: Arc::clone(&state),
            process_id: "test-process".to_string(),
            _owner_lock: tempfile::tempfile()?,
        };
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("session-artifacts")],
            uris: vec!["file:///tmp/object".to_string()],
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: String::new(),
            lease_id: None,
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 0,
            registered_bytes: 0,
        };

        service.enqueue(request);

        let ArtifactCleanupJournalCommand::Persist(pending) =
            journal_receiver.recv_timeout(Duration::from_secs(1))?
        else {
            return Err(std::io::Error::other("expected persisted cleanup command").into());
        };
        assert_eq!(
            pending.artifact_dirs,
            vec![PathBuf::from("session-artifacts")]
        );
        assert_eq!(pending.uris, vec!["file:///tmp/object"]);
        assert_eq!(pending.owner_process_id, "test-process");
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn transient_cleanup_failure_retains_ownership_until_retry_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let journal = root.path().join("journal");
        std::fs::create_dir(&journal)?;
        let artifact_dir = root.path().join("session-artifacts");
        std::fs::File::create(&artifact_dir)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![artifact_dir.clone()],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: None,
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 0,
            registered_bytes: 0,
        };
        let state = ArtifactCleanupState::new(1);

        persist_artifact_cleanup(&journal, &request).map_err(std::io::Error::other)?;
        let Some(claimed) = take_persisted_artifact_cleanup(&journal, "test-process") else {
            return Err(std::io::Error::other("the cleanup request must be claimed").into());
        };
        run_claimed_artifact_cleanup(&runtime, &journal, &state, claimed);
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 1);

        std::fs::remove_file(&artifact_dir)?;
        std::fs::create_dir(&artifact_dir)?;
        std::thread::sleep(Duration::from_millis(150));
        let Some(claimed) = take_persisted_artifact_cleanup(&journal, "test-process") else {
            return Err(std::io::Error::other("the failed cleanup must be retried").into());
        };
        run_claimed_artifact_cleanup(&runtime, &journal, &state, claimed);

        assert!(!artifact_dir.exists());
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn not_found_cleanup_retries_a_late_committed_upload() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let object = root.path().join("late-object");
        let uri = Url::from_file_path(&object)
            .map_err(|()| std::io::Error::other("failed to build test object URI"))?
            .to_string();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: Vec::new(),
            uris: vec![uri],
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: unix_time_millis().saturating_add(60_000),
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: None,
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 0,
            registered_bytes: 0,
        };

        let mut retry = run_artifact_cleanup(&runtime, request)
            .ok_or_else(|| std::io::Error::other("NotFound must remain pending"))?;
        std::fs::write(&object, b"late commit")?;
        retry.not_found_confirmation_until_unix_millis = 0;
        assert!(run_artifact_cleanup(&runtime, retry).is_none());
        assert!(!object.exists());
        Ok(())
    }

    #[tokio::test]
    async fn journal_flush_does_not_wait_for_pending_durable_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = tempfile::tempdir()?;
        let state = Arc::new(ArtifactCleanupState::new(1));
        let (journal_sender, journal_worker) =
            spawn_test_cleanup_journal(journal.path().to_path_buf(), Arc::clone(&state));
        let lease_id = Uuid::new_v4().to_string();
        let request = test_dormant_cleanup_request(lease_id.clone(), 60_000);
        let (persisted, wait_for_persist) = tokio::sync::oneshot::channel();
        journal_sender
            .send(ArtifactCleanupJournalCommand::PersistDormant {
                request,
                result: persisted,
            })
            .map_err(|_| std::io::Error::other("failed to enqueue dormant cleanup lease"))?;
        await_test_cleanup_journal_result(wait_for_persist, "persisting dormant cleanup lease")
            .await?;

        let (flushed, wait_for_flush) = tokio::sync::oneshot::channel();
        journal_sender
            .send(ArtifactCleanupJournalCommand::Flush { result: flushed })
            .map_err(|_| std::io::Error::other("failed to enqueue cleanup journal flush"))?;
        await_test_cleanup_journal_result(wait_for_flush, "flushing cleanup journal").await?;

        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 1);
        let persisted: ArtifactCleanupRequest = serde_json::from_reader(std::fs::File::open(
            artifact_cleanup_lease_path(journal.path(), &lease_id),
        )?)?;
        assert!(persisted.dormant);
        drop(journal_sender);
        journal_worker
            .join()
            .map_err(|_| std::io::Error::other("cleanup journal worker panicked"))?;
        Ok(())
    }

    #[tokio::test]
    async fn journal_flush_barrier_follows_cleanup_lease_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = tempfile::tempdir()?;
        let state = Arc::new(ArtifactCleanupState::new(1));
        state.reserve_activation_slot()?;
        let (journal_sender, journal_worker) =
            spawn_test_cleanup_journal(journal.path().to_path_buf(), Arc::clone(&state));
        let lease_id = Uuid::new_v4().to_string();
        let request = test_dormant_cleanup_request(lease_id.clone(), 60_000);
        let (persisted, wait_for_persist) = tokio::sync::oneshot::channel();
        journal_sender
            .send(ArtifactCleanupJournalCommand::PersistDormant {
                request,
                result: persisted,
            })
            .map_err(|_| std::io::Error::other("failed to enqueue dormant cleanup lease"))?;
        await_test_cleanup_journal_result(wait_for_persist, "persisting dormant cleanup lease")
            .await?;

        journal_sender
            .send(ArtifactCleanupJournalCommand::Activate {
                lease_id: lease_id.clone(),
                registered_artifacts: 0,
                registered_bytes: 0,
            })
            .map_err(|_| std::io::Error::other("failed to enqueue cleanup lease activation"))?;
        let (flushed, wait_for_flush) = tokio::sync::oneshot::channel();
        journal_sender
            .send(ArtifactCleanupJournalCommand::Flush { result: flushed })
            .map_err(|_| std::io::Error::other("failed to enqueue cleanup journal flush"))?;
        await_test_cleanup_journal_result(wait_for_flush, "flushing activated cleanup lease")
            .await?;

        let activated: ArtifactCleanupRequest = serde_json::from_reader(std::fs::File::open(
            artifact_cleanup_lease_path(journal.path(), &lease_id),
        )?)?;
        assert!(!activated.dormant);
        assert!(activated.not_found_confirmation_until_unix_millis >= unix_time_millis());
        assert_eq!(state.activation_slots.load(Ordering::SeqCst), 0);
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 1);
        drop(journal_sender);
        journal_worker
            .join()
            .map_err(|_| std::io::Error::other("cleanup journal worker panicked"))?;
        Ok(())
    }

    #[test]
    fn dormant_cleanup_lease_activates_without_blocking_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = tempfile::tempdir()?;
        let lease_id = Uuid::new_v4().to_string();
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("missing-session-artifacts")],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: Some(lease_id.clone()),
            dormant: true,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 1_000,
            registered_artifacts: 0,
            registered_bytes: 0,
        };
        persist_dormant_cleanup_request(journal.path(), &request).map_err(std::io::Error::other)?;
        assert!(take_persisted_artifact_cleanup(journal.path(), "test-process").is_none());

        let state = ArtifactCleanupState::new(1);
        activate_persisted_cleanup_lease_until_success(
            journal.path(),
            &state,
            &lease_id,
            2,
            2 * 1024 * 1024,
        );
        let claimed = take_persisted_artifact_cleanup(journal.path(), "test-process")
            .ok_or_else(|| std::io::Error::other("active cleanup lease must be claimable"))?;
        assert!(!claimed.request.dormant);
        assert_eq!(claimed.request.ownerships, 5);
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 5);
        assert!(claimed.request.not_found_confirmation_until_unix_millis >= unix_time_millis());
        Ok(())
    }

    #[test]
    fn prefix_cleanup_retries_until_late_object_is_deleted()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let prefix = root.path().join("session-prefix");
        let prefix_uri = Url::from_directory_path(&prefix)
            .map_err(|()| std::io::Error::other("failed to build test prefix URI"))?
            .to_string();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: Vec::new(),
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: unix_time_millis().saturating_add(60_000),
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: Some(Uuid::new_v4().to_string()),
            dormant: false,
            uri_prefixes: vec![prefix_uri],
            confirmation_window_millis: 60_000,
            registered_artifacts: 0,
            registered_bytes: 0,
        };

        let mut retry = run_artifact_cleanup(&runtime, request)
            .ok_or_else(|| std::io::Error::other("empty prefix must remain pending"))?;
        std::fs::create_dir_all(&prefix)?;
        let late_object = prefix.join("late-object");
        std::fs::write(&late_object, b"late commit")?;
        retry.not_found_confirmation_until_unix_millis = 0;
        assert!(run_artifact_cleanup(&runtime, retry).is_none());
        assert!(!late_object.exists());
        Ok(())
    }

    #[test]
    fn restart_recovers_persisted_and_claimed_cleanup_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = tempfile::tempdir()?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("missing-session-artifacts")],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 2,
            owner_process_id: "old-process".to_string(),
            lease_id: None,
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 0,
            registered_bytes: 0,
        };
        persist_artifact_cleanup(journal.path(), &request).map_err(std::io::Error::other)?;
        let persisted = std::fs::read_dir(journal.path())?
            .find_map(|entry| {
                let path = entry.ok()?.path();
                (path.extension().and_then(|extension| extension.to_str()) == Some("json"))
                    .then_some(path)
            })
            .ok_or_else(|| std::io::Error::other("persisted cleanup request not found"))?;
        std::fs::rename(&persisted, journal.path().join(".work-orphan"))?;

        assert_eq!(
            recover_persisted_artifact_cleanup(journal.path(), "new-process", None)
                .map_err(std::io::Error::other)?,
            artifact_cleanup_work_units(
                &request.artifact_dirs,
                &request.uris,
                &request.uri_prefixes,
                request.registered_artifacts,
                request.registered_bytes,
            )
        );
        assert!(std::fs::read_dir(journal.path())?.all(|entry| {
            entry
                .ok()
                .and_then(|entry| entry.file_name().to_str().map(str::to_owned))
                .is_some_and(|name| !name.starts_with(".work-"))
        }));
        Ok(())
    }

    #[test]
    fn recovery_skips_live_owner_and_adopts_released_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = tempfile::tempdir()?;
        let owner_a = Uuid::new_v4().to_string();
        let owner_b = Uuid::new_v4().to_string();
        let owner_a_lock = acquire_artifact_cleanup_owner_lock(journal.path(), &owner_a)
            .map_err(std::io::Error::other)?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("missing-session-artifacts")],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: owner_a.clone(),
            lease_id: Some(Uuid::new_v4().to_string()),
            dormant: true,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 1_000,
            registered_artifacts: 0,
            registered_bytes: 0,
        };
        persist_artifact_cleanup(journal.path(), &request).map_err(std::io::Error::other)?;
        persist_artifact_cleanup(journal.path(), &request).map_err(std::io::Error::other)?;
        let persisted = std::fs::read_dir(journal.path())?
            .find_map(|entry| {
                let path = entry.ok()?.path();
                (path.extension().and_then(|extension| extension.to_str()) == Some("json"))
                    .then_some(path)
            })
            .ok_or_else(|| std::io::Error::other("persisted cleanup request not found"))?;
        std::fs::rename(&persisted, journal.path().join(".work-active"))?;

        assert_eq!(
            recover_persisted_artifact_cleanup(journal.path(), &owner_b, None)
                .map_err(std::io::Error::other)?,
            0
        );
        assert!(journal.path().join(".work-active").exists());

        drop(owner_a_lock);
        let state = ArtifactCleanupState::new(0);
        assert_eq!(
            recover_persisted_artifact_cleanup(journal.path(), &owner_b, Some(&state))
                .map_err(std::io::Error::other)?,
            0
        );
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 2);
        assert!(!journal.path().join(".work-active").exists());
        assert!(!artifact_cleanup_owner_lock_path(journal.path(), &owner_a).exists());
        let recovered = std::fs::read_dir(journal.path())?
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::File::open(entry.path()).ok())
            .filter_map(|file| serde_json::from_reader::<_, ArtifactCleanupRequest>(file).ok())
            .collect::<Vec<_>>();
        assert_eq!(recovered.len(), 2);
        assert!(recovered.iter().all(|request| {
            !request.dormant
                && request.owner_process_id == owner_b
                && request.not_found_confirmation_until_unix_millis >= unix_time_millis()
        }));
        Ok(())
    }

    #[test]
    fn persisted_cleanup_request_has_one_atomic_claimant() -> Result<(), Box<dyn std::error::Error>>
    {
        let journal = tempfile::tempdir()?;
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![PathBuf::from("missing-session-artifacts")],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: None,
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 0,
            registered_bytes: 0,
        };
        persist_artifact_cleanup(journal.path(), &request).map_err(std::io::Error::other)?;

        let claims = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                take_persisted_artifact_cleanup(journal.path(), "test-process").is_some()
            });
            let second = scope.spawn(|| {
                take_persisted_artifact_cleanup(journal.path(), "test-process").is_some()
            });
            usize::from(first.join().unwrap_or(false)) + usize::from(second.join().unwrap_or(false))
        });
        assert_eq!(claims, 1);
        Ok(())
    }

    #[test]
    fn cleanup_health_rejects_unhealthy_admission() {
        let backlog = ArtifactCleanupState::new(ARTIFACT_CLEANUP_BACKLOG_HEALTH_LIMIT);
        assert!(backlog.ensure_capacity().is_err());

        let journal_failure = ArtifactCleanupState::new(0);
        journal_failure.note_journal_failure();
        assert!(journal_failure.ensure_capacity().is_err());
        journal_failure.resolve_journal_failure();
        assert!(journal_failure.ensure_capacity().is_ok());

        journal_failure.mark_workers_unhealthy();
        assert!(journal_failure.ensure_capacity().is_err());
    }

    #[test]
    fn process_reservation_is_released_only_after_cleanup_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let artifact_dir = root.path().join("session-artifacts");
        std::fs::File::create(&artifact_dir)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let budget = Arc::new(SparkArtifactProcessBudget::new(1, 4, 4));
        budget.reserve_artifact(4, false)?;
        let lease_id = Uuid::new_v4().to_string();
        let state = ArtifactCleanupState::new(1);
        state.hold_process_reservation(
            lease_id.clone(),
            Arc::clone(&budget),
            SparkArtifactProcessReservation {
                artifacts: 1,
                bytes: 4,
                inline_bytes: 0,
            },
        );
        let request = ArtifactCleanupRequest {
            artifact_dirs: vec![artifact_dir.clone()],
            uris: Vec::new(),
            retry_attempt: 0,
            retry_after_unix_millis: 0,
            not_found_confirmation_until_unix_millis: 0,
            ownerships: 1,
            owner_process_id: "test-process".to_string(),
            lease_id: Some(lease_id),
            dormant: false,
            uri_prefixes: Vec::new(),
            confirmation_window_millis: 0,
            registered_artifacts: 1,
            registered_bytes: 4,
        };
        let (journal_sender, journal_receiver) = std::sync::mpsc::channel();

        run_queued_artifact_cleanup(&runtime, &journal_sender, &state, request);
        assert_eq!(process_usage(&budget), (1, 4, 0));
        let ArtifactCleanupJournalCommand::Persist(mut retry) =
            journal_receiver.recv_timeout(Duration::from_secs(1))?
        else {
            return Err(std::io::Error::other("cleanup retry was not persisted").into());
        };
        std::fs::remove_file(&artifact_dir)?;
        std::fs::create_dir(&artifact_dir)?;
        retry.retry_after_unix_millis = 0;
        run_queued_artifact_cleanup(&runtime, &journal_sender, &state, retry);

        assert_eq!(process_usage(&budget), (0, 0, 0));
        assert_eq!(state.pending_ownerships.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
