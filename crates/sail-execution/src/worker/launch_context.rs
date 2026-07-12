use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use futures::StreamExt;
use indexmap::IndexMap;
use object_store::{ObjectStoreExt, ObjectStoreScheme, PutPayload};
use prost::Message;
#[cfg(test)]
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_python_udf::config::{PySparkArtifactKind, PySparkPythonArtifact};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use url::Url;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::JobId;
use crate::task::definition::{LocalRelationResource, TaskLaunchContext, TaskResources};
use crate::worker::r#gen;

#[cfg(test)]
const MAX_INLINE_TASK_RESOURCE_BYTES: usize = GRPC_MAX_MESSAGE_LENGTH_DEFAULT / 2;
const MAX_MATERIALIZED_TASK_RESOURCE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_MATERIALIZED_TASK_RESOURCE_PERMITS: usize = 512 * 1024 * 1024;
const MAX_CACHED_TASK_RESOURCE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_CONCURRENT_TASK_RESOURCE_UPLOADS: usize = 8;
const MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS: usize = 8;
const TASK_RESOURCE_CLEANUP_QUEUE_CAPACITY: usize = 256;
const MAX_TASK_RESOURCE_CLEANUP_LEASES: usize = 4096;
const TASK_RESOURCE_CLEANUP_SCAN_INTERVAL: Duration = Duration::from_millis(100);
const TASK_RESOURCE_CLEANUP_RETRY_BASE: Duration = Duration::from_secs(1);
const TASK_RESOURCE_CLEANUP_RETRY_MAX: Duration = Duration::from_secs(60);
const TASK_RESOURCE_DELETE_TIMEOUT_MAX: Duration = Duration::from_secs(30);
const DEFAULT_ARTIFACT_CLEANUP_JOURNAL_DIRECTORY: &str = "sail-artifact-cleanup-journals";

static TASK_RESOURCE_CLEANUP_JOURNAL_ROOT: OnceLock<PathBuf> = OnceLock::new();

type CachedTaskResourceResult = Result<MaterializedTaskResource, String>;

#[derive(Clone)]
struct MaterializedTaskResource {
    data: Arc<[u8]>,
    memory_reservation: Arc<OwnedSemaphorePermit>,
}

enum CachedTaskResource {
    Loading {
        generation: u64,
        receiver: watch::Receiver<Option<CachedTaskResourceResult>>,
    },
    Ready(MaterializedTaskResource),
}

#[derive(Default)]
struct TaskResourceCache {
    entries: IndexMap<String, CachedTaskResource>,
    ready_bytes: u64,
    next_generation: u64,
}

#[derive(Clone, Deserialize, Serialize)]
struct TaskResourceCleanupRequest {
    uris: Vec<String>,
    #[serde(default)]
    retry_for_millis: u64,
    verify_until_unix_millis: u64,
    operation_timeout_millis: u64,
    retry_attempt: u32,
    next_attempt_unix_millis: u64,
    #[serde(default)]
    owner_id: String,
    #[serde(default)]
    dormant: bool,
}

struct TaskResourceCleanupLeaseState {
    uri: String,
    journal_path: PathBuf,
    active: AtomicBool,
}

pub(crate) struct TaskResourceCleanupLease {
    state: Arc<TaskResourceCleanupLeaseState>,
}

impl TaskResourceCleanupLease {
    pub(crate) fn uri(&self) -> &str {
        &self.state.uri
    }

    pub(crate) fn activate(&self) {
        self.state.active.store(true, Ordering::Release);
    }
}

impl Drop for TaskResourceCleanupLease {
    fn drop(&mut self) {
        self.activate();
    }
}

enum TaskResourceCleanupCommand {
    CreateLease {
        request: TaskResourceCleanupRequest,
        state: Arc<TaskResourceCleanupLeaseState>,
        result: oneshot::Sender<Result<(), String>>,
    },
    Active(TaskResourceCleanupRequest),
}

struct TaskResourceCleanupCompletion {
    journal_path: Option<PathBuf>,
    outcome: TaskResourceCleanupOutcome,
}

enum TaskResourceCleanupOutcome {
    Complete,
    Retry(TaskResourceCleanupRequest),
}

struct TaskResourceCleanupOwner {
    sender: mpsc::Sender<TaskResourceCleanupCommand>,
    healthy: Arc<AtomicBool>,
    journal_root: PathBuf,
    owner_id: String,
    lease_count: Arc<AtomicUsize>,
    next_record_id: Arc<AtomicU64>,
    _owner_lock: std::fs::File,
    _cleanup_runtime: tokio::runtime::Runtime,
}

impl TaskResourceCleanupOwner {
    fn start() -> Result<Self, String> {
        let journal_root =
            task_resource_cleanup_journal_root().map_err(|error| error.to_string())?;
        let owner_id = format!("{}-{}", std::process::id(), unix_time_millis());
        let owner_lock = acquire_task_resource_cleanup_owner_lock(&journal_root, &owner_id)?;
        recover_dormant_task_resource_cleanups(&journal_root, &owner_id)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS)
            .thread_name("sail-resource-cleanup")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let (sender, receiver) = mpsc::channel(TASK_RESOURCE_CLEANUP_QUEUE_CAPACITY);
        let healthy = Arc::new(AtomicBool::new(true));
        let leases = Arc::new(Mutex::new(HashMap::new()));
        let lease_count = Arc::new(AtomicUsize::new(0));
        let next_record_id = Arc::new(AtomicU64::new(0));
        let cleanup_owner_id = owner_id.clone();
        runtime.spawn(run_task_resource_cleanup_owner(
            receiver,
            Arc::clone(&healthy),
            journal_root.clone(),
            cleanup_owner_id,
            Arc::clone(&leases),
            Arc::clone(&lease_count),
            Arc::clone(&next_record_id),
        ));
        Ok(Self {
            sender,
            healthy,
            journal_root,
            owner_id,
            lease_count,
            next_record_id,
            _owner_lock: owner_lock,
            _cleanup_runtime: runtime,
        })
    }

    async fn create_lease(
        &self,
        uri: String,
        retry_for: Duration,
        operation_timeout: Duration,
    ) -> Result<TaskResourceCleanupLease, String> {
        if !self.healthy.load(Ordering::Acquire) {
            return Err("task resource cleanup journal is unhealthy".to_string());
        }
        reserve_task_resource_cleanup_lease_slot(&self.healthy, &self.lease_count)?;
        let record_id = self.next_record_id.fetch_add(1, Ordering::Relaxed);
        let journal_path = self.journal_root.join(format!(
            "lease-{}-{}-{record_id}.json",
            std::process::id(),
            unix_time_millis()
        ));
        let state = Arc::new(TaskResourceCleanupLeaseState {
            uri: uri.clone(),
            journal_path,
            active: AtomicBool::new(false),
        });
        let request = new_task_resource_cleanup_request(
            vec![uri],
            retry_for,
            operation_timeout,
            self.owner_id.clone(),
            true,
        );
        let (result, completed) = oneshot::channel();
        if self
            .sender
            .send(TaskResourceCleanupCommand::CreateLease {
                request,
                state: Arc::clone(&state),
                result,
            })
            .await
            .is_err()
        {
            self.lease_count.fetch_sub(1, Ordering::AcqRel);
            return Err("task resource cleanup owner is unavailable".to_string());
        }
        match completed.await {
            Ok(Ok(())) => Ok(TaskResourceCleanupLease { state }),
            Ok(Err(error)) => {
                self.lease_count.fetch_sub(1, Ordering::AcqRel);
                Err(error)
            }
            Err(_) => {
                self.lease_count.fetch_sub(1, Ordering::AcqRel);
                Err("task resource cleanup journal stopped before acknowledgement".to_string())
            }
        }
    }

    fn handoff_blocking(&self, request: TaskResourceCleanupRequest) -> Result<(), String> {
        match persist_task_resource_cleanup_request(
            &self.journal_root,
            &request,
            None,
            &self.next_record_id,
        ) {
            Ok(_) => Ok(()),
            Err(persist_error) => {
                self.healthy.store(false, Ordering::Release);
                self.sender
                    .blocking_send(TaskResourceCleanupCommand::Active(request))
                    .map_err(|send_error| {
                    format!(
                        "failed to persist cleanup ownership ({persist_error}) and the cleanup owner rejected it: {send_error}"
                    )
                    })
            }
        }
    }
}

fn reserve_task_resource_cleanup_lease_slot(
    healthy: &AtomicBool,
    lease_count: &AtomicUsize,
) -> Result<(), String> {
    if !healthy.load(Ordering::Acquire) {
        return Err("task resource cleanup journal is unhealthy".to_string());
    }
    let previous = lease_count.fetch_add(1, Ordering::AcqRel);
    if previous >= MAX_TASK_RESOURCE_CLEANUP_LEASES {
        lease_count.fetch_sub(1, Ordering::AcqRel);
        return Err(format!(
            "at most {MAX_TASK_RESOURCE_CLEANUP_LEASES} task resource cleanup leases may be live"
        ));
    }
    Ok(())
}

fn task_resource_cleanup_owner() -> Result<&'static TaskResourceCleanupOwner, String> {
    static OWNER: OnceLock<Result<TaskResourceCleanupOwner, String>> = OnceLock::new();
    match OWNER.get_or_init(TaskResourceCleanupOwner::start) {
        Ok(owner) => Ok(owner),
        Err(error) => Err(error.clone()),
    }
}

fn task_resource_upload_semaphore() -> &'static Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_TASK_RESOURCE_UPLOADS)))
}

fn task_resource_materialization_semaphore() -> &'static Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(MAX_MATERIALIZED_TASK_RESOURCE_PERMITS)))
}

fn task_resource_materialization_admission_semaphore() -> &'static Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(MAX_MATERIALIZED_TASK_RESOURCE_PERMITS)))
}

pub fn initialize_task_resource_cleanup_journal(journal_root: &Path) -> ExecutionResult<()> {
    resolve_task_resource_cleanup_journal_root(journal_root, &TASK_RESOURCE_CLEANUP_JOURNAL_ROOT)?;
    task_resource_cleanup_owner().map_err(ExecutionError::InternalError)?;
    Ok(())
}

fn task_resource_cleanup_journal_root() -> ExecutionResult<PathBuf> {
    let default_root = std::env::temp_dir()
        .join(DEFAULT_ARTIFACT_CLEANUP_JOURNAL_DIRECTORY)
        .join("execution");
    resolve_task_resource_cleanup_journal_root(&default_root, &TASK_RESOURCE_CLEANUP_JOURNAL_ROOT)
}

fn resolve_task_resource_cleanup_journal_root(
    requested_root: &Path,
    journal_root_slot: &OnceLock<PathBuf>,
) -> ExecutionResult<PathBuf> {
    if !requested_root.is_absolute() {
        return Err(ExecutionError::InvalidArgument(
            "spark.artifact_cleanup_journal_root must be an absolute path".to_string(),
        ));
    }
    let requested_root = prepare_task_resource_cleanup_journal_root(requested_root)?;
    match journal_root_slot.set(requested_root.clone()) {
        Ok(()) => Ok(requested_root),
        Err(requested_root) => {
            let configured_root = journal_root_slot.get().ok_or_else(|| {
                ExecutionError::InternalError(
                    "task resource cleanup journal root was not initialized".to_string(),
                )
            })?;
            if configured_root == &requested_root {
                Ok(requested_root)
            } else {
                Err(ExecutionError::InvalidArgument(format!(
                    "spark.artifact_cleanup_journal_root conflicts with the process-wide execution cleanup journal configuration: configured {}, requested {}",
                    configured_root.display(),
                    requested_root.display()
                )))
            }
        }
    }
}

fn prepare_task_resource_cleanup_journal_root(root: &Path) -> std::io::Result<PathBuf> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "task resource cleanup journal path is not a directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700).recursive(true).create(root)?;
            }
            #[cfg(not(unix))]
            fs::create_dir_all(&root)?;
        }
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "task resource cleanup journal path is not a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let probe_path = root.join(format!(
            ".owner-probe-{}-{}",
            std::process::id(),
            unix_time_millis()
        ));
        let probe = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&probe_path)?;
        let process_uid = probe.metadata()?.uid();
        drop(probe);
        fs::remove_file(&probe_path)?;
        if metadata.uid() != process_uid {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "task resource cleanup journal has an unexpected owner",
            ));
        }
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    }
    let root = fs::canonicalize(root)?;
    recover_task_resource_cleanup_journal(&root)?;
    Ok(root)
}

fn task_resource_cleanup_owner_lock_path(root: &Path, owner_id: &str) -> PathBuf {
    root.join(format!(".owner-{owner_id}.lock"))
}

fn acquire_task_resource_cleanup_owner_lock(
    root: &Path,
    owner_id: &str,
) -> Result<std::fs::File, String> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(task_resource_cleanup_owner_lock_path(root, owner_id))
        .map_err(|error| error.to_string())?;
    FileExt::lock_exclusive(&lock).map_err(|error| error.to_string())?;
    lock.sync_all().map_err(|error| error.to_string())?;
    Ok(lock)
}

fn recover_dormant_task_resource_cleanups(root: &Path, owner_id: &str) -> Result<(), String> {
    let entries = fs::read_dir(root)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let next_record_id = AtomicU64::new(0);
    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let Ok(mut request) = read_task_resource_cleanup_request(&path) else {
            continue;
        };
        if !request.dormant || request.owner_id == owner_id {
            continue;
        }
        let previous_owner_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(task_resource_cleanup_owner_lock_path(
                root,
                &request.owner_id,
            ))
            .map_err(|error| error.to_string())?;
        match FileExt::try_lock_exclusive(&previous_owner_lock) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.to_string()),
        }
        request.dormant = false;
        request.owner_id = owner_id.to_string();
        let now = unix_time_millis();
        request.verify_until_unix_millis = now.saturating_add(request.retry_for_millis);
        request.next_attempt_unix_millis = now;
        persist_task_resource_cleanup_request(root, &request, Some(&path), &next_record_id)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn recover_task_resource_cleanup_journal(root: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".cleanup-") && path.extension().is_some_and(|value| value == "tmp") {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            continue;
        }
        if path.extension().is_some_and(|value| value == "json")
            && read_task_resource_cleanup_request(&path).is_err()
        {
            log::warn!("removing an invalid task resource cleanup journal entry");
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    #[cfg(unix)]
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn persist_task_resource_cleanup_request(
    journal_root: &Path,
    request: &TaskResourceCleanupRequest,
    existing_path: Option<&Path>,
    next_record_id: &AtomicU64,
) -> std::io::Result<PathBuf> {
    let record_id = next_record_id.fetch_add(1, Ordering::Relaxed);
    let now = unix_time_millis();
    let final_path = existing_path.map_or_else(
        || {
            journal_root.join(format!(
                "cleanup-{}-{now}-{record_id}.json",
                std::process::id()
            ))
        },
        Path::to_path_buf,
    );
    let temporary_path = journal_root.join(format!(
        ".cleanup-{}-{now}-{record_id}.tmp",
        std::process::id()
    ));
    let bytes = serde_json::to_vec(request).map_err(std::io::Error::other)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary_path, &final_path)?;
    #[cfg(unix)]
    std::fs::File::open(journal_root)?.sync_all()?;
    Ok(final_path)
}

fn read_task_resource_cleanup_request(path: &Path) -> Result<TaskResourceCleanupRequest, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn due_task_resource_cleanup_journals(
    journal_root: &Path,
    active_journals: &HashSet<PathBuf>,
    available: usize,
) -> Vec<(PathBuf, TaskResourceCleanupRequest)> {
    let Ok(entries) = fs::read_dir(journal_root) else {
        return vec![];
    };
    let now = unix_time_millis();
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .filter(|path| !active_journals.contains(path))
        .filter_map(|path| match read_task_resource_cleanup_request(&path) {
            Ok(request) if !request.dormant && request.next_attempt_unix_millis <= now => {
                Some((path, request))
            }
            Ok(_) => None,
            Err(error) => {
                log::warn!("removing an invalid task resource cleanup journal entry: {error}");
                if let Err(remove_error) = fs::remove_file(&path)
                    && remove_error.kind() != ErrorKind::NotFound
                {
                    log::warn!(
                        "could not remove an invalid task resource cleanup journal entry: {remove_error}"
                    );
                }
                None
            }
        })
        .take(available)
        .collect()
}

async fn run_task_resource_cleanup_owner(
    mut receiver: mpsc::Receiver<TaskResourceCleanupCommand>,
    healthy: Arc<AtomicBool>,
    journal_root: PathBuf,
    owner_id: String,
    leases: Arc<Mutex<HashMap<PathBuf, Arc<TaskResourceCleanupLeaseState>>>>,
    lease_count: Arc<AtomicUsize>,
    next_record_id: Arc<AtomicU64>,
) {
    let mut active_journals = HashSet::new();
    let mut memory_queue = VecDeque::new();
    let mut tasks = JoinSet::new();
    let mut scan = tokio::time::interval(TASK_RESOURCE_CLEANUP_SCAN_INTERVAL);
    loop {
        while tasks.len() < MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS {
            let Some(request) = memory_queue.pop_front() else {
                break;
            };
            tasks.spawn(run_task_resource_cleanup_attempt(None, request));
        }
        let available = MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS.saturating_sub(tasks.len());
        let activated_leases = {
            let leases = leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            leases
                .values()
                .filter(|state| {
                    state.active.load(Ordering::Acquire)
                        && !active_journals.contains(&state.journal_path)
                })
                .take(available)
                .cloned()
                .collect::<Vec<_>>()
        };
        for state in activated_leases {
            match read_task_resource_cleanup_request(&state.journal_path) {
                Ok(mut request) => {
                    request.dormant = false;
                    let now = unix_time_millis();
                    request.verify_until_unix_millis = now.saturating_add(request.retry_for_millis);
                    request.next_attempt_unix_millis = now;
                    match persist_task_resource_cleanup_request(
                        &journal_root,
                        &request,
                        Some(&state.journal_path),
                        &next_record_id,
                    ) {
                        Ok(path) => {
                            active_journals.insert(path.clone());
                            tasks.spawn(run_task_resource_cleanup_attempt(Some(path), request));
                        }
                        Err(error) => {
                            healthy.store(false, Ordering::Release);
                            log::error!("could not activate task resource cleanup lease: {error}");
                        }
                    }
                }
                Err(error) => {
                    healthy.store(false, Ordering::Release);
                    log::error!("could not read task resource cleanup lease: {error}");
                }
            }
        }
        let available = MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS.saturating_sub(tasks.len());
        for (path, request) in
            due_task_resource_cleanup_journals(&journal_root, &active_journals, available)
        {
            active_journals.insert(path.clone());
            tasks.spawn(run_task_resource_cleanup_attempt(Some(path), request));
        }

        tokio::select! {
            work = receiver.recv(), if tasks.len() < MAX_CONCURRENT_TASK_RESOURCE_CLEANUPS => {
                match work {
                    Some(TaskResourceCleanupCommand::CreateLease { request, state, result }) => {
                        if !healthy.load(Ordering::Acquire) {
                            let _ = result.send(Err("task resource cleanup journal is unhealthy".to_string()));
                            continue;
                        }
                        match persist_task_resource_cleanup_request(
                            &journal_root,
                            &request,
                            Some(&state.journal_path),
                            &next_record_id,
                        ) {
                            Ok(path) => {
                                leases
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .insert(path, Arc::clone(&state));
                                if result.send(Ok(())).is_err() {
                                    state.active.store(true, Ordering::Release);
                                }
                            }
                            Err(error) => {
                                healthy.store(false, Ordering::Release);
                                let _ = result.send(Err(format!("could not persist task resource cleanup lease: {error}")));
                            }
                        }
                    }
                    Some(TaskResourceCleanupCommand::Active(request)) => {
                        match persist_task_resource_cleanup_request(
                            &journal_root,
                            &request,
                            None,
                            &next_record_id,
                        ) {
                            Ok(_) => {}
                            Err(error) => {
                                healthy.store(false, Ordering::Release);
                                log::error!("could not persist task resource cleanup ownership: {error}");
                                memory_queue.push_back(request);
                            }
                        }
                    }
                    None => break,
                }
            }
            completion = tasks.join_next(), if !tasks.is_empty() => {
                match completion {
                    Some(Ok(completion)) => {
                        if let Some(path) = completion.journal_path.as_ref() {
                            active_journals.remove(path);
                        }
                        finish_task_resource_cleanup_attempt(
                            completion,
                            &journal_root,
                            &next_record_id,
                            &mut memory_queue,
                            &healthy,
                            &leases,
                            &lease_count,
                        );
                    }
                    Some(Err(error)) => log::error!("task resource cleanup worker failed: {error}"),
                    None => {}
                }
            }
            _ = scan.tick() => {
                if let Err(error) =
                    recover_dormant_task_resource_cleanups(&journal_root, &owner_id)
                {
                    healthy.store(false, Ordering::Release);
                    log::error!("could not recover dormant task resource cleanups: {error}");
                }
            }
        }
    }
}

fn finish_task_resource_cleanup_attempt(
    completion: TaskResourceCleanupCompletion,
    journal_root: &Path,
    next_record_id: &AtomicU64,
    memory_queue: &mut VecDeque<TaskResourceCleanupRequest>,
    healthy: &AtomicBool,
    leases: &Mutex<HashMap<PathBuf, Arc<TaskResourceCleanupLeaseState>>>,
    lease_count: &AtomicUsize,
) {
    match completion.outcome {
        TaskResourceCleanupOutcome::Complete => {
            if let Some(path) = completion.journal_path.as_ref()
                && let Err(error) = fs::remove_file(path)
                && error.kind() != ErrorKind::NotFound
            {
                log::warn!("could not remove completed task resource cleanup journal: {error}");
            }
            if let Some(path) = completion.journal_path.as_ref()
                && leases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(path)
                    .is_some()
            {
                lease_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
        TaskResourceCleanupOutcome::Retry(request) => {
            match persist_task_resource_cleanup_request(
                journal_root,
                &request,
                completion.journal_path.as_deref(),
                next_record_id,
            ) {
                Ok(_) => {}
                Err(error) => {
                    healthy.store(false, Ordering::Release);
                    log::error!("could not persist task resource cleanup retry: {error}");
                    memory_queue.push_back(request);
                }
            }
        }
    }
}

async fn run_task_resource_cleanup_attempt(
    journal_path: Option<PathBuf>,
    mut request: TaskResourceCleanupRequest,
) -> TaskResourceCleanupCompletion {
    let operation_timeout = Duration::from_millis(request.operation_timeout_millis.max(1));
    let verification_complete = unix_time_millis() >= request.verify_until_unix_millis;
    let mut retained_uris = Vec::with_capacity(request.uris.len());
    let mut failed = 0_usize;
    for uri in request.uris {
        let result = delete_task_resource_uri(&uri, operation_timeout).await;
        if !verification_complete || result.is_err() {
            retained_uris.push(uri);
        }
        if result.is_err() {
            failed = failed.saturating_add(1);
        }
    }
    if retained_uris.is_empty() {
        return TaskResourceCleanupCompletion {
            journal_path,
            outcome: TaskResourceCleanupOutcome::Complete,
        };
    }
    if failed > 0 {
        log::warn!(
            "task resource cleanup attempt {} retained {failed} failed URI(s)",
            request.retry_attempt.saturating_add(1)
        );
    }
    request.uris = retained_uris;
    request.retry_attempt = request.retry_attempt.saturating_add(1);
    let delay = task_resource_cleanup_retry_delay(request.retry_attempt);
    let now = unix_time_millis();
    let delayed = now.saturating_add(duration_millis(delay));
    request.next_attempt_unix_millis = if now < request.verify_until_unix_millis {
        delayed.min(request.verify_until_unix_millis)
    } else {
        delayed
    };
    TaskResourceCleanupCompletion {
        journal_path,
        outcome: TaskResourceCleanupOutcome::Retry(request),
    }
}

fn task_resource_cleanup_retry_delay(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(6);
    TASK_RESOURCE_CLEANUP_RETRY_BASE
        .saturating_mul(1_u32 << shift)
        .min(TASK_RESOURCE_CLEANUP_RETRY_MAX)
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn task_launch_context_descriptor_size(mut launch_context: TaskLaunchContext) -> usize {
    for artifact in &mut launch_context.resources.python_artifacts {
        artifact.data = None;
    }
    for resource in &mut launch_context.resources.local_relation_resources {
        resource.data = None;
    }
    r#gen::TaskLaunchContext::from(launch_context).encoded_len()
}

pub(crate) struct StagedTaskLaunchContext {
    launch_context: Option<TaskLaunchContext>,
    cleanup_leases: Vec<TaskResourceCleanupLease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskResourceStagingScope {
    pub(crate) job_id: JobId,
    pub(crate) stage: usize,
    pub(crate) staging_id: u64,
}

impl StagedTaskLaunchContext {
    pub(crate) fn accept(
        mut self,
    ) -> ExecutionResult<(TaskLaunchContext, Vec<TaskResourceCleanupLease>)> {
        let launch_context = self.launch_context.take().ok_or_else(|| {
            ExecutionError::InternalError(
                "staged task launch context was already accepted".to_string(),
            )
        })?;
        let cleanup_leases = std::mem::take(&mut self.cleanup_leases);
        Ok((launch_context, cleanup_leases))
    }
}

impl Drop for StagedTaskLaunchContext {
    fn drop(&mut self) {
        self.cleanup_leases
            .iter()
            .for_each(TaskResourceCleanupLease::activate);
    }
}

struct TaskResourceUploadGuard {
    cleanup_leases: Vec<TaskResourceCleanupLease>,
    cleanup_retry_for: Duration,
    cleanup_operation_timeout: Duration,
}

impl TaskResourceUploadGuard {
    fn new(cleanup_retry_for: Duration, cleanup_operation_timeout: Duration) -> Self {
        Self {
            cleanup_leases: vec![],
            cleanup_retry_for,
            cleanup_operation_timeout,
        }
    }

    async fn create_cleanup_lease(&mut self, uri: String) -> ExecutionResult<()> {
        let lease = task_resource_cleanup_owner()
            .map_err(ExecutionError::InternalError)?
            .create_lease(uri, self.cleanup_retry_for, self.cleanup_operation_timeout)
            .await
            .map_err(ExecutionError::InternalError)?;
        self.cleanup_leases.push(lease);
        Ok(())
    }

    fn finish(mut self) -> Vec<TaskResourceCleanupLease> {
        std::mem::take(&mut self.cleanup_leases)
    }
}

impl Drop for TaskResourceUploadGuard {
    fn drop(&mut self) {
        self.cleanup_leases
            .iter()
            .for_each(TaskResourceCleanupLease::activate);
    }
}

pub(crate) async fn stage_task_launch_context(
    mut launch_context: TaskLaunchContext,
    staging_scope: TaskResourceStagingScope,
    namespace: &str,
    inline_max_bytes: usize,
    inline_total_max_bytes: usize,
    store_uri: Option<&str>,
    transfer_timeout: Duration,
) -> ExecutionResult<StagedTaskLaunchContext> {
    let cleanup_operation_timeout = task_resource_cleanup_operation_timeout(transfer_timeout);
    let mut upload_guard =
        TaskResourceUploadGuard::new(transfer_timeout, cleanup_operation_timeout);
    stage_task_launch_context_resources(
        &mut launch_context,
        staging_scope,
        namespace,
        inline_max_bytes,
        inline_total_max_bytes,
        store_uri,
        transfer_timeout,
        &mut upload_guard,
    )
    .await?;
    Ok(StagedTaskLaunchContext {
        launch_context: Some(launch_context),
        cleanup_leases: upload_guard.finish(),
    })
}

async fn stage_task_launch_context_resources(
    launch_context: &mut TaskLaunchContext,
    staging_scope: TaskResourceStagingScope,
    namespace: &str,
    inline_max_bytes: usize,
    inline_total_max_bytes: usize,
    store_uri: Option<&str>,
    transfer_timeout: Duration,
    upload_guard: &mut TaskResourceUploadGuard,
) -> ExecutionResult<()> {
    let mut inline_bytes = 0_usize;
    for artifact in &mut launch_context.resources.python_artifacts {
        let Some(data) = artifact.data.take() else {
            continue;
        };
        validate_task_resource_data(&artifact.name, &artifact.sha256, artifact.size, &data)?;
        let projected = inline_bytes.checked_add(data.len()).ok_or_else(|| {
            ExecutionError::InvalidArgument("task resource byte count overflow".to_string())
        })?;
        if data.len() <= inline_max_bytes && projected <= inline_total_max_bytes {
            artifact.data = Some(data);
            inline_bytes = projected;
            continue;
        }
        let base_uri = store_uri.ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "task artifact {} cannot fit in the worker RPC and spark.artifact_store_uri is not configured",
                artifact.name
            ))
        })?;
        let uri = task_resource_uri(
            base_uri,
            namespace,
            staging_scope,
            "python",
            &artifact.sha256,
        )?;
        upload_guard.create_cleanup_lease(uri.clone()).await?;
        upload_task_resource(&uri, data, transfer_timeout).await?;
        artifact.uri = Some(uri.clone());
    }
    for resource in &mut launch_context.resources.local_relation_resources {
        let Some(data) = resource.data.take() else {
            continue;
        };
        validate_task_resource_data(&resource.key, &resource.sha256, resource.size, &data)?;
        let projected = inline_bytes.checked_add(data.len()).ok_or_else(|| {
            ExecutionError::InvalidArgument("task resource byte count overflow".to_string())
        })?;
        if data.len() <= inline_max_bytes && projected <= inline_total_max_bytes {
            resource.data = Some(data);
            inline_bytes = projected;
            continue;
        }
        let base_uri = store_uri.ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "LocalRelation resource {} cannot fit in the worker RPC and spark.artifact_store_uri is not configured",
                resource.key
            ))
        })?;
        let uri = task_resource_uri(
            base_uri,
            namespace,
            staging_scope,
            "local-relations",
            &resource.sha256,
        )?;
        upload_guard.create_cleanup_lease(uri.clone()).await?;
        upload_task_resource(&uri, data, transfer_timeout).await?;
        resource.uri = Some(uri.clone());
    }
    Ok(())
}

pub(crate) async fn materialize_task_launch_context(
    launch_context: TaskLaunchContext,
) -> ExecutionResult<TaskLaunchContext> {
    materialize_task_launch_context_resources(launch_context, true).await
}

pub(crate) async fn materialize_driver_task_launch_context(
    launch_context: TaskLaunchContext,
) -> ExecutionResult<TaskLaunchContext> {
    materialize_task_launch_context_resources(launch_context, false).await
}

async fn materialize_task_launch_context_resources(
    mut launch_context: TaskLaunchContext,
    materialize_python_artifacts: bool,
) -> ExecutionResult<TaskLaunchContext> {
    let python_artifact_bytes = launch_context
        .resources
        .python_artifacts
        .iter()
        .filter(|_| materialize_python_artifacts)
        .map(|artifact| artifact.size);
    let declared_bytes = python_artifact_bytes
        .chain(
            launch_context
                .resources
                .local_relation_resources
                .iter()
                .map(|resource| resource.size),
        )
        .try_fold(0_u64, |total, size| total.checked_add(size))
        .ok_or_else(|| {
            ExecutionError::InvalidArgument("task resource byte count overflow".to_string())
        })?;
    if declared_bytes > MAX_MATERIALIZED_TASK_RESOURCE_BYTES {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resources exceeded the aggregate materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        )));
    }
    let materialization_admission = acquire_task_resource_materialization_admission(
        task_resource_materialization_admission_semaphore(),
        declared_bytes,
    )
    .await?;
    if materialize_python_artifacts {
        for artifact in &mut launch_context.resources.python_artifacts {
            if let Some(uri) = artifact.uri.take() {
                let materialized = materialize_cached_task_resource(
                    &uri,
                    &artifact.name,
                    &artifact.sha256,
                    artifact.size,
                )
                .await?;
                artifact.data = Some(materialized.data);
                launch_context
                    .resource_memory_reservations
                    .push(materialized.memory_reservation);
            }
        }
    }
    for resource in &mut launch_context.resources.local_relation_resources {
        if let Some(uri) = resource.uri.take() {
            let materialized = materialize_cached_task_resource(
                &uri,
                &resource.key,
                &resource.sha256,
                resource.size,
            )
            .await?;
            resource.data = Some(materialized.data);
            launch_context
                .resource_memory_reservations
                .push(materialized.memory_reservation);
        }
    }
    drop(materialization_admission);
    Ok(launch_context)
}

async fn acquire_task_resource_materialization_admission(
    semaphore: &Arc<Semaphore>,
    declared_bytes: u64,
) -> ExecutionResult<OwnedSemaphorePermit> {
    if declared_bytes > MAX_MATERIALIZED_TASK_RESOURCE_BYTES {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resources exceeded the aggregate materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        )));
    }
    let permits = u32::try_from(declared_bytes).map_err(|_| {
        ExecutionError::InvalidArgument(format!(
            "task resources exceeded the aggregate materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        ))
    })?;
    Arc::clone(semaphore)
        .acquire_many_owned(permits)
        .await
        .map_err(|_| {
            ExecutionError::InternalError(
                "task resource materialization admission is unavailable".to_string(),
            )
        })
}

fn validate_task_resource_data(
    name: &str,
    expected_sha256: &str,
    expected_size: u64,
    data: &[u8],
) -> ExecutionResult<()> {
    validate_task_resource_sha256(expected_sha256)?;
    if data.len() as u64 != expected_size {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource {name} size mismatch: expected {expected_size}, got {}",
            data.len()
        )));
    }
    let actual = sha256_hex(data);
    if actual != expected_sha256 {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource {name} SHA-256 mismatch: expected {expected_sha256}, got {actual}"
        )));
    }
    Ok(())
}

fn validate_task_resource_sha256(expected_sha256: &str) -> ExecutionResult<()> {
    if expected_sha256.len() != 64 || !expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ExecutionError::InvalidArgument(
            "task resource has an invalid SHA-256 digest".to_string(),
        ));
    }
    Ok(())
}

enum TaskResourceCacheAction {
    Ready(MaterializedTaskResource),
    Wait(watch::Receiver<Option<CachedTaskResourceResult>>),
    Load(TaskResourceCacheLoader),
}

struct TaskResourceCacheLoader {
    key: String,
    generation: u64,
    sender: watch::Sender<Option<CachedTaskResourceResult>>,
    remove_loading_on_drop: bool,
}

impl TaskResourceCacheLoader {
    fn complete(mut self, result: &ExecutionResult<MaterializedTaskResource>) {
        let notification = result.as_ref().cloned().map_err(ToString::to_string);
        let mut cached_result = false;
        {
            let mut cache = lock_task_resource_cache();
            let owns_entry = cache.entries.get(&self.key).is_some_and(|entry| {
                matches!(
                    entry,
                    CachedTaskResource::Loading { generation, .. }
                        if *generation == self.generation
                )
            });
            if owns_entry {
                cache.entries.shift_remove(&self.key);
                if let Ok(materialized) = result {
                    cache.ready_bytes = cache
                        .ready_bytes
                        .saturating_add(materialized.data.len() as u64);
                    cache.entries.insert(
                        self.key.clone(),
                        CachedTaskResource::Ready(materialized.clone()),
                    );
                    cached_result = true;
                    while cache.ready_bytes > MAX_CACHED_TASK_RESOURCE_BYTES {
                        if !evict_oldest_ready_task_resource(&mut cache) {
                            break;
                        }
                    }
                }
            }
        }
        if cached_result {
            task_resource_cache_updates()
                .send_modify(|generation| *generation = generation.wrapping_add(1));
        }
        self.sender.send_replace(Some(notification));
        self.remove_loading_on_drop = false;
    }
}

impl Drop for TaskResourceCacheLoader {
    fn drop(&mut self) {
        if !self.remove_loading_on_drop {
            return;
        }
        let mut cache = lock_task_resource_cache();
        let owns_entry = cache.entries.get(&self.key).is_some_and(|entry| {
            matches!(
                entry,
                CachedTaskResource::Loading { generation, .. }
                    if *generation == self.generation
            )
        });
        if owns_entry {
            cache.entries.shift_remove(&self.key);
        }
    }
}

fn task_resource_cache() -> &'static Mutex<TaskResourceCache> {
    static CACHE: OnceLock<Mutex<TaskResourceCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(TaskResourceCache::default()))
}

fn task_resource_cache_updates() -> &'static watch::Sender<u64> {
    static UPDATES: OnceLock<watch::Sender<u64>> = OnceLock::new();
    UPDATES.get_or_init(|| watch::channel(0).0)
}

fn lock_task_resource_cache() -> MutexGuard<'static, TaskResourceCache> {
    task_resource_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn evict_oldest_ready_task_resource(cache: &mut TaskResourceCache) -> bool {
    let Some(index) = cache
        .entries
        .iter()
        .position(|(_, entry)| matches!(entry, CachedTaskResource::Ready(_)))
    else {
        return false;
    };
    let Some((_, CachedTaskResource::Ready(evicted))) = cache.entries.shift_remove_index(index)
    else {
        return false;
    };
    cache.ready_bytes = cache.ready_bytes.saturating_sub(evicted.data.len() as u64);
    true
}

fn evict_ready_task_resources_for_admission(
    semaphore: &Semaphore,
    expected_size: u64,
) -> ExecutionResult<()> {
    let expected_size = usize::try_from(expected_size).map_err(|_| {
        ExecutionError::InvalidArgument("task resource is too large for this worker".to_string())
    })?;
    let mut cache = lock_task_resource_cache();
    while semaphore.available_permits() < expected_size {
        if !evict_oldest_ready_task_resource(&mut cache) {
            break;
        }
    }
    Ok(())
}

fn acquire_task_resource_cache(key: &str) -> TaskResourceCacheAction {
    let mut cache = lock_task_resource_cache();
    match cache.entries.shift_remove(key) {
        Some(CachedTaskResource::Ready(materialized)) => {
            cache.entries.insert(
                key.to_string(),
                CachedTaskResource::Ready(materialized.clone()),
            );
            TaskResourceCacheAction::Ready(materialized)
        }
        Some(CachedTaskResource::Loading {
            generation,
            receiver,
        }) => {
            cache.entries.insert(
                key.to_string(),
                CachedTaskResource::Loading {
                    generation,
                    receiver: receiver.clone(),
                },
            );
            TaskResourceCacheAction::Wait(receiver)
        }
        None => {
            let generation = cache.next_generation;
            cache.next_generation = cache.next_generation.wrapping_add(1);
            let (sender, receiver) = watch::channel(None);
            cache.entries.insert(
                key.to_string(),
                CachedTaskResource::Loading {
                    generation,
                    receiver,
                },
            );
            TaskResourceCacheAction::Load(TaskResourceCacheLoader {
                key: key.to_string(),
                generation,
                sender,
                remove_loading_on_drop: true,
            })
        }
    }
}

async fn materialize_cached_task_resource(
    uri: &str,
    name: &str,
    expected_sha256: &str,
    expected_size: u64,
) -> ExecutionResult<MaterializedTaskResource> {
    validate_task_resource_sha256(expected_sha256)?;
    if expected_size > MAX_MATERIALIZED_TASK_RESOURCE_BYTES {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource {name} exceeded the materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        )));
    }
    let key = format!("{expected_sha256}:{expected_size}");
    loop {
        let action = acquire_task_resource_cache(&key);
        match action {
            TaskResourceCacheAction::Ready(data) => return Ok(data),
            TaskResourceCacheAction::Wait(mut receiver) => {
                let value = {
                    let result = receiver.wait_for(Option::is_some).await;
                    match result {
                        Ok(value) => value.clone(),
                        Err(_) => None,
                    }
                };
                let Some(value) = value else {
                    let mut cache = lock_task_resource_cache();
                    let loading_matches = cache.entries.get(&key).is_some_and(|entry| {
                        matches!(
                            entry,
                            CachedTaskResource::Loading { receiver: active, .. }
                                if active.same_channel(&receiver)
                        )
                    });
                    if loading_matches {
                        cache.entries.shift_remove(&key);
                    }
                    continue;
                };
                return value.map_err(ExecutionError::InternalError);
            }
            TaskResourceCacheAction::Load(loader) => {
                let semaphore = task_resource_materialization_semaphore();
                let memory_reservation = Arc::new(
                    acquire_task_resource_materialization_permit(semaphore, expected_size).await?,
                );
                let result: ExecutionResult<MaterializedTaskResource> =
                    download_task_resource(uri, name, expected_sha256, expected_size)
                        .await
                        .map(|data| MaterializedTaskResource {
                            data: Arc::from(data),
                            memory_reservation,
                        });
                loader.complete(&result);
                return result;
            }
        }
    }
}

async fn acquire_task_resource_materialization_permit(
    semaphore: &Arc<Semaphore>,
    expected_size: u64,
) -> ExecutionResult<OwnedSemaphorePermit> {
    if expected_size > MAX_MATERIALIZED_TASK_RESOURCE_BYTES {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource exceeded the materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        )));
    }
    let permits = u32::try_from(expected_size).map_err(|_| {
        ExecutionError::InvalidArgument(format!(
            "task resource exceeded the materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        ))
    })?;
    let mut cache_updates = task_resource_cache_updates().subscribe();
    loop {
        evict_ready_task_resources_for_admission(semaphore, expected_size)?;
        let acquire = Arc::clone(semaphore).acquire_many_owned(permits);
        tokio::select! {
            result = acquire => {
                return result.map_err(|_| {
                    ExecutionError::InternalError(
                        "task resource materialization limiter is unavailable".to_string(),
                    )
                });
            }
            update = cache_updates.changed() => {
                if update.is_err() {
                    return Err(ExecutionError::InternalError(
                        "task resource cache admission monitor is unavailable".to_string(),
                    ));
                }
            }
        }
    }
}

fn task_resource_uri(
    base_uri: &str,
    namespace: &str,
    staging_scope: TaskResourceStagingScope,
    category: &str,
    sha256: &str,
) -> ExecutionResult<String> {
    let mut url = Url::parse(base_uri).map_err(|_| {
        ExecutionError::InvalidArgument("invalid spark.artifact_store_uri value".to_string())
    })?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ExecutionError::InvalidArgument(
            "spark.artifact_store_uri must not contain userinfo, query parameters, or a fragment"
                .to_string(),
        ));
    }
    if sha256.len() < 2 {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource SHA-256 is unexpectedly short: {sha256}"
        )));
    }
    let prefix = url.path().trim_end_matches('/');
    let TaskResourceStagingScope {
        job_id,
        stage,
        staging_id,
    } = staging_scope;
    let suffix = format!(
        "sail-artifacts/execution/{namespace}/jobs/{job_id}/{stage}/staging/{staging_id}/{category}/{}/{sha256}",
        &sha256[..2]
    );
    let path = if prefix.is_empty() || prefix == "/" {
        format!("/{suffix}")
    } else {
        format!("{prefix}/{suffix}")
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

async fn upload_task_resource(
    uri: &str,
    data: Arc<[u8]>,
    transfer_timeout: Duration,
) -> ExecutionResult<()> {
    let upload_permit =
        acquire_task_resource_upload_permit(task_resource_upload_semaphore()).await?;
    let upload_uri = uri.to_string();
    let upload = tokio::spawn(async move {
        let result =
            await_task_resource_upload(transfer_timeout, put_task_resource(&upload_uri, data))
                .await;
        drop(upload_permit);
        result
    });
    upload.await.map_err(|error| {
        ExecutionError::InternalError(format!("task resource upload task failed: {error}"))
    })?
}

async fn acquire_task_resource_upload_permit(
    semaphore: &Arc<Semaphore>,
) -> ExecutionResult<OwnedSemaphorePermit> {
    Arc::clone(semaphore).acquire_owned().await.map_err(|_| {
        ExecutionError::InternalError("task resource upload limiter is unavailable".to_string())
    })
}

async fn await_task_resource_upload<F>(transfer_timeout: Duration, upload: F) -> ExecutionResult<()>
where
    F: Future<Output = ExecutionResult<()>>,
{
    tokio::time::timeout(transfer_timeout, upload)
        .await
        .unwrap_or_else(|_| {
            Err(ExecutionError::InvalidArgument(format!(
                "task resource upload timed out after {} ms",
                transfer_timeout.as_millis()
            )))
        })
}

async fn put_task_resource(uri: &str, data: Arc<[u8]>) -> ExecutionResult<()> {
    let url = Url::parse(uri)
        .map_err(|_| ExecutionError::InvalidArgument("invalid task resource URI".to_string()))?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url)
        .map_err(|_| ExecutionError::InvalidArgument("invalid task resource path".to_string()))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|error| ExecutionError::InternalError(error.to_string()))?;
    store
        .put(&path, PutPayload::from(bytes::Bytes::from_owner(data)))
        .await
        .map_err(|error| {
            ExecutionError::InternalError(format!("failed to upload task resource {uri}: {error}"))
        })?;
    Ok(())
}

async fn download_task_resource(
    uri: &str,
    name: &str,
    expected_sha256: &str,
    expected_size: u64,
) -> ExecutionResult<Vec<u8>> {
    if expected_size > MAX_MATERIALIZED_TASK_RESOURCE_BYTES {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource {name} exceeded the materialization limit of {MAX_MATERIALIZED_TASK_RESOURCE_BYTES} bytes"
        )));
    }
    let url = Url::parse(uri)
        .map_err(|_| ExecutionError::InvalidArgument("invalid task resource URI".to_string()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ExecutionError::InvalidArgument(
            "task resource URI must not contain userinfo, query parameters, or a fragment"
                .to_string(),
        ));
    }
    let (_scheme, path) = ObjectStoreScheme::parse(&url)
        .map_err(|_| ExecutionError::InvalidArgument("invalid task resource path".to_string()))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|error| ExecutionError::InternalError(error.to_string()))?;
    let result = store.get(&path).await.map_err(|error| {
        ExecutionError::InternalError(format!("failed to download task resource {uri}: {error}"))
    })?;
    if result.meta.size != expected_size {
        return Err(ExecutionError::InvalidArgument(format!(
            "task resource {name} size mismatch: expected {expected_size}, object metadata reported {}",
            result.meta.size
        )));
    }
    let capacity = usize::try_from(expected_size).map_err(|_| {
        ExecutionError::InvalidArgument(format!("task resource {name} is too large"))
    })?;
    let mut data = Vec::with_capacity(capacity);
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            ExecutionError::InternalError(format!(
                "failed to read task resource bytes {uri}: {error}"
            ))
        })?;
        let observed = data.len().checked_add(chunk.len()).ok_or_else(|| {
            ExecutionError::InvalidArgument("task resource byte count overflow".to_string())
        })?;
        if observed as u64 > expected_size {
            return Err(ExecutionError::InvalidArgument(format!(
                "task resource {name} exceeded its declared size of {expected_size} bytes"
            )));
        }
        data.extend_from_slice(&chunk);
    }
    validate_task_resource_data(name, expected_sha256, expected_size, &data)?;
    Ok(data)
}

#[cfg(test)]
async fn cleanup_task_resource_uris(uris: Vec<String>) {
    for uri in uris {
        let result = delete_task_resource_uri(&uri, TASK_RESOURCE_DELETE_TIMEOUT_MAX).await;
        if let Err(error) = result {
            log::warn!("failed to clean up a task resource: {error}");
        }
    }
}

pub(crate) fn handoff_task_resource_cleanup_blocking(
    uris: Vec<String>,
    retry_for: Duration,
    operation_timeout: Duration,
) -> ExecutionResult<()> {
    if uris.is_empty() {
        return Ok(());
    }
    let owner = task_resource_cleanup_owner().map_err(ExecutionError::InternalError)?;
    let request = new_task_resource_cleanup_request(
        uris,
        retry_for,
        operation_timeout,
        owner.owner_id.clone(),
        false,
    );
    owner.handoff_blocking(request).map_err(|error| {
        ExecutionError::InternalError(format!(
            "failed to hand off task resource cleanup ownership: {error}"
        ))
    })
}

fn new_task_resource_cleanup_request(
    uris: Vec<String>,
    retry_for: Duration,
    operation_timeout: Duration,
    owner_id: String,
    dormant: bool,
) -> TaskResourceCleanupRequest {
    let now = unix_time_millis();
    TaskResourceCleanupRequest {
        uris,
        retry_for_millis: duration_millis(retry_for),
        verify_until_unix_millis: now.saturating_add(duration_millis(retry_for)),
        operation_timeout_millis: duration_millis(operation_timeout.max(Duration::from_millis(1))),
        retry_attempt: 0,
        next_attempt_unix_millis: now,
        owner_id,
        dormant,
    }
}

fn task_resource_cleanup_operation_timeout(transfer_timeout: Duration) -> Duration {
    transfer_timeout
        .max(Duration::from_millis(1))
        .min(TASK_RESOURCE_DELETE_TIMEOUT_MAX)
}

async fn delete_task_resource_uri(uri: &str, timeout: Duration) -> Result<(), String> {
    let url = Url::parse(uri).map_err(|error| error.to_string())?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url).map_err(|error| error.to_string())?;
    let store =
        sail_object_store::get_dynamic_object_store(&url).map_err(|error| error.to_string())?;
    await_task_resource_delete(timeout, store.delete(&path)).await
}

async fn await_task_resource_delete<F>(timeout: Duration, delete: F) -> Result<(), String>
where
    F: Future<Output = object_store::Result<()>>,
{
    match tokio::time::timeout(timeout, delete).await {
        Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!(
            "task resource deletion timed out after {} ms",
            timeout.as_millis()
        )),
    }
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl From<TaskLaunchContext> for r#gen::TaskLaunchContext {
    fn from(value: TaskLaunchContext) -> Self {
        let TaskLaunchContext {
            resources,
            resource_memory_reservations: _,
        } = value;
        r#gen::TaskLaunchContext {
            resources: Some(resources.into()),
        }
    }
}

impl TryFrom<r#gen::TaskLaunchContext> for TaskLaunchContext {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskLaunchContext) -> Result<Self, Self::Error> {
        Ok(TaskLaunchContext {
            resources: value
                .resources
                .map(TaskResources::try_from)
                .transpose()?
                .unwrap_or_default(),
            resource_memory_reservations: vec![],
        })
    }
}

impl From<TaskResources> for r#gen::TaskResources {
    fn from(value: TaskResources) -> Self {
        let TaskResources {
            python_artifacts,
            local_relation_resources,
        } = value;
        r#gen::TaskResources {
            python_artifacts: python_artifacts.into_iter().map(|x| x.into()).collect(),
            local_relation_resources: local_relation_resources
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

impl TryFrom<r#gen::TaskResources> for TaskResources {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskResources) -> Result<Self, Self::Error> {
        Ok(TaskResources {
            python_artifacts: value
                .python_artifacts
                .into_iter()
                .map(|x| x.try_into())
                .collect::<ExecutionResult<Vec<_>>>()?,
            local_relation_resources: value
                .local_relation_resources
                .into_iter()
                .map(|x| x.try_into())
                .collect::<ExecutionResult<Vec<_>>>()?,
        })
    }
}

impl From<LocalRelationResource> for r#gen::LocalRelationResource {
    fn from(value: LocalRelationResource) -> Self {
        let LocalRelationResource {
            key,
            data,
            uri,
            sha256,
            size,
        } = value;
        r#gen::LocalRelationResource {
            key,
            data: data.map(|data| data.to_vec()),
            uri,
            sha256,
            size,
        }
    }
}

impl TryFrom<r#gen::LocalRelationResource> for LocalRelationResource {
    type Error = ExecutionError;

    fn try_from(value: r#gen::LocalRelationResource) -> Result<Self, Self::Error> {
        validate_local_relation_resource(&value)?;
        Ok(Self {
            key: value.key,
            data: value.data.map(Into::into),
            uri: value.uri,
            sha256: value.sha256,
            size: value.size,
        })
    }
}

impl From<PySparkPythonArtifact> for r#gen::PySparkPythonArtifact {
    fn from(value: PySparkPythonArtifact) -> Self {
        let PySparkPythonArtifact {
            scope_id,
            name,
            python_path,
            data,
            uri,
            sha256,
            size,
            kind,
        } = value;
        r#gen::PySparkPythonArtifact {
            scope_id,
            name,
            python_path,
            data: data.map(|data| data.to_vec()),
            uri,
            sha256,
            size,
            kind: encode_pyspark_artifact_kind(kind) as i32,
        }
    }
}

impl TryFrom<r#gen::PySparkPythonArtifact> for PySparkPythonArtifact {
    type Error = ExecutionError;

    fn try_from(value: r#gen::PySparkPythonArtifact) -> Result<Self, Self::Error> {
        let kind = decode_pyspark_artifact_kind(value.kind)?;
        validate_pyspark_artifact(&value)?;
        Ok(Self {
            scope_id: value.scope_id,
            name: value.name,
            python_path: value.python_path,
            data: value.data.map(Into::into),
            uri: value.uri,
            sha256: value.sha256,
            size: value.size,
            kind,
        })
    }
}

fn validate_pyspark_artifact(value: &r#gen::PySparkPythonArtifact) -> ExecutionResult<()> {
    if value.scope_id.len() != 64 || !value.scope_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExecutionError::InvalidArgument(
            "PySpark artifact session scope must be a SHA-256 hex digest".to_string(),
        ));
    }
    if value.name.is_empty() {
        return Err(ExecutionError::InvalidArgument(
            "PySpark artifact name must not be empty".to_string(),
        ));
    }
    if value.python_path.is_empty() {
        return Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have a local Python path",
            value.name
        )));
    }
    if value.sha256.len() != 64 || !value.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have a SHA-256 hex digest",
            value.name
        )));
    }
    match (&value.data, value.uri.as_deref()) {
        (Some(_), Some(_)) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must not have both inline data and an object-store URI",
            value.name
        ))),
        (None, None) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have inline data or an object-store URI",
            value.name
        ))),
        (Some(data), None) if data.len() as u64 != value.size => {
            Err(ExecutionError::InvalidArgument(format!(
                "PySpark artifact {} inline data size does not match declared size",
                value.name
            )))
        }
        (_, Some("")) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must not have an empty object-store URI",
            value.name
        ))),
        _ => Ok(()),
    }
}

fn validate_local_relation_resource(value: &r#gen::LocalRelationResource) -> ExecutionResult<()> {
    if value.key.is_empty() {
        return Err(ExecutionError::InvalidArgument(
            "LocalRelation resource key must not be empty".to_string(),
        ));
    }
    if value.sha256.len() != 64 || !value.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must have a SHA-256 hex digest",
            value.key
        )));
    }
    if value.key != value.sha256 {
        return Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource key {} must match its SHA-256 digest",
            value.key
        )));
    }
    match (&value.data, value.uri.as_deref()) {
        (Some(_), Some(_)) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must not have both inline data and an object-store URI",
            value.key
        ))),
        (None, None) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must have inline data or an object-store URI",
            value.key
        ))),
        (Some(data), None) if data.len() as u64 != value.size => {
            Err(ExecutionError::InvalidArgument(format!(
                "LocalRelation resource {} inline data size does not match declared size",
                value.key
            )))
        }
        (_, Some("")) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must not have an empty object-store URI",
            value.key
        ))),
        _ => Ok(()),
    }
}

fn decode_pyspark_artifact_kind(kind: i32) -> ExecutionResult<PySparkArtifactKind> {
    let kind = r#gen::PySparkArtifactKind::try_from(kind).map_err(|e| {
        ExecutionError::InvalidArgument(format!("invalid PySpark artifact kind: {e}"))
    })?;
    match kind {
        r#gen::PySparkArtifactKind::Unspecified => Err(ExecutionError::InvalidArgument(
            "PySpark artifact kind must not be unspecified".to_string(),
        )),
        r#gen::PySparkArtifactKind::PyFile => Ok(PySparkArtifactKind::PyFile),
        r#gen::PySparkArtifactKind::File => Ok(PySparkArtifactKind::File),
        r#gen::PySparkArtifactKind::Archive => Ok(PySparkArtifactKind::Archive),
    }
}

fn encode_pyspark_artifact_kind(kind: PySparkArtifactKind) -> r#gen::PySparkArtifactKind {
    match kind {
        PySparkArtifactKind::PyFile => r#gen::PySparkArtifactKind::PyFile,
        PySparkArtifactKind::File => r#gen::PySparkArtifactKind::File,
        PySparkArtifactKind::Archive => r#gen::PySparkArtifactKind::Archive,
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn local_relation_context(data: Vec<u8>) -> TaskLaunchContext {
        let sha256 = sha256_hex(&data);
        TaskLaunchContext {
            resources: TaskResources {
                local_relation_resources: vec![LocalRelationResource {
                    key: sha256.clone(),
                    size: data.len() as u64,
                    data: Some(data.into()),
                    uri: None,
                    sha256,
                }],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn staging_scope(staging_id: u64) -> TaskResourceStagingScope {
        TaskResourceStagingScope {
            job_id: 1_u64.into(),
            stage: 2,
            staging_id,
        }
    }

    #[test]
    fn staging_attempts_use_distinct_object_uris() {
        let sha256 = "a".repeat(64);
        let first = task_resource_uri(
            "s3://bucket/prefix",
            "test",
            staging_scope(7),
            "local-relations",
            &sha256,
        )
        .unwrap();
        let retry = task_resource_uri(
            "s3://bucket/prefix",
            "test",
            staging_scope(8),
            "local-relations",
            &sha256,
        )
        .unwrap();

        assert_ne!(first, retry);
        assert!(first.contains("/jobs/1/2/staging/7/"));
        assert!(retry.contains("/jobs/1/2/staging/8/"));
    }

    #[tokio::test]
    async fn staging_requires_store_when_resource_cannot_be_inline() {
        let error = stage_task_launch_context(
            local_relation_context(vec![1]),
            staging_scope(1),
            "test",
            0,
            MAX_INLINE_TASK_RESOURCE_BYTES,
            None,
            Duration::from_secs(1),
        )
        .await
        .err()
        .expect("staging must fail without a store");
        assert!(error.to_string().contains("artifact_store_uri"));
    }

    #[tokio::test]
    async fn staged_resource_round_trips_through_object_store() {
        let temp = tempfile::tempdir().unwrap();
        let store_uri = Url::from_directory_path(temp.path()).unwrap().to_string();
        let staged = stage_task_launch_context(
            local_relation_context(vec![1, 2, 3]),
            staging_scope(1),
            "test",
            0,
            MAX_INLINE_TASK_RESOURCE_BYTES,
            Some(&store_uri),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let (launch_context, cleanup_leases) = staged.accept().unwrap();
        assert_eq!(cleanup_leases.len(), 1);
        let cleanup_request =
            read_task_resource_cleanup_request(&cleanup_leases[0].state.journal_path).unwrap();
        assert!(cleanup_request.dormant);
        assert_eq!(cleanup_request.uris, vec![cleanup_leases[0].uri()]);
        assert!(
            launch_context.resources.local_relation_resources[0]
                .data
                .is_none()
        );

        let materialized = materialize_task_launch_context(launch_context)
            .await
            .unwrap();
        assert_eq!(
            materialized.resources.local_relation_resources[0]
                .data
                .as_deref(),
            Some(&[1, 2, 3][..])
        );
        cleanup_task_resource_uris(
            cleanup_leases
                .iter()
                .map(|lease| lease.uri().to_string())
                .collect(),
        )
        .await;
        cleanup_leases
            .iter()
            .for_each(TaskResourceCleanupLease::activate);
    }

    #[tokio::test]
    async fn driver_materialization_preserves_python_artifact_fallbacks() {
        let launch_context = TaskLaunchContext {
            resources: TaskResources {
                python_artifacts: vec![PySparkPythonArtifact {
                    scope_id: "0".repeat(64),
                    name: "pyfiles/example.py".to_string(),
                    python_path: "/verified/server/path/example.py".to_string(),
                    data: None,
                    uri: Some("invalid://driver-must-not-download".to_string()),
                    sha256: "1".repeat(64),
                    size: 10,
                    kind: PySparkArtifactKind::PyFile,
                }],
                ..Default::default()
            },
            ..Default::default()
        };

        let materialized = materialize_driver_task_launch_context(launch_context)
            .await
            .unwrap();
        let artifact = &materialized.resources.python_artifacts[0];
        assert!(artifact.data.is_none());
        assert_eq!(
            artifact.uri.as_deref(),
            Some("invalid://driver-must-not-download")
        );
    }

    #[tokio::test]
    async fn worker_reuses_validated_task_resource_content() {
        let temp = tempfile::tempdir().unwrap();
        let data = format!(
            "task-resource-cache-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
        )
        .into_bytes();
        let sha256 = sha256_hex(&data);
        let source = temp.path().join("source");
        std::fs::write(&source, &data).unwrap();
        let source_uri = Url::from_file_path(&source).unwrap().to_string();

        let first =
            materialize_cached_task_resource(&source_uri, "resource", &sha256, data.len() as u64)
                .await
                .unwrap();
        assert_eq!(first.data.as_ref(), data.as_slice());

        let missing_uri = Url::from_file_path(temp.path().join("missing"))
            .unwrap()
            .to_string();
        let cached =
            materialize_cached_task_resource(&missing_uri, "resource", &sha256, data.len() as u64)
                .await
                .unwrap();
        assert!(Arc::ptr_eq(&first.data, &cached.data));
        assert!(Arc::ptr_eq(
            &first.memory_reservation,
            &cached.memory_reservation
        ));
    }

    #[tokio::test]
    async fn worker_cache_recovers_after_a_canceled_loader() {
        let temp = tempfile::tempdir().unwrap();
        let data = format!(
            "canceled-task-resource-loader-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
        )
        .into_bytes();
        let sha256 = sha256_hex(&data);
        let key = format!("{}:{}", sha256, data.len());
        let source = temp.path().join("source");
        std::fs::write(&source, &data).unwrap();
        let source_uri = Url::from_file_path(&source).unwrap().to_string();
        let loader = match acquire_task_resource_cache(&key) {
            TaskResourceCacheAction::Load(loader) => Some(loader),
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => None,
        }
        .expect("unique test resource must claim the cache load");
        drop(loader);
        assert!(!lock_task_resource_cache().entries.contains_key(&key));

        let recovered =
            materialize_cached_task_resource(&source_uri, "resource", &sha256, data.len() as u64)
                .await
                .unwrap();
        assert_eq!(recovered.data.as_ref(), data.as_slice());
    }

    #[test]
    fn canceled_cache_loader_does_not_remove_a_new_generation() {
        let key = format!("{}:1", sha256_hex(b"cache-loader-generation"));
        let loader = match acquire_task_resource_cache(&key) {
            TaskResourceCacheAction::Load(loader) => Some(loader),
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => None,
        }
        .expect("unique test resource must claim the cache load");
        let replacement_generation = loader.generation.wrapping_add(1);
        let (replacement_sender, replacement_receiver) = watch::channel(None);
        lock_task_resource_cache().entries.insert(
            key.clone(),
            CachedTaskResource::Loading {
                generation: replacement_generation,
                receiver: replacement_receiver,
            },
        );

        drop(loader);

        assert!(matches!(
            lock_task_resource_cache().entries.get(&key),
            Some(CachedTaskResource::Loading { generation, .. })
                if *generation == replacement_generation
        ));
        lock_task_resource_cache().entries.shift_remove(&key);
        drop(replacement_sender);
    }

    #[tokio::test]
    async fn aggregate_admission_precedes_partial_resource_acquisition() {
        let admission_semaphore = Arc::new(Semaphore::new(10));
        let resource_semaphore = Arc::new(Semaphore::new(10));
        let first_admission =
            acquire_task_resource_materialization_admission(&admission_semaphore, 6)
                .await
                .unwrap();
        let first_resource = acquire_task_resource_materialization_permit(&resource_semaphore, 6)
            .await
            .unwrap();
        let (partial_acquisition, mut partial_acquisition_started) = mpsc::channel(1);
        let second_admission_semaphore = Arc::clone(&admission_semaphore);
        let second_resource_semaphore = Arc::clone(&resource_semaphore);
        let mut second = tokio::spawn(async move {
            let admission =
                acquire_task_resource_materialization_admission(&second_admission_semaphore, 6)
                    .await?;
            partial_acquisition.send(()).await.map_err(|error| {
                ExecutionError::InternalError(format!(
                    "could not report partial resource acquisition: {error}"
                ))
            })?;
            let resource =
                acquire_task_resource_materialization_permit(&second_resource_semaphore, 6).await?;
            Ok::<_, ExecutionError>((admission, resource))
        });

        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                partial_acquisition_started.recv()
            )
            .await
            .is_err(),
            "the second set must not acquire a partial resource before aggregate admission"
        );

        drop(first_admission);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), partial_acquisition_started.recv())
                .await
                .unwrap(),
            Some(())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut second)
                .await
                .is_err(),
            "the first materialized context must retain its resource reservation"
        );

        drop(first_resource);
        let (second_admission, second_resource) =
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        drop(second_resource);
        drop(second_admission);
    }

    #[tokio::test]
    #[expect(clippy::panic, reason = "a different cache action is a test failure")]
    async fn distinct_resource_loaders_wait_for_the_weighted_materialization_budget() {
        let semaphore = Arc::new(Semaphore::new(8));
        let first_key = format!("{}:6", sha256_hex(b"materialization-budget-first"));
        let second_key = format!("{}:4", sha256_hex(b"materialization-budget-second"));
        let first_loader = match acquire_task_resource_cache(&first_key) {
            TaskResourceCacheAction::Load(loader) => loader,
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => {
                panic!("unique first resource must claim the cache load")
            }
        };
        let second_loader = match acquire_task_resource_cache(&second_key) {
            TaskResourceCacheAction::Load(loader) => loader,
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => {
                panic!("unique second resource must claim the cache load")
            }
        };
        let first_permit = acquire_task_resource_materialization_permit(&semaphore, 6)
            .await
            .unwrap();
        let second_semaphore = Arc::clone(&semaphore);
        let mut second = tokio::spawn(async move {
            let permit = acquire_task_resource_materialization_permit(&second_semaphore, 4).await?;
            Ok::<_, ExecutionError>((second_loader, permit))
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut second)
                .await
                .is_err(),
            "the second distinct resource must wait while the aggregate budget is exhausted"
        );

        drop(first_permit);
        let (second_loader, second_permit) = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second_permit.num_permits(), 4);
        drop(second_permit);
        drop(second_loader);
        drop(first_loader);
    }

    #[tokio::test]
    #[expect(clippy::panic, reason = "a different cache action is a test failure")]
    async fn active_context_reservation_survives_cache_eviction_and_blocks_a_new_hash() {
        let semaphore = Arc::new(Semaphore::new(8));
        let first_key = format!("{}:6", sha256_hex(b"active-context-reservation"));
        let second_key = format!("{}:4", sha256_hex(b"post-eviction-loader"));
        let first_loader = match acquire_task_resource_cache(&first_key) {
            TaskResourceCacheAction::Load(loader) => loader,
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => {
                panic!("unique first resource must claim the cache load")
            }
        };
        let first_reservation = Arc::new(
            acquire_task_resource_materialization_permit(&semaphore, 6)
                .await
                .unwrap(),
        );
        let active_context_reservations = vec![Arc::clone(&first_reservation)];
        let second_loader = match acquire_task_resource_cache(&second_key) {
            TaskResourceCacheAction::Load(loader) => loader,
            TaskResourceCacheAction::Ready(_) | TaskResourceCacheAction::Wait(_) => {
                panic!("unique second resource must claim the cache load")
            }
        };
        let second_semaphore = Arc::clone(&semaphore);
        let mut second = tokio::spawn(async move {
            let permit = acquire_task_resource_materialization_permit(&second_semaphore, 4).await?;
            Ok::<_, ExecutionError>((second_loader, permit))
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut second)
                .await
                .is_err(),
            "the second resource must initially wait for the loading reservation"
        );

        let first_result: ExecutionResult<MaterializedTaskResource> =
            Ok(MaterializedTaskResource {
                data: Arc::from(vec![0_u8; 6]),
                memory_reservation: first_reservation,
            });
        first_loader.complete(&first_result);
        drop(first_result);
        tokio::time::timeout(Duration::from_secs(1), async {
            while lock_task_resource_cache().entries.contains_key(&first_key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut second)
                .await
                .is_err(),
            "evicting the cache must not release an active context reservation"
        );

        drop(active_context_reservations);
        let (second_loader, second_permit) = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second_permit.num_permits(), 4);
        drop(second_permit);
        drop(second_loader);
    }

    #[tokio::test]
    async fn unclaimed_completed_upload_cleans_its_object() {
        let temp = tempfile::tempdir().unwrap();
        let object = temp.path().join("late-upload");
        std::fs::write(&object, b"late bytes").unwrap();
        let uri = Url::from_file_path(&object).unwrap().to_string();
        let mut guard =
            TaskResourceUploadGuard::new(Duration::from_millis(10), Duration::from_millis(10));
        guard.create_cleanup_lease(uri).await.unwrap();
        drop(guard);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while object.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn upload_guard_drop_only_activates_durable_cleanup_leases() {
        let temp = tempfile::tempdir().unwrap();
        let state = Arc::new(TaskResourceCleanupLeaseState {
            uri: "file:///tmp/resource".to_string(),
            journal_path: temp.path().join("lease.json"),
            active: AtomicBool::new(false),
        });
        let mut guard =
            TaskResourceUploadGuard::new(Duration::from_secs(2), Duration::from_secs(1));
        guard.cleanup_leases.push(TaskResourceCleanupLease {
            state: Arc::clone(&state),
        });

        drop(guard);

        assert!(state.active.load(Ordering::Acquire));
    }

    #[test]
    fn unhealthy_cleanup_owner_rejects_new_reservations() {
        let healthy = AtomicBool::new(false);
        let lease_count = AtomicUsize::new(0);

        let error = reserve_task_resource_cleanup_lease_slot(&healthy, &lease_count)
            .expect_err("unhealthy cleanup owner must reject new spill work");

        assert!(error.contains("unhealthy"));
        assert_eq!(lease_count.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn task_resource_upload_has_an_internal_timeout() {
        let timeout = Duration::from_millis(10);
        let result = await_task_resource_upload(timeout, std::future::pending()).await;

        assert!(
            result
                .expect_err("hung upload must time out")
                .to_string()
                .contains("timed out")
        );
    }

    #[tokio::test]
    async fn task_resource_upload_limiter_queues_excess_work() {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASK_RESOURCE_UPLOADS));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (release, release_receiver) = watch::channel(false);
        let tasks = (0..12)
            .map(|_| {
                let semaphore = Arc::clone(&semaphore);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let mut release_receiver = release_receiver.clone();
                tokio::spawn(async move {
                    let permit = acquire_task_resource_upload_permit(&semaphore).await?;
                    let current = active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
                    peak.fetch_max(current, Ordering::SeqCst);
                    let _ = release_receiver.wait_for(|released| *released).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(permit);
                    Ok::<(), ExecutionError>(())
                })
            })
            .collect::<Vec<_>>();

        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) < MAX_CONCURRENT_TASK_RESOURCE_UPLOADS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            active.load(Ordering::SeqCst),
            MAX_CONCURRENT_TASK_RESOURCE_UPLOADS
        );
        assert!(tasks.iter().all(|task| !task.is_finished()));

        release.send_replace(true);
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(
            peak.load(Ordering::SeqCst),
            MAX_CONCURRENT_TASK_RESOURCE_UPLOADS
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn task_resource_delete_has_an_attempt_timeout() {
        let timeout = Duration::from_millis(10);
        let result =
            await_task_resource_delete(timeout, std::future::pending::<object_store::Result<()>>())
                .await;

        assert!(
            result
                .expect_err("hung deletion must time out")
                .contains("timed out")
        );
    }

    #[tokio::test]
    async fn failed_cleanup_attempt_retains_the_uri_for_retry() {
        let request = TaskResourceCleanupRequest {
            uris: vec!["not-a-task-resource-uri".to_string()],
            retry_for_millis: 0,
            verify_until_unix_millis: 0,
            operation_timeout_millis: 10,
            retry_attempt: 0,
            next_attempt_unix_millis: 0,
            owner_id: String::new(),
            dormant: false,
        };

        let completion = run_task_resource_cleanup_attempt(None, request).await;
        let retry = match completion.outcome {
            TaskResourceCleanupOutcome::Retry(retry) => Some(retry),
            TaskResourceCleanupOutcome::Complete => None,
        }
        .expect("failed cleanup must retain durable retry ownership");
        assert_eq!(retry.uris, vec!["not-a-task-resource-uri"]);
        assert_eq!(retry.retry_attempt, 1);
        assert!(retry.next_attempt_unix_millis > 0);
    }

    #[test]
    fn cleanup_journal_recovery_removes_temporary_and_invalid_records() {
        let temp = tempfile::tempdir().unwrap();
        let temporary = temp.path().join(".cleanup-test.tmp");
        let invalid = temp.path().join("invalid.json");
        let valid = temp.path().join("valid.json");
        std::fs::write(&temporary, b"partial").unwrap();
        std::fs::write(&invalid, b"not-json").unwrap();
        let request = TaskResourceCleanupRequest {
            uris: vec!["file:///tmp/resource".to_string()],
            retry_for_millis: 0,
            verify_until_unix_millis: 0,
            operation_timeout_millis: 10,
            retry_attempt: 0,
            next_attempt_unix_millis: 0,
            owner_id: String::new(),
            dormant: false,
        };
        std::fs::write(&valid, serde_json::to_vec(&request).unwrap()).unwrap();

        recover_task_resource_cleanup_journal(temp.path()).unwrap();

        assert!(!temporary.exists());
        assert!(!invalid.exists());
        assert!(valid.exists());
    }

    #[test]
    fn configured_cleanup_journal_is_private_and_process_wide() {
        let temp = tempfile::tempdir().unwrap();
        let configured = temp.path().join("configured");
        let conflicting = temp.path().join("conflicting");
        let journal_root_slot = OnceLock::new();

        let resolved =
            resolve_task_resource_cleanup_journal_root(&configured, &journal_root_slot).unwrap();
        assert_eq!(resolved, configured.canonicalize().unwrap());
        assert_eq!(
            resolve_task_resource_cleanup_journal_root(&configured, &journal_root_slot).unwrap(),
            resolved
        );
        let error = resolve_task_resource_cleanup_journal_root(&conflicting, &journal_root_slot)
            .unwrap_err();
        assert!(error.to_string().contains("process-wide"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&resolved).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn configured_cleanup_journal_rejects_relative_or_non_directory_paths() {
        let journal_root_slot = OnceLock::new();
        let relative_error = resolve_task_resource_cleanup_journal_root(
            Path::new("relative-journal"),
            &journal_root_slot,
        )
        .unwrap_err();
        assert!(relative_error.to_string().contains("absolute"));

        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("journal-file");
        std::fs::write(&file, b"not a directory").unwrap();
        assert!(resolve_task_resource_cleanup_journal_root(&file, &OnceLock::new()).is_err());
    }

    #[test]
    fn dormant_cleanup_recovery_retries_after_the_previous_owner_exits() {
        let temp = tempfile::tempdir().unwrap();
        let previous_owner = "previous-owner";
        let current_owner = "current-owner";
        let previous_owner_lock =
            acquire_task_resource_cleanup_owner_lock(temp.path(), previous_owner).unwrap();
        let journal_path = temp.path().join("dormant.json");
        let request = TaskResourceCleanupRequest {
            uris: vec!["file:///tmp/resource".to_string()],
            retry_for_millis: 10_000,
            verify_until_unix_millis: 1,
            operation_timeout_millis: 10,
            retry_attempt: 0,
            next_attempt_unix_millis: 1,
            owner_id: previous_owner.to_string(),
            dormant: true,
        };
        persist_task_resource_cleanup_request(
            temp.path(),
            &request,
            Some(&journal_path),
            &AtomicU64::new(0),
        )
        .unwrap();

        recover_dormant_task_resource_cleanups(temp.path(), current_owner).unwrap();
        let live_owner_request = read_task_resource_cleanup_request(&journal_path).unwrap();
        assert!(live_owner_request.dormant);
        assert_eq!(live_owner_request.owner_id, previous_owner);

        drop(previous_owner_lock);
        let activated_at = unix_time_millis();
        recover_dormant_task_resource_cleanups(temp.path(), current_owner).unwrap();
        let recovered = read_task_resource_cleanup_request(&journal_path).unwrap();
        assert!(!recovered.dormant);
        assert_eq!(recovered.owner_id, current_owner);
        assert!(
            recovered.verify_until_unix_millis
                >= activated_at.saturating_add(recovered.retry_for_millis)
        );
        assert!(recovered.next_attempt_unix_millis >= activated_at);
    }
}
