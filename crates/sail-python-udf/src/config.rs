use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::StreamExt;
use object_store::ObjectStoreExt;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{PyDict, PyDictMethods, PyModule, PyModuleMethods};
use pyo3::{Python, pyclass};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use url::Url;

use crate::archive::{
    ArchiveExtractionLimits, ArchiveExtractionSummary, extract_archive as extract_archive_contents,
};
use crate::error::{PyUdfError, PyUdfResult};

const PYFILES_PREFIX: &str = "pyfiles/";
const FILES_PREFIX: &str = "files/";
const ARCHIVES_PREFIX: &str = "archives/";
const MAX_ARTIFACT_NAME_BYTES: usize = 4096;
const MAX_ARTIFACT_PATH_COMPONENT_BYTES: usize = 255;
const MAX_ARTIFACT_PATH_COMPONENTS: usize = 128;
const PYTHON_ARTIFACT_CONTEXT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
const ARCHIVE_COMPLETION_MARKER_VERSION: &str = "sail-archive-manifest-v2";
static ARTIFACT_PROCESS_ROOT: OnceLock<Result<TempDir, String>> = OnceLock::new();
static INSTALLED_ARTIFACT_PYTHON_PATHS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static INSTALLED_ARTIFACT_CONTEXT_KEY: OnceLock<Mutex<Option<String>>> = OnceLock::new();
type ArchiveManifestCache = Mutex<HashMap<ArchiveManifestCacheKey, CachedArchiveSummary>>;
static AUTHENTICATED_ARCHIVE_MANIFESTS: OnceLock<ArchiveManifestCache> = OnceLock::new();
static PYTHON_ARTIFACT_CONTEXT_LOCK: OnceLock<Arc<PythonArtifactContextLock>> = OnceLock::new();
static PYTHON_ARTIFACT_CLEANUP_STARTED: OnceLock<Mutex<bool>> = OnceLock::new();
static PYTHON_ARTIFACT_MATERIALIZATION_LOCK: OnceLock<PythonArtifactMaterializationLock> =
    OnceLock::new();
const SPARK_FILES_SHIM_SOURCE: &str = include_str!("python/spark_files.py");
const SPARK_FILES_SHIM_REGISTER_SOURCE: &str = include_str!("python/spark_files_register.py");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PySparkArtifactKind {
    PyFile,
    File,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PySparkPythonArtifact {
    pub scope_id: String,
    pub name: String,
    pub python_path: String,
    pub data: Option<Arc<[u8]>>,
    pub uri: Option<String>,
    pub sha256: String,
    pub size: u64,
    pub kind: PySparkArtifactKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
#[pyclass(frozen, from_py_object)]
pub struct PySparkUdfConfig {
    #[pyo3(get)]
    pub session_timezone: String,
    #[pyo3(get, name = "window_bound_types")]
    pub pandas_window_bound_types: Option<String>,
    #[pyo3(get, name = "assign_columns_by_name")]
    pub pandas_grouped_map_assign_columns_by_name: bool,
    #[pyo3(get, name = "arrow_convert_safely")]
    pub pandas_convert_to_arrow_array_safely: bool,
    #[pyo3(get)]
    pub arrow_max_records_per_batch: usize,
    #[pyo3(get)]
    pub python_udf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udtf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udf_pandas_int_to_decimal_coercion_enabled: bool,
    #[pyo3(get)]
    pub binary_as_bytes: bool,
    pub python_artifacts: Arc<[PySparkPythonArtifact]>,
}

struct PythonArtifactContextLock {
    state: Mutex<PythonArtifactContextState>,
    available: Condvar,
}

struct PythonArtifactContextState {
    active_key: Option<String>,
    active_scope: Option<String>,
    holders: usize,
    preparing: bool,
    shutting_down: bool,
    generations: HashMap<String, u64>,
    context_keys: HashMap<String, String>,
    cleaning: HashSet<String>,
    cleanup_deadlines: HashMap<String, PythonArtifactCleanupDeadline>,
}

#[derive(Clone)]
struct PythonArtifactCleanupDeadline {
    root: PathBuf,
    generation: u64,
    deadline: Instant,
}

pub struct PythonArtifactContextGuard {
    lock: Arc<PythonArtifactContextLock>,
    key: String,
    scope_id: String,
    root: PathBuf,
    materialization_generation: u64,
}

#[derive(Clone)]
struct PythonArtifactMaterializationContext {
    scope_id: String,
    context_key: String,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArchiveManifestCacheKey {
    scope_id: String,
    context_key: String,
    generation: u64,
    destination_key: String,
    archive_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CachedArchiveSummary {
    expanded_bytes: u64,
    entries: usize,
    records: usize,
}

fn authenticated_archive_manifests() -> &'static ArchiveManifestCache {
    AUTHENTICATED_ARCHIVE_MANIFESTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn invalidate_archive_manifest_scope_in(manifests: &ArchiveManifestCache, scope_id: &str) {
    let mut manifests = manifests.lock().unwrap_or_else(|error| {
        log::warn!("recovering poisoned archive manifest cache: {error}");
        error.into_inner()
    });
    manifests.retain(|key, _| key.scope_id != scope_id);
}

fn invalidate_archive_manifest_scope(scope_id: &str) {
    invalidate_archive_manifest_scope_in(authenticated_archive_manifests(), scope_id);
}

fn invalidate_all_archive_manifests() {
    authenticated_archive_manifests()
        .lock()
        .unwrap_or_else(|error| {
            log::warn!("recovering poisoned archive manifest cache: {error}");
            error.into_inner()
        })
        .clear();
}

struct PythonArtifactMaterializationLock {
    active: Mutex<bool>,
    available: Condvar,
}

struct PythonArtifactMaterializationGuard(&'static PythonArtifactMaterializationLock);

impl PythonArtifactMaterializationLock {
    fn new() -> Self {
        Self {
            active: Mutex::new(false),
            available: Condvar::new(),
        }
    }

    fn acquire(&'static self) -> PyUdfResult<PythonArtifactMaterializationGuard> {
        let mut active = self.active.lock().map_err(|error| {
            PyUdfError::internal(format!(
                "failed to lock Python artifact materialization: {error}"
            ))
        })?;
        while *active {
            active = self.available.wait(active).map_err(|error| {
                PyUdfError::internal(format!(
                    "failed to wait for Python artifact materialization: {error}"
                ))
            })?;
        }
        *active = true;
        Ok(PythonArtifactMaterializationGuard(self))
    }
}

impl Drop for PythonArtifactMaterializationGuard {
    fn drop(&mut self) {
        match self.0.active.lock() {
            Ok(mut active) => {
                *active = false;
                self.0.available.notify_one();
            }
            Err(error) => log::error!("failed to unlock Python artifact materialization: {error}"),
        }
    }
}

impl PythonArtifactContextLock {
    fn new() -> Self {
        Self {
            state: Mutex::new(PythonArtifactContextState {
                active_key: None,
                active_scope: None,
                holders: 0,
                preparing: false,
                shutting_down: false,
                generations: HashMap::new(),
                context_keys: HashMap::new(),
                cleaning: HashSet::new(),
                cleanup_deadlines: HashMap::new(),
            }),
            available: Condvar::new(),
        }
    }

    fn acquire(
        key: String,
        scope_id: String,
        root: PathBuf,
    ) -> PyUdfResult<PythonArtifactContextGuard> {
        let lock = Arc::clone(
            PYTHON_ARTIFACT_CONTEXT_LOCK.get_or_init(|| Arc::new(PythonArtifactContextLock::new())),
        );
        start_python_artifact_cleanup_thread(Arc::clone(&lock));
        Self::acquire_context(lock, key, scope_id, root)
    }

    fn acquire_context(
        lock: Arc<Self>,
        key: String,
        scope_id: String,
        root: PathBuf,
    ) -> PyUdfResult<PythonArtifactContextGuard> {
        let (generation, context_changed, cleanups) = {
            let mut state = lock.state.lock().map_err(|e| {
                PyUdfError::internal(format!("failed to lock Python artifact context: {e}"))
            })?;
            while state.shutting_down
                || state.preparing
                || !state.cleaning.is_empty()
                || state
                    .active_key
                    .as_ref()
                    .is_some_and(|active| active != &key)
            {
                let (next_state, wait) = lock
                    .available
                    .wait_timeout(state, PYTHON_ARTIFACT_CONTEXT_WAIT_TIMEOUT)
                    .map_err(|e| {
                        PyUdfError::internal(format!(
                            "failed to wait for Python artifact context: {e}"
                        ))
                    })?;
                state = next_state;
                if wait.timed_out()
                    && (state.shutting_down
                        || state.preparing
                        || !state.cleaning.is_empty()
                        || state
                            .active_key
                            .as_ref()
                            .is_some_and(|active| active != &key))
                {
                    return Err(PyUdfError::invalid(
                        "timed out waiting for another Python artifact context to finish",
                    ));
                }
            }
            if state.active_key.is_some() {
                if state.active_scope.as_ref() != Some(&scope_id) {
                    return Err(PyUdfError::internal(
                        "Python artifact context key was reused across session scopes",
                    ));
                }
                let generation = state.generations.get(&scope_id).copied().ok_or_else(|| {
                    PyUdfError::internal("active Python artifact context has no generation")
                })?;
                state.holders += 1;
                return Ok(PythonArtifactContextGuard {
                    lock: Arc::clone(&lock),
                    key,
                    scope_id,
                    root,
                    materialization_generation: generation,
                });
            }

            state.preparing = true;
            state.active_key = Some(key.clone());
            state.active_scope = Some(scope_id.clone());
            state.cleanup_deadlines.remove(&scope_id);
            let context_changed = state.context_keys.get(&scope_id) != Some(&key)
                || !state.generations.contains_key(&scope_id);
            let generation = if context_changed {
                let generation = state
                    .generations
                    .get(&scope_id)
                    .copied()
                    .unwrap_or_default()
                    .wrapping_add(1)
                    .max(1);
                state.generations.insert(scope_id.clone(), generation);
                state.context_keys.insert(scope_id.clone(), key.clone());
                generation
            } else {
                state.generations[&scope_id]
            };
            let cleanups = state
                .cleanup_deadlines
                .iter()
                .filter(|(candidate, _)| *candidate != &scope_id)
                .map(|(candidate, cleanup)| (candidate.clone(), cleanup.clone()))
                .collect::<Vec<_>>();
            for (candidate, _) in &cleanups {
                state.cleanup_deadlines.remove(candidate);
                state.cleaning.insert(candidate.clone());
            }
            (generation, context_changed, cleanups)
        };

        if context_changed {
            invalidate_archive_manifest_scope(&scope_id);
        }
        let cleanup_results = cleanups
            .into_iter()
            .map(|(candidate, cleanup)| {
                invalidate_archive_manifest_scope(&candidate);
                let result = remove_python_artifact_scope_root(&cleanup);
                (candidate, cleanup, result)
            })
            .collect::<Vec<_>>();

        {
            let mut state = lock.state.lock().map_err(|error| {
                PyUdfError::internal(format!(
                    "failed to finish Python artifact scope transition: {error}"
                ))
            })?;
            let mut cleanup_error = None;
            for (candidate, cleanup, result) in cleanup_results {
                state.cleaning.remove(&candidate);
                if state.generations.get(&candidate).copied() != Some(cleanup.generation) {
                    continue;
                }
                match result {
                    Ok(()) => {
                        state.generations.remove(&candidate);
                        state.context_keys.remove(&candidate);
                    }
                    Err(error) => {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                        state.cleanup_deadlines.insert(
                            candidate,
                            PythonArtifactCleanupDeadline {
                                deadline: Instant::now() + Duration::from_secs(60),
                                ..cleanup
                            },
                        );
                    }
                }
            }
            state.preparing = false;
            if let Some(error) = cleanup_error {
                state.active_key = None;
                state.active_scope = None;
                lock.available.notify_all();
                return Err(PyUdfError::internal(format!(
                    "failed to clean inactive Python artifact scope: {error}"
                )));
            }
            state.holders += 1;
            lock.available.notify_all();
        }
        Ok(PythonArtifactContextGuard {
            lock,
            key,
            scope_id,
            root,
            materialization_generation: generation,
        })
    }
}

impl PythonArtifactContextGuard {
    fn materialization_context(&self) -> PythonArtifactMaterializationContext {
        PythonArtifactMaterializationContext {
            scope_id: self.scope_id.clone(),
            context_key: self.key.clone(),
            generation: self.materialization_generation,
        }
    }
}

impl Drop for PythonArtifactContextGuard {
    fn drop(&mut self) {
        match self.lock.state.lock() {
            Ok(mut state) => {
                if state.active_key.as_ref() != Some(&self.key)
                    || state.active_scope.as_ref() != Some(&self.scope_id)
                    || state.holders == 0
                {
                    log::error!("inconsistent Python artifact context lock state");
                    return;
                }
                state.holders -= 1;
                if state.holders == 0 {
                    state.active_key = None;
                    state.active_scope = None;
                    self.lock.available.notify_all();
                    let generation = state
                        .generations
                        .get(&self.scope_id)
                        .copied()
                        .unwrap_or_default();
                    state.cleanup_deadlines.insert(
                        self.scope_id.clone(),
                        PythonArtifactCleanupDeadline {
                            root: self.root.clone(),
                            generation,
                            deadline: Instant::now() + Duration::from_secs(60),
                        },
                    );
                    self.lock.available.notify_all();
                }
            }
            Err(error) => {
                log::error!("failed to unlock Python artifact context: {error}");
            }
        }
    }
}

impl Default for PySparkUdfConfig {
    fn default() -> Self {
        Self {
            session_timezone: "UTC".to_string(),
            pandas_window_bound_types: None,
            pandas_grouped_map_assign_columns_by_name: true,
            pandas_convert_to_arrow_array_safely: false,
            arrow_max_records_per_batch: 10000,
            python_udf_pandas_conversion_enabled: false,
            python_udtf_pandas_conversion_enabled: false,
            python_udf_pandas_int_to_decimal_coercion_enabled: false,
            binary_as_bytes: true,
            python_artifacts: Arc::from([]),
        }
    }
}

impl PySparkUdfConfig {
    pub fn enter_python_artifact_context(
        &self,
        py: Python,
    ) -> PyUdfResult<PythonArtifactContextGuard> {
        validate_artifact_targets(&self.python_artifacts)?;
        let key = python_artifact_context_key(&self.python_artifacts);
        let scope_id = python_artifact_scope_id(&self.python_artifacts);
        let root = spark_files_root(&self.python_artifacts)?;
        let guard = py.detach(move || PythonArtifactContextLock::acquire(key, scope_id, root))?;
        let materialization_lock = PYTHON_ARTIFACT_MATERIALIZATION_LOCK
            .get_or_init(PythonArtifactMaterializationLock::new);
        let _materialization = py.detach(|| materialization_lock.acquire())?;
        let materialization_context = guard.materialization_context();
        self.install_python_artifacts_for_context(py, Some(&materialization_context))?;
        Ok(guard)
    }

    pub fn install_python_artifacts(&self, py: Python) -> PyUdfResult<()> {
        self.install_python_artifacts_for_context(py, None)
    }

    fn install_python_artifacts_for_context(
        &self,
        py: Python,
        materialization_context: Option<&PythonArtifactMaterializationContext>,
    ) -> PyUdfResult<()> {
        if self.python_artifacts.is_empty() {
            clear_python_artifacts(py)?;
            return Ok(());
        }
        let installed = self.resolve_artifacts(py, materialization_context)?;
        let context_changed = sync_installed_artifact_context_key(python_artifact_context_key(
            &self.python_artifacts,
        ))?;
        if context_changed {
            evict_python_modules_from_paths(py, &installed.python_paths)?;
        }
        if sync_artifact_python_paths(py, &installed.python_paths)? || context_changed {
            invalidate_import_caches(py)?;
        }
        Ok(())
    }

    fn resolve_artifacts(
        &self,
        py: Python,
        materialization_context: Option<&PythonArtifactMaterializationContext>,
    ) -> PyUdfResult<InstalledArtifacts> {
        validate_artifact_targets(&self.python_artifacts)?;
        let root = spark_files_root(&self.python_artifacts)?;
        std::fs::create_dir_all(&root)?;
        let private_root = private_artifact_root(&root)?;
        configure_spark_files(py, &root)?;

        let mut python_paths = vec![root.to_string_lossy().into_owned()];
        let mut archive_budget = RemainingArchiveExtraction::default();
        for artifact in self.python_artifacts.iter() {
            match artifact.kind {
                PySparkArtifactKind::PyFile => {
                    let target = root.join(artifact_spark_files_relative_path(artifact)?);
                    py.detach(|| materialize_artifact_file(artifact, &target))?;
                    python_paths.push(python_artifact_import_path(&artifact.name, &target)?);
                }
                PySparkArtifactKind::File => {
                    let target = root.join(artifact_spark_files_relative_path(artifact)?);
                    py.detach(|| materialize_artifact_file(artifact, &target))?;
                }
                PySparkArtifactKind::Archive => {
                    let archive = private_root.join(archive_storage_relative_path(artifact)?);
                    let destination = root.join(artifact_spark_files_relative_path(artifact)?);
                    let mut extraction_limits = archive_budget.limits();
                    extraction_limits.max_input_bytes = artifact.size;
                    let manifest_cache_key =
                        materialization_context.map(|context| ArchiveManifestCacheKey {
                            scope_id: context.scope_id.clone(),
                            context_key: context.context_key.clone(),
                            generation: context.generation,
                            destination_key: sha256_hex(destination.to_string_lossy().as_bytes()),
                            archive_sha256: artifact.sha256.clone(),
                        });
                    let summary = py.detach(|| {
                        materialize_artifact_file(artifact, &archive)?;
                        extract_archive(
                            &artifact.name,
                            &archive,
                            &destination,
                            &artifact.sha256,
                            extraction_limits,
                            &private_root,
                            manifest_cache_key.as_ref(),
                        )
                    })?;
                    archive_budget.consume(&summary)?;
                }
            }
        }

        Ok(InstalledArtifacts { python_paths })
    }

    pub fn with_pandas_window_bound_types(mut self, value: Option<String>) -> Self {
        self.pandas_window_bound_types = value;
        self
    }

    /// Converts the configuration to a list of key-value pairs,
    /// so that it can be read by `worker.py` in PySpark.
    /// Missing values are not included.
    pub fn to_key_value_pairs(&self) -> Vec<(String, String)> {
        let mut out = vec![];
        out.push((
            "spark.sql.session.timeZone".to_string(),
            self.session_timezone.clone(),
        ));
        if let Some(value) = &self.pandas_window_bound_types {
            out.push(("pandas_window_bound_types".to_string(), value.clone()));
        }
        out.push((
            "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName".to_string(),
            self.pandas_grouped_map_assign_columns_by_name.to_string(),
        ));
        out.push((
            "spark.sql.execution.pandas.convertToArrowArraySafely".to_string(),
            self.pandas_convert_to_arrow_array_safely.to_string(),
        ));
        out.push((
            "spark.sql.execution.arrow.maxRecordsPerBatch".to_string(),
            self.arrow_max_records_per_batch.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled".to_string(),
            self.python_udf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled".to_string(),
            self.python_udtf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled".to_string(),
            self.python_udf_pandas_int_to_decimal_coercion_enabled
                .to_string(),
        ));
        out.push((
            "spark.sql.execution.pyspark.binaryAsBytes".to_string(),
            self.binary_as_bytes.to_string(),
        ));
        out
    }
}

fn validate_artifact_targets(artifacts: &[PySparkPythonArtifact]) -> PyUdfResult<()> {
    let mut targets: Vec<(&str, PathBuf)> = Vec::with_capacity(artifacts.len());
    let expected_scope = artifacts.first().map(|artifact| artifact.scope_id.as_str());
    for artifact in artifacts {
        if artifact.scope_id.len() != 64
            || !artifact
                .scope_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(PyUdfError::invalid(format!(
                "artifact {} has an invalid session scope",
                artifact.name
            )));
        }
        if Some(artifact.scope_id.as_str()) != expected_scope {
            return Err(PyUdfError::invalid(
                "Python artifacts from different session scopes cannot share one task context",
            ));
        }
        let target = artifact_spark_files_relative_path(artifact)?;
        for (existing_name, existing) in &targets {
            if existing == &target || existing.starts_with(&target) || target.starts_with(existing)
            {
                return Err(PyUdfError::invalid(format!(
                    "artifact {} has an overlapping SparkFiles target with {existing_name}",
                    artifact.name
                )));
            }
        }
        targets.push((artifact.name.as_str(), target));
    }
    Ok(())
}

fn start_python_artifact_cleanup_thread(lock: Arc<PythonArtifactContextLock>) {
    let started = PYTHON_ARTIFACT_CLEANUP_STARTED.get_or_init(|| Mutex::new(false));
    let mut started = match started.lock() {
        Ok(started) => started,
        Err(error) => error.into_inner(),
    };
    if *started {
        return;
    }
    match std::thread::Builder::new()
        .name("sail-python-artifact-cleanup".to_string())
        .spawn(move || python_artifact_cleanup_loop(lock))
    {
        Ok(_) => *started = true,
        Err(error) => log::warn!("failed to start Python artifact cleanup thread: {error}"),
    }
}

fn remove_python_artifact_scope_root(cleanup: &PythonArtifactCleanupDeadline) -> PyUdfResult<()> {
    if let Err(error) = std::fs::remove_dir_all(&cleanup.root)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error.into());
    }
    let private_root = private_artifact_root(&cleanup.root)?;
    if let Err(error) = std::fs::remove_dir_all(&private_root)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error.into());
    }
    if let Some(parent) = cleanup.root.parent() {
        let _ = std::fs::remove_dir(parent);
    }
    if let Some(parent) = private_root.parent() {
        let _ = std::fs::remove_dir(parent);
    }
    Ok(())
}

fn python_artifact_cleanup_loop(lock: Arc<PythonArtifactContextLock>) {
    loop {
        let (scope_id, cleanup) = {
            let mut state = match lock.state.lock() {
                Ok(state) => state,
                Err(error) => {
                    log::warn!("failed to lock Python artifact cleanup state: {error}");
                    return;
                }
            };
            loop {
                if state.shutting_down || state.preparing {
                    state = match lock.available.wait(state) {
                        Ok(state) => state,
                        Err(error) => {
                            log::warn!("failed to wait for Python artifact cleanup: {error}");
                            return;
                        }
                    };
                    continue;
                }
                let next = state
                    .cleanup_deadlines
                    .iter()
                    .min_by_key(|(_, cleanup)| cleanup.deadline)
                    .map(|(scope_id, cleanup)| (scope_id.clone(), cleanup.clone()));
                let Some((scope_id, cleanup)) = next else {
                    state = match lock.available.wait(state) {
                        Ok(state) => state,
                        Err(error) => {
                            log::warn!("failed to wait for Python artifact cleanup: {error}");
                            return;
                        }
                    };
                    continue;
                };
                let now = Instant::now();
                if cleanup.deadline > now {
                    let wait = cleanup.deadline.saturating_duration_since(now);
                    state = match lock.available.wait_timeout(state, wait) {
                        Ok((state, _)) => state,
                        Err(error) => {
                            log::warn!("failed to wait for Python artifact cleanup: {error}");
                            return;
                        }
                    };
                    continue;
                }
                state.cleanup_deadlines.remove(&scope_id);
                if state.active_scope.as_ref() == Some(&scope_id)
                    || state.generations.get(&scope_id).copied() != Some(cleanup.generation)
                {
                    continue;
                }
                state.cleaning.insert(scope_id.clone());
                break (scope_id, cleanup);
            }
        };

        invalidate_archive_manifest_scope(&scope_id);
        let cleanup_result = remove_python_artifact_scope_root(&cleanup);
        if let Err(error) = &cleanup_result {
            log::warn!(
                "failed to remove idle Python artifact scope {}: {error}",
                cleanup.root.display()
            );
        }
        match lock.state.lock() {
            Ok(mut state) => {
                state.cleaning.remove(&scope_id);
                if state.generations.get(&scope_id).copied() == Some(cleanup.generation) {
                    if cleanup_result.is_ok() {
                        state.generations.remove(&scope_id);
                        state.context_keys.remove(&scope_id);
                    } else {
                        state.cleanup_deadlines.insert(
                            scope_id.clone(),
                            PythonArtifactCleanupDeadline {
                                deadline: Instant::now() + Duration::from_secs(60),
                                ..cleanup
                            },
                        );
                    }
                }
                lock.available.notify_all();
            }
            Err(error) => {
                log::warn!("failed to finish Python artifact cleanup: {error}");
                return;
            }
        }
    }
}

struct InstalledArtifacts {
    python_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct RemainingArchiveExtraction {
    expanded_bytes: u64,
    entries: usize,
    records: usize,
}

impl Default for RemainingArchiveExtraction {
    fn default() -> Self {
        let limits = ArchiveExtractionLimits::default();
        Self {
            expanded_bytes: limits.max_total_bytes,
            entries: limits.max_entries,
            records: limits.max_records,
        }
    }
}

impl RemainingArchiveExtraction {
    fn limits(self) -> ArchiveExtractionLimits {
        ArchiveExtractionLimits {
            max_records: self.records,
            max_entries: self.entries,
            max_total_bytes: self.expanded_bytes,
            ..ArchiveExtractionLimits::default()
        }
    }

    fn consume(&mut self, summary: &ArchiveExtractionSummary) -> PyUdfResult<()> {
        let expanded_bytes = self
            .expanded_bytes
            .checked_sub(summary.expanded_bytes)
            .ok_or_else(|| {
                PyUdfError::invalid("Python archives exceeded the aggregate expanded size limit")
            })?;
        let entries = self.entries.checked_sub(summary.entries).ok_or_else(|| {
            PyUdfError::invalid("Python archives exceeded the aggregate materialized entry limit")
        })?;
        let records = self.records.checked_sub(summary.records).ok_or_else(|| {
            PyUdfError::invalid("Python archives exceeded the aggregate archive record limit")
        })?;
        self.expanded_bytes = expanded_bytes;
        self.entries = entries;
        self.records = records;
        Ok(())
    }
}

fn python_artifact_context_key(artifacts: &[PySparkPythonArtifact]) -> String {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        hasher.update(artifact.scope_id.as_bytes());
        hasher.update([0]);
        hasher.update((artifact.kind as u8).to_be_bytes());
        hasher.update(artifact.name.as_bytes());
        hasher.update([0]);
        hasher.update(artifact.sha256.as_bytes());
        hasher.update(artifact.size.to_be_bytes());
        hasher.update([0]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn python_artifact_scope_id(artifacts: &[PySparkPythonArtifact]) -> String {
    artifacts.first().map_or_else(
        || python_artifact_context_key(artifacts),
        |artifact| artifact.scope_id.clone(),
    )
}

fn spark_files_root(artifacts: &[PySparkPythonArtifact]) -> PyUdfResult<PathBuf> {
    Ok(artifact_process_root()?
        .join("spark-files")
        .join(python_artifact_scope_id(artifacts)))
}

fn invalidate_import_caches(py: Python) -> PyUdfResult<()> {
    let importlib = PyModule::import(py, "importlib")?;
    if let Err(error) = importlib.call_method0("invalidate_caches") {
        log::debug!("failed to invalidate Python import caches: {error}");
    }
    Ok(())
}

fn artifact_process_root() -> PyUdfResult<&'static Path> {
    let root = ARTIFACT_PROCESS_ROOT
        .get_or_init(|| {
            tempfile::Builder::new()
                .prefix("sail-python-artifacts-")
                .tempdir()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map(|root| root.path())
        .map_err(|error| {
            PyUdfError::internal(format!("failed to create Python artifact root: {error}"))
        })?;
    ensure_private_artifact_root(root)?;
    Ok(root)
}

fn ensure_private_artifact_root(root: &Path) -> PyUdfResult<()> {
    loop {
        match std::fs::symlink_metadata(root) {
            Ok(metadata) => {
                if !metadata.file_type().is_dir() {
                    return Err(PyUdfError::internal(format!(
                        "Python artifact root is not a private directory: {}",
                        root.display()
                    )));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;

                    let mut permissions = metadata.permissions();
                    permissions.set_mode(0o700);
                    match std::fs::set_permissions(root, permissions) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = std::fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;

                    builder.mode(0o700);
                }
                match builder.create(root) {
                    Ok(()) => continue,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub fn shutdown_python_artifact_cache() -> PyUdfResult<()> {
    let Some(root) = ARTIFACT_PROCESS_ROOT.get() else {
        return Ok(());
    };
    let root = root.as_ref().map_err(|error| {
        PyUdfError::internal(format!("failed to locate Python artifact root: {error}"))
    })?;
    let root = root.path().to_path_buf();
    let lock = Arc::clone(
        PYTHON_ARTIFACT_CONTEXT_LOCK.get_or_init(|| Arc::new(PythonArtifactContextLock::new())),
    );
    {
        let mut state = lock.state.lock().unwrap_or_else(|error| {
            log::warn!("recovering poisoned Python artifact shutdown lock: {error}");
            error.into_inner()
        });
        let deadline = Instant::now() + PYTHON_ARTIFACT_CONTEXT_WAIT_TIMEOUT;
        while state.shutting_down
            || state.preparing
            || state.holders != 0
            || state.active_key.is_some()
            || !state.cleaning.is_empty()
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(PyUdfError::invalid(
                    "timed out waiting to shut down the Python artifact cache",
                ));
            }
            let (next_state, wait) =
                lock.available
                    .wait_timeout(state, remaining)
                    .map_err(|error| {
                        PyUdfError::internal(format!(
                            "failed to wait for Python artifact shutdown: {error}"
                        ))
                    })?;
            state = next_state;
            if wait.timed_out()
                && (state.shutting_down
                    || state.preparing
                    || state.holders != 0
                    || state.active_key.is_some()
                    || !state.cleaning.is_empty())
            {
                return Err(PyUdfError::invalid(
                    "timed out waiting to shut down the Python artifact cache",
                ));
            }
        }
        state.shutting_down = true;
    }
    invalidate_all_archive_manifests();

    let cleanup = (|| -> PyUdfResult<()> {
        Python::attach(clear_python_artifacts)?;
        match std::fs::remove_dir_all(&root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(PyUdfError::internal(format!(
                "failed to remove Python artifact cache: {error}"
            ))),
        }
    })();
    {
        let mut state = lock.state.lock().unwrap_or_else(|error| {
            log::warn!("recovering poisoned Python artifact shutdown lock: {error}");
            error.into_inner()
        });
        if cleanup.is_ok() {
            state.generations.clear();
            state.context_keys.clear();
            state.cleanup_deadlines.clear();
        }
        state.shutting_down = false;
        lock.available.notify_all();
    }
    cleanup
}

fn private_artifact_root(spark_files_root: &Path) -> PyUdfResult<PathBuf> {
    let key = spark_files_root.file_name().ok_or_else(|| {
        PyUdfError::internal(format!(
            "SparkFiles root does not have a context key: {}",
            spark_files_root.display()
        ))
    })?;
    Ok(artifact_process_root()?.join("private").join(key))
}

fn configure_spark_files(py: Python, root: &Path) -> PyUdfResult<()> {
    let module = spark_files_module(py)?;
    let spark_files = module.getattr("SparkFiles")?;
    spark_files.setattr("_root_directory", root.to_string_lossy().as_ref())?;
    spark_files.setattr("_is_running_on_worker", true)?;
    Ok(())
}

fn clear_python_artifacts(py: Python) -> PyUdfResult<()> {
    let paths_changed = sync_artifact_python_paths(py, &[])?;
    sync_installed_artifact_context_key(String::new())?;
    clear_spark_files(py);
    if paths_changed {
        invalidate_import_caches(py)?;
    }
    Ok(())
}

fn sync_installed_artifact_context_key(key: String) -> PyUdfResult<bool> {
    let state = INSTALLED_ARTIFACT_CONTEXT_KEY.get_or_init(|| Mutex::new(None));
    let mut installed = state.lock().map_err(|error| {
        PyUdfError::internal(format!(
            "failed to lock installed Python artifact context: {error}"
        ))
    })?;
    let key = (!key.is_empty()).then_some(key);
    if *installed == key {
        return Ok(false);
    }
    *installed = key;
    Ok(true)
}

fn clear_spark_files(py: Python) {
    if let Ok(module) = PyModule::import(py, "pyspark.core.files")
        && let Ok(spark_files) = module.getattr("SparkFiles")
    {
        if let Err(error) = spark_files.setattr("_root_directory", Option::<String>::None) {
            log::debug!("failed to clear SparkFiles root directory: {error}");
        }
        if let Err(error) = spark_files.setattr("_is_running_on_worker", false) {
            log::debug!("failed to clear SparkFiles worker state: {error}");
        }
    }
}

fn sync_artifact_python_paths(py: Python, paths: &[String]) -> PyUdfResult<bool> {
    let sys = PyModule::import(py, "sys")?;
    let path_list = sys.getattr("path")?;
    let state = INSTALLED_ARTIFACT_PYTHON_PATHS.get_or_init(|| Mutex::new(vec![]));
    let mut installed = state.lock().map_err(|e| {
        PyUdfError::internal(format!("failed to lock installed artifact paths: {e}"))
    })?;
    if *installed == paths {
        return Ok(false);
    }
    let removed_paths = installed
        .iter()
        .filter(|path| !paths.iter().any(|next| next == *path))
        .cloned()
        .collect::<Vec<_>>();

    evict_python_modules_from_paths(py, &removed_paths)?;

    for path in &removed_paths {
        while path_list_contains(&path_list, path)? {
            path_list.call_method1("remove", (path.as_str(),))?;
        }
    }
    for path in paths {
        if !path_list_contains(&path_list, path)? {
            path_list.call_method1("insert", (0, path.as_str()))?;
        }
    }

    *installed = paths.to_vec();
    Ok(true)
}

fn evict_python_modules_from_paths(py: Python, paths: &[String]) -> PyUdfResult<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let paths = paths.iter().map(Path::new).collect::<Vec<_>>();
    let modules = PyModule::import(py, "sys")?
        .getattr("modules")?
        .cast_into::<PyDict>()
        .map_err(|e| {
            PyUdfError::internal(format!("Python sys.modules is not a dictionary: {e}"))
        })?;
    let mut names = vec![];
    for (name, module) in modules.iter() {
        if !python_module_uses_artifact_path(&module, &paths) {
            continue;
        }
        let Ok(mut name) = name.extract::<String>() else {
            continue;
        };
        loop {
            if !names.contains(&name) {
                names.push(name.clone());
            }
            let Some((parent, _)) = name.rsplit_once('.') else {
                break;
            };
            name = parent.to_string();
        }
    }
    for name in names {
        if modules.contains(&name)? {
            modules.del_item(name)?;
        }
    }
    Ok(())
}

fn python_module_uses_artifact_path(
    module: &pyo3::Bound<'_, pyo3::PyAny>,
    paths: &[&Path],
) -> bool {
    module
        .cast::<PyModule>()
        .ok()
        .and_then(|module| module.dict().get_item("__file__").ok().flatten())
        .and_then(|path| path.extract::<String>().ok())
        .is_some_and(|path| paths.iter().any(|root| Path::new(&path).starts_with(root)))
}

fn path_list_contains(path_list: &pyo3::Bound<'_, pyo3::PyAny>, path: &str) -> PyUdfResult<bool> {
    Ok(path_list.call_method1("__contains__", (path,))?.extract()?)
}

fn spark_files_module<'py>(py: Python<'py>) -> PyUdfResult<pyo3::Bound<'py, PyModule>> {
    match PyModule::import(py, "pyspark.core.files") {
        Ok(module) => Ok(module),
        Err(_) => install_spark_files_shim(py),
    }
}

fn install_spark_files_shim<'py>(py: Python<'py>) -> PyUdfResult<pyo3::Bound<'py, PyModule>> {
    let source = c_string(SPARK_FILES_SHIM_SOURCE, "SparkFiles shim source")?;
    let filename = c_string("files.py", "SparkFiles shim filename")?;
    let module_name = c_string("pyspark.core.files", "SparkFiles shim module name")?;
    let module = PyModule::from_code(
        py,
        source.as_c_str(),
        filename.as_c_str(),
        module_name.as_c_str(),
    )?;

    PyModule::import(py, "sys")?
        .getattr("modules")?
        .call_method1("__setitem__", ("pyspark.core.files", &module))?;

    let register_source = c_string(
        SPARK_FILES_SHIM_REGISTER_SOURCE,
        "SparkFiles shim registration source",
    )?;
    let register_filename = c_string("", "SparkFiles shim registration filename")?;
    let register_module_name = c_string(
        "_sail_spark_files_shim_register",
        "SparkFiles shim registration module name",
    )?;
    PyModule::from_code(
        py,
        register_source.as_c_str(),
        register_filename.as_c_str(),
        register_module_name.as_c_str(),
    )?;

    Ok(module)
}

fn c_string(value: &'static str, label: &str) -> PyUdfResult<CString> {
    CString::new(value).map_err(|e| PyUdfError::internal(format!("invalid {label}: {e}")))
}

fn materialize_artifact_file(
    artifact: &PySparkPythonArtifact,
    target_path: &Path,
) -> PyUdfResult<()> {
    if file_matches_artifact(target_path, artifact)? {
        return Ok(());
    }
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = target_path.parent().ok_or_else(|| {
        PyUdfError::internal(format!(
            "artifact target does not have a parent: {}",
            target_path.display()
        ))
    })?;
    let staging = NamedTempFile::new_in(parent)?;
    materialize_artifact_payload(artifact, staging.path())?;
    if !file_matches_artifact(staging.path(), artifact)? {
        return Err(PyUdfError::invalid(format!(
            "artifact {} changed while it was being materialized",
            artifact.name
        )));
    }
    if let Err(error) = staging.persist_noclobber(target_path)
        && !file_matches_artifact(target_path, artifact)?
    {
        return Err(error.error.into());
    }
    Ok(())
}

fn materialize_artifact_payload(
    artifact: &PySparkPythonArtifact,
    target_path: &Path,
) -> PyUdfResult<()> {
    let source = Path::new(&artifact.python_path);
    if source.exists() && file_matches_artifact(source, artifact)? {
        std::fs::copy(source, target_path)?;
        return Ok(());
    }
    if let Some(data) = &artifact.data {
        verify_artifact_data(artifact, data)?;
        std::fs::write(target_path, data.as_ref())?;
        return Ok(());
    }
    if let Some(uri) = &artifact.uri {
        return download_artifact_uri_to_file(uri, target_path, artifact);
    }
    Err(PyUdfError::invalid(format!(
        "artifact {} has no accessible local path, inline data, or object-store URI",
        artifact.name
    )))
}

fn file_matches_artifact(path: &Path, artifact: &PySparkPythonArtifact) -> PyUdfResult<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != artifact.size {
        return Ok(false);
    }
    Ok(file_sha256(path)? == artifact.sha256)
}

fn file_sha256(path: &Path) -> PyUdfResult<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn download_artifact_uri_to_file(
    uri: &str,
    target_path: &Path,
    artifact: &PySparkPythonArtifact,
) -> PyUdfResult<()> {
    let uri = uri.to_string();
    let target_path = target_path.to_path_buf();
    let artifact_name = artifact.name.clone();
    let expected_sha256 = artifact.sha256.clone();
    let expected_size = artifact.size;
    let handle = std::thread::Builder::new()
        .name("sail-artifact-download".to_string())
        .spawn(move || -> Result<(), String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create artifact download runtime: {e}"))?;
            runtime.block_on(async move {
                let url = Url::parse(&uri)
                    .map_err(|e| format!("invalid artifact object-store URI {uri}: {e}"))?;
                let (_scheme, path) = object_store::ObjectStoreScheme::parse(&url)
                    .map_err(|e| format!("invalid artifact object-store path {uri}: {e}"))?;
                let store = sail_object_store::get_dynamic_object_store(&url)
                    .map_err(|e| format!("failed to create artifact object store {uri}: {e}"))?;
                let result = store
                    .get(&path)
                    .await
                    .map_err(|e| format!("failed to read artifact {uri}: {e}"))?;
                if result.meta.size != expected_size {
                    return Err(format!(
                        "artifact {artifact_name} size mismatch: expected {expected_size}, object metadata reported {}",
                        result.meta.size
                    ));
                }
                let mut file = File::create(&target_path)
                    .map_err(|e| format!("failed to create artifact file {}: {e}", target_path.display()))?;
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                let mut stream = result.into_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| format!("failed to read artifact bytes {uri}: {e}"))?;
                    let next_size = size
                        .checked_add(chunk.len() as u64)
                        .ok_or_else(|| format!("artifact {artifact_name} byte count overflow"))?;
                    if next_size > expected_size {
                        return Err(format!(
                            "artifact {artifact_name} exceeded its declared size of {expected_size} bytes"
                        ));
                    }
                    hasher.update(&chunk);
                    file.write_all(&chunk)
                        .map_err(|e| format!("failed to write artifact file {}: {e}", target_path.display()))?;
                    size = next_size;
                }
                if size != expected_size {
                    return Err(format!(
                        "artifact {artifact_name} size mismatch: expected {expected_size}, got {size}"
                    ));
                }
                let actual: String = hasher
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                if actual != expected_sha256 {
                    return Err(format!(
                        "artifact {artifact_name} SHA-256 mismatch: expected {expected_sha256}, got {actual}"
                    ));
                }
                Ok(())
            })
        })
        .map_err(|e| PyUdfError::internal(format!("failed to spawn artifact download: {e}")))?;
    match handle.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(PyUdfError::internal(error)),
        Err(_) => Err(PyUdfError::internal("artifact download thread panicked")),
    }
}

fn verify_artifact_data(artifact: &PySparkPythonArtifact, data: &[u8]) -> PyUdfResult<()> {
    if data.len() as u64 != artifact.size {
        return Err(PyUdfError::invalid(format!(
            "artifact {} size mismatch: expected {}, got {}",
            artifact.name,
            artifact.size,
            data.len()
        )));
    }
    let actual = sha256_hex(data);
    if actual != artifact.sha256 {
        return Err(PyUdfError::invalid(format!(
            "artifact {} SHA-256 mismatch: expected {}, got {}",
            artifact.name, artifact.sha256, actual
        )));
    }
    Ok(())
}

impl CachedArchiveSummary {
    fn from_summary(summary: &ArchiveExtractionSummary) -> Self {
        Self {
            expanded_bytes: summary.expanded_bytes,
            entries: summary.entries,
            records: summary.records,
        }
    }

    fn to_summary(self, limits: ArchiveExtractionLimits) -> PyUdfResult<ArchiveExtractionSummary> {
        if self.expanded_bytes > limits.max_total_bytes
            || self.entries > limits.max_entries
            || self.records > limits.max_records
        {
            return Err(PyUdfError::invalid(
                "cached archive manifest exceeded the current extraction limits",
            ));
        }
        Ok(ArchiveExtractionSummary {
            expanded_bytes: self.expanded_bytes,
            entries: self.entries,
            records: self.records,
        })
    }
}

fn invalidate_archive_manifest_context_in(
    manifests: &ArchiveManifestCache,
    cache_key: &ArchiveManifestCacheKey,
) {
    manifests
        .lock()
        .unwrap_or_else(|error| {
            log::warn!("recovering poisoned archive manifest cache: {error}");
            error.into_inner()
        })
        .retain(|candidate, _| {
            candidate.scope_id != cache_key.scope_id
                || candidate.context_key != cache_key.context_key
                || candidate.generation != cache_key.generation
        });
}

fn resolve_authenticated_archive_destination<F>(
    manifests: &ArchiveManifestCache,
    cache_key: &ArchiveManifestCacheKey,
    destination: &Path,
    marker: &Path,
    limits: ArchiveExtractionLimits,
    authenticate: F,
) -> PyUdfResult<Option<ArchiveExtractionSummary>>
where
    F: FnOnce() -> PyUdfResult<Option<ArchiveExtractionSummary>>,
{
    // A retained generation trusts its first authenticated manifest. Context-key changes and
    // root cleanup invalidate the generation before materialization resumes.
    if destination.is_dir() && marker.is_file() {
        let cached = manifests
            .lock()
            .unwrap_or_else(|error| {
                log::warn!("recovering poisoned archive manifest cache: {error}");
                error.into_inner()
            })
            .get(cache_key)
            .copied();
        if let Some(cached) = cached {
            return match cached.to_summary(limits) {
                Ok(summary) => Ok(Some(summary)),
                Err(error) => {
                    invalidate_archive_manifest_context_in(manifests, cache_key);
                    Err(error)
                }
            };
        }
    } else {
        invalidate_archive_manifest_context_in(manifests, cache_key);
    }

    match authenticate() {
        Ok(Some(summary)) => {
            manifests
                .lock()
                .unwrap_or_else(|error| {
                    log::warn!("recovering poisoned archive manifest cache: {error}");
                    error.into_inner()
                })
                .insert(
                    cache_key.clone(),
                    CachedArchiveSummary::from_summary(&summary),
                );
            Ok(Some(summary))
        }
        Ok(None) => {
            invalidate_archive_manifest_context_in(manifests, cache_key);
            Ok(None)
        }
        Err(error) => {
            invalidate_archive_manifest_context_in(manifests, cache_key);
            Err(error)
        }
    }
}

fn cache_authenticated_archive_summary(
    cache_key: &ArchiveManifestCacheKey,
    summary: &ArchiveExtractionSummary,
) {
    authenticated_archive_manifests()
        .lock()
        .unwrap_or_else(|error| {
            log::warn!("recovering poisoned archive manifest cache: {error}");
            error.into_inner()
        })
        .insert(
            cache_key.clone(),
            CachedArchiveSummary::from_summary(summary),
        );
}

fn extract_archive(
    name: &str,
    archive_path: &Path,
    destination: &Path,
    sha256: &str,
    limits: ArchiveExtractionLimits,
    private_root: &Path,
    manifest_cache_key: Option<&ArchiveManifestCacheKey>,
) -> PyUdfResult<ArchiveExtractionSummary> {
    let destination_key = sha256_hex(destination.to_string_lossy().as_bytes());
    let marker = private_root.join("markers").join(&destination_key);
    let lock_path = private_root.join("locks").join(destination_key);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _lock = acquire_artifact_lock(&lock_path)?;
    let completed = if let Some(cache_key) = manifest_cache_key {
        resolve_authenticated_archive_destination(
            authenticated_archive_manifests(),
            cache_key,
            destination,
            &marker,
            limits,
            || complete_archive_destination(destination, &marker, sha256, limits),
        )?
    } else {
        complete_archive_destination(destination, &marker, sha256, limits)?
    };
    if let Some(summary) = completed {
        return Ok(summary);
    }
    if destination.exists() {
        std::fs::remove_dir_all(destination)?;
    }
    let parent = destination.parent().ok_or_else(|| {
        PyUdfError::internal(format!(
            "archive destination does not have a parent: {}",
            destination.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;
    let staging = TempDir::with_prefix_in(".sail-archive-", parent)?;
    let summary =
        extract_archive_contents(archive_name(name), archive_path, staging.path(), limits)?;
    if !directory_has_non_marker_entry(staging.path())? {
        return Err(PyUdfError::invalid(format!(
            "archive {name} did not extract any entries"
        )));
    }
    let destination_manifest = directory_manifest_bounded(
        staging.path(),
        limits.max_total_bytes,
        limits.max_entries,
        summary.records,
        limits.max_records,
    )?;
    if destination_manifest.summary.expanded_bytes != summary.expanded_bytes
        || destination_manifest.summary.entries != summary.entries
    {
        return Err(PyUdfError::invalid(format!(
            "archive {name} extraction summary did not match its materialized contents"
        )));
    }

    std::fs::rename(staging.path(), destination)?;
    write_archive_completion_marker(&marker, sha256, &destination_manifest)?;
    if let Some(cache_key) = manifest_cache_key {
        cache_authenticated_archive_summary(cache_key, &destination_manifest.summary);
    }
    Ok(destination_manifest.summary)
}

#[derive(Debug)]
struct ArchiveDirectoryManifest {
    summary: ArchiveExtractionSummary,
    content_sha256: String,
}

fn directory_manifest_bounded(
    path: &Path,
    max_bytes: u64,
    max_entries: usize,
    records: usize,
    max_records: usize,
) -> PyUdfResult<ArchiveDirectoryManifest> {
    if records > max_records {
        return Err(PyUdfError::invalid(format!(
            "Python archives exceeded the aggregate archive record limit of {max_records}"
        )));
    }
    let root_metadata = std::fs::symlink_metadata(path)?;
    if !root_metadata.file_type().is_dir() {
        return Err(PyUdfError::invalid(format!(
            "archive destination contains an unsafe entry: {}",
            path.display()
        )));
    }
    let mut paths = vec![];
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let relative = entry
                .path()
                .strip_prefix(path)
                .map_err(|error| {
                    PyUdfError::internal(format!("failed to derive archive manifest path: {error}"))
                })?
                .to_path_buf();
            let entry_count = paths
                .len()
                .checked_add(1)
                .ok_or_else(|| PyUdfError::invalid("archive entry count overflow"))?;
            if entry_count > max_entries {
                return Err(PyUdfError::invalid(format!(
                    "Python archives exceeded the aggregate materialized entry limit of {max_entries}"
                )));
            }
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_dir() {
                pending.push(entry.path());
            } else if !metadata.file_type().is_file() {
                return Err(PyUdfError::invalid(format!(
                    "archive destination contains an unsafe entry: {}",
                    entry.path().display()
                )));
            }
            paths.push((relative, entry.path()));
        }
    }
    paths.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

    let mut total = 0_u64;
    let mut hasher = Sha256::new();
    hasher.update(b"sail-archive-tree-v1\0");
    for (relative, entry_path) in &paths {
        let metadata = std::fs::symlink_metadata(entry_path)?;
        let path_bytes = relative.as_os_str().as_encoded_bytes();
        hasher.update((path_bytes.len() as u64).to_be_bytes());
        hasher.update(path_bytes);
        if metadata.file_type().is_dir() {
            hasher.update(b"d");
            continue;
        }
        if !metadata.file_type().is_file() {
            return Err(PyUdfError::invalid(format!(
                "archive destination contains an unsafe entry: {}",
                entry_path.display()
            )));
        }
        hasher.update(b"f");
        hasher.update(metadata.len().to_be_bytes());
        total = total
            .checked_add(metadata.len())
            .ok_or_else(|| PyUdfError::invalid("archive expanded byte count overflow"))?;
        if total > max_bytes {
            return Err(PyUdfError::invalid(format!(
                "Python archives exceeded the aggregate expanded size limit of {max_bytes} bytes"
            )));
        }
        let mut file = File::open(entry_path)?;
        let mut observed = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            observed = observed
                .checked_add(read as u64)
                .ok_or_else(|| PyUdfError::invalid("archive file size overflow"))?;
            if observed > metadata.len() {
                return Err(PyUdfError::invalid(
                    "archive destination changed while its contents were authenticated",
                ));
            }
            hasher.update(&buffer[..read]);
        }
        if observed != metadata.len() {
            return Err(PyUdfError::invalid(
                "archive destination changed while its contents were authenticated",
            ));
        }
    }
    let content_sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(ArchiveDirectoryManifest {
        summary: ArchiveExtractionSummary {
            expanded_bytes: total,
            entries: paths.len(),
            records,
        },
        content_sha256,
    })
}

fn write_archive_completion_marker(
    marker: &Path,
    archive_sha256: &str,
    manifest: &ArchiveDirectoryManifest,
) -> PyUdfResult<()> {
    std::fs::write(
        marker,
        format!(
            "{ARCHIVE_COMPLETION_MARKER_VERSION}\n{archive_sha256}\n{}\n{}\n{}\n{}\n",
            manifest.summary.expanded_bytes,
            manifest.summary.entries,
            manifest.summary.records,
            manifest.content_sha256
        ),
    )?;
    Ok(())
}

fn complete_archive_destination(
    destination: &Path,
    marker: &Path,
    sha256: &str,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<Option<ArchiveExtractionSummary>> {
    if !marker.exists() || !destination.is_dir() {
        return Ok(None);
    }
    let marker = std::fs::read_to_string(marker)?;
    let mut lines = marker.lines();
    if lines.next() != Some(ARCHIVE_COMPLETION_MARKER_VERSION) || lines.next() != Some(sha256) {
        return Ok(None);
    }
    let Some(expected_bytes) = lines.next().and_then(|value| value.parse::<u64>().ok()) else {
        return Ok(None);
    };
    let Some(expected_entries) = lines.next().and_then(|value| value.parse::<usize>().ok()) else {
        return Ok(None);
    };
    let Some(expected_records) = lines.next().and_then(|value| value.parse::<usize>().ok()) else {
        return Ok(None);
    };
    let Some(expected_content_sha256) = lines.next() else {
        return Ok(None);
    };
    if expected_entries == 0
        || expected_records == 0
        || expected_content_sha256.len() != 64
        || !expected_content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || lines.next().is_some()
    {
        return Ok(None);
    }
    let manifest = directory_manifest_bounded(
        destination,
        limits.max_total_bytes,
        limits.max_entries,
        expected_records,
        limits.max_records,
    )?;
    if manifest.summary.expanded_bytes != expected_bytes
        || manifest.summary.entries != expected_entries
        || manifest.content_sha256 != expected_content_sha256
    {
        return Ok(None);
    }
    Ok(Some(manifest.summary))
}

fn directory_has_non_marker_entry(path: &Path) -> std::io::Result<bool> {
    Ok(std::fs::read_dir(path)?.next().transpose()?.is_some())
}

struct ArtifactLock {
    path: PathBuf,
}

impl Drop for ArtifactLock {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir(&self.path) {
            log::warn!(
                "failed to remove artifact materialization lock {}: {error}",
                self.path.display()
            );
        }
    }
}

fn acquire_artifact_lock(path: &Path) -> PyUdfResult<ArtifactLock> {
    const ATTEMPTS: usize = 6000;
    const SLEEP: Duration = Duration::from_millis(10);
    for _ in 0..ATTEMPTS {
        match std::fs::create_dir(path) {
            Ok(()) => {
                return Ok(ArtifactLock {
                    path: path.to_path_buf(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::thread::sleep(SLEEP);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(PyUdfError::internal(format!(
        "timed out acquiring artifact materialization lock {}",
        path.display()
    )))
}

fn artifact_spark_files_relative_path(artifact: &PySparkPythonArtifact) -> PyUdfResult<PathBuf> {
    match artifact.kind {
        PySparkArtifactKind::PyFile => {
            let Some(file_name) = artifact.name.strip_prefix(PYFILES_PREFIX) else {
                return Err(PyUdfError::invalid(format!(
                    "Python artifact name must use the pyfiles/ prefix: {}",
                    artifact.name
                )));
            };
            validate_artifact_relative_path(file_name)
        }
        PySparkArtifactKind::File => {
            let Some(file_name) = artifact.name.strip_prefix(FILES_PREFIX) else {
                return Err(PyUdfError::invalid(format!(
                    "file artifact name must use the files/ prefix: {}",
                    artifact.name
                )));
            };
            validate_artifact_relative_path(file_name)
        }
        PySparkArtifactKind::Archive => {
            let Some(rest) = artifact.name.strip_prefix(ARCHIVES_PREFIX) else {
                return Err(PyUdfError::invalid(format!(
                    "archive artifact name must use the archives/ prefix: {}",
                    artifact.name
                )));
            };
            let (archive_name, fragment) = archive_name_and_fragment(rest)?;
            let destination = if fragment.is_empty() {
                default_archive_directory(archive_name)?
            } else {
                fragment.to_string()
            };
            validate_artifact_relative_path(&destination)
        }
    }
}

fn archive_storage_relative_path(artifact: &PySparkPythonArtifact) -> PyUdfResult<PathBuf> {
    let name = archive_name(&artifact.name);
    Ok(PathBuf::from("archives")
        .join(&artifact.sha256)
        .join(validate_artifact_relative_path(name)?))
}

fn archive_name(name: &str) -> &str {
    name.strip_prefix(ARCHIVES_PREFIX)
        .and_then(|rest| archive_name_and_fragment(rest).ok().map(|(path, _)| path))
        .unwrap_or(name)
}

fn archive_name_and_fragment(rest: &str) -> PyUdfResult<(&str, &str)> {
    if rest.matches('#').count() > 1 {
        return Err(PyUdfError::invalid(format!(
            "'#' in the path is not supported for archive artifact: {rest}"
        )));
    }
    let (path, fragment) = rest.split_once('#').unwrap_or((rest, ""));
    if path.is_empty() {
        return Err(PyUdfError::invalid(
            "archive artifact path must not be empty",
        ));
    }
    validate_artifact_relative_path(path)?;
    if !fragment.is_empty() {
        validate_artifact_relative_path(fragment)?;
    }
    Ok((path, fragment))
}

fn default_archive_directory(path: &str) -> PyUdfResult<String> {
    Path::new(path)
        .file_name()
        .and_then(|x| x.to_str())
        .map(ToString::to_string)
        .ok_or_else(|| PyUdfError::invalid(format!("invalid archive artifact path: {path}")))
}

fn validate_artifact_relative_path(path: &str) -> PyUdfResult<PathBuf> {
    if path.len() > MAX_ARTIFACT_NAME_BYTES
        || path.contains('\\')
        || path.contains(':')
        || path.contains('\0')
    {
        return Err(PyUdfError::invalid(format!(
            "artifact name must use a platform-neutral relative path: {path:?}"
        )));
    }
    let path = Path::new(path);
    let mut out = PathBuf::new();
    let mut component_count = 0;
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                component_count += 1;
                if component_count > MAX_ARTIFACT_PATH_COMPONENTS
                    || part.as_encoded_bytes().len() > MAX_ARTIFACT_PATH_COMPONENT_BYTES
                {
                    return Err(PyUdfError::invalid(format!(
                        "artifact name exceeded the path limits: {}",
                        path.display()
                    )));
                }
                let part = part.to_str().ok_or_else(|| {
                    PyUdfError::invalid("artifact name must contain valid UTF-8 path components")
                })?;
                if part.trim_end_matches([' ', '.']) != part || is_windows_reserved_name(part) {
                    return Err(PyUdfError::invalid(format!(
                        "artifact name contains a platform-reserved path component: {part:?}"
                    )));
                }
                out.push(part);
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(PyUdfError::invalid(format!(
                    "artifact name must be a relative path without '.' or '..': {}",
                    path.display()
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(PyUdfError::invalid("artifact name must not be empty"));
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
        return Err(PyUdfError::invalid(format!(
            "artifact name uses a reserved path: {}",
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

fn python_artifact_import_path(name: &str, target_path: &Path) -> PyUdfResult<String> {
    let Some(file_name) = name.strip_prefix(PYFILES_PREFIX) else {
        return Err(PyUdfError::invalid(format!(
            "Python artifact name must use the pyfiles/ prefix: {name}"
        )));
    };
    if file_name.ends_with(".py") {
        let dir = target_path.parent().ok_or_else(|| {
            PyUdfError::internal(format!(
                "Python artifact file has no parent directory: {}",
                target_path.display()
            ))
        })?;
        Ok(dir.to_string_lossy().into_owned())
    } else if file_name.ends_with(".zip")
        || file_name.ends_with(".egg")
        || file_name.ends_with(".jar")
    {
        Ok(target_path.to_string_lossy().into_owned())
    } else {
        Err(PyUdfError::invalid(format!(
            "unsupported Python artifact type: {file_name}"
        )))
    }
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::sync::mpsc::{RecvTimeoutError, channel};
    use std::time::Duration;

    use super::*;

    static PROCESS_ARTIFACT_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn archive_budget_is_aggregate_and_atomic() {
        let mut budget = RemainingArchiveExtraction {
            expanded_bytes: 10,
            entries: 2,
            records: 2,
        };
        budget
            .consume(&ArchiveExtractionSummary {
                expanded_bytes: 6,
                entries: 1,
                records: 1,
            })
            .unwrap();
        let limits = budget.limits();
        assert_eq!(limits.max_total_bytes, 4);
        assert_eq!(limits.max_entries, 1);
        assert_eq!(limits.max_records, 1);

        let error = budget
            .consume(&ArchiveExtractionSummary {
                expanded_bytes: 4,
                entries: 1,
                records: 2,
            })
            .unwrap_err();
        assert!(error.to_string().contains("archive record limit"));
        let limits = budget.limits();
        assert_eq!(limits.max_total_bytes, 4);
        assert_eq!(limits.max_entries, 1);
        assert_eq!(limits.max_records, 1);
    }

    #[test]
    fn completion_marker_preserves_archive_record_count() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("value"), b"value").unwrap();
        let marker = temp.path().join("marker");
        let manifest = directory_manifest_bounded(&destination, 1024, 10, 2, 10).unwrap();
        write_archive_completion_marker(&marker, "digest", &manifest).unwrap();
        let limited = ArchiveExtractionLimits {
            max_records: 1,
            ..ArchiveExtractionLimits::default()
        };

        let error =
            complete_archive_destination(&destination, &marker, "digest", limited).unwrap_err();
        assert!(error.to_string().contains("record limit of 1"));

        let summary = complete_archive_destination(
            &destination,
            &marker,
            "digest",
            ArchiveExtractionLimits::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(summary.records, 2);
        assert_eq!(summary.entries, 1);
        assert_eq!(summary.expanded_bytes, 5);
    }

    #[test]
    fn completion_marker_does_not_cache_an_empty_archive() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        let marker = temp.path().join("marker");
        std::fs::write(
            &marker,
            format!(
                "{ARCHIVE_COMPLETION_MARKER_VERSION}\ndigest\n0\n0\n0\n{}\n",
                "0".repeat(64)
            ),
        )
        .unwrap();

        assert!(
            complete_archive_destination(
                &destination,
                &marker,
                "digest",
                ArchiveExtractionLimits::default(),
            )
            .unwrap()
            .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn directory_summary_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        let outside = temp.path().join("outside");
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, destination.join("link")).unwrap();

        let error = directory_manifest_bounded(&destination, 1024, 10, 1, 10).unwrap_err();
        assert!(error.to_string().contains("unsafe entry"));
    }

    #[test]
    fn completion_marker_authenticates_file_contents() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("value"), b"first").unwrap();
        let marker = temp.path().join("marker");
        let manifest = directory_manifest_bounded(&destination, 1024, 10, 1, 10).unwrap();
        write_archive_completion_marker(&marker, "digest", &manifest).unwrap();

        std::fs::write(destination.join("value"), b"other").unwrap();
        assert!(
            complete_archive_destination(
                &destination,
                &marker,
                "digest",
                ArchiveExtractionLimits::default(),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn manifest_cache_authenticates_once_per_context_generation() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("value"), b"first").unwrap();
        let marker = temp.path().join("marker");
        let manifest = directory_manifest_bounded(&destination, 1024, 10, 1, 10).unwrap();
        write_archive_completion_marker(&marker, "digest", &manifest).unwrap();
        let manifests = Mutex::new(HashMap::new());
        let cache_key = ArchiveManifestCacheKey {
            scope_id: "scope".to_string(),
            context_key: "context-a".to_string(),
            generation: 1,
            destination_key: "destination".to_string(),
            archive_sha256: "digest".to_string(),
        };
        let authentications = std::cell::Cell::new(0_usize);
        let authenticate = || {
            authentications.set(authentications.get() + 1);
            complete_archive_destination(
                &destination,
                &marker,
                "digest",
                ArchiveExtractionLimits::default(),
            )
        };

        assert!(
            resolve_authenticated_archive_destination(
                &manifests,
                &cache_key,
                &destination,
                &marker,
                ArchiveExtractionLimits::default(),
                authenticate,
            )
            .unwrap()
            .is_some()
        );
        assert!(
            resolve_authenticated_archive_destination(
                &manifests,
                &cache_key,
                &destination,
                &marker,
                ArchiveExtractionLimits::default(),
                authenticate,
            )
            .unwrap()
            .is_some()
        );
        std::fs::write(destination.join("value"), b"other").unwrap();
        assert!(
            resolve_authenticated_archive_destination(
                &manifests,
                &cache_key,
                &destination,
                &marker,
                ArchiveExtractionLimits::default(),
                authenticate,
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(authentications.get(), 1);

        invalidate_archive_manifest_scope_in(&manifests, "scope");
        let changed_key = ArchiveManifestCacheKey {
            context_key: "context-b".to_string(),
            generation: 2,
            ..cache_key
        };
        assert!(
            resolve_authenticated_archive_destination(
                &manifests,
                &changed_key,
                &destination,
                &marker,
                ArchiveExtractionLimits::default(),
                authenticate,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(authentications.get(), 2);
    }

    fn test_artifact(scope_id: &str, name: &str, sha256: &str) -> PySparkPythonArtifact {
        PySparkPythonArtifact {
            scope_id: scope_id.to_string(),
            name: name.to_string(),
            python_path: name.to_string(),
            data: Some(Arc::from([])),
            uri: None,
            sha256: sha256.to_string(),
            size: 0,
            kind: PySparkArtifactKind::File,
        }
    }

    #[test]
    fn spark_files_root_is_stable_within_a_session_scope() {
        let _test_lock = PROCESS_ARTIFACT_TEST_LOCK.lock().unwrap();
        let scope_id = "a".repeat(64);
        let first = vec![test_artifact(&scope_id, "files/first", &"1".repeat(64))];
        let second = vec![
            test_artifact(&scope_id, "files/first", &"1".repeat(64)),
            test_artifact(&scope_id, "files/second", &"2".repeat(64)),
        ];

        assert_ne!(
            python_artifact_context_key(&first),
            python_artifact_context_key(&second)
        );
        assert_eq!(
            spark_files_root(&first).unwrap(),
            spark_files_root(&second).unwrap()
        );
    }

    #[test]
    fn artifact_cache_shutdown_clears_python_and_process_state() {
        let _test_lock = PROCESS_ARTIFACT_TEST_LOCK.lock().unwrap();
        Python::initialize();
        let root = artifact_process_root().unwrap().to_path_buf();
        let cached = root.join("shutdown-test");
        std::fs::create_dir_all(&cached).unwrap();
        let module_path = cached.join("shutdown_module.py");
        std::fs::write(&module_path, b"value = 1").unwrap();
        let cached = cached.to_string_lossy().into_owned();
        *INSTALLED_ARTIFACT_PYTHON_PATHS
            .get_or_init(|| Mutex::new(vec![]))
            .lock()
            .unwrap() = vec![cached.clone()];
        *INSTALLED_ARTIFACT_CONTEXT_KEY
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = Some("shutdown-context".to_string());
        Python::attach(|py| {
            let sys = PyModule::import(py, "sys").unwrap();
            sys.getattr("path")
                .unwrap()
                .call_method1("insert", (0, cached.as_str()))
                .unwrap();
            let modules = sys
                .getattr("modules")
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            let module = PyModule::new(py, "sail_shutdown_module").unwrap();
            module
                .setattr("__file__", module_path.to_string_lossy().as_ref())
                .unwrap();
            modules.set_item("sail_shutdown_module", module).unwrap();
        });

        shutdown_python_artifact_cache().unwrap();

        assert!(!root.exists());
        assert!(
            INSTALLED_ARTIFACT_PYTHON_PATHS
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(
            INSTALLED_ARTIFACT_CONTEXT_KEY
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .is_none()
        );
        Python::attach(|py| {
            let sys = PyModule::import(py, "sys").unwrap();
            assert!(!path_list_contains(&sys.getattr("path").unwrap(), &cached).unwrap());
            let modules = sys
                .getattr("modules")
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            assert!(!modules.contains("sail_shutdown_module").unwrap());
        });

        let recreated = artifact_process_root().unwrap();
        assert_eq!(recreated, root);
        let metadata = std::fs::symlink_metadata(recreated).unwrap();
        assert!(metadata.file_type().is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        }
        let context_root = spark_files_root(&[]).unwrap();
        std::fs::create_dir_all(context_root).unwrap();
    }

    #[test]
    fn artifact_process_root_rejects_a_regular_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact-root");
        std::fs::write(&path, b"not a directory").unwrap();

        let error = ensure_private_artifact_root(&path).unwrap_err();

        assert!(error.to_string().contains("not a private directory"));
    }

    #[cfg(unix)]
    #[test]
    fn artifact_process_root_rejects_a_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let path = temp.path().join("artifact-root");
        symlink(target, &path).unwrap();

        let error = ensure_private_artifact_root(&path).unwrap_err();

        assert!(error.to_string().contains("not a private directory"));
    }

    #[test]
    fn new_artifact_set_cancels_old_scope_cleanup() {
        let lock = Arc::new(PythonArtifactContextLock::new());
        let scope_id = "scope".to_string();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("scope");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("value"), b"value").unwrap();
        let first = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key-a".to_string(),
            scope_id.clone(),
            root.clone(),
        )
        .unwrap();
        drop(first);
        let first_generation = {
            let state = lock.state.lock().unwrap();
            assert!(state.cleanup_deadlines.contains_key(&scope_id));
            state.generations[&scope_id]
        };

        let second = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key-b".to_string(),
            scope_id.clone(),
            root.clone(),
        )
        .unwrap();
        {
            let state = lock.state.lock().unwrap();
            assert!(!state.cleanup_deadlines.contains_key(&scope_id));
            assert!(state.generations[&scope_id] > first_generation);
        }
        assert!(root.exists());
        drop(second);
    }

    #[test]
    fn context_switch_removes_inactive_scope_before_acquire_returns() {
        let _test_lock = PROCESS_ARTIFACT_TEST_LOCK.lock().unwrap();
        let lock = Arc::new(PythonArtifactContextLock::new());
        let temp = tempfile::tempdir().unwrap();
        let scope_a = format!(
            "scope-a-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );
        let root_a = temp.path().join(&scope_a);
        std::fs::create_dir(&root_a).unwrap();
        std::fs::write(root_a.join("value"), b"value").unwrap();
        let first = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key-a".to_string(),
            scope_a.clone(),
            root_a.clone(),
        )
        .unwrap();
        drop(first);

        let second = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key-b".to_string(),
            "scope-b".to_string(),
            temp.path().join("scope-b"),
        )
        .unwrap();

        assert!(!root_a.exists());
        let state = lock.state.lock().unwrap();
        assert!(!state.cleanup_deadlines.contains_key(&scope_a));
        assert!(!state.cleaning.contains(&scope_a));
        assert!(!state.generations.contains_key(&scope_a));
        assert!(!state.context_keys.contains_key(&scope_a));
        drop(state);
        drop(second);
    }

    #[test]
    fn repeated_context_key_reuses_materialization_generation() {
        let lock = Arc::new(PythonArtifactContextLock::new());
        let first = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key".to_string(),
            "scope".to_string(),
            PathBuf::from("root"),
        )
        .unwrap();
        let generation = first.materialization_generation;
        drop(first);

        let second = PythonArtifactContextLock::acquire_context(
            lock,
            "key".to_string(),
            "scope".to_string(),
            PathBuf::from("root"),
        )
        .unwrap();

        assert_eq!(second.materialization_generation, generation);
    }

    #[test]
    fn artifact_context_key_change_waits_for_active_guard() {
        let _test_lock = PROCESS_ARTIFACT_TEST_LOCK.lock().unwrap();
        let lock = Arc::new(PythonArtifactContextLock::new());
        let guard_a = PythonArtifactContextLock::acquire_context(
            Arc::clone(&lock),
            "key-a".to_string(),
            "scope-a".to_string(),
            PathBuf::from("root-a"),
        )
        .unwrap();
        let (started_tx, started_rx) = channel();
        let (acquired_tx, acquired_rx) = channel();
        let waiting_lock = Arc::clone(&lock);
        let waiting = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let guard_b = PythonArtifactContextLock::acquire_context(
                waiting_lock,
                "key-b".to_string(),
                "scope-b".to_string(),
                PathBuf::from("root-b"),
            )
            .unwrap();
            acquired_tx.send(()).unwrap();
            guard_b
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            acquired_rx.recv_timeout(Duration::from_millis(200)),
            Err(RecvTimeoutError::Timeout)
        );

        drop(guard_a);
        acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(waiting.join().unwrap());
    }
}
