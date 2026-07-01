use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use num_bigint::BigUint;
use object_store::ObjectStoreExt;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{pyclass, Python};
use sha2::{Digest, Sha256};
use url::Url;

use crate::error::{PyUdfError, PyUdfResult};

const PYFILES_PREFIX: &str = "pyfiles/";
const FILES_PREFIX: &str = "files/";
const ARCHIVES_PREFIX: &str = "archives/";
static ARTIFACT_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static ARTIFACT_PROCESS_NAMESPACE: OnceLock<String> = OnceLock::new();
static INSTALLED_ARTIFACT_PYTHON_PATHS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static PYTHON_ARTIFACT_CONTEXT_LOCK: OnceLock<Arc<PythonArtifactContextLock>> = OnceLock::new();
const SPARK_FILES_SHIM_SOURCE: &str = include_str!("python/spark_files.py");
const SPARK_FILES_SHIM_REGISTER_SOURCE: &str = include_str!("python/spark_files_register.py");
const ARCHIVE_VALIDATION_SOURCE: &str = include_str!("python/archive_validation.py");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PySparkArtifactKind {
    PyFile,
    File,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PySparkPythonArtifact {
    pub name: String,
    pub python_path: String,
    pub data: Option<Vec<u8>>,
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
    pub python_artifacts: Vec<PySparkPythonArtifact>,
}

struct PythonArtifactContextLock {
    state: Mutex<PythonArtifactContextState>,
    available: Condvar,
}

struct PythonArtifactContextState {
    active_key: Option<String>,
    holders: usize,
}

pub struct PythonArtifactContextGuard {
    lock: Arc<PythonArtifactContextLock>,
    key: String,
}

impl PythonArtifactContextLock {
    fn new() -> Self {
        Self {
            state: Mutex::new(PythonArtifactContextState {
                active_key: None,
                holders: 0,
            }),
            available: Condvar::new(),
        }
    }

    fn acquire(key: String) -> PyUdfResult<PythonArtifactContextGuard> {
        let lock = Arc::clone(
            PYTHON_ARTIFACT_CONTEXT_LOCK.get_or_init(|| Arc::new(PythonArtifactContextLock::new())),
        );
        {
            let mut state = lock.state.lock().map_err(|e| {
                PyUdfError::internal(format!("failed to lock Python artifact context: {e}"))
            })?;
            while state
                .active_key
                .as_ref()
                .is_some_and(|active| active != &key)
            {
                state = lock.available.wait(state).map_err(|e| {
                    PyUdfError::internal(format!("failed to wait for Python artifact context: {e}"))
                })?;
            }
            if state.active_key.is_none() {
                state.active_key = Some(key.clone());
            }
            state.holders += 1;
        }
        Ok(PythonArtifactContextGuard { lock, key })
    }
}

impl Drop for PythonArtifactContextGuard {
    fn drop(&mut self) {
        match self.lock.state.lock() {
            Ok(mut state) => {
                if state.active_key.as_ref() != Some(&self.key) || state.holders == 0 {
                    log::error!("inconsistent Python artifact context lock state");
                    return;
                }
                state.holders -= 1;
                if state.holders == 0 {
                    state.active_key = None;
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
            python_artifacts: vec![],
        }
    }
}

impl PySparkUdfConfig {
    pub fn enter_python_artifact_context(
        &self,
        py: Python,
    ) -> PyUdfResult<PythonArtifactContextGuard> {
        let key = python_artifact_context_key(&self.python_artifacts);
        let guard = py.detach(move || PythonArtifactContextLock::acquire(key))?;
        self.install_python_artifacts(py)?;
        Ok(guard)
    }

    pub fn install_python_artifacts(&self, py: Python) -> PyUdfResult<()> {
        if self.python_artifacts.is_empty() {
            clear_python_artifacts(py)?;
            return Ok(());
        }
        let installed = self.resolve_artifacts(py)?;
        sync_artifact_python_paths(py, &installed.python_paths)?;
        invalidate_import_caches(py)?;
        Ok(())
    }

    fn resolve_artifacts(&self, py: Python) -> PyUdfResult<InstalledArtifacts> {
        let root = spark_files_root(&self.python_artifacts)?;
        std::fs::create_dir_all(&root)?;
        configure_spark_files(py, &root)?;

        let mut python_paths = vec![root.to_string_lossy().into_owned()];
        for artifact in &self.python_artifacts {
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
                    let archive = root.join(archive_storage_relative_path(artifact)?);
                    py.detach(|| materialize_artifact_file(artifact, &archive))?;
                    let destination = root.join(artifact_spark_files_relative_path(artifact)?);
                    extract_archive(py, &artifact.name, &archive, &destination, &artifact.sha256)?;
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

struct InstalledArtifacts {
    python_paths: Vec<String>,
}

fn python_artifact_context_key(artifacts: &[PySparkPythonArtifact]) -> String {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
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

fn spark_files_root(artifacts: &[PySparkPythonArtifact]) -> PyUdfResult<PathBuf> {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        hasher.update((artifact.kind as u8).to_be_bytes());
        hasher.update(artifact.name.as_bytes());
        hasher.update([0]);
        hasher.update(artifact.sha256.as_bytes());
        hasher.update(artifact.size.to_be_bytes());
        hasher.update([0]);
    }
    let hash = BigUint::from_bytes_be(&hasher.finalize()).to_str_radix(36);
    Ok(std::env::temp_dir()
        .join("sail-python-artifacts")
        .join("spark-files")
        .join(artifact_process_namespace())
        .join(hash))
}

fn invalidate_import_caches(py: Python) -> PyUdfResult<()> {
    let importlib = PyModule::import(py, "importlib")?;
    if let Err(error) = importlib.call_method0("invalidate_caches") {
        log::debug!("failed to invalidate Python import caches: {error}");
    }
    Ok(())
}

fn artifact_process_namespace() -> &'static str {
    ARTIFACT_PROCESS_NAMESPACE
        .get_or_init(|| {
            let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_nanos(),
                Err(_) => 0,
            };
            format!("{}-{nanos}", std::process::id())
        })
        .as_str()
}

fn configure_spark_files(py: Python, root: &Path) -> PyUdfResult<()> {
    let module = spark_files_module(py)?;
    let spark_files = module.getattr("SparkFiles")?;
    spark_files.setattr("_root_directory", root.to_string_lossy().as_ref())?;
    spark_files.setattr("_is_running_on_worker", true)?;
    Ok(())
}

fn clear_python_artifacts(py: Python) -> PyUdfResult<()> {
    sync_artifact_python_paths(py, &[])?;
    clear_spark_files(py);
    invalidate_import_caches(py)?;
    Ok(())
}

fn clear_spark_files(py: Python) {
    if let Ok(module) = PyModule::import(py, "pyspark.core.files") {
        if let Ok(spark_files) = module.getattr("SparkFiles") {
            if let Err(error) = spark_files.setattr("_root_directory", Option::<String>::None) {
                log::debug!("failed to clear SparkFiles root directory: {error}");
            }
            if let Err(error) = spark_files.setattr("_is_running_on_worker", false) {
                log::debug!("failed to clear SparkFiles worker state: {error}");
            }
        }
    }
}

fn sync_artifact_python_paths(py: Python, paths: &[String]) -> PyUdfResult<()> {
    let sys = PyModule::import(py, "sys")?;
    let path_list = sys.getattr("path")?;
    let state = INSTALLED_ARTIFACT_PYTHON_PATHS.get_or_init(|| Mutex::new(vec![]));
    let mut installed = state.lock().map_err(|e| {
        PyUdfError::internal(format!("failed to lock installed artifact paths: {e}"))
    })?;

    for path in installed
        .iter()
        .filter(|path| !paths.iter().any(|next| next == *path))
    {
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
    Ok(())
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
    let tmp_path = artifact_temp_path(target_path);
    if let Err(error) = materialize_artifact_payload(artifact, &tmp_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(error);
    }
    match std::fs::rename(&tmp_path, target_path) {
        Ok(()) => Ok(()),
        Err(error) => {
            if file_matches_artifact(target_path, artifact)? {
                let _ = std::fs::remove_file(&tmp_path);
                Ok(())
            } else {
                let _ = std::fs::remove_file(&tmp_path);
                Err(error.into())
            }
        }
    }
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
        std::fs::write(target_path, data)?;
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
                let mut file = File::create(&target_path)
                    .map_err(|e| format!("failed to create artifact file {}: {e}", target_path.display()))?;
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                let mut stream = result.into_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| format!("failed to read artifact bytes {uri}: {e}"))?;
                    size += chunk.len() as u64;
                    hasher.update(&chunk);
                    file.write_all(&chunk)
                        .map_err(|e| format!("failed to write artifact file {}: {e}", target_path.display()))?;
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

fn extract_archive(
    py: Python,
    name: &str,
    archive_path: &Path,
    destination: &Path,
    sha256: &str,
) -> PyUdfResult<()> {
    let marker = destination.join(".sail-artifact.sha256");
    let lock_path = artifact_lock_path(destination);
    let _lock = py.detach(|| acquire_artifact_lock(&lock_path))?;
    if archive_destination_is_complete(destination, &marker, sha256)? {
        return Ok(());
    }
    if destination.exists() {
        std::fs::remove_dir_all(destination)?;
    }
    let tmp_path = artifact_temp_path(destination);
    if tmp_path.exists() {
        std::fs::remove_dir_all(&tmp_path)?;
    }
    std::fs::create_dir_all(&tmp_path)?;

    validate_archive(py, name, archive_path, &tmp_path)?;

    let archive = archive_path.to_string_lossy();
    let tmp = tmp_path.to_string_lossy();
    if archive_name(name).ends_with(".jar") {
        let zipfile = PyModule::import(py, "zipfile")?;
        let zip_file = zipfile.getattr("ZipFile")?.call1((archive.as_ref(),))?;
        zip_file.call_method1("extractall", (tmp.as_ref(),))?;
        zip_file.call_method0("close")?;
    } else {
        let shutil = PyModule::import(py, "shutil")?;
        shutil.call_method1("unpack_archive", (archive.as_ref(), tmp.as_ref()))?;
    }
    validate_extracted_archive_tree(py, &tmp_path)?;
    if !directory_has_non_marker_entry(&tmp_path)? {
        return Err(PyUdfError::invalid(format!(
            "archive {name} did not extract any entries"
        )));
    }

    std::fs::write(tmp_path.join(".sail-artifact.sha256"), sha256)?;
    match std::fs::rename(&tmp_path, destination) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = std::fs::remove_dir_all(&tmp_path);
            Err(error.into())
        }
    }
}

fn archive_destination_is_complete(
    destination: &Path,
    marker: &Path,
    sha256: &str,
) -> PyUdfResult<bool> {
    if !marker.exists() || std::fs::read_to_string(marker)?.trim() != sha256 {
        return Ok(false);
    }
    directory_has_non_marker_entry(destination).map_err(Into::into)
}

fn directory_has_non_marker_entry(path: &Path) -> std::io::Result<bool> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name() != ".sail-artifact.sha256" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn artifact_temp_path(path: &Path) -> PathBuf {
    let counter = ARTIFACT_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("sail-tmp-{}-{counter}", std::process::id()))
}

fn artifact_lock_path(path: &Path) -> PathBuf {
    path.with_extension("sail-lock")
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

fn validate_archive(py: Python, name: &str, archive_path: &Path, root: &Path) -> PyUdfResult<()> {
    let module = archive_validation_module(py)?;
    let archive_name = archive_name(name);
    module.getattr("validate_archive")?.call1((
        archive_name,
        archive_path.to_string_lossy().as_ref(),
        root.to_string_lossy().as_ref(),
    ))?;
    Ok(())
}

fn validate_extracted_archive_tree(py: Python, root: &Path) -> PyUdfResult<()> {
    let module = archive_validation_module(py)?;
    module
        .getattr("validate_extracted_tree")?
        .call1((root.to_string_lossy().as_ref(),))?;
    Ok(())
}

fn archive_validation_module<'py>(py: Python<'py>) -> PyUdfResult<pyo3::Bound<'py, PyModule>> {
    let source = c_string(ARCHIVE_VALIDATION_SOURCE, "archive validation source")?;
    let filename = c_string("archive_validation.py", "archive validation filename")?;
    let module_name = c_string("_sail_archive_validation", "archive validation module name")?;
    Ok(PyModule::from_code(
        py,
        source.as_c_str(),
        filename.as_c_str(),
        module_name.as_c_str(),
    )?)
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
    Ok(PathBuf::from(".archives")
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
    let path = Path::new(path);
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
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
    Ok(out)
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
