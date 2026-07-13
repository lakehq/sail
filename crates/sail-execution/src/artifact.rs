use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use log::warn;
use pin_project_lite::pin_project;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use sail_common_datafusion::extension::SessionExtension;
use sail_common_datafusion::session::artifact::{
    ArtifactManifest, RuntimeArtifact, RuntimeArtifactKind,
};
use sha2::{Digest, Sha256};
use tempfile::{Builder, TempDir};
use tokio::sync::{Mutex, watch};
use zip::ZipArchive;

#[derive(Debug, Clone, Copy)]
pub struct ArtifactRuntimeOptions {
    pub max_artifact_bytes: usize,
    pub max_session_bytes: usize,
    pub max_artifacts: usize,
    pub max_archive_entries: usize,
    pub max_archive_expanded_bytes: usize,
}

pub struct ArtifactRuntime {
    root: TempDir,
    options: ArtifactRuntimeOptions,
    materialized: Mutex<MaterializedArtifactCache>,
}

type ArtifactSetKey = (String, [u8; 32]);
type MaterializedArtifactCache = HashMap<ArtifactSetKey, Arc<MaterializedArtifactSet>>;

impl ArtifactRuntime {
    pub fn try_new(options: ArtifactRuntimeOptions) -> Result<Self> {
        if options.max_artifact_bytes == 0
            || options.max_session_bytes == 0
            || options.max_artifacts == 0
            || options.max_archive_entries == 0
            || options.max_archive_expanded_bytes == 0
        {
            return Err(DataFusionError::Configuration(
                "Spark artifact runtime limits must be greater than zero".to_string(),
            ));
        }
        if options.max_artifact_bytes > options.max_session_bytes {
            return Err(DataFusionError::Configuration(
                "Spark artifact max_artifact_bytes cannot exceed max_session_bytes".to_string(),
            ));
        }
        let root = Builder::new()
            .prefix("sail-artifacts-")
            .tempdir()
            .map_err(external_error)?;
        Ok(Self {
            root,
            options,
            materialized: Mutex::new(HashMap::new()),
        })
    }

    pub async fn materialize(
        &self,
        manifest: &ArtifactManifest,
    ) -> Result<Arc<MaterializedArtifactSet>> {
        validate_set_id(&manifest.set_id)?;
        let key = (manifest.set_id.clone(), manifest.fingerprint);
        let mut materialized = self.materialized.lock().await;
        if let Some(artifacts) = materialized.get(&key) {
            return Ok(artifacts.clone());
        }

        let directory = Builder::new()
            .prefix("set-")
            .tempdir_in(self.root.path())
            .map_err(external_error)?;
        let manifest = manifest.clone();
        let options = self.options;
        let artifacts = tokio::task::spawn_blocking(move || {
            materialize_manifest(directory, &manifest, options)
        })
        .await
        .map_err(external_error)??;
        let artifacts = Arc::new(artifacts);
        // TODO: Add lease-aware LRU eviction for long-lived workers that serve many sessions.
        materialized.insert(key, artifacts.clone());
        Ok(artifacts)
    }

    pub async fn activate(&self, manifest: &ArtifactManifest) -> Result<ArtifactActivationGuard> {
        let artifacts = self.materialize(manifest).await?;
        let key = (manifest.set_id.clone(), manifest.fingerprint);
        acquire_python_artifacts(key, artifacts).await
    }
}

impl SessionExtension for ArtifactRuntime {
    fn name() -> &'static str {
        "ArtifactRuntime"
    }
}

pub struct MaterializedArtifactSet {
    _directory: TempDir,
    root: PathBuf,
    python_includes: Vec<PathBuf>,
}

impl MaterializedArtifactSet {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn python_includes(&self) -> &[PathBuf] {
        &self.python_includes
    }
}

pub struct ArtifactActivationGuard {
    key: ArtifactSetKey,
}

impl Drop for ArtifactActivationGuard {
    fn drop(&mut self) {
        let coordinator = python_artifact_coordinator();
        let mut state = match coordinator.state.lock() {
            Ok(state) => state,
            Err(error) => {
                warn!("failed to release Spark artifact activation: {error}");
                return;
            }
        };
        if state.active_key.as_ref() != Some(&self.key) || state.user_count == 0 {
            warn!("Spark artifact activation state is inconsistent");
            return;
        }
        state.user_count -= 1;
        if state.user_count == 0 {
            if let Err(error) =
                Python::attach(|py| clear_python_artifact_state(py, &mut state.python))
            {
                warn!("failed to deactivate Spark artifacts: {error}");
            }
            state.active_key = None;
            state.active_artifacts = None;
            drop(state);
            coordinator
                .changed
                .send_modify(|version| *version = version.wrapping_add(1));
        }
    }
}

pin_project! {
    struct ActivatedArtifactStream {
        #[pin]
        stream: SendableRecordBatchStream,
        _activation: ArtifactActivationGuard,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MaterializedTargetKind {
    File,
    Directory,
}

struct ArchiveExtractionBudget {
    max_entries: usize,
    remaining_entries: usize,
    max_expanded_bytes: usize,
    remaining_expanded_bytes: usize,
}

impl ArchiveExtractionBudget {
    fn new(options: ArtifactRuntimeOptions) -> Self {
        Self {
            max_entries: options.max_archive_entries,
            remaining_entries: options.max_archive_entries,
            max_expanded_bytes: options.max_archive_expanded_bytes,
            remaining_expanded_bytes: options.max_archive_expanded_bytes,
        }
    }
}

impl Stream for ActivatedArtifactStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl RecordBatchStream for ActivatedArtifactStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

pub fn hold_artifact_activation(
    stream: SendableRecordBatchStream,
    activation: ArtifactActivationGuard,
) -> SendableRecordBatchStream {
    Box::pin(ActivatedArtifactStream {
        stream,
        _activation: activation,
    })
}

fn materialize_manifest(
    directory: TempDir,
    manifest: &ArtifactManifest,
    options: ArtifactRuntimeOptions,
) -> Result<MaterializedArtifactSet> {
    validate_manifest(manifest, options)?;
    let root = directory.path().to_path_buf();
    let mut targets = HashMap::<PathBuf, ([u8; 32], MaterializedTargetKind)>::new();
    let mut python_includes = Vec::new();
    let mut archive_budget = ArchiveExtractionBudget::new(options);

    for artifact in &manifest.artifacts {
        let file_name = runtime_file_name(artifact)?;
        match artifact.kind {
            RuntimeArtifactKind::PythonFile | RuntimeArtifactKind::File => {
                let target = root.join(file_name);
                if reserve_target(
                    &mut targets,
                    &target,
                    artifact.digest,
                    MaterializedTargetKind::File,
                )? {
                    write_new_file(&target, &artifact.data)?;
                }
                if artifact.kind == RuntimeArtifactKind::PythonFile
                    && is_python_package(file_name)
                    && !python_includes.contains(&target)
                {
                    python_includes.push(target);
                }
            }
            RuntimeArtifactKind::Archive => {
                let archive_name = artifact.archive_name.as_deref().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "archive artifact has no extraction name: {}",
                        artifact.name
                    ))
                })?;
                validate_single_component(archive_name, "archive extraction name")?;
                let target = root.join(archive_name);
                if reserve_target(
                    &mut targets,
                    &target,
                    artifact.digest,
                    MaterializedTargetKind::Directory,
                )? {
                    extract_zip(&artifact.data, &target, &mut archive_budget)?;
                }
            }
        }
    }

    Ok(MaterializedArtifactSet {
        _directory: directory,
        root,
        python_includes,
    })
}

fn validate_manifest(manifest: &ArtifactManifest, options: ArtifactRuntimeOptions) -> Result<()> {
    if manifest.artifacts.len() > options.max_artifacts {
        return Err(DataFusionError::Execution(format!(
            "artifact manifest contains {} artifacts, exceeding the {} artifact limit",
            manifest.artifacts.len(),
            options.max_artifacts
        )));
    }
    let mut hasher = Sha256::new();
    let mut total_bytes = 0usize;
    for artifact in &manifest.artifacts {
        if artifact.data.len() > options.max_artifact_bytes {
            return Err(DataFusionError::Execution(format!(
                "artifact {} contains {} bytes, exceeding the {} byte limit",
                artifact.name,
                artifact.data.len(),
                options.max_artifact_bytes
            )));
        }
        total_bytes = total_bytes
            .checked_add(artifact.data.len())
            .ok_or_else(|| {
                DataFusionError::Execution("artifact byte count overflow".to_string())
            })?;
        if total_bytes > options.max_session_bytes {
            return Err(DataFusionError::Execution(format!(
                "artifact manifest contains {total_bytes} bytes, exceeding the {} byte limit",
                options.max_session_bytes
            )));
        }
        let digest: [u8; 32] = Sha256::digest(&artifact.data).into();
        if digest != artifact.digest {
            return Err(DataFusionError::Execution(format!(
                "artifact content digest mismatch: {}",
                artifact.name
            )));
        }
        let kind = match artifact.kind {
            RuntimeArtifactKind::PythonFile => 1u8,
            RuntimeArtifactKind::File => 2u8,
            RuntimeArtifactKind::Archive => 3u8,
        };
        hasher.update([kind]);
        update_fingerprint_field(&mut hasher, artifact.name.as_bytes());
        update_fingerprint_field(
            &mut hasher,
            artifact
                .archive_name
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(artifact.digest);
    }
    let fingerprint: [u8; 32] = hasher.finalize().into();
    if fingerprint != manifest.fingerprint {
        return Err(DataFusionError::Execution(
            "artifact manifest fingerprint mismatch".to_string(),
        ));
    }
    Ok(())
}

fn update_fingerprint_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn runtime_file_name(artifact: &RuntimeArtifact) -> Result<&str> {
    let path = Path::new(&artifact.name);
    let components = path.components().collect::<Vec<_>>();
    if path.is_absolute()
        || components.len() < 2
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DataFusionError::Execution(format!(
            "invalid runtime artifact path: {}",
            artifact.name
        )));
    }
    let namespace = artifact.name.split('/').next().unwrap_or_default();
    let expected_namespace = match artifact.kind {
        RuntimeArtifactKind::PythonFile => "pyfiles",
        RuntimeArtifactKind::File => "files",
        RuntimeArtifactKind::Archive => "archives",
    };
    if namespace != expected_namespace {
        return Err(DataFusionError::Execution(format!(
            "artifact namespace {namespace:?} does not match runtime kind {expected_namespace:?}"
        )));
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "artifact has no UTF-8 file name: {}",
                artifact.name
            ))
        })?;
    validate_single_component(file_name, "artifact file name")?;
    Ok(file_name)
}

fn validate_single_component(value: &str, description: &str) -> Result<()> {
    let mut components = Path::new(value).components();
    if value.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(DataFusionError::Execution(format!(
            "invalid {description}: {value:?}"
        )));
    }
    Ok(())
}

fn validate_set_id(set_id: &str) -> Result<()> {
    if set_id.is_empty()
        || !set_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(DataFusionError::Execution(format!(
            "invalid artifact set ID: {set_id:?}"
        )));
    }
    Ok(())
}

fn reserve_target(
    targets: &mut HashMap<PathBuf, ([u8; 32], MaterializedTargetKind)>,
    target: &Path,
    digest: [u8; 32],
    kind: MaterializedTargetKind,
) -> Result<bool> {
    match targets.get(target) {
        Some(existing) if *existing == (digest, kind) => Ok(false),
        Some(_) => Err(DataFusionError::Execution(format!(
            "runtime artifacts resolve to conflicting target: {}",
            target.display()
        ))),
        None => {
            targets.insert(target.to_path_buf(), (digest, kind));
            Ok(true)
        }
    }
}

fn write_new_file(path: &Path, data: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(external_error)?;
    file.write_all(data).map_err(external_error)?;
    file.sync_all().map_err(external_error)
}

fn extract_zip(data: &[u8], target: &Path, budget: &mut ArchiveExtractionBudget) -> Result<()> {
    std::fs::create_dir(target).map_err(external_error)?;
    let mut archive = ZipArchive::new(Cursor::new(data)).map_err(external_error)?;
    if archive.len() > budget.remaining_entries {
        return Err(DataFusionError::Execution(format!(
            "archives contain more than {} entries",
            budget.max_entries
        )));
    }
    budget.remaining_entries -= archive.len();

    for index in 0..archive.len() {
        let entry = archive.by_index(index).map_err(external_error)?;
        let enclosed = entry.enclosed_name().ok_or_else(|| {
            DataFusionError::Execution(format!("unsafe ZIP entry path: {}", entry.name()))
        })?;
        if enclosed
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(DataFusionError::Execution(format!(
                "unsafe ZIP entry path: {}",
                entry.name()
            )));
        }
        if let Some(mode) = entry.unix_mode() {
            let file_type = mode & 0o170000;
            if file_type != 0 && file_type != 0o040000 && file_type != 0o100000 {
                return Err(DataFusionError::Execution(format!(
                    "unsupported ZIP entry type: {}",
                    entry.name()
                )));
            }
        }

        let output = target.join(enclosed);
        if entry.is_dir() {
            std::fs::create_dir_all(&output).map_err(external_error)?;
            continue;
        }
        let entry_name = entry.name().to_string();
        let declared_size_u64 = entry.size();
        let declared_size = usize::try_from(declared_size_u64).map_err(|_| {
            DataFusionError::Execution(format!("ZIP entry is too large: {}", entry.name()))
        })?;
        if declared_size > budget.remaining_expanded_bytes {
            return Err(DataFusionError::Execution(format!(
                "archive expands to more than {} bytes",
                budget.max_expanded_bytes
            )));
        }
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).map_err(external_error)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)
            .map_err(external_error)?;
        let remaining = budget.remaining_expanded_bytes;
        let mut limited = entry.take(
            u64::try_from(remaining)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        );
        let copied = std::io::copy(&mut limited, &mut file).map_err(external_error)?;
        if copied > u64::try_from(remaining).unwrap_or(u64::MAX) {
            return Err(DataFusionError::Execution(format!(
                "archive expands to more than {} bytes",
                budget.max_expanded_bytes
            )));
        }
        if copied != declared_size_u64 {
            return Err(DataFusionError::Execution(format!(
                "ZIP entry size mismatch: {}",
                entry_name
            )));
        }
        file.sync_all().map_err(external_error)?;
        budget.remaining_expanded_bytes -= declared_size;
    }
    Ok(())
}

#[derive(Default)]
struct PythonArtifactState {
    managed_paths: Vec<String>,
    managed_root: Option<String>,
    previous_spark_files_root: Option<Py<PyAny>>,
    previous_spark_files_worker: Option<Py<PyAny>>,
}

#[derive(Default)]
struct PythonArtifactCoordinatorState {
    active_key: Option<ArtifactSetKey>,
    active_artifacts: Option<Arc<MaterializedArtifactSet>>,
    user_count: usize,
    python: PythonArtifactState,
}

struct PythonArtifactCoordinator {
    state: StdMutex<PythonArtifactCoordinatorState>,
    changed: watch::Sender<u64>,
}

fn python_artifact_coordinator() -> &'static PythonArtifactCoordinator {
    // TODO: Replace process-wide activation with per-session Python interpreters.
    static COORDINATOR: OnceLock<PythonArtifactCoordinator> = OnceLock::new();
    COORDINATOR.get_or_init(|| PythonArtifactCoordinator {
        state: StdMutex::new(PythonArtifactCoordinatorState::default()),
        changed: watch::channel(0).0,
    })
}

async fn acquire_python_artifacts(
    key: ArtifactSetKey,
    artifacts: Arc<MaterializedArtifactSet>,
) -> Result<ArtifactActivationGuard> {
    let coordinator = python_artifact_coordinator();
    loop {
        let mut changed = coordinator.changed.subscribe();
        {
            let mut state = coordinator.state.lock().map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to acquire Spark artifact activation: {error}"
                ))
            })?;
            if state.active_key.as_ref() == Some(&key) {
                if state.active_artifacts.is_none() {
                    return Err(DataFusionError::Execution(
                        "Spark artifact activation has no materialized artifact set".to_string(),
                    ));
                }
                state.user_count = state.user_count.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution(
                        "Spark artifact activation user count overflow".to_string(),
                    )
                })?;
                return Ok(ArtifactActivationGuard { key });
            }
            if state.active_key.is_none() {
                if let Err(error) = Python::attach(|py| {
                    activate_python_artifacts(py, &artifacts, &mut state.python)
                }) {
                    if let Err(cleanup_error) =
                        Python::attach(|py| clear_python_artifact_state(py, &mut state.python))
                    {
                        warn!("failed to clean up Spark artifact activation: {cleanup_error}");
                    }
                    return Err(external_error(error));
                }
                state.active_key = Some(key.clone());
                state.active_artifacts = Some(artifacts);
                state.user_count = 1;
                return Ok(ArtifactActivationGuard { key });
            }
        }
        changed.changed().await.map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to wait for Spark artifact activation: {error}"
            ))
        })?;
    }
}

fn activate_python_artifacts(
    py: Python<'_>,
    artifacts: &MaterializedArtifactSet,
    state: &mut PythonArtifactState,
) -> PyResult<()> {
    let root = path_to_string(&artifacts.root)?;
    let includes = artifacts
        .python_includes
        .iter()
        .map(|path| path_to_string(path))
        .collect::<PyResult<Vec<_>>>()?;
    clear_python_artifact_state(py, state)?;

    let sys = PyModule::import(py, "sys")?;
    let sys_path = sys.getattr("path")?;
    let sys_path = sys_path.cast::<PyList>()?;
    let spark_files = PyModule::import(py, "pyspark")?.getattr("SparkFiles")?;
    state.previous_spark_files_root = Some(spark_files.getattr("_root_directory")?.unbind());
    state.previous_spark_files_worker =
        Some(spark_files.getattr("_is_running_on_worker")?.unbind());
    state.managed_paths = std::iter::once(root.clone())
        .chain(includes.iter().cloned())
        .collect();
    state.managed_root = Some(root.clone());

    sys_path.insert(1, &root)?;
    for include in &includes {
        sys_path.insert(1, include)?;
    }
    PyModule::import(py, "importlib")?
        .getattr("invalidate_caches")?
        .call0()?;

    spark_files.setattr("_root_directory", &root)?;
    spark_files.setattr("_is_running_on_worker", true)?;
    Ok(())
}

fn clear_python_artifact_state(py: Python<'_>, state: &mut PythonArtifactState) -> PyResult<()> {
    if state.managed_paths.is_empty()
        && state.managed_root.is_none()
        && state.previous_spark_files_root.is_none()
        && state.previous_spark_files_worker.is_none()
    {
        return Ok(());
    }

    let sys = PyModule::import(py, "sys")?;
    let sys_path = sys.getattr("path")?;
    let sys_path = sys_path.cast::<PyList>()?;
    for managed in &state.managed_paths {
        while sys_path.contains(managed)? {
            let index = sys_path.index(managed)?;
            sys_path.del_item(index)?;
        }
    }
    if let Some(root) = &state.managed_root {
        remove_modules_below_root(&sys, root)?;
    }

    let spark_files = PyModule::import(py, "pyspark")?.getattr("SparkFiles")?;
    if let Some(root) = &state.previous_spark_files_root {
        spark_files.setattr("_root_directory", root.bind(py))?;
    }
    if let Some(worker) = &state.previous_spark_files_worker {
        spark_files.setattr("_is_running_on_worker", worker.bind(py))?;
    }
    PyModule::import(py, "importlib")?
        .getattr("invalidate_caches")?
        .call0()?;

    state.managed_paths.clear();
    state.managed_root = None;
    state.previous_spark_files_root = None;
    state.previous_spark_files_worker = None;
    Ok(())
}

fn remove_modules_below_root(sys: &Bound<'_, PyModule>, root: &str) -> PyResult<()> {
    let modules = sys.getattr("modules")?;
    let modules = modules.cast::<PyDict>()?;
    let mut names = Vec::new();
    for (name, module) in modules.iter() {
        let Ok(module_file) = module.getattr("__file__") else {
            continue;
        };
        let Ok(module_file) = module_file.extract::<String>() else {
            continue;
        };
        if Path::new(&module_file).starts_with(root) {
            names.push(name.unbind());
        }
    }
    for name in names {
        modules.del_item(name.bind(sys.py()))?;
    }
    Ok(())
}

fn path_to_string(path: &Path) -> PyResult<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| PyRuntimeError::new_err(format!("path is not UTF-8: {}", path.display())))
}

fn is_python_package(file_name: &str) -> bool {
    [".zip", ".egg", ".jar"]
        .iter()
        .any(|suffix| file_name.ends_with(suffix))
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn Error>>;

    fn options() -> ArtifactRuntimeOptions {
        ArtifactRuntimeOptions {
            max_artifact_bytes: 1024,
            max_session_bytes: 4096,
            max_artifacts: 16,
            max_archive_entries: 8,
            max_archive_expanded_bytes: 1024,
        }
    }

    fn artifact(
        name: &str,
        kind: RuntimeArtifactKind,
        archive_name: Option<&str>,
        data: Vec<u8>,
    ) -> RuntimeArtifact {
        let digest = Sha256::digest(&data).into();
        RuntimeArtifact {
            name: name.to_string(),
            kind,
            archive_name: archive_name.map(str::to_string),
            digest,
            data: data.into(),
        }
    }

    fn manifest(artifacts: Vec<RuntimeArtifact>) -> ArtifactManifest {
        let mut hasher = Sha256::new();
        for artifact in &artifacts {
            let kind = match artifact.kind {
                RuntimeArtifactKind::PythonFile => 1u8,
                RuntimeArtifactKind::File => 2u8,
                RuntimeArtifactKind::Archive => 3u8,
            };
            hasher.update([kind]);
            update_fingerprint_field(&mut hasher, artifact.name.as_bytes());
            update_fingerprint_field(
                &mut hasher,
                artifact
                    .archive_name
                    .as_deref()
                    .unwrap_or_default()
                    .as_bytes(),
            );
            hasher.update(artifact.digest);
        }
        ArtifactManifest {
            set_id: "session-1".to_string(),
            fingerprint: hasher.finalize().into(),
            artifacts,
        }
    }

    fn zip_files(entries: &[(&str, &[u8])]) -> TestResult<Vec<u8>> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let file_options =
            SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, data) in entries {
            writer.start_file(name, file_options)?;
            writer.write_all(data)?;
        }
        Ok(writer.finish()?.into_inner())
    }

    #[test]
    fn materializes_runtime_files_and_archive() -> TestResult {
        let archive = zip_files(&[("nested/value.txt", b"archive")])?;
        let artifacts = manifest(vec![
            artifact(
                "files/config.txt",
                RuntimeArtifactKind::File,
                None,
                b"config".to_vec(),
            ),
            artifact(
                "pyfiles/module.py",
                RuntimeArtifactKind::PythonFile,
                None,
                b"VALUE = 1".to_vec(),
            ),
            artifact(
                "pyfiles/package.zip",
                RuntimeArtifactKind::PythonFile,
                None,
                zip_files(&[("package/__init__.py", b"VALUE = 2")])?,
            ),
            artifact(
                "archives/archive.zip",
                RuntimeArtifactKind::Archive,
                Some("expanded"),
                archive,
            ),
        ]);

        let materialized = materialize_manifest(tempfile::tempdir()?, &artifacts, options())?;
        assert_eq!(
            std::fs::read(materialized.root().join("config.txt"))?,
            b"config"
        );
        assert_eq!(
            std::fs::read(materialized.root().join("module.py"))?,
            b"VALUE = 1"
        );
        assert_eq!(
            std::fs::read(materialized.root().join("expanded/nested/value.txt"))?,
            b"archive"
        );
        assert_eq!(
            materialized.python_includes(),
            &[materialized.root().join("package.zip")]
        );
        Ok(())
    }

    #[test]
    fn rejects_unsafe_archive_entry() -> TestResult {
        let archive = artifact(
            "archives/archive.zip",
            RuntimeArtifactKind::Archive,
            Some("expanded"),
            zip_files(&[("../outside.txt", b"unsafe")])?,
        );
        let parent = tempfile::tempdir()?;
        let directory = tempfile::tempdir_in(parent.path())?;
        let result = materialize_manifest(directory, &manifest(vec![archive]), options());
        assert!(matches!(result, Err(DataFusionError::Execution(_))));
        assert!(!parent.path().join("outside.txt").exists());
        Ok(())
    }

    #[test]
    fn rejects_archive_link() -> TestResult {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer.add_symlink(
            "link",
            "target",
            SimpleFileOptions::default().unix_permissions(0o120777),
        )?;
        let archive = writer.finish()?.into_inner();
        let artifact = artifact(
            "archives/archive.zip",
            RuntimeArtifactKind::Archive,
            Some("expanded"),
            archive,
        );

        let result =
            materialize_manifest(tempfile::tempdir()?, &manifest(vec![artifact]), options());
        assert!(matches!(result, Err(DataFusionError::Execution(_))));
        Ok(())
    }

    #[test]
    fn enforces_archive_expanded_size() -> TestResult {
        let artifact = artifact(
            "archives/archive.zip",
            RuntimeArtifactKind::Archive,
            Some("expanded"),
            zip_files(&[("value.txt", b"12345")])?,
        );
        let mut limits = options();
        limits.max_archive_expanded_bytes = 4;

        let result = materialize_manifest(tempfile::tempdir()?, &manifest(vec![artifact]), limits);
        assert!(matches!(result, Err(DataFusionError::Execution(_))));
        Ok(())
    }

    #[test]
    fn enforces_manifest_and_cumulative_archive_limits() -> TestResult {
        let two_files = || {
            manifest(vec![
                artifact(
                    "files/first.txt",
                    RuntimeArtifactKind::File,
                    None,
                    b"123".to_vec(),
                ),
                artifact(
                    "files/second.txt",
                    RuntimeArtifactKind::File,
                    None,
                    b"456".to_vec(),
                ),
            ])
        };

        let mut limits = options();
        limits.max_artifacts = 1;
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &two_files(), limits),
            Err(DataFusionError::Execution(_))
        ));

        let mut limits = options();
        limits.max_artifact_bytes = 2;
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &two_files(), limits),
            Err(DataFusionError::Execution(_))
        ));

        let mut limits = options();
        limits.max_session_bytes = 5;
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &two_files(), limits),
            Err(DataFusionError::Execution(_))
        ));

        let archives = manifest(vec![
            artifact(
                "archives/first.zip",
                RuntimeArtifactKind::Archive,
                Some("first"),
                zip_files(&[("value.txt", b"123")])?,
            ),
            artifact(
                "archives/second.zip",
                RuntimeArtifactKind::Archive,
                Some("second"),
                zip_files(&[("value.txt", b"456")])?,
            ),
        ]);
        let mut limits = options();
        limits.max_archive_entries = 1;
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &archives, limits),
            Err(DataFusionError::Execution(_))
        ));

        let mut limits = options();
        limits.max_archive_expanded_bytes = 5;
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &archives, limits),
            Err(DataFusionError::Execution(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_digest_and_fingerprint_mismatch() -> TestResult {
        let mut bad_digest = manifest(vec![artifact(
            "files/value.txt",
            RuntimeArtifactKind::File,
            None,
            b"value".to_vec(),
        )]);
        bad_digest.artifacts[0].digest = [1; 32];
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &bad_digest, options()),
            Err(DataFusionError::Execution(_))
        ));

        let mut bad_fingerprint = manifest(vec![artifact(
            "files/value.txt",
            RuntimeArtifactKind::File,
            None,
            b"value".to_vec(),
        )]);
        bad_fingerprint.fingerprint = [1; 32];
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &bad_fingerprint, options()),
            Err(DataFusionError::Execution(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_flattened_target_conflict() -> TestResult {
        let artifacts = manifest(vec![
            artifact(
                "files/first/value.txt",
                RuntimeArtifactKind::File,
                None,
                b"first".to_vec(),
            ),
            artifact(
                "files/second/value.txt",
                RuntimeArtifactKind::File,
                None,
                b"second".to_vec(),
            ),
        ]);

        let result = materialize_manifest(tempfile::tempdir()?, &artifacts, options());
        assert!(matches!(result, Err(DataFusionError::Execution(_))));

        let archive_data = zip_files(&[("nested.txt", b"value")])?;
        let kind_conflict = manifest(vec![
            artifact(
                "files/value",
                RuntimeArtifactKind::File,
                None,
                archive_data.clone(),
            ),
            artifact(
                "archives/value.zip",
                RuntimeArtifactKind::Archive,
                Some("value"),
                archive_data,
            ),
        ]);
        let result = materialize_manifest(tempfile::tempdir()?, &kind_conflict, options());
        assert!(matches!(result, Err(DataFusionError::Execution(_))));
        Ok(())
    }

    #[test]
    fn rejects_invalid_runtime_paths_and_archive_names() -> TestResult {
        let invalid_path = manifest(vec![artifact(
            "files",
            RuntimeArtifactKind::File,
            None,
            b"value".to_vec(),
        )]);
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &invalid_path, options()),
            Err(DataFusionError::Execution(_))
        ));

        let invalid_archive_name = manifest(vec![artifact(
            "archives/value.zip",
            RuntimeArtifactKind::Archive,
            Some("nested/value"),
            zip_files(&[("value", b"value")])?,
        )]);
        assert!(matches!(
            materialize_manifest(tempfile::tempdir()?, &invalid_archive_name, options()),
            Err(DataFusionError::Execution(_))
        ));
        Ok(())
    }
}
