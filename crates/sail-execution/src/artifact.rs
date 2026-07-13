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
use tokio::sync::{Mutex, OwnedMutexGuard};
use zip::ZipArchive;

#[derive(Debug, Clone, Copy)]
pub struct ArtifactRuntimeOptions {
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
        if options.max_archive_entries == 0 || options.max_archive_expanded_bytes == 0 {
            return Err(DataFusionError::Configuration(
                "Spark archive limits must be greater than zero".to_string(),
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
        let lease = python_artifact_lease().lock_owned().await;
        if let Err(error) = Python::attach(|py| activate_python_artifacts(py, &artifacts)) {
            let _ = Python::attach(deactivate_python_artifacts);
            return Err(external_error(error));
        }
        Ok(ArtifactActivationGuard {
            _lease: lease,
            _artifacts: artifacts,
        })
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
    _lease: OwnedMutexGuard<()>,
    _artifacts: Arc<MaterializedArtifactSet>,
}

impl Drop for ArtifactActivationGuard {
    fn drop(&mut self) {
        if let Err(error) = Python::attach(deactivate_python_artifacts) {
            warn!("failed to deactivate Spark artifacts: {error}");
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
    validate_manifest(manifest)?;
    let root = directory.path().to_path_buf();
    let mut targets = HashMap::<PathBuf, ([u8; 32], MaterializedTargetKind)>::new();
    let mut python_includes = Vec::new();

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
                    extract_zip(&artifact.data, &target, options)?;
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

fn validate_manifest(manifest: &ArtifactManifest) -> Result<()> {
    let mut hasher = Sha256::new();
    for artifact in &manifest.artifacts {
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

fn extract_zip(data: &[u8], target: &Path, options: ArtifactRuntimeOptions) -> Result<()> {
    std::fs::create_dir(target).map_err(external_error)?;
    let mut archive = ZipArchive::new(Cursor::new(data)).map_err(external_error)?;
    if archive.len() > options.max_archive_entries {
        return Err(DataFusionError::Execution(format!(
            "archive contains {} entries, exceeding the {} entry limit",
            archive.len(),
            options.max_archive_entries
        )));
    }

    let mut expanded_bytes = 0usize;
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
        let next_size = expanded_bytes.checked_add(declared_size).ok_or_else(|| {
            DataFusionError::Execution("archive expanded size overflow".to_string())
        })?;
        if next_size > options.max_archive_expanded_bytes {
            return Err(DataFusionError::Execution(format!(
                "archive expands to more than {} bytes",
                options.max_archive_expanded_bytes
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
        let remaining = options.max_archive_expanded_bytes - expanded_bytes;
        let mut limited = entry.take(
            u64::try_from(remaining)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        );
        let copied = std::io::copy(&mut limited, &mut file).map_err(external_error)?;
        if copied > u64::try_from(remaining).unwrap_or(u64::MAX) {
            return Err(DataFusionError::Execution(format!(
                "archive expands to more than {} bytes",
                options.max_archive_expanded_bytes
            )));
        }
        if copied != declared_size_u64 {
            return Err(DataFusionError::Execution(format!(
                "ZIP entry size mismatch: {}",
                entry_name
            )));
        }
        file.sync_all().map_err(external_error)?;
        expanded_bytes = next_size;
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

fn python_artifact_lease() -> Arc<Mutex<()>> {
    // TODO: Replace process-wide activation with per-session Python interpreters.
    static LEASE: OnceLock<Arc<Mutex<()>>> = OnceLock::new();
    LEASE.get_or_init(|| Arc::new(Mutex::new(()))).clone()
}

fn python_artifact_state() -> &'static StdMutex<PythonArtifactState> {
    static STATE: OnceLock<StdMutex<PythonArtifactState>> = OnceLock::new();
    STATE.get_or_init(|| StdMutex::new(PythonArtifactState::default()))
}

fn activate_python_artifacts(py: Python<'_>, artifacts: &MaterializedArtifactSet) -> PyResult<()> {
    let root = path_to_string(&artifacts.root)?;
    let includes = artifacts
        .python_includes
        .iter()
        .map(|path| path_to_string(path))
        .collect::<PyResult<Vec<_>>>()?;
    let mut state = python_artifact_state()
        .lock()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    clear_python_artifact_state(py, &mut state)?;

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

fn deactivate_python_artifacts(py: Python<'_>) -> PyResult<()> {
    let mut state = python_artifact_state()
        .lock()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    clear_python_artifact_state(py, &mut state)
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
