use std::collections::HashMap;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{ObjectStoreExt, ObjectStoreScheme, PutPayload};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_python_udf::config::{PySparkArtifactKind, PySparkPythonArtifact};
use sha2::{Digest, Sha256};
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;
use url::Url;

use crate::artifact::SparkArtifactRegistry;
use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::add_artifacts_request::{ArtifactChunk, Payload};
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

const PYFILES_PREFIX: &str = "pyfiles/";
const FILES_PREFIX: &str = "files/";
const ARCHIVES_PREFIX: &str = "archives/";
const FORWARD_TO_FS_PREFIX: &str = "forward_to_fs/";

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
    Ok(name.replace('\\', "/"))
}

fn validate_relative_path(path: &str, description: &str) -> SparkResult<PathBuf> {
    let path = Path::new(path);
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
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
    Ok(out)
}

fn artifact_storage_name(name: &str) -> SparkResult<String> {
    let normalized = normalize_artifact_name(name)?;
    if let Some(rest) = normalized.strip_prefix(ARCHIVES_PREFIX) {
        if rest.matches('#').count() > 1 {
            return Err(SparkError::invalid(format!(
                "'#' in the path is not supported for archive artifact: {name}"
            )));
        }
        if let Some((path, _fragment)) = rest.split_once('#') {
            if path.is_empty() {
                return Err(SparkError::invalid(format!(
                    "archive artifact path must not be empty: {name}"
                )));
            }
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
    kind: Option<PySparkArtifactKind>,
}

/// Processes a complete artifact (name + assembled data) and stores it appropriately.
fn store_artifact(name: &str, data: &[u8], artifact_dir: &Path) -> SparkResult<StoredArtifact> {
    let normalized_name = normalize_artifact_name(name)?;
    if let Some(dest_path) = normalized_name.strip_prefix(FORWARD_TO_FS_PREFIX) {
        let relative_dest = validate_relative_path(dest_path, "forward_to_fs destination")?;
        let dest = Path::new("/").join(relative_dest);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                SparkError::internal(format!(
                    "failed to create parent directory for {}: {e}",
                    dest.display()
                ))
            })?;
        }
        let mut file = std::fs::File::create(&dest).map_err(|e| {
            SparkError::internal(format!("failed to create file {}: {e}", dest.display()))
        })?;
        file.write_all(data).map_err(|e| {
            SparkError::internal(format!("failed to write file {}: {e}", dest.display()))
        })?;
        return Ok(StoredArtifact {
            normalized_name,
            local_path: None,
            kind: None,
        });
    }

    let target_path = artifact_target_path(name, artifact_dir)?;
    let kind = artifact_kind(&normalized_name)?;
    if target_path.exists() {
        let existing = std::fs::read(&target_path).map_err(|e| {
            SparkError::internal(format!(
                "failed to read existing artifact file {}: {e}",
                target_path.display()
            ))
        })?;
        if existing == data {
            let python_path =
                python_artifact_import_path(&normalized_name, &target_path, artifact_dir)?;
            if let Some(path) = &python_path {
                add_to_sys_path(path)?;
            }
            return Ok(StoredArtifact {
                normalized_name,
                local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
                kind,
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
    let mut file = std::fs::File::create(&target_path).map_err(|e| {
        SparkError::internal(format!(
            "failed to create artifact file {}: {e}",
            target_path.display()
        ))
    })?;
    file.write_all(data).map_err(|e| {
        SparkError::internal(format!(
            "failed to write artifact file {}: {e}",
            target_path.display()
        ))
    })?;

    let python_path = python_artifact_import_path(&normalized_name, &target_path, artifact_dir)?;
    if let Some(path) = &python_path {
        add_to_sys_path(path)?;
    }
    Ok(StoredArtifact {
        normalized_name,
        local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
        kind,
    })
}

fn python_artifact_import_path(
    normalized_name: &str,
    target_path: &Path,
    artifact_dir: &Path,
) -> SparkResult<Option<String>> {
    let Some(file_name) = normalized_name.strip_prefix(PYFILES_PREFIX) else {
        return Ok(None);
    };
    if file_name.ends_with(".py") {
        let dir = target_path.parent().unwrap_or(artifact_dir);
        Ok(Some(dir.to_string_lossy().into_owned()))
    } else if file_name.ends_with(".zip")
        || file_name.ends_with(".egg")
        || file_name.ends_with(".jar")
    {
        Ok(Some(target_path.to_string_lossy().into_owned()))
    } else {
        Err(SparkError::invalid(format!(
            "unsupported Python artifact type: {file_name}"
        )))
    }
}

fn add_to_sys_path(path: &str) -> SparkResult<()> {
    use pyo3::prelude::PyAnyMethods;
    use pyo3::types::PyModule;
    use pyo3::Python;

    let path = path.to_string();
    Python::attach(|py| {
        let sys = PyModule::import(py, "sys")?;
        let path_list = sys.getattr("path")?;
        let contains: bool = path_list
            .call_method1("__contains__", (&path,))?
            .extract()?;
        if !contains {
            path_list.call_method1("insert", (0, &path))?;
        }
        PyModule::import(py, "importlib")?.call_method0("invalidate_caches")?;
        Ok::<(), pyo3::PyErr>(())
    })
    .map_err(|e: pyo3::PyErr| SparkError::internal(format!("failed to add to sys.path: {e}")))
}

async fn artifact_transport(
    ctx: &SessionContext,
    artifacts: &SparkArtifactRegistry,
    name: &str,
    sha256: &str,
    data: &[u8],
) -> SparkResult<(Option<Vec<u8>>, Option<String>)> {
    let options = artifacts.options();
    if data.len() <= options.inline_max_bytes {
        return Ok((Some(data.to_vec()), None));
    }

    let Some(base_uri) = &options.store_uri else {
        return Err(SparkError::invalid(format!(
            "artifact {name} is {} bytes, exceeding spark.artifact_inline_max_bytes={}, and spark.artifact_store_uri is not configured",
            data.len(),
            options.inline_max_bytes
        )));
    };
    let uri = upload_artifact(ctx, base_uri, artifacts.session_id(), sha256, data).await?;
    Ok((None, Some(uri)))
}

async fn upload_artifact(
    ctx: &SessionContext,
    base_uri: &str,
    session_id: &str,
    sha256: &str,
    data: &[u8],
) -> SparkResult<String> {
    let uri = artifact_object_uri(base_uri, session_id, sha256)?;
    let url = Url::parse(&uri).map_err(|e| {
        SparkError::invalid(format!("invalid artifact object-store URI {uri}: {e}"))
    })?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url).map_err(|e| {
        SparkError::invalid(format!("invalid artifact object-store path {uri}: {e}"))
    })?;
    let object_store_url = artifact_object_store_url(&url)?;
    let store = ctx
        .runtime_env()
        .object_store(object_store_url)
        .map_err(|e| {
            SparkError::internal(format!("failed to create artifact object store {uri}: {e}"))
        })?;
    store
        .put(&path, PutPayload::from(data.to_vec()))
        .await
        .map_err(|e| SparkError::internal(format!("failed to write artifact {uri}: {e}")))?;
    Ok(uri)
}

fn artifact_object_store_url(
    url: &Url,
) -> SparkResult<datafusion::execution::object_store::ObjectStoreUrl> {
    if url.scheme() == "file" {
        return Ok(datafusion::execution::object_store::ObjectStoreUrl::local_filesystem());
    }
    let root = &url[..url::Position::BeforePath];
    datafusion::execution::object_store::ObjectStoreUrl::parse(root)
        .map_err(|e| SparkError::invalid(format!("invalid artifact object-store URL {root}: {e}")))
}

fn artifact_object_uri(base_uri: &str, session_id: &str, sha256: &str) -> SparkResult<String> {
    let mut url = Url::parse(base_uri).map_err(|e| {
        SparkError::invalid(format!(
            "invalid spark.artifact_store_uri value {base_uri}: {e}"
        ))
    })?;
    if sha256.len() < 2 {
        return Err(SparkError::internal(format!(
            "artifact SHA-256 is unexpectedly short: {sha256}"
        )));
    }
    let session_hash = sha256_hex(session_id.as_bytes());
    let prefix = url.path().trim_end_matches('/');
    let path = if prefix.is_empty() {
        format!(
            "/sail-artifacts/sessions/{session_hash}/{}/{sha256}",
            &sha256[..2]
        )
    } else {
        format!(
            "{prefix}/sail-artifacts/sessions/{session_hash}/{}/{sha256}",
            &sha256[..2]
        )
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
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
    data: Vec<u8>,
    expected_chunks: usize,
    total_bytes: usize,
    chunks_seen: usize,
    is_crc_successful: bool,
}

impl ChunkedArtifact {
    fn try_new(
        name: String,
        total_bytes: i64,
        num_chunks: i64,
        initial_chunk: Option<ArtifactChunk>,
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
        let mut out = Self {
            name,
            data: Vec::with_capacity(total_bytes),
            expected_chunks,
            total_bytes,
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
        if self.data.len() + chunk.data.len() > self.total_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} exceeded expected byte count",
                self.name
            )));
        }
        let chunk_ok = validate_crc(&chunk.data, chunk.crc);
        if !chunk_ok {
            self.is_crc_successful = false;
        }
        self.data.extend_from_slice(&chunk.data);
        self.chunks_seen += 1;
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.chunks_seen == self.expected_chunks
    }

    fn validate_complete(&self) -> SparkResult<()> {
        if self.chunks_seen != self.expected_chunks || self.data.len() != self.total_bytes {
            return Err(SparkError::invalid(format!(
                "missing data chunks for artifact: {}; expected {} chunks and {} bytes, received {} chunks and {} bytes",
                self.name,
                self.expected_chunks,
                self.total_bytes,
                self.chunks_seen,
                self.data.len()
            )));
        }
        Ok(())
    }
}

fn process_chunk(artifact: &mut ChunkedArtifact, chunk: &ArtifactChunk) -> SparkResult<()> {
    artifact.process_chunk(chunk)
}

async fn add_artifact_summary(
    ctx: &SessionContext,
    name: String,
    data: &[u8],
    is_crc_successful: bool,
    artifact_dir: &Path,
    artifacts: &SparkArtifactRegistry,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<()> {
    if is_crc_successful {
        let stored = store_artifact(&name, data, artifact_dir)?;
        if let Some(kind) = stored.kind {
            let sha256 = sha256_hex(data);
            let size = data.len() as u64;
            let (data, uri) =
                artifact_transport(ctx, artifacts, &stored.normalized_name, &sha256, data).await?;
            artifacts.add_artifact(PySparkPythonArtifact {
                name: stored.normalized_name,
                python_path: stored.local_path.unwrap_or_default(),
                data,
                uri,
                sha256,
                size,
                kind,
            })?;
        }
    }
    summaries.push(ArtifactSummary {
        name,
        is_crc_successful,
    });
    Ok(())
}

async fn add_single_chunk_artifact(
    ctx: &SessionContext,
    name: String,
    chunk: ArtifactChunk,
    artifact_dir: &Path,
    artifacts: &SparkArtifactRegistry,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<()> {
    let chunk_ok = validate_crc(&chunk.data, chunk.crc);
    add_artifact_summary(
        ctx,
        name,
        &chunk.data,
        chunk_ok,
        artifact_dir,
        artifacts,
        summaries,
    )
    .await
}

async fn finalize_chunked_artifact(
    ctx: &SessionContext,
    chunked: ChunkedArtifact,
    artifact_dir: &Path,
    artifacts: &SparkArtifactRegistry,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<()> {
    chunked.validate_complete()?;
    add_artifact_summary(
        ctx,
        chunked.name,
        &chunked.data,
        chunked.is_crc_successful,
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
    let artifact_dir = artifacts.artifact_dir()?;

    let mut summaries: Vec<ArtifactSummary> = Vec::new();
    let mut current_chunked: Option<ChunkedArtifact> = None;

    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        let payload = item.map_err(|e| SparkError::internal(e.to_string()))?;
        match payload {
            Payload::Batch(batch) => {
                if let Some(chunked) = current_chunked.take() {
                    return Err(SparkError::invalid(format!(
                        "received artifact batch before chunked artifact {} was complete",
                        chunked.name
                    )));
                }
                for artifact in batch.artifacts {
                    let name = artifact.name;
                    let chunk = artifact.data.required("artifact data")?;
                    add_single_chunk_artifact(
                        ctx,
                        name,
                        chunk,
                        &artifact_dir,
                        &artifacts,
                        &mut summaries,
                    )
                    .await?;
                }
            }
            Payload::BeginChunk(begin) => {
                if let Some(chunked) = current_chunked.take() {
                    return Err(SparkError::invalid(format!(
                        "received new chunked artifact before chunked artifact {} was complete",
                        chunked.name
                    )));
                }
                let chunked = ChunkedArtifact::try_new(
                    begin.name,
                    begin.total_bytes,
                    begin.num_chunks,
                    begin.initial_chunk,
                )?;
                if chunked.is_complete() {
                    finalize_chunked_artifact(
                        ctx,
                        chunked,
                        &artifact_dir,
                        &artifacts,
                        &mut summaries,
                    )
                    .await?;
                } else {
                    current_chunked = Some(chunked);
                }
            }
            Payload::Chunk(chunk) => {
                let is_complete = if let Some(ref mut chunked) = current_chunked {
                    process_chunk(chunked, &chunk)?;
                    chunked.is_complete()
                } else {
                    return Err(SparkError::invalid(
                        "received artifact chunk without an active chunked artifact",
                    ));
                };
                if is_complete {
                    if let Some(chunked) = current_chunked.take() {
                        finalize_chunked_artifact(
                            ctx,
                            chunked,
                            &artifact_dir,
                            &artifacts,
                            &mut summaries,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    // Finalize any remaining chunked artifact
    if let Some(chunked) = current_chunked.take() {
        finalize_chunked_artifact(ctx, chunked, &artifact_dir, &artifacts, &mut summaries).await?;
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
        let exists = artifacts.has_artifact(&normalized)?;
        statuses.insert(name, ArtifactStatus { exists });
    }
    Ok(statuses)
}
