use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
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
use uuid::Uuid;

use crate::artifact::{cleanup_artifact_uris, SparkArtifactRegistry};
use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::config::SPARK_SQL_ARTIFACT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL;
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
const SPARK_CONNECT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL: &str =
    "spark.connect.copyFromLocalToFs.allowDestLocal";

fn validate_crc(data: &[u8], expected_crc: i64) -> bool {
    if !(0..=u32::MAX as i64).contains(&expected_crc) {
        return false;
    }
    let computed = crc32fast::hash(data) as i64;
    computed == expected_crc
}

fn parse_bool_config(key: &str, value: Option<&str>) -> SparkResult<Option<bool>> {
    value
        .map(|value| {
            value.trim().to_lowercase().parse::<bool>().map_err(|e| {
                SparkError::invalid(format!("invalid boolean value for {key}: {value}: {e}"))
            })
        })
        .transpose()
}

fn allow_local_fs_destination(ctx: &SessionContext) -> SparkResult<bool> {
    let spark = ctx.extension::<SparkSession>()?;
    let values = spark.get_config_option(vec![
        SPARK_SQL_ARTIFACT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL.to_string(),
        SPARK_CONNECT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL.to_string(),
    ])?;
    let sql_value = values.first().and_then(|x| x.value.as_deref());
    let connect_value = values.get(1).and_then(|x| x.value.as_deref());
    Ok(parse_bool_config(
        SPARK_SQL_ARTIFACT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL,
        sql_value,
    )?
    .or(parse_bool_config(
        SPARK_CONNECT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL,
        connect_value,
    )?)
    .unwrap_or(false))
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
    target_path: Option<PathBuf>,
    created_target_path: Option<PathBuf>,
    kind: Option<PySparkArtifactKind>,
    cache_hash: Option<String>,
}

#[derive(Default)]
struct PendingArtifactUpdates {
    updates: Vec<PendingArtifactUpdate>,
    committed: bool,
}

struct PendingArtifactUpdate {
    cache_hash: Option<String>,
    created_target_path: Option<PathBuf>,
    python_artifact: Option<PySparkPythonArtifact>,
    created_artifact_uri: Option<String>,
}

impl PendingArtifactUpdate {
    fn is_empty(&self) -> bool {
        self.cache_hash.is_none()
            && self.created_target_path.is_none()
            && self.python_artifact.is_none()
            && self.created_artifact_uri.is_none()
    }
}

impl PendingArtifactUpdates {
    fn push(&mut self, update: Option<PendingArtifactUpdate>) {
        if let Some(update) = update {
            if !update.is_empty() {
                self.updates.push(update);
            }
        }
    }

    fn commit(mut self, artifacts: &SparkArtifactRegistry) -> SparkResult<()> {
        for update in &self.updates {
            if let Some(hash) = &update.cache_hash {
                artifacts.add_cache_artifact(hash.clone())?;
            }
            if let Some(artifact) = &update.python_artifact {
                artifacts.add_artifact(artifact.clone())?;
            }
        }
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingArtifactUpdates {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        let artifact_uris = self
            .updates
            .iter()
            .filter_map(|update| update.created_artifact_uri.clone())
            .collect::<Vec<_>>();
        cleanup_artifact_uris(artifact_uris);

        let created_paths = self
            .updates
            .iter()
            .filter_map(|update| update.created_target_path.clone())
            .collect::<HashSet<_>>();
        for path in created_paths {
            if let Err(error) = std::fs::remove_file(&path) {
                log::debug!(
                    "failed to remove uncommitted artifact file {}: {error}",
                    path.display()
                );
            }
        }
    }
}

enum ArtifactPayload<'a> {
    Bytes(&'a [u8]),
    File(&'a Path),
}

impl ArtifactPayload<'_> {
    fn read_all(&self) -> SparkResult<Vec<u8>> {
        match self {
            Self::Bytes(data) => Ok(data.to_vec()),
            Self::File(path) => std::fs::read(path).map_err(|e| {
                SparkError::internal(format!(
                    "failed to read artifact file {}: {e}",
                    path.display()
                ))
            }),
        }
    }

    fn copy_to(&self, target: &Path) -> SparkResult<()> {
        match self {
            Self::Bytes(data) => {
                let mut file = File::create(target).map_err(|e| {
                    SparkError::internal(format!("failed to create file {}: {e}", target.display()))
                })?;
                file.write_all(data).map_err(|e| {
                    SparkError::internal(format!("failed to write file {}: {e}", target.display()))
                })
            }
            Self::File(source) => {
                std::fs::copy(source, target).map_err(|e| {
                    SparkError::internal(format!(
                        "failed to copy artifact file {} to {}: {e}",
                        source.display(),
                        target.display()
                    ))
                })?;
                Ok(())
            }
        }
    }

    fn content_equals(&self, target: &Path) -> SparkResult<bool> {
        match self {
            Self::Bytes(data) => file_content_equals_bytes(target, data),
            Self::File(source) => file_content_equals_file(source, target),
        }
    }
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
fn store_artifact(
    name: &str,
    payload: &ArtifactPayload<'_>,
    artifact_dir: &Path,
    allow_local_fs_destination: bool,
) -> SparkResult<StoredArtifact> {
    let normalized_name = normalize_artifact_name(name)?;
    if let Some(dest_path) = normalized_name.strip_prefix(FORWARD_TO_FS_PREFIX) {
        if !allow_local_fs_destination {
            return Err(SparkError::unsupported(
                "copyFromLocalToFs to the server local filesystem is disabled; set spark.sql.artifact.copyFromLocalToFs.allowDestLocal=true only for trusted clients",
            ));
        }
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
        payload.copy_to(&dest)?;
        return Ok(StoredArtifact {
            normalized_name,
            local_path: None,
            target_path: None,
            created_target_path: None,
            kind: None,
            cache_hash: None,
        });
    }

    let target_path = artifact_target_path(name, artifact_dir)?;
    let kind = artifact_kind(&normalized_name)?;
    let cache_hash = cache_artifact_hash(&normalized_name)?;
    if target_path.exists() {
        if payload.content_equals(&target_path)? {
            return Ok(StoredArtifact {
                normalized_name,
                local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
                target_path: Some(target_path),
                created_target_path: None,
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
    payload.copy_to(&target_path)?;

    Ok(StoredArtifact {
        normalized_name,
        local_path: kind.map(|_| target_path.to_string_lossy().into_owned()),
        target_path: Some(target_path.clone()),
        created_target_path: Some(target_path),
        kind,
        cache_hash,
    })
}

async fn artifact_transport(
    artifacts: &SparkArtifactRegistry,
    name: &str,
    sha256: &str,
    size: usize,
    payload: &ArtifactPayload<'_>,
) -> SparkResult<ArtifactTransport> {
    let options = artifacts.options();
    if size <= options.inline_max_bytes {
        return Ok(ArtifactTransport {
            data: Some(payload.read_all()?),
            uri: None,
            created_uri: None,
        });
    }

    let Some(base_uri) = &options.store_uri else {
        return Err(SparkError::invalid(format!(
            "artifact {name} is {} bytes, exceeding spark.artifact_inline_max_bytes={}, and spark.artifact_store_uri is not configured",
            size,
            options.inline_max_bytes
        )));
    };
    let uri = artifact_object_uri(base_uri, artifacts.session_id(), sha256)?;
    let created_uri = if artifacts.has_artifact_uri(&uri)? {
        None
    } else {
        Some(uri.clone())
    };
    upload_artifact(&uri, payload).await?;
    Ok(ArtifactTransport {
        data: None,
        uri: Some(uri),
        created_uri,
    })
}

struct ArtifactTransport {
    data: Option<Vec<u8>>,
    uri: Option<String>,
    created_uri: Option<String>,
}

async fn upload_artifact(uri: &str, payload: &ArtifactPayload<'_>) -> SparkResult<()> {
    let url = Url::parse(uri).map_err(|e| {
        SparkError::invalid(format!("invalid artifact object-store URI {uri}: {e}"))
    })?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url).map_err(|e| {
        SparkError::invalid(format!("invalid artifact object-store path {uri}: {e}"))
    })?;
    let store = sail_object_store::get_dynamic_object_store(&url).map_err(|e| {
        SparkError::internal(format!("failed to create artifact object store {uri}: {e}"))
    })?;
    let data = payload.read_all()?;
    store
        .put(&path, PutPayload::from(data))
        .await
        .map_err(|e| SparkError::internal(format!("failed to write artifact {uri}: {e}")))?;
    Ok(())
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
    path: Option<PathBuf>,
    file: File,
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
        let path = create_staging_artifact_path(artifact_dir)?;
        let file = File::create(&path).map_err(|e| {
            SparkError::internal(format!(
                "failed to create staging artifact file {}: {e}",
                path.display()
            ))
        })?;
        let mut out = Self {
            name,
            path: Some(path),
            file,
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
        if self.bytes_seen + chunk.data.len() > self.total_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} exceeded expected byte count",
                self.name
            )));
        }
        let chunk_ok = validate_crc(&chunk.data, chunk.crc);
        if !chunk_ok {
            self.is_crc_successful = false;
        }
        self.file.write_all(&chunk.data).map_err(|e| {
            SparkError::internal(format!(
                "failed to write chunked artifact {}: {e}",
                self.name
            ))
        })?;
        self.sha256.update(&chunk.data);
        self.bytes_seen += chunk.data.len();
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
        self.file.flush().map_err(|e| {
            SparkError::internal(format!(
                "failed to flush chunked artifact {}: {e}",
                self.name
            ))
        })?;
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
            path,
            sha256,
            size: self.bytes_seen,
            is_crc_successful: self.is_crc_successful,
        })
    }
}

impl Drop for ChunkedArtifact {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            if let Err(error) = std::fs::remove_file(path) {
                log::debug!(
                    "failed to remove staging artifact file {}: {error}",
                    path.display()
                );
            }
        }
    }
}

struct CompletedChunkedArtifact {
    name: String,
    path: PathBuf,
    sha256: String,
    size: usize,
    is_crc_successful: bool,
}

impl Drop for CompletedChunkedArtifact {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            log::debug!(
                "failed to remove staging artifact file {}: {error}",
                self.path.display()
            );
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

#[expect(clippy::too_many_arguments)]
async fn add_artifact_summary(
    name: String,
    payload: &ArtifactPayload<'_>,
    sha256: &str,
    size: usize,
    is_crc_successful: bool,
    artifact_dir: &Path,
    artifacts: &SparkArtifactRegistry,
    allow_local_fs_destination: bool,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let mut pending_update = None;
    if is_crc_successful {
        let normalized_name = normalize_artifact_name(&name)?;
        if let Some(cache_hash) = cache_artifact_hash(&normalized_name)? {
            if cache_hash != sha256 {
                return Err(SparkError::invalid(format!(
                    "cache artifact {normalized_name} content hash does not match its name"
                )));
            }
        }
        let stored = store_artifact(&name, payload, artifact_dir, allow_local_fs_destination)?;
        let mut update = PendingArtifactUpdate {
            cache_hash: stored.cache_hash.clone(),
            created_target_path: stored.created_target_path.clone(),
            python_artifact: None,
            created_artifact_uri: None,
        };
        if let Some(kind) = stored.kind {
            let target_path = stored.target_path.as_deref().ok_or_else(|| {
                SparkError::internal(format!(
                    "stored Python artifact {} did not have a target path",
                    stored.normalized_name
                ))
            })?;
            let python_path = stored.local_path.clone().ok_or_else(|| {
                SparkError::internal(format!(
                    "stored Python artifact {} did not have a local path",
                    stored.normalized_name
                ))
            })?;
            let transport_payload = ArtifactPayload::File(target_path);
            let transport = match artifact_transport(
                artifacts,
                &stored.normalized_name,
                sha256,
                size,
                &transport_payload,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    if let Some(path) = &stored.created_target_path {
                        if let Err(remove_error) = std::fs::remove_file(path) {
                            log::debug!(
                                "failed to remove uncommitted artifact file {}: {remove_error}",
                                path.display()
                            );
                        }
                    }
                    return Err(error);
                }
            };
            update.created_artifact_uri = transport.created_uri;
            update.python_artifact = Some(PySparkPythonArtifact {
                name: stored.normalized_name,
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
    artifacts: &SparkArtifactRegistry,
    allow_local_fs_destination: bool,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let chunk_ok = validate_crc(&chunk.data, chunk.crc);
    let sha256 = sha256_hex(&chunk.data);
    let size = chunk.data.len();
    let payload = ArtifactPayload::Bytes(&chunk.data);
    add_artifact_summary(
        name,
        &payload,
        &sha256,
        size,
        chunk_ok,
        artifact_dir,
        artifacts,
        allow_local_fs_destination,
        summaries,
    )
    .await
}

async fn finalize_chunked_artifact(
    chunked: ChunkedArtifact,
    artifact_dir: &Path,
    artifacts: &SparkArtifactRegistry,
    allow_local_fs_destination: bool,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<Option<PendingArtifactUpdate>> {
    let completed = chunked.finish()?;
    let payload = ArtifactPayload::File(&completed.path);
    add_artifact_summary(
        completed.name.clone(),
        &payload,
        &completed.sha256,
        completed.size,
        completed.is_crc_successful,
        artifact_dir,
        artifacts,
        allow_local_fs_destination,
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
    let artifact_dir = artifacts.artifact_dir()?;
    let allow_local_fs_destination = allow_local_fs_destination(ctx)?;

    let mut summaries: Vec<ArtifactSummary> = Vec::new();
    let mut current_chunked: Option<ChunkedArtifact> = None;
    let mut pending_updates = PendingArtifactUpdates::default();

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
                    pending_updates.push(
                        add_single_chunk_artifact(
                            name,
                            chunk,
                            &artifact_dir,
                            &artifacts,
                            allow_local_fs_destination,
                            &mut summaries,
                        )
                        .await?,
                    );
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
                    &artifact_dir,
                )?;
                if chunked.is_complete() {
                    pending_updates.push(
                        finalize_chunked_artifact(
                            chunked,
                            &artifact_dir,
                            &artifacts,
                            allow_local_fs_destination,
                            &mut summaries,
                        )
                        .await?,
                    );
                } else {
                    current_chunked = Some(chunked);
                }
            }
            Payload::Chunk(chunk) => {
                let is_complete = if let Some(ref mut chunked) = current_chunked {
                    chunked.process_chunk(&chunk)?;
                    chunked.is_complete()
                } else {
                    return Err(SparkError::invalid(
                        "received artifact chunk without an active chunked artifact",
                    ));
                };
                if is_complete {
                    if let Some(chunked) = current_chunked.take() {
                        pending_updates.push(
                            finalize_chunked_artifact(
                                chunked,
                                &artifact_dir,
                                &artifacts,
                                allow_local_fs_destination,
                                &mut summaries,
                            )
                            .await?,
                        );
                    }
                }
            }
        }
    }

    // Finalize any remaining chunked artifact
    if let Some(chunked) = current_chunked.take() {
        pending_updates.push(
            finalize_chunked_artifact(
                chunked,
                &artifact_dir,
                &artifacts,
                allow_local_fs_destination,
                &mut summaries,
            )
            .await?,
        );
    }

    pending_updates.commit(&artifacts)?;
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
