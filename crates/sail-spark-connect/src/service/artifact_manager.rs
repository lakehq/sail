use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::add_artifacts_request::{ArtifactChunk, Payload};
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

const PYFILES_PREFIX: &str = "pyfiles/";
const FORWARD_TO_FS_PREFIX: &str = "forward_to_fs";

fn validate_crc(data: &[u8], expected_crc: i64) -> bool {
    let computed = crc32fast::hash(data) as i64;
    computed == expected_crc
}

/// Processes a complete artifact (name + assembled data) and stores it appropriately.
fn store_artifact(name: &str, data: &[u8], artifact_dir: &PathBuf) -> SparkResult<()> {
    use std::path::Path;

    if name.starts_with(FORWARD_TO_FS_PREFIX) {
        // forward_to_fs/absolute/path → write data to the absolute path
        let dest_path = &name[FORWARD_TO_FS_PREFIX.len()..];
        let dest = Path::new(dest_path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                SparkError::internal(format!(
                    "failed to create parent directory for {dest_path}: {e}"
                ))
            })?;
        }
        let mut file = std::fs::File::create(dest)
            .map_err(|e| SparkError::internal(format!("failed to create file {dest_path}: {e}")))?;
        file.write_all(data)
            .map_err(|e| SparkError::internal(format!("failed to write file {dest_path}: {e}")))?;
        return Ok(());
    }

    // Determine target path within artifact_dir
    let target_path = artifact_dir.join(name);
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            SparkError::internal(format!("failed to create artifact subdirectory: {e}"))
        })?;
    }
    let mut file = std::fs::File::create(&target_path)
        .map_err(|e| SparkError::internal(format!("failed to create artifact file: {e}")))?;
    file.write_all(data)
        .map_err(|e| SparkError::internal(format!("failed to write artifact file: {e}")))?;

    // For Python files, add to sys.path
    if name.starts_with(PYFILES_PREFIX) {
        let file_name = &name[PYFILES_PREFIX.len()..];
        if file_name.ends_with(".py") {
            // Add the directory containing the .py file to sys.path
            let dir = target_path.parent().unwrap_or(artifact_dir);
            add_to_sys_path(&dir.to_string_lossy())?;
        } else if file_name.ends_with(".zip")
            || file_name.ends_with(".egg")
            || file_name.ends_with(".jar")
        {
            // Add the zip/egg/jar itself to sys.path
            add_to_sys_path(&target_path.to_string_lossy())?;
        }
    }

    Ok(())
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
        Ok::<(), pyo3::PyErr>(())
    })
    .map_err(|e: pyo3::PyErr| SparkError::internal(format!("failed to add to sys.path: {e}")))
}

struct ChunkedArtifact {
    name: String,
    data: Vec<u8>,
    is_crc_successful: bool,
}

fn process_chunk(artifact: &mut ChunkedArtifact, chunk: &ArtifactChunk) {
    let chunk_ok = validate_crc(&chunk.data, chunk.crc);
    if !chunk_ok {
        artifact.is_crc_successful = false;
    }
    artifact.data.extend_from_slice(&chunk.data);
}

fn finalize_chunked_artifact(
    chunked: ChunkedArtifact,
    artifact_dir: &PathBuf,
    spark: &SparkSession,
    summaries: &mut Vec<ArtifactSummary>,
) -> SparkResult<()> {
    let name = chunked.name.clone();
    let is_crc_successful = chunked.is_crc_successful;
    if is_crc_successful {
        store_artifact(&name, &chunked.data, artifact_dir).unwrap_or_else(|e| {
            log::warn!("Failed to store artifact {name}: {e}");
        });
    }
    spark.add_artifact(name.clone())?;
    summaries.push(ArtifactSummary {
        name,
        is_crc_successful,
    });
    Ok(())
}

pub(crate) async fn handle_add_artifacts(
    ctx: &SessionContext,
    stream: impl Stream<Item = Result<Payload, Status>>,
) -> SparkResult<Vec<ArtifactSummary>> {
    let spark = ctx.extension::<SparkSession>()?;
    let artifact_dir = spark.artifact_dir()?;

    let mut summaries: Vec<ArtifactSummary> = Vec::new();
    let mut current_chunked: Option<ChunkedArtifact> = None;

    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        let payload = item.map_err(|e| SparkError::internal(e.to_string()))?;
        match payload {
            Payload::Batch(batch) => {
                if let Some(chunked) = current_chunked.take() {
                    finalize_chunked_artifact(chunked, &artifact_dir, &spark, &mut summaries)?;
                }
                for artifact in batch.artifacts {
                    let name = artifact.name.clone();
                    if let Some(chunk) = artifact.data {
                        let is_crc_successful = validate_crc(&chunk.data, chunk.crc);
                        if is_crc_successful {
                            store_artifact(&name, &chunk.data, &artifact_dir).unwrap_or_else(|e| {
                                log::warn!("Failed to store artifact {name}: {e}");
                            });
                        }
                        spark.add_artifact(name.clone())?;
                        summaries.push(ArtifactSummary {
                            name,
                            is_crc_successful,
                        });
                    }
                }
            }
            Payload::BeginChunk(begin) => {
                if let Some(chunked) = current_chunked.take() {
                    finalize_chunked_artifact(chunked, &artifact_dir, &spark, &mut summaries)?;
                }
                let name = begin.name.clone();
                let (initial_data, initial_crc_ok) =
                    if let Some(chunk) = begin.initial_chunk.as_ref() {
                        let ok = validate_crc(&chunk.data, chunk.crc);
                        (chunk.data.clone(), ok)
                    } else {
                        (Vec::new(), true)
                    };
                current_chunked = Some(ChunkedArtifact {
                    name,
                    data: initial_data,
                    is_crc_successful: initial_crc_ok,
                });
            }
            Payload::Chunk(chunk) => {
                if let Some(ref mut chunked) = current_chunked {
                    process_chunk(chunked, &chunk);
                }
            }
        }
    }

    // Finalize any remaining chunked artifact
    if let Some(chunked) = current_chunked.take() {
        finalize_chunked_artifact(chunked, &artifact_dir, &spark, &mut summaries)?;
    }

    Ok(summaries)
}

pub(crate) async fn handle_artifact_statuses(
    ctx: &SessionContext,
    names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut statuses = HashMap::new();
    for name in names {
        let exists = spark.has_artifact(&name)?;
        statuses.insert(name, ArtifactStatus { exists });
    }
    Ok(statuses)
}
