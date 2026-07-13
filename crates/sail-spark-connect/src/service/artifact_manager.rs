use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt, pin_mut};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::artifact::{Artifact, ArtifactAddOutcome, ArtifactKind, ArtifactLimits};
use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::add_artifacts_request::{ArtifactChunk, Payload, SingleChunkArtifact};
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

#[derive(Debug)]
struct ParsedArtifactName {
    original: String,
    normalized: String,
    kind: ArtifactKind,
    archive_name: Option<String>,
}

#[derive(Debug)]
struct StagedArtifact {
    name: ParsedArtifactName,
    data: Vec<u8>,
    crc_successful: bool,
}

#[derive(Debug)]
struct ActiveChunkedArtifact {
    staged_index: usize,
    expected_chunks: usize,
    received_chunks: usize,
    expected_bytes: usize,
}

#[derive(Debug)]
struct ArtifactUpload {
    limits: ArtifactLimits,
    staged: Vec<StagedArtifact>,
    staged_bytes: usize,
    active: Option<ActiveChunkedArtifact>,
}

impl ArtifactUpload {
    fn new(limits: ArtifactLimits) -> Self {
        Self {
            limits,
            staged: Vec::new(),
            staged_bytes: 0,
            active: None,
        }
    }

    fn stage_batch_artifact(&mut self, artifact: SingleChunkArtifact) -> SparkResult<()> {
        let name = parse_artifact_name(&artifact.name)?;
        let chunk = artifact.data.required("artifact data")?;
        self.reserve_artifact(&name.original, chunk.data.len())?;
        let crc_successful = chunk_crc_matches(&chunk);
        self.staged.push(StagedArtifact {
            name,
            data: chunk.data,
            crc_successful,
        });
        Ok(())
    }

    fn begin_chunked_artifact(
        &mut self,
        name: String,
        total_bytes: i64,
        num_chunks: i64,
        initial_chunk: Option<ArtifactChunk>,
    ) -> SparkResult<()> {
        if self.active.is_some() {
            return Err(SparkError::invalid(
                "cannot begin an artifact while another chunked artifact is active",
            ));
        }
        let name = parse_artifact_name(&name)?;
        let expected_bytes = usize::try_from(total_bytes)
            .map_err(|_| SparkError::invalid("artifact total_bytes must be non-negative"))?;
        let expected_chunks = usize::try_from(num_chunks)
            .map_err(|_| SparkError::invalid("artifact num_chunks must be positive"))?;
        if expected_chunks == 0 {
            return Err(SparkError::invalid("artifact num_chunks must be positive"));
        }
        if expected_chunks > self.limits.max_chunks {
            return Err(SparkError::invalid(format!(
                "artifact {} declares {expected_chunks} chunks, exceeding the {} chunk limit",
                name.original, self.limits.max_chunks
            )));
        }
        if expected_bytes > self.limits.max_artifact_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} declares {expected_bytes} bytes, exceeding the {} byte limit",
                name.original, self.limits.max_artifact_bytes
            )));
        }

        let initial_chunk = initial_chunk.required("initial artifact chunk")?;
        self.reserve_artifact(&name.original, initial_chunk.data.len())?;
        if initial_chunk.data.len() > expected_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} received more than its declared {expected_bytes} bytes",
                name.original
            )));
        }
        let crc_successful = chunk_crc_matches(&initial_chunk);
        self.staged.push(StagedArtifact {
            name,
            data: initial_chunk.data,
            crc_successful,
        });
        let staged_index = self.staged.len() - 1;
        self.active = Some(ActiveChunkedArtifact {
            staged_index,
            expected_chunks,
            received_chunks: 1,
            expected_bytes,
        });
        self.finish_active_if_complete()
    }

    fn append_chunk(&mut self, chunk: ArtifactChunk) -> SparkResult<()> {
        let active = self.active.as_ref().ok_or_else(|| {
            SparkError::invalid("received an artifact chunk without an active artifact")
        })?;
        if active.received_chunks >= active.expected_chunks {
            let artifact = &self.staged[active.staged_index];
            return Err(SparkError::invalid(format!(
                "Excessive data chunks for artifact: {}, expected {} chunks in total. Processed {} bytes out of {} bytes.",
                artifact.name.original,
                active.expected_chunks,
                artifact.data.len(),
                active.expected_bytes
            )));
        }

        let staged_index = active.staged_index;
        let expected_bytes = active.expected_bytes;
        let artifact_name = self.staged[staged_index].name.original.clone();
        self.reserve_chunk(&artifact_name, chunk.data.len())?;
        let artifact = &mut self.staged[staged_index];
        artifact.crc_successful &= chunk_crc_matches(&chunk);
        artifact.data.extend_from_slice(&chunk.data);
        if artifact.data.len() > expected_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} received more than its declared {expected_bytes} bytes",
                artifact.name.original
            )));
        }
        if let Some(active) = self.active.as_mut() {
            active.received_chunks += 1;
        }
        self.finish_active_if_complete()
    }

    fn finish_active_if_complete(&mut self) -> SparkResult<()> {
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };
        if active.received_chunks != active.expected_chunks {
            return Ok(());
        }
        let artifact = &self.staged[active.staged_index];
        if artifact.data.len() != active.expected_bytes {
            return Err(SparkError::invalid(format!(
                "Missing data chunks for artifact: {}. Expected {} chunks and received {} chunks. Processed {} bytes out of {} bytes.",
                artifact.name.original,
                active.expected_chunks,
                active.received_chunks,
                artifact.data.len(),
                active.expected_bytes
            )));
        }
        self.active = None;
        Ok(())
    }

    fn finish(self) -> SparkResult<Vec<StagedArtifact>> {
        if let Some(active) = &self.active {
            let artifact = &self.staged[active.staged_index];
            return Err(SparkError::invalid(format!(
                "Missing data chunks for artifact: {}. Expected {} chunks and received {} chunks. Processed {} bytes out of {} bytes.",
                artifact.name.original,
                active.expected_chunks,
                active.received_chunks,
                artifact.data.len(),
                active.expected_bytes
            )));
        }
        Ok(self.staged)
    }

    fn reserve_artifact(&mut self, name: &str, bytes: usize) -> SparkResult<()> {
        if self.staged.len() >= self.limits.max_artifacts {
            return Err(SparkError::invalid(format!(
                "artifact upload exceeds the {} artifact limit",
                self.limits.max_artifacts
            )));
        }
        if bytes > self.limits.max_artifact_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {name} contains {bytes} bytes, exceeding the {} byte limit",
                self.limits.max_artifact_bytes
            )));
        }
        self.reserve_staged_bytes(bytes)
    }

    fn reserve_chunk(&mut self, name: &str, bytes: usize) -> SparkResult<()> {
        let active = self.active.as_ref().ok_or_else(|| {
            SparkError::invalid("received an artifact chunk without an active artifact")
        })?;
        let artifact_bytes = self.staged[active.staged_index]
            .data
            .len()
            .checked_add(bytes)
            .ok_or_else(|| SparkError::invalid("artifact byte count overflow"))?;
        if artifact_bytes > self.limits.max_artifact_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {name} contains {artifact_bytes} bytes, exceeding the {} byte limit",
                self.limits.max_artifact_bytes
            )));
        }
        self.reserve_staged_bytes(bytes)
    }

    fn reserve_staged_bytes(&mut self, bytes: usize) -> SparkResult<()> {
        let staged_bytes = self
            .staged_bytes
            .checked_add(bytes)
            .ok_or_else(|| SparkError::invalid("artifact upload byte count overflow"))?;
        if staged_bytes > self.limits.max_session_bytes {
            return Err(SparkError::invalid(format!(
                "artifact upload contains {staged_bytes} bytes, exceeding the {} byte limit",
                self.limits.max_session_bytes
            )));
        }
        self.staged_bytes = staged_bytes;
        Ok(())
    }
}

pub(crate) async fn handle_add_artifacts(
    ctx: &SessionContext,
    stream: impl Stream<Item = SparkResult<Payload>>,
) -> SparkResult<Vec<ArtifactSummary>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut upload = ArtifactUpload::new(spark.artifacts().limits());
    pin_mut!(stream);
    while let Some(payload) = stream.next().await {
        match payload? {
            Payload::Batch(batch) => {
                for artifact in batch.artifacts {
                    upload.stage_batch_artifact(artifact)?;
                }
            }
            Payload::BeginChunk(begin) => upload.begin_chunked_artifact(
                begin.name,
                begin.total_bytes,
                begin.num_chunks,
                begin.initial_chunk,
            )?,
            Payload::Chunk(chunk) => upload.append_chunk(chunk)?,
        }
    }

    let staged = upload.finish()?;
    let summaries = staged
        .iter()
        .map(|artifact| ArtifactSummary {
            name: artifact.name.original.clone(),
            is_crc_successful: artifact.crc_successful,
        })
        .collect();

    let mut first_conflict = None;
    for artifact in staged {
        if !artifact.crc_successful {
            continue;
        }
        let summary_name = artifact.name.original.clone();
        let artifact = Artifact::new(
            artifact.name.normalized,
            artifact.name.kind,
            artifact.name.archive_name,
            artifact.data,
        );
        match spark.artifacts().add(artifact)? {
            ArtifactAddOutcome::Conflict => {
                if first_conflict.is_none() {
                    first_conflict = Some(summary_name);
                }
            }
            ArtifactAddOutcome::Added | ArtifactAddOutcome::Unchanged => {}
        }
    }
    if let Some(name) = first_conflict {
        return Err(SparkError::invalid(format!(
            "artifact {name} already exists with different content"
        )));
    }
    Ok(summaries)
}

pub(crate) async fn handle_artifact_statuses(
    ctx: &SessionContext,
    names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    let spark = ctx.extension::<SparkSession>()?;
    names
        .into_iter()
        .map(|name| {
            let exists = spark.artifacts().cache_exists(&name)?;
            Ok((name, ArtifactStatus { exists }))
        })
        .collect()
}

fn parse_artifact_name(name: &str) -> SparkResult<ParsedArtifactName> {
    if name.is_empty() || name.starts_with('/') || name.contains('\\') {
        return Err(invalid_artifact_path(name));
    }
    let mut fragment_parts = name.split('#');
    let path = fragment_parts.next().unwrap_or_default();
    let fragment = fragment_parts.next();
    if fragment_parts.next().is_some() {
        return Err(invalid_artifact_path(name));
    }
    let components: Vec<_> = path.split('/').collect();
    if components.len() < 2
        || components
            .iter()
            .any(|component| component.is_empty() || *component == "." || *component == "..")
    {
        return Err(invalid_artifact_path(name));
    }

    let kind = match components[0] {
        "cache" => {
            if components.len() != 2 || fragment.is_some() {
                return Err(invalid_artifact_path(name));
            }
            ArtifactKind::Cache
        }
        "pyfiles" => {
            if fragment.is_some() {
                return Err(invalid_artifact_path(name));
            }
            let file_name = components.last().copied().unwrap_or_default();
            if ![".py", ".zip", ".egg", ".jar"]
                .iter()
                .any(|suffix| file_name.ends_with(suffix))
            {
                return Err(SparkError::unsupported(format!(
                    "unsupported Python artifact: {file_name}"
                )));
            }
            ArtifactKind::PythonFile
        }
        "files" => {
            if fragment.is_some() {
                return Err(invalid_artifact_path(name));
            }
            ArtifactKind::File
        }
        "archives" => {
            let file_name = components.last().copied().unwrap_or_default();
            // TODO: Add safe tar, tar.gz, tgz, and tar.bz2 extraction after the ZIP path is
            // covered by expanded-size, entry-count, and link tests.
            if !file_name.ends_with(".zip") {
                return Err(SparkError::unsupported(format!(
                    "unsupported archive artifact: {file_name}"
                )));
            }
            if let Some(fragment) = fragment
                && (fragment.is_empty()
                    || fragment.contains('/')
                    || fragment.contains('\\')
                    || fragment == "."
                    || fragment == "..")
            {
                return Err(invalid_artifact_path(name));
            }
            ArtifactKind::Archive
        }
        // TODO: Add jars/classes when Sail has a JVM classloader boundary, and add
        // forward_to_fs only with an explicit filesystem authority policy.
        "jars" | "classes" | "forward_to_fs" => {
            return Err(SparkError::unsupported(format!(
                "Spark artifact namespace is not supported: {}",
                components[0]
            )));
        }
        namespace => {
            return Err(SparkError::unsupported(format!(
                "unknown Spark artifact namespace: {namespace}"
            )));
        }
    };

    let archive_name = if kind == ArtifactKind::Archive {
        fragment
            .map(str::to_string)
            .or_else(|| components.last().map(|component| (*component).to_string()))
    } else {
        None
    };
    Ok(ParsedArtifactName {
        original: name.to_string(),
        normalized: path.to_string(),
        kind,
        archive_name,
    })
}

fn invalid_artifact_path(name: &str) -> SparkError {
    SparkError::invalid(format!(
        "invalid artifact path {name:?}: expected a safe relative path below a supported namespace"
    ))
}

fn chunk_crc_matches(chunk: &ArtifactChunk) -> bool {
    i64::from(crc32fast::hash(&chunk.data)) == chunk.crc
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::stream;

    use super::*;
    use crate::session::SparkSessionOptions;
    use crate::spark::connect::add_artifacts_request::{Batch, BeginChunkedArtifact};

    fn context() -> SparkResult<SessionContext> {
        let spark = SparkSession::try_new(
            "client-session".to_string(),
            "user".to_string(),
            SparkSessionOptions {
                execution_heartbeat_interval: std::time::Duration::from_secs(1),
                artifact_limits: ArtifactLimits {
                    max_artifact_bytes: 32,
                    max_session_bytes: 64,
                    max_artifacts: 8,
                    max_chunks: 4,
                },
            },
        )?;
        Ok(SessionContext::new_with_config(
            SessionConfig::new().with_extension(Arc::new(spark)),
        ))
    }

    fn chunk(data: &[u8]) -> ArtifactChunk {
        ArtifactChunk {
            data: data.to_vec(),
            crc: i64::from(crc32fast::hash(data)),
        }
    }

    fn single(name: &str, data: &[u8]) -> Payload {
        Payload::Batch(Batch {
            artifacts: vec![SingleChunkArtifact {
                name: name.to_string(),
                data: Some(chunk(data)),
            }],
        })
    }

    #[tokio::test]
    async fn bad_crc_is_reported_and_discarded() -> SparkResult<()> {
        let ctx = context()?;
        let payload = Payload::Batch(Batch {
            artifacts: vec![SingleChunkArtifact {
                name: "cache/hash".to_string(),
                data: Some(ArtifactChunk {
                    data: b"data".to_vec(),
                    crc: 0,
                }),
            }],
        });
        let summaries = handle_add_artifacts(&ctx, stream::iter([Ok(payload)])).await?;
        assert!(!summaries[0].is_crc_successful);
        let statuses = handle_artifact_statuses(&ctx, vec!["cache/hash".to_string()]).await?;
        assert_eq!(
            statuses.get("cache/hash").map(|status| status.exists),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_chunk_begin_is_complete() -> SparkResult<()> {
        let ctx = context()?;
        let payload = Payload::BeginChunk(BeginChunkedArtifact {
            name: "cache/hash".to_string(),
            total_bytes: 4,
            num_chunks: 1,
            initial_chunk: Some(chunk(b"data")),
        });
        handle_add_artifacts(&ctx, stream::iter([Ok(payload)])).await?;
        let spark = ctx.extension::<SparkSession>()?;
        assert!(spark.artifacts().cache_exists("cache/hash")?);
        Ok(())
    }

    #[tokio::test]
    async fn batch_can_interleave_with_chunked_artifact() -> SparkResult<()> {
        let ctx = context()?;
        let payloads = [
            Payload::BeginChunk(BeginChunkedArtifact {
                name: "cache/hash".to_string(),
                total_bytes: 4,
                num_chunks: 2,
                initial_chunk: Some(chunk(b"da")),
            }),
            single("files/item", b"file"),
            Payload::Chunk(chunk(b"ta")),
        ];
        handle_add_artifacts(&ctx, stream::iter(payloads.map(Ok))).await?;
        let spark = ctx.extension::<SparkSession>()?;
        assert!(spark.artifact("cache/hash")?.is_some());
        assert!(spark.artifact("files/item")?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_chunked_artifact_does_not_commit() -> SparkResult<()> {
        let ctx = context()?;
        let payload = Payload::BeginChunk(BeginChunkedArtifact {
            name: "cache/hash".to_string(),
            total_bytes: 4,
            num_chunks: 2,
            initial_chunk: Some(chunk(b"da")),
        });
        let result = handle_add_artifacts(&ctx, stream::iter([Ok(payload)])).await;
        assert!(matches!(result, Err(SparkError::InvalidArgument(_))));
        let spark = ctx.extension::<SparkSession>()?;
        assert!(!spark.artifacts().cache_exists("cache/hash")?);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_conflict_does_not_skip_later_artifacts() -> SparkResult<()> {
        let ctx = context()?;
        handle_add_artifacts(&ctx, stream::iter([Ok(single("files/item", b"old"))])).await?;

        let payloads = [
            single("files/item", b"new"),
            single("files/later", b"later"),
        ];
        let result = handle_add_artifacts(&ctx, stream::iter(payloads.map(Ok))).await;
        assert!(matches!(result, Err(SparkError::InvalidArgument(_))));
        let spark = ctx.extension::<SparkSession>()?;
        assert!(spark.artifact("files/later")?.is_some());
        Ok(())
    }
}
