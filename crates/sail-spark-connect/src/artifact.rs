use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{SparkError, SparkResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactKind {
    Cache,
    PythonFile,
    File,
    Archive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactAddOutcome {
    Added,
    Unchanged,
    Conflict,
}

#[derive(Debug, Clone)]
pub(crate) struct Artifact {
    name: String,
    kind: ArtifactKind,
    #[expect(dead_code)]
    archive_name: Option<String>,
    data: Arc<[u8]>,
}

impl Artifact {
    pub(crate) fn new(
        name: String,
        kind: ArtifactKind,
        archive_name: Option<String>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            name,
            kind,
            archive_name,
            data: data.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArtifactLimits {
    pub(crate) max_artifact_bytes: usize,
    pub(crate) max_session_bytes: usize,
    pub(crate) max_artifacts: usize,
    pub(crate) max_chunks: usize,
}

#[derive(Debug)]
pub(crate) struct SessionArtifacts {
    limits: ArtifactLimits,
    state: RwLock<SessionArtifactState>,
}

#[derive(Debug, Default)]
struct SessionArtifactState {
    entries: HashMap<String, Artifact>,
    total_bytes: usize,
}

impl SessionArtifacts {
    pub(crate) fn new(limits: ArtifactLimits) -> SparkResult<Self> {
        if limits.max_artifact_bytes == 0
            || limits.max_session_bytes == 0
            || limits.max_artifacts == 0
            || limits.max_chunks == 0
        {
            return Err(SparkError::invalid(
                "Spark artifact limits must be greater than zero",
            ));
        }
        if limits.max_artifact_bytes > limits.max_session_bytes {
            return Err(SparkError::invalid(
                "Spark artifact max_artifact_bytes cannot exceed max_session_bytes",
            ));
        }
        Ok(Self {
            limits,
            state: RwLock::new(SessionArtifactState::default()),
        })
    }

    pub(crate) fn limits(&self) -> ArtifactLimits {
        self.limits
    }

    pub(crate) fn add(&self, artifact: Artifact) -> SparkResult<ArtifactAddOutcome> {
        if artifact.data.len() > self.limits.max_artifact_bytes {
            return Err(SparkError::invalid(format!(
                "artifact {} contains {} bytes, exceeding the {} byte limit",
                artifact.name,
                artifact.data.len(),
                self.limits.max_artifact_bytes
            )));
        }

        let mut state = self.state.write()?;
        if let Some(existing) = state.entries.get(&artifact.name) {
            if artifact.kind != ArtifactKind::Cache {
                if existing.data.as_ref() == artifact.data.as_ref() {
                    return Ok(ArtifactAddOutcome::Unchanged);
                }
                return Ok(ArtifactAddOutcome::Conflict);
            }
        } else if state.entries.len() >= self.limits.max_artifacts {
            return Err(SparkError::invalid(format!(
                "Spark session artifact count exceeds the {} artifact limit",
                self.limits.max_artifacts
            )));
        }

        let replaced_bytes = state
            .entries
            .get(&artifact.name)
            .map(|entry| entry.data.len())
            .unwrap_or_default();
        let total_bytes = state
            .total_bytes
            .checked_sub(replaced_bytes)
            .and_then(|size| size.checked_add(artifact.data.len()))
            .ok_or_else(|| SparkError::invalid("Spark session artifact byte count overflow"))?;
        if total_bytes > self.limits.max_session_bytes {
            return Err(SparkError::invalid(format!(
                "Spark session artifacts contain {total_bytes} bytes, exceeding the {} byte limit",
                self.limits.max_session_bytes
            )));
        }

        state.total_bytes = total_bytes;
        state.entries.insert(artifact.name.clone(), artifact);
        Ok(ArtifactAddOutcome::Added)
    }

    #[cfg(test)]
    pub(crate) fn get(&self, name: &str) -> SparkResult<Option<Artifact>> {
        Ok(self.state.read()?.entries.get(name).cloned())
    }

    pub(crate) fn cache_exists(&self, name: &str) -> SparkResult<bool> {
        if !name.starts_with("cache/") {
            return Ok(false);
        }
        Ok(self.state.read()?.entries.contains_key(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> SparkResult<SessionArtifacts> {
        SessionArtifacts::new(ArtifactLimits {
            max_artifact_bytes: 16,
            max_session_bytes: 24,
            max_artifacts: 2,
            max_chunks: 4,
        })
    }

    #[test]
    fn duplicate_artifacts_are_content_idempotent() -> SparkResult<()> {
        let registry = registry()?;
        assert_eq!(
            registry.add(Artifact::new(
                "files/item".to_string(),
                ArtifactKind::File,
                None,
                b"one".to_vec(),
            ))?,
            ArtifactAddOutcome::Added
        );
        assert_eq!(
            registry.add(Artifact::new(
                "files/item".to_string(),
                ArtifactKind::File,
                None,
                b"one".to_vec(),
            ))?,
            ArtifactAddOutcome::Unchanged
        );

        let outcome = registry.add(Artifact::new(
            "files/item".to_string(),
            ArtifactKind::File,
            None,
            b"two".to_vec(),
        ))?;
        assert_eq!(outcome, ArtifactAddOutcome::Conflict);
        Ok(())
    }

    #[test]
    fn cache_artifacts_can_be_replaced() -> SparkResult<()> {
        let registry = registry()?;
        for data in [b"one".to_vec(), b"two".to_vec()] {
            let outcome = registry.add(Artifact::new(
                "cache/hash".to_string(),
                ArtifactKind::Cache,
                None,
                data,
            ))?;
            assert_eq!(outcome, ArtifactAddOutcome::Added);
        }
        assert_eq!(
            registry.get("cache/hash")?.as_ref().map(Artifact::data),
            Some(b"two".as_slice())
        );
        Ok(())
    }
}
