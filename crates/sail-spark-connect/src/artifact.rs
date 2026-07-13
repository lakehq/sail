use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use prost::Message;
use sail_common::spec;
use sail_common_datafusion::session::artifact::{
    CachedLocalRelationData, CachedLocalRelationLoader,
};
use sha2::{Digest, Sha256};

use crate::error::{SparkError, SparkResult};
use crate::proto::data_type::{DEFAULT_FIELD_NAME, parse_spark_data_type};
use crate::session::SparkSession;
use crate::spark::connect as sc;

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
    digest: [u8; 32],
    data: Arc<[u8]>,
}

impl Artifact {
    pub(crate) fn new(
        name: String,
        kind: ArtifactKind,
        archive_name: Option<String>,
        data: Vec<u8>,
    ) -> Self {
        let digest = Sha256::digest(&data).into();
        Self {
            name,
            kind,
            archive_name,
            digest,
            data: data.into(),
        }
    }

    pub(crate) fn data(&self) -> Arc<[u8]> {
        self.data.clone()
    }

    pub(crate) fn digest(&self) -> &[u8; 32] {
        &self.digest
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
        if artifact.kind == ArtifactKind::Cache {
            let expected_name = format!("cache/{}", sha256_hex(artifact.digest()));
            if artifact.name != expected_name {
                return Err(SparkError::invalid(format!(
                    "cache artifact name {} does not match its SHA-256 digest {expected_name}",
                    artifact.name
                )));
            }
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

fn sha256_hex(digest: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) struct SparkCachedLocalRelationLoader {
    spark: Arc<SparkSession>,
}

impl SparkCachedLocalRelationLoader {
    pub(crate) fn new(spark: Arc<SparkSession>) -> Self {
        Self { spark }
    }

    fn cache_data(&self, hash: &str) -> DataFusionResult<Arc<[u8]>> {
        if hash.is_empty() {
            return Err(DataFusionError::Plan(
                "cached local relation hash cannot be empty".to_string(),
            ));
        }
        let name = format!("cache/{hash}");
        let artifact = self
            .spark
            .artifact(&name)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?
            .ok_or_else(|| {
                DataFusionError::Plan(format!("cached local relation artifact not found: {hash}"))
            })?;
        if artifact.kind != ArtifactKind::Cache {
            return Err(DataFusionError::Plan(format!(
                "artifact is not cached local relation data: {hash}"
            )));
        }
        Ok(artifact.data())
    }

    fn parse_schema(schema: &str) -> DataFusionResult<spec::Schema> {
        parse_spark_data_type(schema)
            .map(|data_type| data_type.into_schema(DEFAULT_FIELD_NAME, true))
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }

    fn checked_relation_size(
        &self,
        sizes: impl IntoIterator<Item = usize>,
    ) -> DataFusionResult<()> {
        let limit = self.spark.artifacts().limits().max_session_bytes;
        let mut total = 0usize;
        for size in sizes {
            total = total.checked_add(size).ok_or_else(|| {
                DataFusionError::Plan("cached local relation size overflow".to_string())
            })?;
            if total > limit {
                return Err(DataFusionError::Plan(format!(
                    "cached local relation contains {total} bytes, exceeding the {limit} byte limit"
                )));
            }
        }
        Ok(())
    }
}

impl CachedLocalRelationLoader for SparkCachedLocalRelationLoader {
    fn load_legacy(&self, hash: &str) -> DataFusionResult<CachedLocalRelationData> {
        let data = self.cache_data(hash)?;
        let relation = sc::LocalRelation::decode(data.as_ref()).map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to decode cached local relation {hash}: {error}"
            ))
        })?;
        let schema = relation
            .schema
            .filter(|schema| !schema.is_empty())
            .map(|schema| Self::parse_schema(&schema))
            .transpose()?;
        Ok(CachedLocalRelationData {
            data: relation.data.map(Arc::<[u8]>::from).into_iter().collect(),
            schema,
        })
    }

    fn load_chunked(
        &self,
        data_hashes: &[String],
        schema_hash: Option<&str>,
    ) -> DataFusionResult<CachedLocalRelationData> {
        if data_hashes.is_empty() {
            return Err(DataFusionError::Plan(
                "chunked cached local relation requires at least one data hash".to_string(),
            ));
        }
        let chunk_limit = self.spark.artifacts().limits().max_chunks;
        if data_hashes.len() > chunk_limit {
            return Err(DataFusionError::Plan(format!(
                "chunked cached local relation contains {} chunks, exceeding the {chunk_limit} chunk limit",
                data_hashes.len()
            )));
        }

        let data = data_hashes
            .iter()
            .map(|hash| self.cache_data(hash))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let schema_data = schema_hash.map(|hash| self.cache_data(hash)).transpose()?;
        self.checked_relation_size(
            data.iter()
                .map(|data| data.len())
                .chain(schema_data.iter().map(|data| data.len())),
        )?;
        let schema = schema_data
            .as_deref()
            .map(|data| {
                let schema = std::str::from_utf8(data).map_err(|error| {
                    DataFusionError::Plan(format!(
                        "cached local relation schema is not UTF-8: {error}"
                    ))
                })?;
                Self::parse_schema(schema)
            })
            .transpose()?;
        Ok(CachedLocalRelationData { data, schema })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::session::SparkSessionOptions;

    fn registry() -> SparkResult<SessionArtifacts> {
        SessionArtifacts::new(ArtifactLimits {
            max_artifact_bytes: 16,
            max_session_bytes: 24,
            max_artifacts: 2,
            max_chunks: 4,
        })
    }

    fn cache_name(data: &[u8]) -> String {
        let digest: [u8; 32] = Sha256::digest(data).into();
        format!("cache/{}", sha256_hex(&digest))
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
    fn cache_artifacts_require_content_hash() -> SparkResult<()> {
        let registry = registry()?;
        let data = b"one".to_vec();
        let name = cache_name(&data);
        let outcome = registry.add(Artifact::new(
            name.clone(),
            ArtifactKind::Cache,
            None,
            data.clone(),
        ))?;
        assert_eq!(outcome, ArtifactAddOutcome::Added);
        assert_eq!(
            registry.get(&name)?.as_ref().map(Artifact::data),
            Some(Arc::<[u8]>::from(data))
        );
        let result = registry.add(Artifact::new(
            name,
            ArtifactKind::Cache,
            None,
            b"two".to_vec(),
        ));
        assert!(matches!(result, Err(SparkError::InvalidArgument(_))));
        Ok(())
    }

    fn spark() -> SparkResult<Arc<SparkSession>> {
        Ok(Arc::new(SparkSession::try_new(
            "session".to_string(),
            "user".to_string(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(1),
                artifact_limits: ArtifactLimits {
                    max_artifact_bytes: 1024,
                    max_session_bytes: 4096,
                    max_artifacts: 16,
                    max_chunks: 8,
                },
            },
        )?))
    }

    fn add_cache(spark: &SparkSession, data: Vec<u8>) -> SparkResult<String> {
        let name = cache_name(&data);
        let outcome =
            spark
                .artifacts()
                .add(Artifact::new(name.clone(), ArtifactKind::Cache, None, data))?;
        assert_eq!(outcome, ArtifactAddOutcome::Added);
        Ok(name.strip_prefix("cache/").unwrap_or_default().to_string())
    }

    #[test]
    fn legacy_cached_relation_decodes_local_relation_message() -> SparkResult<()> {
        let spark = spark()?;
        let relation = sc::LocalRelation {
            data: Some(b"ipc".to_vec()),
            schema: Some("value INT".to_string()),
        };
        let hash = add_cache(&spark, relation.encode_to_vec())?;

        let relation = SparkCachedLocalRelationLoader::new(spark).load_legacy(&hash)?;
        assert_eq!(relation.data, vec![Arc::<[u8]>::from(b"ipc".as_slice())]);
        assert_eq!(
            relation
                .schema
                .as_ref()
                .and_then(|schema| schema.fields.first())
                .map(|field| field.name.as_str()),
            Some("value")
        );
        Ok(())
    }

    #[test]
    fn chunked_cached_relation_preserves_hash_order() -> SparkResult<()> {
        let spark = spark()?;
        let first_hash = add_cache(&spark, b"first".to_vec())?;
        let second_hash = add_cache(&spark, b"second".to_vec())?;
        let schema_hash = add_cache(&spark, b"value INT".to_vec())?;

        let relation = SparkCachedLocalRelationLoader::new(spark)
            .load_chunked(&[second_hash, first_hash], Some(&schema_hash))?;
        assert_eq!(
            relation.data,
            vec![
                Arc::<[u8]>::from(b"second".as_slice()),
                Arc::<[u8]>::from(b"first".as_slice())
            ]
        );
        assert!(relation.schema.is_some());
        Ok(())
    }

    #[test]
    fn chunked_cached_relation_requires_data() -> SparkResult<()> {
        let loader = SparkCachedLocalRelationLoader::new(spark()?);
        let result = loader.load_chunked(&[], None);
        assert!(matches!(result, Err(DataFusionError::Plan(_))));
        Ok(())
    }
}
