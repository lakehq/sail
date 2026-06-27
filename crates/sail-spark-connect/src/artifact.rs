use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use object_store::{ObjectStoreExt, ObjectStoreScheme};
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_plan::config::PlanConfig;
use sail_python_udf::config::PySparkPythonArtifact;
use url::Url;

use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;

#[derive(Debug, Clone)]
pub(crate) struct SparkArtifactOptions {
    pub root: Option<PathBuf>,
    pub inline_max_bytes: usize,
    pub store_uri: Option<String>,
}

pub(crate) struct SparkArtifactRegistry {
    session_id: String,
    options: SparkArtifactOptions,
    state: Mutex<SparkArtifactState>,
}

impl Drop for SparkArtifactRegistry {
    fn drop(&mut self) {
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(error) => error.into_inner(),
        };
        if let Some(dir) = state.artifact_dir.take() {
            if let Err(error) = std::fs::remove_dir_all(&dir) {
                log::warn!(
                    "Failed to remove Spark session artifact directory {}: {error}",
                    dir.display()
                );
            }
        }
        let artifact_uris = state
            .artifacts
            .iter()
            .filter_map(|artifact| artifact.uri.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        cleanup_artifact_uris(artifact_uris);
    }
}

impl SessionExtension for SparkArtifactRegistry {
    fn name() -> &'static str {
        "spark artifact registry"
    }
}

impl SparkArtifactRegistry {
    pub(crate) fn new(session_id: String, options: SparkArtifactOptions) -> Self {
        Self {
            session_id,
            options,
            state: Mutex::new(SparkArtifactState::new()),
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn options(&self) -> &SparkArtifactOptions {
        &self.options
    }

    pub(crate) fn artifact_dir(&self) -> SparkResult<PathBuf> {
        let mut state = self.state.lock()?;
        if let Some(dir) = &state.artifact_dir {
            return Ok(dir.clone());
        }
        let root = self
            .options
            .root
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("sail-artifacts"));
        let dir = root.join(&self.session_id);
        std::fs::create_dir_all(&dir).map_err(|e| {
            SparkError::internal(format!("failed to create artifact directory: {e}"))
        })?;
        state.artifact_dir = Some(dir.clone());
        Ok(dir)
    }

    pub(crate) fn add_artifact(&self, artifact: PySparkPythonArtifact) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        if let Some(existing) = state
            .artifacts
            .iter_mut()
            .find(|existing| existing.name == artifact.name)
        {
            *existing = artifact;
        } else {
            state.artifacts.push(artifact);
        }
        Ok(())
    }

    pub(crate) fn has_artifact(&self, name: &str) -> SparkResult<bool> {
        let state = self.state.lock()?;
        Ok(state.artifacts.iter().any(|artifact| artifact.name == name))
    }

    pub(crate) fn artifacts(&self) -> SparkResult<Vec<PySparkPythonArtifact>> {
        let state = self.state.lock()?;
        Ok(state.artifacts.clone())
    }
}

struct SparkArtifactState {
    artifacts: Vec<PySparkPythonArtifact>,
    artifact_dir: Option<PathBuf>,
}

impl SparkArtifactState {
    fn new() -> Self {
        Self {
            artifacts: vec![],
            artifact_dir: None,
        }
    }
}

pub(crate) fn resolve_plan_config(ctx: &SessionContext) -> SparkResult<Arc<PlanConfig>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut config = (*spark.plan_config()?).clone();
    let artifacts = ctx.extension::<SparkArtifactRegistry>()?.artifacts()?;
    let mut pyspark_udf_config = (*config.pyspark_udf_config).clone();
    pyspark_udf_config.python_artifact_paths = vec![];
    pyspark_udf_config.python_artifacts = artifacts;
    config.pyspark_udf_config = Arc::new(pyspark_udf_config);
    Ok(Arc::new(config))
}

fn cleanup_artifact_uris(uris: Vec<String>) {
    if uris.is_empty() {
        return;
    }
    let handle = std::thread::Builder::new()
        .name("sail-artifact-cleanup".to_string())
        .spawn(move || -> Result<(), String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create artifact cleanup runtime: {e}"))?;
            runtime.block_on(async move {
                let mut errors = vec![];
                for uri in uris {
                    if let Err(error) = cleanup_artifact_uri(&uri).await {
                        errors.push(error);
                    }
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.join("; "))
                }
            })
        });
    match handle {
        Ok(handle) => match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::warn!("Failed to clean up Spark session artifact objects: {error}")
            }
            Err(_) => log::warn!("Spark session artifact cleanup thread panicked"),
        },
        Err(error) => log::warn!("Failed to spawn Spark session artifact cleanup: {error}"),
    }
}

async fn cleanup_artifact_uri(uri: &str) -> Result<(), String> {
    let url =
        Url::parse(uri).map_err(|e| format!("invalid artifact object-store URI {uri}: {e}"))?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url)
        .map_err(|e| format!("invalid artifact object-store path {uri}: {e}"))?;
    let store = sail_object_store::get_dynamic_object_store(&url)
        .map_err(|e| format!("failed to create artifact object store {uri}: {e}"))?;
    match store.delete(&path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("failed to delete artifact {uri}: {e}")),
    }
}
