use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use sail_execution::job::JobRunner;
use sail_plan::config::PlanConfig;
use tokio::time::Instant;

use crate::config::{ConfigKeyValue, SparkRuntimeConfig};
use crate::error::{SparkError, SparkResult};
use crate::executor::Executor;

pub(crate) const DEFAULT_SPARK_SCHEMA: &str = "default";
pub(crate) const DEFAULT_SPARK_CATALOG: &str = "spark_catalog";

/// A Spark-specific extension to the DataFusion [SessionContext].
pub(crate) struct SparkExtension {
    user_id: Option<String>,
    session_id: String,
    job_runner: Box<dyn JobRunner>,
    state: Mutex<SparkExtensionState>,
}

impl Debug for SparkExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkExtension")
            .field("user_id", &self.user_id)
            .field("session_id", &self.session_id)
            .finish()
    }
}

impl SparkExtension {
    pub(crate) fn new(
        user_id: Option<String>,
        session_id: String,
        job_runner: Box<dyn JobRunner>,
    ) -> Self {
        Self {
            user_id,
            session_id,
            job_runner,
            state: Mutex::new(SparkExtensionState::new()),
        }
    }

    /// Get the Spark extension from the DataFusion [SessionContext].
    pub(crate) fn get(context: &SessionContext) -> SparkResult<Arc<SparkExtension>> {
        context
            .state_ref()
            .read()
            .config()
            .get_extension::<SparkExtension>()
            .ok_or_else(|| SparkError::invalid("Spark extension not found in the session context"))
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    #[allow(dead_code)]
    pub(crate) fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    pub(crate) fn plan_config(&self) -> SparkResult<Arc<PlanConfig>> {
        let state = self.state.lock()?;
        let mut config = PlanConfig::try_from(&state.config)?;
        if let Some(user_id) = self.user_id() {
            config.session_user_id = user_id.to_string();
        }
        Ok(Arc::new(config))
    }

    pub(crate) fn get_config(&self, keys: Vec<String>) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        keys.into_iter()
            .map(|key| {
                let value = state.config.get(&key)?.map(|v| v.to_string());
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()
    }

    pub(crate) fn get_config_with_default(
        &self,
        kv: Vec<ConfigKeyValue>,
    ) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        kv.into_iter()
            .map(|ConfigKeyValue { key, value }| {
                let value = state
                    .config
                    .get_with_default(&key, value.as_deref())?
                    .map(|x| x.to_string());
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()
    }

    pub(crate) fn set_config(&self, kv: Vec<ConfigKeyValue>) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        for ConfigKeyValue { key, value } in kv {
            if let Some(value) = value {
                state.config.set(key, value)?;
            } else {
                return Err(SparkError::invalid(format!(
                    "value is required for configuration: {key}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn unset_config(&self, keys: Vec<String>) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        for key in keys {
            state.config.unset(&key)?
        }
        Ok(())
    }

    pub(crate) fn get_all_config(&self, prefix: Option<&str>) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        state.config.get_all(prefix)
    }

    pub(crate) fn add_executor(&self, executor: Executor) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        let id = executor.metadata.operation_id.clone();
        state.executors.insert(id, Arc::new(executor));
        Ok(())
    }

    pub(crate) fn get_executor(&self, id: &str) -> SparkResult<Option<Arc<Executor>>> {
        let state = self.state.lock()?;
        Ok(state.executors.get(id).cloned())
    }

    pub(crate) fn remove_executor(&self, id: &str) -> SparkResult<Option<Arc<Executor>>> {
        let mut state = self.state.lock()?;
        Ok(state
            .executors
            .remove_entry(id)
            .map(|(_, executor)| executor))
    }

    pub(crate) fn remove_all_executors(&self) -> SparkResult<Vec<Arc<Executor>>> {
        let mut state = self.state.lock()?;
        let mut out = Vec::new();
        for (_, executor) in state.executors.drain() {
            out.push(executor);
        }
        Ok(out)
    }

    pub(crate) fn remove_executors_by_tag(&self, tag: &str) -> SparkResult<Vec<Arc<Executor>>> {
        let mut state = self.state.lock()?;
        let tag = tag.to_string();
        let mut ids = Vec::new();
        let mut removed = Vec::new();
        for (key, executor) in &state.executors {
            if executor.metadata.tags.contains(&tag) {
                ids.push(key.clone());
            }
        }
        for key in ids.iter() {
            if let Some(executor) = state.executors.remove(key) {
                removed.push(executor);
            }
        }
        Ok(removed)
    }

    pub(crate) fn job_runner(&self) -> &dyn JobRunner {
        self.job_runner.as_ref()
    }

    pub(crate) fn track_activity(&self) -> SparkResult<Instant> {
        let mut state = self.state.lock()?;
        state.active_at = Instant::now();
        Ok(state.active_at)
    }

    pub(crate) fn active_at(&self) -> SparkResult<Instant> {
        let state = self.state.lock()?;
        Ok(state.active_at)
    }
}

struct SparkExtensionState {
    config: SparkRuntimeConfig,
    executors: HashMap<String, Arc<Executor>>,
    /// The time when the Spark session is last seen as active.
    active_at: Instant,
}

impl SparkExtensionState {
    fn new() -> Self {
        Self {
            config: SparkRuntimeConfig::new(),
            executors: HashMap::new(),
            active_at: Instant::now(),
        }
    }
}
