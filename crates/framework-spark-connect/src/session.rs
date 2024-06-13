use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use framework_plan::config::{PlanConfig, TimestampType};

use crate::config::{ConfigKeyValue, ConfigKeyValueList, SparkRuntimeConfig};
use crate::error::SparkResult;
use crate::executor::Executor;
use crate::spark::config::SPARK_SQL_SESSION_TIME_ZONE;
use crate::utils::SparkDataTypeFormatter;
use framework_plan::new_query_planner;

const DEFAULT_SPARK_SCHEMA: &str = "default";
const DEFAULT_SPARK_CATALOG: &str = "spark_catalog";

pub(crate) struct Session {
    user_id: Option<String>,
    session_id: String,
    context: SessionContext,
    state: Mutex<SparkSessionState>,
}

impl Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("user_id", &self.user_id)
            .field("session_id", &self.session_id)
            .finish()
    }
}

impl Session {
    pub(crate) fn new(user_id: Option<String>, session_id: String) -> Self {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            .with_information_schema(true);
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::new_with_config_rt(config, runtime);
        let state = state.with_query_planner(new_query_planner());
        Self {
            user_id,
            session_id,
            context: SessionContext::new_with_state(state),
            state: Mutex::new(SparkSessionState::new()),
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    #[allow(dead_code)]
    pub(crate) fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    pub(crate) fn context(&self) -> &SessionContext {
        &self.context
    }

    pub(crate) fn plan_config(&self) -> SparkResult<Arc<PlanConfig>> {
        let state = self.state.lock()?;
        Ok(Arc::new(PlanConfig {
            time_zone: state
                .config
                .get(SPARK_SQL_SESSION_TIME_ZONE)?
                .map(|x| x.to_string())
                .unwrap_or_else(|| "UTC".into()),
            // TODO: get the default timestamp type from configuration
            timestamp_type: TimestampType::TimestampLtz,
            data_type_formatter: Arc::new(SparkDataTypeFormatter),
        }))
    }

    pub(crate) fn get_config(&self, keys: Vec<String>) -> SparkResult<ConfigKeyValueList> {
        let state = self.state.lock()?;
        Ok(keys
            .into_iter()
            .map(|key| {
                let value = state.config.get(&key)?.map(|v| v.to_string());
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()?
            .into())
    }

    pub(crate) fn get_config_with_default(
        &self,
        kv: ConfigKeyValueList,
    ) -> SparkResult<ConfigKeyValueList> {
        let state = self.state.lock()?;
        let kv: Vec<ConfigKeyValue> = kv.into();
        Ok(kv
            .into_iter()
            .map(|ConfigKeyValue { key, value }| {
                let value = state.config.get(&key)?.map(|v| v.to_string()).or(value);
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()?
            .into())
    }

    pub(crate) fn set_config(&self, kv: ConfigKeyValueList) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        let kv: Vec<ConfigKeyValue> = kv.into();
        for ConfigKeyValue { key, value } in kv {
            if let Some(value) = value {
                state.config.set(key, value)?;
            } else {
                state.config.unset(&key)?;
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

    pub(crate) fn get_all_config(&self, prefix: Option<&str>) -> SparkResult<ConfigKeyValueList> {
        let state = self.state.lock()?;
        state.config.get_all(prefix)
    }

    pub(crate) fn add_executor(&self, executor: Executor) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        let id = executor.metadata.operation_id.clone();
        state.executors.insert(id, executor);
        Ok(())
    }

    pub(crate) fn remove_executor(&self, id: &str) -> SparkResult<Option<Executor>> {
        let mut state = self.state.lock()?;
        Ok(state.executors.remove(id))
    }

    pub(crate) fn remove_all_executors(&self) -> SparkResult<Vec<Executor>> {
        let mut state = self.state.lock()?;
        let mut out = Vec::new();
        for (_, executor) in state.executors.drain() {
            out.push(executor);
        }
        Ok(out)
    }

    pub(crate) fn remove_executors_by_tag(&self, tag: &str) -> SparkResult<Vec<Executor>> {
        let mut state = self.state.lock()?;
        let tag = tag.to_string();
        let mut ids = Vec::new();
        let mut removed = Vec::new();
        for (key, executor) in &state.executors {
            if executor.metadata.tags.contains(&tag) {
                ids.push(key.clone());
            }
        }
        for key in ids {
            if let Some(executor) = state.executors.remove(&key) {
                removed.push(executor);
            }
        }
        Ok(removed)
    }
}

struct SparkSessionState {
    config: SparkRuntimeConfig,
    executors: HashMap<String, Executor>,
}

impl SparkSessionState {
    fn new() -> Self {
        Self {
            config: SparkRuntimeConfig::new(),
            executors: HashMap::new(),
        }
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub(crate) struct SessionKey {
    pub user_id: Option<String>,
    pub session_id: String,
}

type SessionStore = HashMap<SessionKey, Arc<Session>>;

#[derive(Debug)]
pub(crate) struct SessionManager {
    sessions: Mutex<SessionStore>,
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        Self {
            sessions: Mutex::new(SessionStore::new()),
        }
    }

    pub(crate) fn get_session(&self, key: SessionKey) -> SparkResult<Arc<Session>> {
        let mut sessions = self.sessions.lock()?;
        let session = sessions.entry(key).or_insert_with_key(|k| {
            Arc::new(Session::new(k.user_id.clone(), k.session_id.clone()))
        });
        Ok(session.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn delete_session(&self, key: &SessionKey) -> SparkResult<()> {
        self.sessions.lock()?.remove(key);
        Ok(())
    }
}
