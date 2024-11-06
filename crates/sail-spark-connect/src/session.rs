use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use sail_common::config::ConfigKeyValue;
use sail_execution::job::JobRunner;
use sail_plan::config::{PlanConfig, TimestampType};
use sail_plan::formatter::DefaultPlanFormatter;
use sail_python_udf::config::SparkUdfConfig;

use crate::config::SparkRuntimeConfig;
use crate::error::{SparkError, SparkResult};
use crate::executor::Executor;
use crate::spark::config::{
    SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH,
    SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY, SPARK_SQL_GLOBAL_TEMP_DATABASE,
    SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME,
    SPARK_SQL_SESSION_TIME_ZONE, SPARK_SQL_SOURCES_DEFAULT, SPARK_SQL_WAREHOUSE_DIR,
};

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
        let time_zone = state
            .config
            .get(SPARK_SQL_SESSION_TIME_ZONE)?
            .map(|x| x.to_string())
            .unwrap_or_else(|| "UTC".into());
        let spark_udf_config = SparkUdfConfig {
            timezone: ConfigKeyValue {
                key: "spark.sql.session.timeZone".to_string(),
                value: Some(time_zone.clone()),
            },
            // FIXME: pandas_window_bound_types is not a proper Spark configuration.
            pandas_window_bound_types: ConfigKeyValue {
                key: "pandas_window_bound_types".to_string(),
                value: None,
            },
            pandas_grouped_map_assign_columns_by_name: ConfigKeyValue {
                key: SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME
                    .to_string(),
                value: state
                    .config
                    .get(SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME)?
                    .map(|s| s.to_string()),
            },
            pandas_convert_to_arrow_array_safely: ConfigKeyValue {
                key: SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY.to_string(),
                value: state
                    .config
                    .get(SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY)?
                    .map(|s| s.to_string()),
            },
            arrow_max_records_per_batch: ConfigKeyValue {
                key: SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH.to_string(),
                value: state
                    .config
                    .get(SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH)?
                    .map(|s| s.to_string()),
            },
        };
        let default_bounded_table_file_format = state
            .config
            .get(SPARK_SQL_SOURCES_DEFAULT)?
            .map(|x| x.to_string())
            .unwrap_or_else(|| PlanConfig::default().default_bounded_table_file_format);
        let default_warehouse_directory = state
            .config
            .get(SPARK_SQL_WAREHOUSE_DIR)?
            .map(|x| x.to_string())
            .unwrap_or_else(|| PlanConfig::default().default_warehouse_directory);
        let global_temp_database = state
            .config
            .get(SPARK_SQL_GLOBAL_TEMP_DATABASE)?
            .map(|x| x.to_string())
            .unwrap_or_else(|| PlanConfig::default().global_temp_database);
        let plan_config_default = PlanConfig::default();
        Ok(Arc::new(PlanConfig {
            time_zone,
            // TODO: get the default timestamp type from configuration
            timestamp_type: TimestampType::TimestampLtz,
            plan_formatter: Arc::new(DefaultPlanFormatter),
            spark_udf_config,
            default_bounded_table_file_format,
            default_warehouse_directory,
            global_temp_database,
            session_user_id: self
                .user_id()
                .unwrap_or(&plan_config_default.session_user_id)
                .to_string(),
            ..plan_config_default
        }))
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
}

struct SparkExtensionState {
    config: SparkRuntimeConfig,
    executors: HashMap<String, Arc<Executor>>,
}

impl SparkExtensionState {
    fn new() -> Self {
        Self {
            config: SparkRuntimeConfig::new(),
            executors: HashMap::new(),
        }
    }
}
