use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use arrow::array::RecordBatch;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::{ConfigKeyValue, SparkUdfConfig};
use sail_common::spec;
use sail_common::utils::rename_physical_plan;
use sail_plan::config::{PlanConfig, TimestampType};
use sail_plan::formatter::DefaultPlanFormatter;
use sail_plan::function::BUILT_IN_SCALAR_FUNCTIONS;
use sail_plan::object_store::{DynamicObjectStoreRegistry, ObjectStoreConfig};
use sail_plan::resolver::plan::NamedPlan;
use sail_plan::resolver::PlanResolver;
use sail_plan::temp_view::TemporaryViewManager;
use sail_plan::{execute_logical_plan, new_query_planner};

use crate::config::{ConfigKeyValueList, SparkRuntimeConfig};
use crate::error::{SparkError, SparkResult};
#[cfg(test)]
use crate::executor::read_stream;
use crate::executor::Executor;
use crate::spark::config::{
    SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH,
    SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY, SPARK_SQL_GLOBAL_TEMP_DATABASE,
    SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME,
    SPARK_SQL_SESSION_TIME_ZONE, SPARK_SQL_SOURCES_DEFAULT, SPARK_SQL_WAREHOUSE_DIR,
};

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
    pub(crate) fn try_new(
        user_id: Option<String>,
        session_id: String,
        object_store_config: Arc<ObjectStoreConfig>,
    ) -> SparkResult<Self> {
        // TODO: support more systematic configuration
        // TODO: return error on invalid environment variables
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            .with_information_schema(true)
            .with_extension(Arc::new(TemporaryViewManager::default()))
            .set_usize(
                "datafusion.execution.batch_size",
                std::env::var("DATAFUSION_BATCH_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(8192),
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_parallel_row_group_writers",
                std::env::var("DATAFUSION_PARQUET_MAX_PARALLEL_ROW_GROUP_WRITERS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(2),
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_buffered_record_batches_per_stream",
                std::env::var("DATAFUSION_PARQUET_MAX_BUFFERED_RECORD_BATCHES_PER_STREAM")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(16),
            )
            // Spark defaults to false:
            //  https://spark.apache.org/docs/latest/sql-data-sources-csv.html
            .set_bool("datafusion.catalog.has_header", false);
        let runtime = {
            let registry = DynamicObjectStoreRegistry::new().with_config(object_store_config);
            let config = RuntimeConfig::default().with_object_store_registry(Arc::new(registry));
            Arc::new(RuntimeEnv::new(config)?)
        };
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);

        // TODO: This is a temp workaround to deregister all built-in functions that we define.
        //  We should deregister all context.udfs() once we have better coverage of functions.
        for (&name, _function) in BUILT_IN_SCALAR_FUNCTIONS.iter() {
            context.deregister_udf(name);
        }

        Ok(Self {
            user_id,
            session_id,
            context,
            state: Mutex::new(SparkSessionState::new()),
        })
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
                let value = state
                    .config
                    .get_with_default(&key, value.as_deref())?
                    .map(|x| x.to_string());
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()?
            .into())
    }

    pub(crate) fn set_config(&self, kv: ConfigKeyValueList) -> SparkResult<()> {
        use chrono::{Offset, Utc};
        use chrono_tz::Tz;
        let mut state = self.state.lock()?;
        let kv: Vec<ConfigKeyValue> = kv.into();
        for ConfigKeyValue { key, value } in kv {
            if let Some(value) = value {
                if key == SPARK_SQL_SESSION_TIME_ZONE {
                    let state = self.context.state_ref();
                    let mut state = state.write();
                    let offset_string = if value.starts_with("+") || value.starts_with("-") {
                        value.clone()
                    } else if value.to_lowercase() == "z" {
                        "+00:00".to_string()
                    } else {
                        let tz: Tz = value
                            .parse()
                            .map_err(|e| SparkError::invalid(format!("invalid time zone: {e}")))?;
                        let local_time_offset = Utc::now()
                            .with_timezone(&tz)
                            .offset()
                            .fix()
                            .local_minus_utc();
                        format!(
                            "{:+03}:{:02}",
                            local_time_offset / 3600,
                            (local_time_offset.abs() % 3600) / 60
                        )
                    };
                    state.config_mut().options_mut().execution.time_zone = Some(offset_string);
                }
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

    pub(crate) fn get_all_config(&self, prefix: Option<&str>) -> SparkResult<ConfigKeyValueList> {
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

    pub(crate) async fn execute_plan(
        &self,
        plan: spec::Plan,
    ) -> SparkResult<SendableRecordBatchStream> {
        let ctx = self.context();
        let resolver = PlanResolver::new(ctx, self.plan_config()?);
        let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
        let df = execute_logical_plan(ctx, plan).await?;
        let plan = df.create_physical_plan().await?;
        let plan = if let Some(fields) = fields {
            rename_physical_plan(plan, fields.as_slice())?
        } else {
            plan
        };
        Ok(execute_stream(plan, ctx.task_ctx())?)
    }

    #[cfg(test)]
    pub(crate) async fn execute_query(&self, query: &str) -> SparkResult<Vec<RecordBatch>> {
        use crate::spark::connect::relation::RelType;
        use crate::spark::connect::{Relation, Sql};

        let relation = Relation {
            common: None,
            rel_type: Some(RelType::Sql(Sql {
                query: query.to_string(),
                args: HashMap::new(),
                pos_args: vec![],
            })),
        };
        let stream = self.execute_plan(relation.try_into()?).await?;
        read_stream(stream).await
    }
}

struct SparkSessionState {
    config: SparkRuntimeConfig,
    executors: HashMap<String, Arc<Executor>>,
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
pub struct SessionManager {
    sessions: Mutex<SessionStore>,
    object_store_config: Arc<ObjectStoreConfig>,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(SessionStore::new()),
            object_store_config: Arc::new(ObjectStoreConfig::default()),
        }
    }

    pub fn with_object_store_config(mut self, object_store_config: ObjectStoreConfig) -> Self {
        self.object_store_config = Arc::new(object_store_config);
        self
    }

    pub(crate) fn get_session(&self, key: SessionKey) -> SparkResult<Arc<Session>> {
        use std::collections::hash_map::Entry;

        let mut sessions = self.sessions.lock()?;
        let entry = sessions.entry(key);
        match entry {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let session = Session::try_new(
                    v.key().user_id.clone(),
                    v.key().session_id.clone(),
                    Arc::clone(&self.object_store_config),
                )?;
                Ok(v.insert(Arc::new(session)).clone())
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn delete_session(&self, key: &SessionKey) -> SparkResult<()> {
        self.sessions.lock()?.remove(key);
        Ok(())
    }
}
