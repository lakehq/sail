use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use log::warn;
use sail_common::spec;
use sail_common::utils::datetime::get_system_timezone;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::parser::parse_native_config_value;

use crate::config::ConfigKeyValue;
use crate::error::{SparkError, SparkResult};
use crate::service::time_zone::is_valid_time_zone;
use crate::session::SparkSession;
use crate::spark::config::SparkConfigKey;
use crate::spark::connect::{ConfigResponse, KeyValue};

const DATAFUSION_TIME_ZONE_KEY: &str = "datafusion.execution.time_zone";

pub(crate) async fn handle_sql_config(
    ctx: &SessionContext,
    plan: &spec::Plan,
) -> SparkResult<Option<RecordBatch>> {
    let spec::Plan::Command(command) = plan else {
        return Ok(None);
    };
    match &command.node {
        spec::CommandNode::SetVariable { variable, .. }
            if native_config_key(variable)
                .is_some_and(|key| !key.starts_with(DATAFUSION_TIME_ZONE_KEY)) =>
        {
            let spark = ctx.extension::<SparkSession>()?;
            execute_native_config(ctx, command.node.clone()).await?;
            if let Some(key) = native_config_key(variable) {
                spark.add_native_config_key(key.to_string())?;
            }
            Ok(Some(RecordBatch::new_empty(Arc::new(Schema::empty()))))
        }
        spec::CommandNode::SetVariable { variable, value } => {
            // `datafusion.execution.time_zone` sets the session time zone, so that the DataFusion
            // time zone cannot differ from it. Spark stores it as an unrelated key.
            let (key, value) = if variable == DATAFUSION_TIME_ZONE_KEY {
                // A DataFusion setting decodes a quoted SQL literal.
                let value = parse_native_config_value(value).unwrap_or_else(|_| value.clone());
                (SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE, value)
            } else {
                (variable.as_str(), value.clone())
            };
            if key == SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE && !is_valid_time_zone(&value) {
                warn!(
                    "invalid time zone {:?} for {variable}: the statement is rejected and the \
                    session keeps its time zone, as Spark does",
                    value.chars().take(64).collect::<String>()
                );
                return Err(SparkError::invalid(format!(
                    "[INVALID_CONF_VALUE.TIME_ZONE] The value '{value}' in the config \"{variable}\" \
                    is invalid. Cannot resolve the given timezone. SQLSTATE: 22022"
                )));
            }
            handle_config_set(
                ctx,
                vec![KeyValue {
                    key: key.to_string(),
                    value: Some(value.clone()),
                }],
            )?;
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            Ok(Some(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![variable.as_str()])),
                    Arc::new(StringArray::from(vec![value.as_str()])),
                ],
            )?))
        }
        spec::CommandNode::ResetVariable { variable } => {
            let spark = ctx.extension::<SparkSession>()?;
            // `datafusion.execution.time_zone` resets the session time zone. Spark unsets only the
            // key.
            let variable = match variable.as_deref() {
                Some(DATAFUSION_TIME_ZONE_KEY) => {
                    Some(SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE.to_string())
                }
                _ => variable.clone(),
            };
            if let Some(key) = variable.as_deref().and_then(native_config_key) {
                reset_native_config(ctx, &spark, key).await?;
                return Ok(Some(RecordBatch::new_empty(Arc::new(Schema::empty()))));
            }
            if variable.is_none() {
                // Only reset native settings changed by SQL, preserving Sail's other defaults.
                for key in spark.native_config_keys()? {
                    reset_native_config(ctx, &spark, &key).await?;
                }
            }
            let keys = match &variable {
                Some(key) => vec![key.clone()],
                None => spark
                    .get_all_config(None)?
                    .into_iter()
                    .map(|kv| kv.key)
                    .collect(),
            };
            handle_config_unset(ctx, keys)?;
            // The session starts with the system time zone rather than a static config default.
            if variable
                .as_deref()
                .is_none_or(|key| key == SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE)
            {
                handle_config_set(
                    ctx,
                    vec![KeyValue {
                        key: SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE.to_string(),
                        value: Some(get_system_timezone()?),
                    }],
                )?;
            }
            Ok(Some(RecordBatch::new_empty(Arc::new(Schema::empty()))))
        }
        _ => Ok(None),
    }
}

fn native_config_key(variable: &str) -> Option<&str> {
    if variable.starts_with("datafusion.") {
        Some(variable)
    } else {
        None
    }
}

async fn execute_native_config(
    ctx: &SessionContext,
    command: spec::CommandNode,
) -> SparkResult<()> {
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let plan = spec::Plan::Command(spec::CommandPlan::new(command));
    let plan = resolver.resolve_named_plan(plan).await?.plan;
    ctx.execute_logical_plan(plan).await?;
    Ok(())
}

async fn reset_native_config(
    ctx: &SessionContext,
    spark: &SparkSession,
    key: &str,
) -> SparkResult<()> {
    execute_native_config(
        ctx,
        spec::CommandNode::ResetVariable {
            variable: Some(key.to_string()),
        },
    )
    .await?;
    spark.remove_native_config_key(key)
}

/// Mirrors `spark.sql.parquet.binaryAsString` onto the DataFusion session configuration, which is
/// where Parquet read options take their session defaults from. Sail otherwise keeps the key in the
/// Spark session only, so setting it has no effect on reads. The value set in the session overrides
/// the Sail `parquet.binary_as_string` option, and unsetting it restores that option. A read option
/// still overrides both, since option layers are merged after the session defaults.
///
/// The DataFusion write lock is taken before the Spark session value is read, so that two
/// concurrent requests cannot interleave their read and their write and leave the mirror holding
/// the value of whichever request read first. The Spark session has its own lock, which is only
/// ever taken while holding the DataFusion one, never the other way round.
fn sync_parquet_binary_as_string(ctx: &SessionContext, spark: &SparkSession) -> SparkResult<()> {
    let key = SparkConfigKey::SPARK_SQL_PARQUET_BINARY_AS_STRING;
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let value = match spark.get_config_if_set(key)? {
        Some(value) => value.trim().to_lowercase().parse::<bool>()?,
        None => spark.options().parquet_binary_as_string,
    };
    state
        .config_mut()
        .options_mut()
        .execution
        .parquet
        .binary_as_string = value;
    Ok(())
}

pub(crate) fn handle_config_get(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let warnings = spark.get_config_warnings_by_keys(&keys)?;
    let pairs = spark.get_config(keys)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_set(
    ctx: &SessionContext,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let kv: Vec<ConfigKeyValue> = kv.into_iter().map(Into::into).collect();
    let warnings = spark.get_config_warnings(&kv)?;
    let sets_session_timezone = {
        let key = SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE.to_string();
        kv.iter().any(|pair| pair.key == key)
    };
    // A request that fails on one pair keeps the pairs applied before it, as Spark does, so the
    // mirror follows what the session holds on the error path as well. Returning the error without
    // mirroring would leave `conf.get` reporting a value that Parquet reads ignore. The pairs are
    // applied one at a time to tell whether the request reached the key: a rejected value, or a
    // failure before the key, leaves the DataFusion option alone, including one set directly with
    // `SET datafusion.execution.parquet.binary_as_string`. The error of the request is the one the
    // caller needs, so it takes precedence over a mirroring error.
    let parquet_binary_as_string_key = SparkConfigKey::SPARK_SQL_PARQUET_BINARY_AS_STRING;
    let mut sets_parquet_binary_as_string = false;
    let result = kv.into_iter().try_for_each(|pair| {
        let is_parquet_binary_as_string = pair.key == parquet_binary_as_string_key;
        spark.set_config(vec![pair])?;
        sets_parquet_binary_as_string |= is_parquet_binary_as_string;
        Ok(())
    });
    let mirrored = if sets_parquet_binary_as_string {
        sync_parquet_binary_as_string(ctx, &spark)
    } else {
        Ok(())
    };
    result.and(mirrored)?;
    if sets_session_timezone {
        // Mirror `spark.sql.session.timeZone` onto the DataFusion session configuration. Parquet
        // listing-table schema inference reads `execution.time_zone` to reconcile mixed-timezone
        // timestamps and to render adjusted timestamps in the session zone; Sail otherwise keeps
        // the session timezone only in `PlanConfig`, leaving `execution.time_zone` unset. The
        // optimizer rules read it as the session time zone.
        let session_timezone = spark.plan_config()?.session_timezone.to_string();
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .time_zone = Some(session_timezone);
    }
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_get_with_default(
    ctx: &SessionContext,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let kv: Vec<ConfigKeyValue> = kv.into_iter().map(Into::into).collect();
    let warnings = spark.get_config_warnings(&kv)?;
    let pairs = spark.get_config_with_default(kv)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_option(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let warnings = spark.get_config_warnings_by_keys(&keys)?;
    let pairs = spark.get_config_option(keys)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_all(
    ctx: &SessionContext,
    prefix: Option<String>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let kv = spark.get_all_config(prefix.as_deref())?;
    let warnings = spark.get_config_warnings(&kv)?;
    let pairs = kv.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_unset(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let warnings = spark.get_config_warnings_by_keys(&keys)?;
    let unsets_session_timezone = {
        let key = SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE.to_string();
        keys.iter().any(|k| k == &key)
    };
    let unsets_parquet_binary_as_string = {
        let key = SparkConfigKey::SPARK_SQL_PARQUET_BINARY_AS_STRING.to_string();
        keys.iter().any(|k| k == &key)
    };
    spark.unset_config(keys)?;
    // Mirror before anything else can return an error, for the same reason as in
    // `handle_config_set`. Unsetting either removes every key or none of them, so there is no
    // partially applied request to mirror here.
    if unsets_parquet_binary_as_string {
        sync_parquet_binary_as_string(ctx, &spark)?;
    }
    if unsets_session_timezone {
        // Unsetting `spark.sql.session.timeZone` reverts it toward the default, so re-sync
        // `execution.time_zone` to the now-effective session zone instead of leaving the last
        // explicitly-set value behind. Mirrors the sync in `handle_config_set`.
        let session_timezone = spark.plan_config()?.session_timezone.to_string();
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .time_zone = Some(session_timezone);
    }
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_is_modifiable(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let warnings = spark.get_config_warnings_by_keys(&keys)?;
    let pairs = keys
        .into_iter()
        .map(|key| -> SparkResult<_> {
            let modifiable = spark.is_config_modifiable(key.as_str())?;
            let value = if modifiable { "true" } else { "false" };
            Ok(KeyValue {
                key: key.clone(),
                value: Some(value.to_string()),
            })
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.server_session_id().to_string(),
        pairs,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::prelude::SessionConfig;
    use pyo3::Python;

    use super::*;
    use crate::session::SparkSessionOptions;

    const KEY: &str = SparkConfigKey::SPARK_SQL_PARQUET_BINARY_AS_STRING;

    fn test_context(parquet_binary_as_string: bool) -> SparkResult<SessionContext> {
        // The session configuration reads the PySpark version through the Python interpreter,
        // which a test binary has to initialize itself. Each test runs in its own process under
        // `cargo nextest`, so no other test can have initialized it.
        Python::initialize();
        let spark = Arc::new(SparkSession::try_new(
            "server-session".to_string(),
            "session".to_string(),
            "user".to_string(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(60),
                parquet_binary_as_string,
            },
        )?);
        let mut config = SessionConfig::new().with_extension(spark);
        // The session factory seeds the DataFusion option from the Sail configuration, so the
        // mirror starts out agreeing with `SparkSessionOptions`.
        config.options_mut().execution.parquet.binary_as_string = parquet_binary_as_string;
        Ok(SessionContext::new_with_config(config))
    }

    fn set(ctx: &SessionContext, value: &str) -> SparkResult<ConfigResponse> {
        handle_config_set(
            ctx,
            vec![KeyValue {
                key: KEY.to_string(),
                value: Some(value.to_string()),
            }],
        )
    }

    fn session_value(ctx: &SessionContext) -> SparkResult<Option<String>> {
        ctx.extension::<SparkSession>()?.get_config_if_set(KEY)
    }

    fn mirrored_value(ctx: &SessionContext) -> bool {
        ctx.state_ref()
            .read()
            .config()
            .options()
            .execution
            .parquet
            .binary_as_string
    }

    /// Sets the DataFusion option directly, as `SET datafusion.execution.parquet.binary_as_string`
    /// does, which leaves the Spark key as it was.
    fn set_native_value(ctx: &SessionContext, value: bool) {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .binary_as_string = value;
    }

    #[test]
    fn test_set_and_unset_mirror_the_session_value() -> SparkResult<()> {
        let ctx = test_context(true)?;
        set(&ctx, "false")?;
        assert!(!mirrored_value(&ctx));
        set(&ctx, " TRUE ")?;
        assert!(mirrored_value(&ctx));
        handle_config_unset(&ctx, vec![KEY.to_string()])?;
        assert!(mirrored_value(&ctx));
        Ok(())
    }

    #[test]
    fn test_non_boolean_value_changes_neither_configuration() -> SparkResult<()> {
        let ctx = test_context(false)?;
        set(&ctx, "true")?;
        assert!(set(&ctx, "yes").is_err());
        assert_eq!(session_value(&ctx)?, Some("true".to_string()));
        assert!(mirrored_value(&ctx));
        Ok(())
    }

    #[test]
    fn test_a_failed_batch_mirrors_the_pair_it_applied() -> SparkResult<()> {
        let ctx = test_context(false)?;
        let kv = vec![
            KeyValue {
                key: KEY.to_string(),
                value: Some("true".to_string()),
            },
            KeyValue {
                key: SparkConfigKey::SPARK_SQL_SHUFFLE_PARTITIONS.to_string(),
                value: Some("0".to_string()),
            },
        ];
        assert!(handle_config_set(&ctx, kv).is_err());
        assert_eq!(session_value(&ctx)?, Some("true".to_string()));
        assert!(mirrored_value(&ctx));
        Ok(())
    }

    #[test]
    fn test_a_failed_batch_does_not_mirror_a_pair_it_did_not_apply() -> SparkResult<()> {
        let ctx = test_context(false)?;
        set_native_value(&ctx, true);
        let kv = vec![
            KeyValue {
                key: SparkConfigKey::SPARK_SQL_SHUFFLE_PARTITIONS.to_string(),
                value: Some("0".to_string()),
            },
            KeyValue {
                key: KEY.to_string(),
                value: Some("true".to_string()),
            },
        ];
        assert!(handle_config_set(&ctx, kv).is_err());
        assert_eq!(session_value(&ctx)?, None);
        assert!(mirrored_value(&ctx));
        Ok(())
    }

    #[test]
    fn test_non_boolean_value_of_an_unset_key_changes_neither_configuration() -> SparkResult<()> {
        let ctx = test_context(false)?;
        set_native_value(&ctx, true);
        assert!(set(&ctx, "yes").is_err());
        assert_eq!(session_value(&ctx)?, None);
        assert!(mirrored_value(&ctx));
        Ok(())
    }
}
