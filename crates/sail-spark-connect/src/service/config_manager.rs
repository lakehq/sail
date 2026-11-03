use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use log::warn;
use regex::Regex;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common::utils::datetime::get_system_timezone;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::config::PlanConfig;
use sail_plan::resolver::PlanResolver;
use sail_session::catalog::SessionCatalogFactory;
use sail_sql_analyzer::parser::parse_native_config_value;

use crate::config::ConfigKeyValue;
use crate::error::{SparkError, SparkResult};
use crate::service::time_zone::is_valid_time_zone;
use crate::session::SparkSession;
use crate::spark::config::SparkConfigKey;
use crate::spark::connect::{ConfigResponse, KeyValue};

const DATAFUSION_TIME_ZONE_KEY: &str = "datafusion.execution.time_zone";

/// The prefix of the configuration keys that define a catalog in a Spark session:
/// `spark.sql.catalog.<name>` holds the catalog type and
/// `spark.sql.catalog.<name>.<option>` an option of the catalog.
const CATALOG_KEY_PREFIX: &str = "spark.sql.catalog.";
const REDACTED_VALUE: &str = "*********(redacted)";

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
            let spark = ctx.extension::<SparkSession>()?;
            let echoed = redact_config_value(&spark, variable, &value)?;
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            Ok(Some(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![variable.as_str()])),
                    Arc::new(StringArray::from(vec![echoed.as_str()])),
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

/// Applies a change of the configuration, and then registers or drops the session catalogs that
/// the changed keys define. When a catalog cannot be built, the configuration keeps its previous
/// values, so the configuration and the catalogs of the session never disagree.
fn change_session_catalogs(
    ctx: &SessionContext,
    spark: &SparkSession,
    keys: &[String],
    change: impl FnOnce() -> SparkResult<()>,
) -> SparkResult<()> {
    let catalog_keys = keys
        .iter()
        .filter(|key| key.starts_with(CATALOG_KEY_PREFIX))
        .cloned()
        .collect::<Vec<_>>();
    if catalog_keys.is_empty() {
        return change();
    }
    let previous = spark.get_config_option(catalog_keys.clone())?;
    change()?;
    if let Err(error) = sync_session_catalogs(ctx, spark, &catalog_keys) {
        for ConfigKeyValue { key, value } in previous {
            match value {
                Some(value) => spark.set_config(vec![ConfigKeyValue {
                    key,
                    value: Some(value),
                }])?,
                None => spark.unset_config(vec![key])?,
            }
        }
        // The previous configuration built its catalogs before, so this does not fail again.
        if let Err(restore) = sync_session_catalogs(ctx, spark, &catalog_keys) {
            warn!("failed to restore the session catalogs: {restore}");
        }
        return Err(error);
    }
    Ok(())
}

/// Makes each session catalog named by the keys match the configuration of the session.
fn sync_session_catalogs(
    ctx: &SessionContext,
    spark: &SparkSession,
    keys: &[String],
) -> SparkResult<()> {
    let names = keys
        .iter()
        .filter_map(|key| key.strip_prefix(CATALOG_KEY_PREFIX))
        .filter_map(|rest| rest.split('.').next())
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>();
    if names.is_empty() {
        return Ok(());
    }
    let factory = ctx.extension::<SessionCatalogFactory>()?;
    let manager = ctx.extension::<CatalogManager>()?;
    for name in names {
        let key = format!("{CATALOG_KEY_PREFIX}{name}");
        let kind = spark
            .get_config_option(vec![key.clone()])?
            .pop()
            .and_then(|pair| pair.value);
        let option_prefix = format!("{key}.");
        let options = spark
            .get_all_config(Some(&option_prefix))?
            .into_iter()
            .filter_map(|ConfigKeyValue { key, value }| {
                let option = key.strip_prefix(&option_prefix)?.to_string();
                Some((option, value?))
            })
            .collect::<HashMap<_, _>>();
        factory
            .apply(&manager, name, kind.as_deref(), &options)
            .map_err(|error| {
                SparkError::invalid(format!("cannot configure catalog {name}: {error}"))
            })?;
    }
    Ok(())
}

/// Hides the value of a sensitive key, as Spark does when it lists the configuration.
/// The keys are those that match `spark.redaction.regex`. A pattern that cannot be
/// compiled hides every value.
fn redact_config_value(spark: &SparkSession, key: &str, value: &str) -> SparkResult<String> {
    let pattern = spark.plan_config()?.redaction_regex.clone();
    let sensitive = Regex::new(&pattern)
        .map(|pattern| pattern.is_match(key))
        .unwrap_or(true);
    Ok(if sensitive {
        REDACTED_VALUE.to_string()
    } else {
        value.to_string()
    })
}

/// Hides the sensitive values of a configuration request or response in the debug log,
/// by the default `spark.redaction.regex`, since the log has no session to read it from.
pub(crate) fn redact_config_pairs(pairs: &mut [KeyValue]) {
    let Ok(pattern) = Regex::new(&PlanConfig::default().redaction_regex) else {
        return;
    };
    for pair in pairs {
        if pattern.is_match(&pair.key) && pair.value.is_some() {
            pair.value = Some(REDACTED_VALUE.to_string());
        }
    }
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
        server_side_session_id: spark.session_id().to_string(),
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
    let keys = kv.iter().map(|pair| pair.key.clone()).collect::<Vec<_>>();
    change_session_catalogs(ctx, &spark, &keys, || spark.set_config(kv))?;
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
        server_side_session_id: spark.session_id().to_string(),
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
        server_side_session_id: spark.session_id().to_string(),
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
        server_side_session_id: spark.session_id().to_string(),
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
    let pairs = kv
        .into_iter()
        .map(|mut pair| -> SparkResult<_> {
            if let Some(value) = &pair.value {
                pair.value = Some(redact_config_value(&spark, &pair.key, value)?);
            }
            Ok(pair.into())
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        server_side_session_id: spark.session_id().to_string(),
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
    change_session_catalogs(ctx, &spark, &keys, || spark.unset_config(keys.clone()))?;
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
        server_side_session_id: spark.session_id().to_string(),
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
        server_side_session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use datafusion::prelude::SessionConfig;
    use sail_catalog::manager::CatalogManagerOptions;
    use sail_catalog::provider::CatalogProvider;
    use sail_catalog_memory::MemoryCatalogProvider;
    use sail_common::runtime::RuntimeHandle;

    use super::*;
    use crate::session::SparkSessionOptions;

    fn session() -> SessionContext {
        // The session reads the PySpark version to choose its configuration entries.
        pyo3::Python::initialize();
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let server_catalog = MemoryCatalogProvider::new(
            "sail".to_string(),
            vec![Arc::from("default")].try_into().unwrap(),
            None,
        );
        let manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "sail".to_string(),
                Arc::new(server_catalog) as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "sail".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })
        .unwrap();
        let spark = SparkSession::try_new(
            "session".to_string(),
            "user".to_string(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(1),
            },
        )
        .unwrap();
        SessionContext::new_with_config(
            SessionConfig::new()
                .with_extension(Arc::new(manager))
                .with_extension(Arc::new(spark))
                .with_extension(Arc::new(SessionCatalogFactory::new(runtime))),
        )
    }

    fn set(ctx: &SessionContext, key: &str, value: &str) -> SparkResult<()> {
        handle_config_set(
            ctx,
            vec![KeyValue {
                key: key.to_string(),
                value: Some(value.to_string()),
            }],
        )
        .map(|_| ())
    }

    fn catalogs(ctx: &SessionContext) -> Vec<String> {
        let mut names = ctx
            .extension::<CatalogManager>()
            .unwrap()
            .list_catalogs(None)
            .unwrap()
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn register(ctx: &SessionContext, name: &str) {
        let key = format!("spark.sql.catalog.{name}");
        set(ctx, &key, "iceberg-rest").unwrap();
        set(
            ctx,
            &format!("{key}.uri"),
            "http://catalog.invalid:8181/catalog",
        )
        .unwrap();
        set(ctx, &format!("{key}.warehouse"), name).unwrap();
        set(ctx, &format!("{key}.bearer_access_token"), "secret-token").unwrap();
    }

    #[tokio::test]
    async fn catalog_is_registered_once_its_uri_is_set() {
        let ctx = session();
        set(&ctx, "spark.sql.catalog.org_1", "iceberg-rest").unwrap();
        assert_eq!(catalogs(&ctx), vec!["sail"]);
        register(&ctx, "org_1");
        assert_eq!(catalogs(&ctx), vec!["org_1", "sail"]);
    }

    #[tokio::test]
    async fn catalog_is_visible_to_its_session_only() {
        let first = session();
        let second = session();
        register(&first, "org_1");
        assert_eq!(catalogs(&first), vec!["org_1", "sail"]);
        assert_eq!(catalogs(&second), vec!["sail"]);
    }

    #[tokio::test]
    async fn catalog_name_is_the_whole_segment_after_the_prefix() {
        let ctx = session();
        register(&ctx, "org_1");
        register(&ctx, "org_10");
        handle_config_unset(&ctx, vec!["spark.sql.catalog.org_1".to_string()]).unwrap();
        assert_eq!(catalogs(&ctx), vec!["org_10", "sail"]);
    }

    #[tokio::test]
    async fn unset_type_drops_the_catalog() {
        let ctx = session();
        register(&ctx, "org_1");
        handle_config_unset(&ctx, vec!["spark.sql.catalog.org_1".to_string()]).unwrap();
        assert_eq!(catalogs(&ctx), vec!["sail"]);
    }

    #[tokio::test]
    async fn a_type_that_is_not_supported_registers_nothing() {
        let ctx = session();
        set(
            &ctx,
            "spark.sql.catalog.org_1",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .unwrap();
        set(
            &ctx,
            "spark.sql.catalog.org_1.uri",
            "http://catalog.invalid",
        )
        .unwrap();
        assert_eq!(catalogs(&ctx), vec!["sail"]);
    }

    #[tokio::test]
    async fn changing_the_uri_replaces_the_catalog() {
        let ctx = session();
        register(&ctx, "org_1");
        set(
            &ctx,
            "spark.sql.catalog.org_1.uri",
            "http://other.invalid/catalog",
        )
        .unwrap();
        assert_eq!(catalogs(&ctx), vec!["org_1", "sail"]);
    }

    #[tokio::test]
    async fn a_server_catalog_cannot_be_replaced_and_the_configuration_is_kept() {
        let ctx = session();
        set(&ctx, "spark.sql.catalog.sail", "iceberg-rest").unwrap_err();
        let spark = ctx.extension::<SparkSession>().unwrap();
        let value = spark
            .get_config_option(vec!["spark.sql.catalog.sail".to_string()])
            .unwrap();
        assert_eq!(value[0].value, None);
        assert_eq!(catalogs(&ctx), vec!["sail"]);
    }

    #[tokio::test]
    async fn a_token_file_is_refused_for_a_session_catalog() {
        let ctx = session();
        set(&ctx, "spark.sql.catalog.org_1", "iceberg-rest").unwrap();
        set(
            &ctx,
            "spark.sql.catalog.org_1.uri",
            "http://catalog.invalid",
        )
        .unwrap();
        set(
            &ctx,
            "spark.sql.catalog.org_1.bearer_access_token_file",
            "/var/run/secrets/token",
        )
        .unwrap_err();
        let spark = ctx.extension::<SparkSession>().unwrap();
        let file = spark
            .get_config_option(vec![
                "spark.sql.catalog.org_1.bearer_access_token_file".to_string(),
            ])
            .unwrap();
        assert_eq!(file[0].value, None);
        assert_eq!(catalogs(&ctx), vec!["org_1", "sail"]);
    }

    #[tokio::test]
    async fn get_all_hides_the_token() {
        let ctx = session();
        register(&ctx, "org_1");
        let response =
            handle_config_get_all(&ctx, Some("spark.sql.catalog.org_1".to_string())).unwrap();
        let pairs = response
            .pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value.unwrap()))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            pairs["spark.sql.catalog.org_1.bearer_access_token"],
            REDACTED_VALUE
        );
        assert_eq!(pairs["spark.sql.catalog.org_1.warehouse"], "org_1");
        assert!(!format!("{pairs:?}").contains("secret-token"));
    }

    #[tokio::test]
    async fn sql_set_does_not_echo_the_token() {
        let ctx = session();
        let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::SetVariable {
            variable: "spark.sql.catalog.org_1.bearer_access_token".to_string(),
            value: "secret-token".to_string(),
        }));
        let batch = handle_sql_config(&ctx, &plan).await.unwrap().unwrap();
        let echoed = format!("{batch:?}");
        assert!(echoed.contains(REDACTED_VALUE));
        assert!(!echoed.contains("secret-token"));
    }

    #[test]
    fn debug_log_hides_the_token() {
        let mut pairs = vec![
            KeyValue {
                key: "spark.sql.catalog.org_1.bearer_access_token".to_string(),
                value: Some("secret-token".to_string()),
            },
            KeyValue {
                key: "spark.sql.catalog.org_1.uri".to_string(),
                value: Some("http://catalog.invalid".to_string()),
            },
        ];
        redact_config_pairs(&mut pairs);
        assert_eq!(pairs[0].value.as_deref(), Some(REDACTED_VALUE));
        assert_eq!(pairs[1].value.as_deref(), Some("http://catalog.invalid"));
    }
}
