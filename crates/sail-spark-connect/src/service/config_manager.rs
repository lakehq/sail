use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigTimeZone;
use datafusion::prelude::SessionContext;
use log::warn;
use sail_common::spec;
use sail_common::utils::datetime::get_system_timezone;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::utils::datetime::arrow_timezone_label;
use sail_plan::config::PlanConfig;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::parser::parse_native_config_value;

use crate::config::{ConfigKeyValue, SparkRuntimeConfig};
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
    // Parse before storing any values. Spark keeps the original spelling; DataFusion needs
    // the Arrow label, just like the timestamp types produced by the resolver.
    let kv = kv
        .into_iter()
        .map(|pair| {
            let time_zone = if pair.key == SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE {
                pair.value
                    .as_deref()
                    .map(|value| arrow_timezone_label(value).parse::<ConfigTimeZone>())
                    .transpose()?
            } else {
                None
            };
            Ok((pair, time_zone))
        })
        .collect::<SparkResult<Vec<_>>>()?;
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        for (pair, time_zone) in kv {
            spark.set_config(vec![pair])?;
            if let Some(time_zone) = time_zone {
                // Mirror each successful change even if a later pair fails. Parquet inference
                // and optimizer rules read this setting rather than Spark's PlanConfig.
                state.config_mut().options_mut().execution.time_zone = Some(time_zone);
            }
        }
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
    let pairs = kv.into_iter().map(Into::into).collect();
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
    // Resolve and parse the effective default before removing Spark's override.
    let time_zone = if unsets_session_timezone {
        Some(
            PlanConfig::try_from(&SparkRuntimeConfig::try_new()?)?
                .session_timezone_label()
                .parse::<ConfigTimeZone>()?,
        )
    } else {
        None
    };
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        spark.unset_config(keys)?;
        if let Some(time_zone) = time_zone {
            state.config_mut().options_mut().execution.time_zone = Some(time_zone);
        }
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
mod tests {
    use std::time::Duration;

    use datafusion::prelude::SessionConfig;
    use pyo3::Python;

    use super::*;
    use crate::session::SparkSessionOptions;

    const TIME_ZONE_KEY: &str = SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE;

    fn pair(key: &str, value: Option<&str>) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: value.map(Into::into),
        }
    }

    fn context() -> SparkResult<SessionContext> {
        Python::initialize();
        let spark = Arc::new(SparkSession::try_new(
            "timezone-test".into(),
            "".into(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(1),
            },
        )?);
        spark.set_config(vec![pair(TIME_ZONE_KEY, Some("UTC")).into()])?;
        let mut config = SessionConfig::new().with_extension(spark);
        config.options_mut().execution.time_zone = Some("UTC".parse()?);
        Ok(SessionContext::new_with_config(config))
    }

    fn assert_timezone(
        ctx: &SessionContext,
        raw: Option<&str>,
        effective: &str,
        arrow: &str,
    ) -> SparkResult<()> {
        let response = handle_config_get(ctx, vec![TIME_ZONE_KEY.into()])?;
        assert_eq!(response.pairs[0].value.as_deref(), raw);
        assert_eq!(
            ctx.extension::<SparkSession>()?
                .plan_config()?
                .session_timezone
                .as_ref(),
            effective
        );
        assert_eq!(
            ctx.copied_config()
                .options()
                .execution
                .time_zone
                .as_ref()
                .map(|tz| tz.as_str()),
            Some(arrow)
        );
        Ok(())
    }

    #[tokio::test]
    async fn set_timezone_preserves_spark_spelling_and_normalizes_datafusion() -> SparkResult<()> {
        let ctx = context()?;
        // Spark SQLConfSuite keeps the supplied spelling, including GMT+8:00 and short IDs.
        for (raw, arrow) in [
            ("UTC", "UTC"),
            ("+08:00", "+08:00"),
            ("-08", "-08"),
            ("+0530", "+0530"),
            ("Asia/Kolkata", "Asia/Kolkata"),
            ("America/Chicago", "America/Chicago"),
            ("Z", "+00:00"),
            ("UT", "+00:00"),
            ("GMT+8:00", "+08:00"),
            ("UTC+08:00", "+08:00"),
            ("+5:30", "+05:30"),
            ("+05:30:00", "+05:30"),
            ("+053000", "+05:30"),
            ("PST", "America/Los_Angeles"),
            ("CST", "America/Chicago"),
            ("MIT", "Pacific/Apia"),
            // Preserve the existing Arrow-label fallback for offsets with seconds.
            ("+05:30:45", "UTC"),
        ] {
            handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some(raw))])?;
            assert_timezone(&ctx, Some(raw), raw, arrow)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_timezone_keeps_both_configurations() -> SparkResult<()> {
        let ctx = context()?;
        handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("PST"))])?;
        for invalid in ["Nope", "Asia/shanghai", "pst", "", "'Asia/Kolkata'"] {
            assert!(matches!(
                handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some(invalid))]),
                Err(SparkError::DataFusionError(_))
            ));
            assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
        }
        // Validate all timezone values before even the first pair is stored.
        assert!(
            handle_config_set(
                &ctx,
                vec![
                    pair(TIME_ZONE_KEY, Some("Asia/Tokyo")),
                    pair(TIME_ZONE_KEY, Some("Nope")),
                ]
            )
            .is_err()
        );
        assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
        Ok(())
    }

    #[tokio::test]
    async fn later_config_failure_does_not_desynchronize_timezone() -> SparkResult<()> {
        let ctx = context()?;
        for invalid in [
            pair("missing.value", None),
            pair(SparkConfigKey::SPARK_SQL_SHUFFLE_PARTITIONS, Some("0")),
            pair(TIME_ZONE_KEY, None),
        ] {
            assert!(
                handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("PST")), invalid,]).is_err()
            );
            assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
            handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("UTC"))])?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn unset_timezone_restores_effective_default() -> SparkResult<()> {
        let ctx = context()?;
        handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("GMT+8:00"))])?;
        handle_config_set(&ctx, vec![pair("unrelated", Some("value"))])?;
        handle_config_unset(&ctx, vec!["unrelated".into()])?;
        assert_timezone(&ctx, Some("GMT+8:00"), "GMT+8:00", "+08:00")?;
        for _ in 0..2 {
            handle_config_unset(&ctx, vec![TIME_ZONE_KEY.into()])?;
            assert_timezone(&ctx, None, "UTC", "UTC")?;
        }
        Ok(())
    }
}
