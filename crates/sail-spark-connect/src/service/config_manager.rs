use chrono::{Offset, Utc};
use chrono_tz::Tz;
use datafusion::prelude::SessionContext;
use sail_common::config::ConfigKeyValue;

use crate::config::SparkRuntimeConfig;
use crate::error::{SparkError, SparkResult};
use crate::session::SparkExtension;
use crate::spark::config::SPARK_SQL_SESSION_TIME_ZONE;
use crate::spark::connect::{ConfigResponse, KeyValue};

fn config_set_time_zone(ctx: &SessionContext, value: String) -> SparkResult<()> {
    let state = ctx.state_ref();
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
    Ok(())
}

pub(crate) fn handle_config_get(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    let pairs = spark.get_config(keys)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_set(
    ctx: &SessionContext,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let kv: Vec<ConfigKeyValue> = kv.into_iter().map(Into::into).collect();
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    for ConfigKeyValue { key, value } in &kv {
        if key == SPARK_SQL_SESSION_TIME_ZONE {
            if let Some(value) = value {
                config_set_time_zone(ctx, value.clone())?;
            }
        }
    }
    spark.set_config(kv)?;
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_get_with_default(
    ctx: &SessionContext,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let kv: Vec<ConfigKeyValue> = kv.into_iter().map(Into::into).collect();
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    let pairs = spark.get_config_with_default(kv)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_option(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    let kv = keys
        .into_iter()
        .map(|key| ConfigKeyValue { key, value: None })
        .collect::<Vec<_>>();
    let pairs = spark.get_config_with_default(kv)?;
    let pairs = pairs.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_all(
    ctx: &SessionContext,
    prefix: Option<String>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let kv = spark.get_all_config(prefix.as_deref())?;
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    let pairs = kv.into_iter().map(Into::into).collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_unset(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    spark.unset_config(keys)?;
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_is_modifiable(
    ctx: &SessionContext,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let spark = SparkExtension::get(ctx)?;
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    let pairs = keys
        .into_iter()
        .map(|key| {
            let modifiable = SparkRuntimeConfig::is_modifiable(key.as_str());
            let value = if modifiable { "true" } else { "false" };
            KeyValue {
                key: key.clone(),
                value: Some(value.to_string()),
            }
        })
        .collect();
    Ok(ConfigResponse {
        session_id: spark.session_id().to_string(),
        pairs,
        warnings,
    })
}
