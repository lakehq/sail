use datafusion::prelude::SessionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::config::ConfigKeyValue;
use crate::error::SparkResult;
use crate::session::SparkSession;
use crate::spark::config::SparkConfigKey;
use crate::spark::connect::{ConfigResponse, KeyValue};

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
    spark.set_config(kv)?;
    if sets_session_timezone {
        // Mirror `spark.sql.session.timeZone` onto the DataFusion session configuration. Parquet
        // listing-table schema inference reads `execution.time_zone` to reconcile mixed-timezone
        // timestamps and to render adjusted timestamps in the session zone; Sail otherwise keeps
        // the session timezone only in `PlanConfig`, leaving `execution.time_zone` unset.
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
    spark.unset_config(keys)?;
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
