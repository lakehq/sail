use datafusion::prelude::SessionContext;

use crate::config::{ConfigKeyValue, SparkRuntimeConfig};
use crate::error::SparkResult;
use crate::session::SparkExtension;
use crate::spark::connect::{ConfigResponse, KeyValue};

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
