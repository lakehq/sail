use std::sync::Arc;

use sail_common::config::ConfigKeyValue;

use crate::config::{ConfigKeyValueList, SparkRuntimeConfig};
use crate::error::SparkResult;
use crate::session::Session;
use crate::spark::connect::{ConfigResponse, KeyValue};

pub(crate) fn handle_config_get(
    session: Arc<Session>,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    let pairs = session.get_config(keys)?.into();
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_set(
    session: Arc<Session>,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let kv: ConfigKeyValueList = kv.into();
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    session.set_config(kv)?;
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_get_with_default(
    session: Arc<Session>,
    kv: Vec<KeyValue>,
) -> SparkResult<ConfigResponse> {
    let kv: ConfigKeyValueList = kv.into();
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    let pairs = session.get_config_with_default(kv)?.into();
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_option(
    session: Arc<Session>,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    let kv = keys
        .into_iter()
        .map(|key| ConfigKeyValue { key, value: None })
        .collect::<Vec<_>>()
        .into();
    let pairs = session.get_config_with_default(kv)?.into();
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_get_all(
    session: Arc<Session>,
    prefix: Option<String>,
) -> SparkResult<ConfigResponse> {
    let kv = session.get_all_config(prefix.as_deref())?;
    let warnings = SparkRuntimeConfig::get_warnings(&kv);
    let pairs = kv.into();
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs,
        warnings,
    })
}

pub(crate) fn handle_config_unset(
    session: Arc<Session>,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
    let warnings = SparkRuntimeConfig::get_warnings_by_keys(&keys);
    session.unset_config(keys)?;
    Ok(ConfigResponse {
        session_id: session.session_id().to_string(),
        pairs: Vec::new(),
        warnings,
    })
}

pub(crate) fn handle_config_is_modifiable(
    session: Arc<Session>,
    keys: Vec<String>,
) -> SparkResult<ConfigResponse> {
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
        session_id: session.session_id().to_string(),
        pairs,
        warnings,
    })
}
