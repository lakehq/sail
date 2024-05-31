use std::sync::Arc;

use crate::error::SparkResult;
use crate::session::Session;
use crate::spark::connect::KeyValue;

pub(crate) fn handle_config_get(
    session: Arc<Session>,
    keys: Vec<String>,
    pairs: &mut Vec<KeyValue>,
) -> SparkResult<()> {
    let state = session.lock()?;
    for key in keys {
        pairs.push(KeyValue {
            key: key.clone(),
            value: state.get_config(&key).map(|v| v.clone()),
        });
    }
    Ok(())
}

pub(crate) fn handle_config_set(session: Arc<Session>, kv: Vec<KeyValue>) -> SparkResult<()> {
    let mut state = session.lock()?;
    for KeyValue { key, value } in kv {
        if let Some(value) = value {
            state.set_config(&key, &value);
        } else {
            state.unset_config(&key);
        }
    }
    Ok(())
}

pub(crate) fn handle_config_get_with_default(
    session: Arc<Session>,
    kv: Vec<KeyValue>,
    pairs: &mut Vec<KeyValue>,
) -> SparkResult<()> {
    let state = session.lock()?;
    for KeyValue { key, value } in kv {
        pairs.push(KeyValue {
            key: key.clone(),
            value: state.get_config(&key).map(|v| v.clone()).or(value),
        });
    }
    Ok(())
}

pub(crate) fn handle_config_get_option(
    session: Arc<Session>,
    keys: Vec<String>,
    pairs: &mut Vec<KeyValue>,
) -> SparkResult<()> {
    let state = session.lock()?;
    for key in keys {
        if let Some(value) = state.get_config(&key) {
            pairs.push(KeyValue {
                key: key.clone(),
                value: Some(value.clone()),
            });
        }
    }
    Ok(())
}

pub(crate) fn handle_config_get_all(
    session: Arc<Session>,
    prefix: Option<String>,
    pairs: &mut Vec<KeyValue>,
) -> SparkResult<()> {
    let state = session.lock()?;
    for (k, v) in state.iter_config(&prefix) {
        pairs.push(KeyValue {
            key: k.clone(),
            value: Some(v.clone()),
        });
    }
    Ok(())
}

pub(crate) fn handle_config_unset(session: Arc<Session>, keys: Vec<String>) -> SparkResult<()> {
    let mut state = session.lock()?;
    for key in keys {
        state.unset_config(&key);
    }
    Ok(())
}

pub(crate) fn handle_config_is_modifiable(
    _session: Arc<Session>,
    keys: Vec<String>,
    pairs: &mut Vec<KeyValue>,
) -> SparkResult<()> {
    for key in keys {
        pairs.push(KeyValue {
            key: key.clone(),
            value: Some("true".to_string()),
        });
    }
    Ok(())
}
