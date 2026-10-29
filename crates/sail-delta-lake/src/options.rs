// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::catalog::Session;
use datafusion::prelude::SessionConfig;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::session::config::SessionConfigService;
pub(crate) use sail_data_source::options::{BuildPartialOptions, PartialOptions, ResolveOptions};
use serde::{Deserialize, Serialize};

use crate::error::{DataSourceError, DataSourceResult};

pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/options/delta.rs"));
}

pub(crate) mod parsers {
    pub(crate) use sail_data_source::options::parsers::*;

    pub(crate) use super::{parse_delta_log_replay_strategy, parse_user_metadata};
}

const USER_METADATA_CONFIG: &str = "spark.databricks.delta.commitInfo.userMetadata";

pub(crate) fn session_user_metadata(
    config: &SessionConfig,
) -> datafusion_common::Result<Option<String>> {
    match config.get_extension::<SessionConfigService>() {
        Some(service) => service.get_option(USER_METADATA_CONFIG),
        None => Ok(None),
    }
}

pub(crate) fn parse_user_metadata(_key: &str, value: &str) -> DataSourceResult<Option<String>> {
    // An empty string is an explicit value that overrides the session default.
    Ok(Some(value.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaLogReplayStrategy {
    #[default]
    Auto,
    Sort,
    Hash,
}

pub fn parse_delta_log_replay_strategy(
    key: &str,
    value: &str,
) -> DataSourceResult<DeltaLogReplayStrategy> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(DeltaLogReplayStrategy::Auto),
        "sort" => Ok(DeltaLogReplayStrategy::Sort),
        "hash" => Ok(DeltaLogReplayStrategy::Hash),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        }),
    }
}

impl ResolveOptions for r#gen::DeltaReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::DeltaReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

impl ResolveOptions for r#gen::DeltaWriteOptions {
    fn resolve(ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::DeltaWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        let mut options = partial.finalize()?;
        // An explicit value, including an empty string, takes precedence.
        if options.user_metadata.is_none() {
            options.user_metadata = session_user_metadata(ctx.config())?;
        }
        Ok(options)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion_common::{DataFusionError, Result};
    use sail_common_datafusion::session::config::SessionConfigProvider;

    use super::*;
    use crate::transaction::CommitProperties;

    #[derive(Default)]
    struct Provider {
        value: Mutex<Option<String>>,
        reads: AtomicUsize,
        fail: bool,
    }

    impl SessionConfigProvider for Provider {
        fn get_options(&self, keys: &[&str]) -> Result<Vec<Option<String>>> {
            assert_eq!(keys, &[USER_METADATA_CONFIG]);
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(DataFusionError::Plan("provider failed".to_string()));
            }
            Ok(vec![self.value.lock().unwrap().clone()])
        }
    }

    fn state(provider: Arc<Provider>) -> SessionState {
        SessionStateBuilder::new()
            .with_config(
                SessionConfig::new().with_extension(Arc::new(SessionConfigService::new(provider))),
            )
            .build()
    }

    fn metadata_option(key: &str, value: &str) -> OptionLayer {
        OptionLayer::OptionList {
            items: vec![(key.to_string(), value.to_string())],
        }
    }

    #[test]
    fn missing_provider_has_no_metadata_default() {
        let state = SessionStateBuilder::new().build();
        assert_eq!(
            r#gen::DeltaWriteOptions::resolve(&state, vec![])
                .unwrap()
                .user_metadata,
            None
        );
        assert_eq!(
            CommitProperties::from_session_config(state.config())
                .unwrap()
                .user_metadata,
            None
        );
        assert_eq!(
            r#gen::DeltaWriteOptions::resolve(&state, vec![metadata_option("userMetadata", "")])
                .unwrap()
                .user_metadata,
            Some("".to_string())
        );
    }

    #[test]
    fn session_metadata_is_captured_once_per_construction() {
        let provider = Arc::new(Provider::default());
        let state = state(provider.clone());
        let expected = [Some("first"), Some("second"), Some(""), None];
        let mut captured = Vec::new();
        for (index, value) in expected.iter().enumerate() {
            *provider.value.lock().unwrap() = value.map(str::to_string);
            let options = r#gen::DeltaWriteOptions::resolve(&state, vec![]).unwrap();
            let properties = CommitProperties::from_session_config(state.config()).unwrap();
            captured.push((options, properties));
            assert_eq!(provider.reads.load(Ordering::SeqCst), (index + 1) * 2);
        }
        for ((options, properties), expected) in captured.iter().zip(expected) {
            assert_eq!(options.user_metadata.as_deref(), expected);
            assert_eq!(properties.user_metadata.as_deref(), expected);
            assert_eq!(properties.clone().user_metadata.as_deref(), expected);
        }
        assert_eq!(provider.reads.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn explicit_metadata_overrides_session_and_earlier_options() {
        let provider = Arc::new(Provider {
            value: Mutex::new(Some("session".to_string())),
            ..Default::default()
        });
        let state = state(provider.clone());
        // Delta TransactionHelper.getUserMetadata: an explicit Some("") also wins.
        for alias in ["userMetadata", "user_metadata"] {
            for value in ["per-write", ""] {
                let options = r#gen::DeltaWriteOptions::resolve(
                    &state,
                    vec![
                        metadata_option("userMetadata", "earlier"),
                        metadata_option(alias, value),
                    ],
                )
                .unwrap();
                assert_eq!(options.user_metadata.as_deref(), Some(value));
            }
        }
        assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
        assert_eq!(provider.value.lock().unwrap().as_deref(), Some("session"));
    }

    #[test]
    fn metadata_provider_errors_propagate_when_a_default_is_needed() {
        let provider = Arc::new(Provider {
            fail: true,
            ..Default::default()
        });
        let state = state(provider.clone());
        assert!(r#gen::DeltaReadOptions::resolve(&state, vec![]).is_ok());
        assert!(
            r#gen::DeltaWriteOptions::resolve(&state, vec![metadata_option("userMetadata", "")])
                .is_ok()
        );
        assert_eq!(provider.reads.load(Ordering::SeqCst), 0);

        let error = r#gen::DeltaWriteOptions::resolve(&state, vec![]).unwrap_err();
        assert!(matches!(
            DataFusionError::from(error),
            DataFusionError::Plan(message) if message == "provider failed"
        ));
        assert!(matches!(
            CommitProperties::from_session_config(state.config()),
            Err(DataFusionError::Plan(message)) if message == "provider failed"
        ));
        assert_eq!(provider.reads.load(Ordering::SeqCst), 2);
    }
}
