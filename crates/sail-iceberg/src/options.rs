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

use std::collections::BTreeMap;

use datafusion::catalog::Session;
use datafusion::prelude::SessionConfig;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::session::config::SessionConfigService;
pub(crate) use sail_data_source::options::{
    BuildPartialOptions, PartialOptions, ResolveOptions, parsers,
};

use crate::error::{DataSourceError, DataSourceResult};
use crate::spec::snapshots::summary::validate_snapshot_properties;

const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";
/// Iceberg's Spark runtime reads this prefix as `SparkSQLProperties.SNAPSHOT_PROPERTY_PREFIX`.
const SESSION_SNAPSHOT_PROPERTY_PREFIX: &str = "spark.sql.iceberg.snapshot-property.";

fn is_snapshot_property_option(key: &str) -> bool {
    key.get(..SNAPSHOT_PROPERTY_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(SNAPSHOT_PROPERTY_PREFIX))
}

/// Only operation options may annotate a snapshot; persisted table properties never do.
pub(crate) fn snapshot_properties_from_options(
    options: &[OptionLayer],
) -> DataSourceResult<BTreeMap<String, String>> {
    let mut properties = BTreeMap::new();
    for layer in options {
        if let OptionLayer::OptionList { items } = layer {
            for (key, value) in items {
                if is_snapshot_property_option(key) {
                    // Spark's CaseInsensitiveStringMap exposes lowercased option keys.
                    let suffix = key[SNAPSHOT_PROPERTY_PREFIX.len()..].to_lowercase();
                    properties.insert(suffix, value.clone());
                }
            }
        }
    }
    validate_snapshot_properties(&properties).map_err(|cause| DataSourceError::InvalidOption {
        key: SNAPSHOT_PROPERTY_PREFIX.to_string(),
        value: String::new(),
        cause: Some(cause),
    })?;
    Ok(properties)
}

/// Snapshot properties set for the session, with the keys lowercased like the write options.
fn session_snapshot_properties(
    config: &SessionConfig,
) -> DataSourceResult<BTreeMap<String, String>> {
    let options = match config.get_extension::<SessionConfigService>() {
        Some(service) => service.get_options_with_prefix(SESSION_SNAPSHOT_PROPERTY_PREFIX)?,
        None => BTreeMap::new(),
    };
    let properties = options
        .into_iter()
        .filter_map(|(key, value)| {
            let suffix = key.strip_prefix(SESSION_SNAPSHOT_PROPERTY_PREFIX)?;
            Some((suffix.to_lowercase(), value))
        })
        .collect();
    validate_snapshot_properties(&properties).map_err(|cause| DataSourceError::InvalidOption {
        key: SESSION_SNAPSHOT_PROPERTY_PREFIX.to_string(),
        value: String::new(),
        cause: Some(cause),
    })?;
    Ok(properties)
}

pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/options/iceberg.rs"));
}

impl ResolveOptions for r#gen::IcebergReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        validate_snapshot_selectors(&options)?;
        let mut partial = r#gen::IcebergReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

// Validate before merging so that contradictory aliases cannot overwrite one another.
fn validate_snapshot_selectors(options: &[OptionLayer]) -> DataSourceResult<()> {
    let mut selected: Option<(&str, &str)> = None;
    for layer in options {
        let OptionLayer::OptionList { items } = layer else {
            continue;
        };
        for (key, value) in items {
            let selector = match key.to_ascii_lowercase().as_str() {
                "versionasof" | "version_as_of" => "versionAsOf",
                "snapshotid" | "snapshot_id" | "snapshot-id" => "snapshotId",
                "timestampasof" | "timestamp_as_of" => "timestampAsOf",
                "ref" | "use_ref" | "iceberg.ref" => "ref",
                "branch" => "branch",
                "tag" => "tag",
                _ => continue,
            };
            if value.trim().is_empty() {
                return Err(DataSourceError::InvalidOption {
                    key: key.clone(),
                    value: value.clone(),
                    cause: Some("Iceberg snapshot selector must not be empty".to_string()),
                });
            }
            if let Some((previous, previous_value)) = selected
                && (previous != selector || previous_value != value)
            {
                return Err(DataSourceError::InvalidOption {
                    key: key.clone(),
                    value: value.clone(),
                    cause: Some(format!(
                        "Conflicting Iceberg snapshot selectors: {previous} and {key}. \
                         Specify only one selector, with identical values for aliases"
                    )),
                });
            }
            selected = Some((selector, value));
        }
    }
    Ok(())
}

impl ResolveOptions for r#gen::IcebergWriteOptions {
    fn resolve(ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::IcebergWritePartialOptions::initialize();
        // A write option takes precedence over the session property with the same key.
        let mut snapshot_properties = session_snapshot_properties(ctx.config())?;
        snapshot_properties.extend(snapshot_properties_from_options(&options)?);
        partial.snapshot_properties = Some(snapshot_properties);
        for layer in options {
            if let OptionLayer::OptionList { items } = &layer
                && let Some((key, value)) = items.iter().find(|(key, _)| {
                    key.eq_ignore_ascii_case("branch") || key.eq_ignore_ascii_case("wap-id")
                })
            {
                return Err(crate::error::DataSourceError::InvalidOption {
                    key: key.clone(),
                    value: value.clone(),
                    cause: Some("Iceberg branch and WAP writes are not supported".to_string()),
                });
            }
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::context::SessionContext;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion_common::DataFusionError;
    use sail_common_datafusion::session::config::SessionConfigProvider;

    use super::*;
    use crate::spec::snapshots::summary::RESERVED_SNAPSHOT_PROPERTIES;

    struct Provider {
        options: BTreeMap<String, String>,
        fail: bool,
    }

    impl SessionConfigProvider for Provider {
        fn get_options(&self, keys: &[&str]) -> datafusion_common::Result<Vec<Option<String>>> {
            Ok(vec![None; keys.len()])
        }

        fn get_options_with_prefix(
            &self,
            prefix: &str,
        ) -> datafusion_common::Result<BTreeMap<String, String>> {
            assert_eq!(prefix, SESSION_SNAPSHOT_PROPERTY_PREFIX);
            if self.fail {
                return Err(DataFusionError::Plan("provider failed".to_string()));
            }
            Ok(self.options.clone())
        }
    }

    fn state(properties: &[(&str, &str)], fail: bool) -> SessionState {
        let options = properties
            .iter()
            .map(|(key, value)| {
                (
                    format!("{SESSION_SNAPSHOT_PROPERTY_PREFIX}{key}"),
                    value.to_string(),
                )
            })
            .collect();
        let service = SessionConfigService::new(Arc::new(Provider { options, fail }));
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_extension(Arc::new(service)))
            .build()
    }

    #[test]
    fn session_snapshot_properties_are_the_lowest_option_layer() -> DataSourceResult<()> {
        let state = state(&[("Run-ID", "session"), ("job", "session-job")], false);
        let resolve = |items: Vec<(String, String)>| {
            r#gen::IcebergWriteOptions::resolve(&state, vec![OptionLayer::OptionList { items }])
                .map(|options| options.snapshot_properties)
        };
        assert_eq!(
            resolve(vec![])?,
            BTreeMap::from([
                ("run-id".into(), "session".into()),
                ("job".into(), "session-job".into()),
            ])
        );
        assert_eq!(
            resolve(vec![("snapshot-property.RUN-ID".into(), "write".into())])?,
            BTreeMap::from([
                ("run-id".into(), "write".into()),
                ("job".into(), "session-job".into()),
            ])
        );
        Ok(())
    }

    #[test]
    fn session_snapshot_properties_reject_engine_keys_and_provider_errors() {
        for suffix in ["Operation", ""] {
            let result =
                r#gen::IcebergWriteOptions::resolve(&state(&[(suffix, "x")], false), vec![]);
            assert!(
                matches!(
                    &result,
                    Err(DataSourceError::InvalidOption { key, .. })
                        if key == SESSION_SNAPSHOT_PROPERTY_PREFIX
                ),
                "{suffix}"
            );
        }
        let result = r#gen::IcebergWriteOptions::resolve(&state(&[], true), vec![]);
        assert!(matches!(
            result.map_err(DataFusionError::from),
            Err(DataFusionError::Plan(message)) if message == "provider failed"
        ));
    }

    #[test]
    fn snapshot_properties_are_case_insensitive_operation_options_only() -> DataSourceResult<()> {
        let layers = vec![
            OptionLayer::OptionList {
                items: vec![
                    ("snapshot-property.Run.ID".into(), "First".into()),
                    ("snapshot-property.empty".into(), "".into()),
                    (
                        "snapshot-property.snapshot-property.inner".into(),
                        " Once ".into(),
                    ),
                    ("snapshot-property.added-custom".into(), "Allowed".into()),
                    ("snapshot-property.total-custom".into(), "Allowed".into()),
                    ("snapshot-property".into(), "not-a-prefix".into()),
                ],
            },
            OptionLayer::TablePropertyList {
                items: vec![
                    ("snapshot-property.table-only".into(), "ignored".into()),
                    ("option.snapshot-property.run.id".into(), "ignored".into()),
                    ("snapshot-property.operation".into(), "ignored".into()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![("SNAPSHOT-PROPERTY.RUN.ID".into(), "CaseValue".into())],
            },
        ];
        let ctx = SessionContext::new();
        let options = r#gen::IcebergWriteOptions::resolve(&ctx.state(), layers)?;
        assert_eq!(
            options.snapshot_properties,
            BTreeMap::from([
                ("run.id".into(), "CaseValue".into()),
                ("empty".into(), "".into()),
                ("snapshot-property.inner".into(), " Once ".into()),
                ("added-custom".into(), "Allowed".into()),
                ("total-custom".into(), "Allowed".into()),
            ])
        );
        assert!(
            r#gen::IcebergWriteOptions::resolve(&ctx.state(), vec![])?
                .snapshot_properties
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn snapshot_properties_reject_all_engine_keys_and_empty_suffix() {
        for suffix in RESERVED_SNAPSHOT_PROPERTIES.into_iter().chain([""]) {
            for suffix in [suffix.to_string(), suffix.to_uppercase()] {
                let result = snapshot_properties_from_options(&[OptionLayer::OptionList {
                    items: vec![(format!("snapshot-property.{suffix}"), "override".into())],
                }]);
                assert!(
                    matches!(result, Err(DataSourceError::InvalidOption { .. })),
                    "{suffix}"
                );
            }
        }
    }
}
