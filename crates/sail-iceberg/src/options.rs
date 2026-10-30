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
use sail_common_datafusion::datasource::OptionLayer;
pub(crate) use sail_data_source::options::{
    BuildPartialOptions, PartialOptions, ResolveOptions, parsers,
};

use crate::error::{DataSourceError, DataSourceResult};
use crate::spec::snapshots::summary::validate_snapshot_properties;

const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";

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

pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/options/iceberg.rs"));
}

impl ResolveOptions for r#gen::IcebergReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::IcebergReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

impl ResolveOptions for r#gen::IcebergWriteOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::IcebergWritePartialOptions::initialize();
        partial.snapshot_properties = Some(snapshot_properties_from_options(&options)?);
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
    use datafusion::execution::context::SessionContext;

    use super::*;
    use crate::spec::snapshots::summary::RESERVED_SNAPSHOT_PROPERTIES;

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
