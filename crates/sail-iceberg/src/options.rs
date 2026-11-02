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
use sail_common_datafusion::datasource::OptionLayer;
pub(crate) use sail_data_source::options::{
    BuildPartialOptions, PartialOptions, ResolveOptions, parsers,
};

use crate::error::{DataSourceError, DataSourceResult};

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
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::IcebergWritePartialOptions::initialize();
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
