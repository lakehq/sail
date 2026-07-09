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

use std::num::NonZeroUsize;

use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;
pub(crate) use sail_data_source::options::{BuildPartialOptions, PartialOptions, ResolveOptions};
use serde::{Deserialize, Serialize};

use crate::error::{DataSourceError, DataSourceResult};

pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/options/delta.rs"));
}

pub(crate) mod parsers {
    pub(crate) use sail_data_source::options::parsers::*;

    pub(crate) use super::parse_delta_log_replay_strategy;
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

pub fn default_delta_log_replay_hash_threshold() -> NonZeroUsize {
    non_zero_usize(100)
}

fn non_zero_usize(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => unreachable!("non-zero default"),
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
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = r#gen::DeltaWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
