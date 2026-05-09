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
use sail_data_source::error::DataSourceResult;
pub use sail_data_source::options::types::delta::{
    parse_delta_log_replay_strategy, DeltaLogReplayStrategy,
};
use sail_data_source::options::ResolveOptions;
pub use sail_data_source::options::{parsers, types, BuildPartialOptions, PartialOptions};

pub mod gen {
    include!(concat!(env!("OUT_DIR"), "/options/delta.rs"));
}

pub fn default_delta_log_replay_hash_threshold() -> usize {
    100
}

impl ResolveOptions for gen::DeltaReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = gen::DeltaReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

impl ResolveOptions for gen::DeltaWriteOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = gen::DeltaWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
