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

use datafusion::common::{plan_err, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaLogReplayStrategyOption {
    #[default]
    Auto,
    Sort,
    Hash,
}

pub fn default_delta_log_replay_hash_threshold() -> usize {
    100
}

pub fn parse_delta_log_replay_strategy(s: &str) -> Result<DeltaLogReplayStrategyOption> {
    match s.to_ascii_lowercase().as_str() {
        "auto" => Ok(DeltaLogReplayStrategyOption::Auto),
        "sort" => Ok(DeltaLogReplayStrategyOption::Sort),
        "hash" => Ok(DeltaLogReplayStrategyOption::Hash),
        other => {
            plan_err!("invalid value for deltaLogReplayStrategy: {other}, expected auto/sort/hash")
        }
    }
}
