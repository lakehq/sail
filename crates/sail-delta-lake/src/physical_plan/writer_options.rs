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

use std::collections::HashMap;

use sail_data_source::options::gen::DeltaWriteOptions;
use serde::{Deserialize, Serialize};

/// Options for the Delta Lake writer execution plan.
///
/// This is a subset of `DeltaWriteOptions` containing only the fields used
/// during physical writing. It derives serde for use in the physical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaWriterExecOptions {
    pub target_file_size: u64,
    pub write_batch_size: std::num::NonZeroUsize,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub replace_where: Option<String>,
    #[serde(default)]
    pub generation_expressions: HashMap<String, String>,
}

impl From<DeltaWriteOptions> for DeltaWriterExecOptions {
    fn from(options: DeltaWriteOptions) -> Self {
        Self {
            target_file_size: options.target_file_size,
            write_batch_size: options.write_batch_size,
            merge_schema: options.merge_schema,
            overwrite_schema: options.overwrite_schema,
            replace_where: options.replace_where,
            generation_expressions: HashMap::new(),
        }
    }
}

impl DeltaWriterExecOptions {
    /// Attach column-level generation expressions resolved from the write input's
    /// logical schema.
    pub fn with_generation_expressions(
        mut self,
        generation_expressions: HashMap<String, String>,
    ) -> Self {
        self.generation_expressions = generation_expressions;
        self
    }
}
