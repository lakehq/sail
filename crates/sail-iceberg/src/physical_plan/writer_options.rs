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

use sail_data_source::options::gen::IcebergWriteOptions;
use serde::{Deserialize, Serialize};

/// Options for the Iceberg writer execution plan.
/// This is a subset of `IcebergWriteOptions` containing only the fields used
/// during physical writing. It derives serde for use in the physical plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IcebergWriterExecOptions {
    pub merge_schema: bool,
    pub overwrite_schema: bool,
}

impl From<IcebergWriteOptions> for IcebergWriterExecOptions {
    fn from(options: IcebergWriteOptions) -> Self {
        Self {
            merge_schema: options.merge_schema,
            overwrite_schema: options.overwrite_schema,
        }
    }
}
