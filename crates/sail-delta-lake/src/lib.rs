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

pub mod conversion;
pub mod datasource;
pub mod error;
mod kernel;
pub mod operations;
pub mod options;
pub mod physical_plan;
pub mod planner;
pub mod schema;
pub mod storage;
pub mod table;
pub mod table_format;

use std::sync::Once;

use sail_physical_plan::{register_format_type, FormatTag};
pub use table::create_delta_provider;
pub use table_format::DeltaTableFormat;

pub use crate::error::{DeltaError, DeltaError as DeltaTableError, DeltaResult, KernelError};

static INIT: Once = Once::new();

pub fn init_delta_types() {
    INIT.call_once(|| {
        let _ = register_format_type::<physical_plan::DeltaCommitExec>(FormatTag::Delta);
        let _ = register_format_type::<physical_plan::DeltaWriterExec>(FormatTag::Delta);
        let _ = register_format_type::<physical_plan::DeltaDiscoveryExec>(FormatTag::Delta);
        let _ = register_format_type::<physical_plan::DeltaScanByAddsExec>(FormatTag::Delta);
        let _ = register_format_type::<physical_plan::DeltaRemoveActionsExec>(FormatTag::Delta);
        let _ = register_format_type::<physical_plan::DeltaLogReplayExec>(FormatTag::Delta);
    });
}
