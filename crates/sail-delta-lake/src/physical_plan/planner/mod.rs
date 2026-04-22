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

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::PhysicalSinkMode;

mod commit;
pub mod context;
mod log_scan;
mod log_segment;
pub(crate) mod metadata_predicate;
pub mod utils;

mod op_delete;
mod op_merge;
mod op_update;
mod op_write;

pub use context::{DeltaPlannerConfig, PlannerContext};
pub use op_delete::{build_delete_plan as plan_delete, build_delete_plan_mor as plan_delete_mor};
pub use op_merge::build_merge_plan as plan_merge;
pub use op_update::build_update_plan as plan_update;

pub struct DeltaPhysicalPlanner<'a> {
    ctx: PlannerContext<'a>,
}

impl<'a> DeltaPhysicalPlanner<'a> {
    pub fn new(ctx: PlannerContext<'a>) -> Self {
        Self { ctx }
    }

    pub async fn create_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        sink_mode: PhysicalSinkMode,
        sort_order: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        op_write::build_write_plan(&self.ctx, input, sink_mode, sort_order).await
    }
}
