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

use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

use super::context::PlannerContext;

pub async fn build_update_plan(
    _ctx: &PlannerContext<'_>,
    _input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Err(DataFusionError::NotImplemented(
        "UPDATE planner not implemented".to_string(),
    ))
}
