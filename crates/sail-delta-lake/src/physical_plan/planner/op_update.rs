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

use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use sail_common_datafusion::datasource::{OPERATION_COLUMN, RowLevelOperationType};

use super::context::PlannerContext;
use super::op_merge::{
    RowLevelWriteInfo, assemble_row_level_mor_plan, build_row_level_rewrite_plan,
};

pub async fn build_update_plan(
    ctx: &PlannerContext<'_>,
    info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    build_row_level_rewrite_plan(ctx, info).await
}

/// Merge-on-Read UPDATE appends changed rows and invalidates their original row indices with DVs.
pub async fn build_update_plan_mor(
    ctx: &PlannerContext<'_>,
    info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    if info.deletion_vector_plan.is_none() {
        return internal_err!("Merge-on-Read UPDATE requires file-local row-index metadata");
    }
    let expanded = info.expanded_input.clone().ok_or_else(|| {
        DataFusionError::Plan("pre-expanded UPDATE plan missing expanded input".to_string())
    })?;
    let operation_index = expanded
        .schema()
        .index_of(OPERATION_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let update_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new(OPERATION_COLUMN, operation_index)),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Int32(Some(
            RowLevelOperationType::Update.as_i32(),
        )))),
    ));
    let changed_rows: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(update_predicate, expanded)?);

    assemble_row_level_mor_plan(ctx, info, changed_rows).await
}
