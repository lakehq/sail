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
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::{DFSchema, DataFusionError, Result, ToDFSchema};
use datafusion::physical_expr::expressions::{CaseExpr, CastExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::RowLevelWriteInfo;
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::commit::assemble_commit_plan;
use super::context::PlannerContext;
use super::metadata_predicate::{build_metadata_filter, predicate_requires_stats};
use super::utils::{build_log_replay_pipeline_with_options, LogReplayOptions};
use crate::kernel::DeltaOperation;
use crate::physical_plan::{
    DeltaDiscoveryExec, DeltaPhysicalExprAdapterFactory, DeltaScanByAddsExec,
    DeltaWriterExecOptions,
};
use crate::table::DeltaSnapshot;

pub async fn build_update_plan(
    ctx: &PlannerContext<'_>,
    info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = snapshot_state.version();

    let table_schema = snapshot_state
        .input_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();
    let table_df_schema = table_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let condition = info.condition;
    let assignments = info.assignments.unwrap_or_default();

    let physical_condition = match &condition {
        Some(cond) => Some(
            ctx.session()
                .create_physical_expr(cond.expr.clone(), &table_df_schema)?,
        ),
        None => None,
    };

    let find_files_exec = build_find_files_plan(
        ctx,
        snapshot_state,
        version,
        condition.as_ref(),
        &partition_columns,
    )
    .await?;

    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        version,
        table_schema.clone(),
        table_schema.clone(),
        crate::datasource::DeltaScanConfig::default(),
        None,
        None,
        None,
    ));

    let projection_exec = build_update_projection(
        ctx,
        scan_exec,
        &table_schema,
        &table_df_schema,
        physical_condition,
        assignments,
    )?;

    let operation = Some(DeltaOperation::Update {
        predicate: condition.and_then(|c| c.source),
    });

    assemble_commit_plan(
        projection_exec,
        Some(find_files_exec),
        ctx.table_url().clone(),
        DeltaWriterExecOptions::from(ctx.options().clone()),
        ctx.metadata_configuration().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        operation,
    )
}

async fn build_find_files_plan(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
    version: i64,
    condition: Option<&ExprWithSource>,
    partition_columns: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_only = match condition {
        Some(cond) => !predicate_requires_stats(&cond.expr, partition_columns),
        None => true,
    };

    let log_replay_options = LogReplayOptions {
        include_stats_json: !partition_only,
        ..Default::default()
    };

    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot, log_replay_options).await?;

    let meta_scan: Arc<dyn ExecutionPlan> = match condition {
        Some(cond) => build_metadata_filter(ctx.session(), meta_scan, snapshot, cond.expr.clone())?,
        None => meta_scan,
    };

    // UPDATE must always scan file content to produce updated rows, even when the predicate
    // touches only partition columns. `partition_scan=true` is a DELETE optimization that emits
    // empty batches and drops touched files — for UPDATE that would silently delete data.
    Ok(Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.to_vec(),
        false,
    )?))
}

fn build_update_projection(
    ctx: &PlannerContext<'_>,
    scan_exec: Arc<dyn ExecutionPlan>,
    table_schema: &ArrowSchemaRef,
    table_df_schema: &DFSchema,
    physical_condition: Option<Arc<dyn PhysicalExpr>>,
    assignments: Vec<(String, ExprWithSource)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let scan_schema = scan_exec.schema();

    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory
        .create(table_schema.clone(), scan_schema.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let adapted_condition = match physical_condition {
        Some(cond) => Some(
            adapter
                .rewrite(cond)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        None => None,
    };

    let mut assignment_exprs: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col_name, rhs) in assignments {
        let rhs_physical = ctx
            .session()
            .create_physical_expr(rhs.expr.clone(), table_df_schema)?;
        let adapted_rhs = adapter
            .rewrite(rhs_physical)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let target_field = table_schema.field_with_name(&col_name)?;
        let rhs_type = adapted_rhs.data_type(&scan_schema)?;
        let final_rhs: Arc<dyn PhysicalExpr> = if &rhs_type == target_field.data_type() {
            adapted_rhs
        } else {
            Arc::new(CastExpr::new(
                adapted_rhs,
                target_field.data_type().clone(),
                None,
            ))
        };
        assignment_exprs.insert(col_name.to_ascii_lowercase(), final_rhs);
    }

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        let name = field.name().clone();
        let scan_index = scan_schema.index_of(&name)?;
        let original_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, scan_index));

        let expr: Arc<dyn PhysicalExpr> = match assignment_exprs.get(&name.to_ascii_lowercase()) {
            Some(rhs) => match &adapted_condition {
                Some(cond) => Arc::new(CaseExpr::try_new(
                    None,
                    vec![(Arc::clone(cond), Arc::clone(rhs))],
                    Some(original_col),
                )?),
                None => Arc::clone(rhs),
            },
            None => original_col,
        };

        projection_exprs.push((expr, name));
    }

    Ok(Arc::new(ProjectionExec::try_new(
        projection_exprs,
        scan_exec,
    )?))
}
