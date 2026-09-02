use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_logical_plan::merge::MergeCardinalityCheckNode;
use sail_logical_plan::row_level::RowLevelWriteNode;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;

use crate::lake_source::{IcebergWriteNode, plan_iceberg_write};
use crate::logical::IcebergTableSource;
use crate::physical::row_level_planner::plan_iceberg_row_level_write;
use crate::physical_plan::{
    IcebergManifestScanExec, IcebergProcedureExec, IcebergScanByDataFilesExec,
};
use crate::procedure::{IcebergProcedureNode, RewriteDataFilesScanNode};

pub struct IcebergPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<IcebergProcedureNode>() {
            let procedure = if let Some(rewrite_data_files) = node.rewrite_data_files() {
                let [input] = physical_inputs else {
                    return datafusion_common::internal_err!(
                        "rewrite_data_files requires exactly one physical input"
                    );
                };
                IcebergProcedureExec::try_new_rewrite_data_files(
                    node.call().clone(),
                    Arc::clone(input),
                    rewrite_data_files.clone(),
                )?
            } else {
                if !physical_inputs.is_empty() {
                    return datafusion_common::internal_err!(
                        "leaf Iceberg procedure cannot have physical inputs"
                    );
                }
                IcebergProcedureExec::try_new(node.call().clone(), node.planned_table().cloned())?
            };
            return Ok(Some(Arc::new(procedure)));
        }

        if let Some(node) = node.as_any().downcast_ref::<RewriteDataFilesScanNode>() {
            if !physical_inputs.is_empty() {
                return datafusion_common::internal_err!(
                    "IcebergRewriteDataFilesScan does not accept physical inputs"
                );
            }
            let schema = node.arrow_schema();
            if node.snapshot_json().is_empty() {
                return Ok(Some(Arc::new(EmptyExec::new(schema))));
            }
            let snapshot = serde_json::from_str(node.snapshot_json()).map_err(|error| {
                datafusion_common::DataFusionError::Plan(format!(
                    "failed to decode rewrite_data_files snapshot: {error}"
                ))
            })?;
            let manifests: Arc<dyn ExecutionPlan> = Arc::new(
                IcebergManifestScanExec::new(node.table_url().to_string(), snapshot)
                    .with_selected_data_file_paths(node.selected_data_file_paths().to_vec()),
            );
            return Ok(Some(Arc::new(IcebergScanByDataFilesExec::new(
                manifests,
                node.table_url().to_string(),
                schema,
            ))));
        }

        if let Some(node) = node.as_any().downcast_ref::<IcebergWriteNode>() {
            let [logical_input] = logical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one logical input"
                );
            };
            let [physical_input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one physical input"
                );
            };
            return plan_iceberg_write(session, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }

        if let Some(node) = node.as_any().downcast_ref::<MergeCardinalityCheckNode>() {
            let [input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "MergeCardinalityCheckNode requires exactly one physical input"
                );
            };
            let exec = MergeCardinalityCheckExec::new(
                Arc::clone(input),
                node.target_row_id_col(),
                node.target_present_col(),
                node.source_present_col(),
            )?;
            return Ok(Some(Arc::new(exec)));
        }

        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !node.target_format().eq_ignore_ascii_case("iceberg") {
                return Ok(None);
            }
            return plan_iceberg_row_level_write(session, planner, node, physical_inputs)
                .await
                .map(Some);
        }

        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
            return Ok(None);
        };
        let filters = unnormalize_cols(scan.filters.clone());
        let plan = source
            .provider()
            .scan(session, scan.projection.as_ref(), &filters, scan.fetch)
            .await?;
        Ok(Some(plan))
    }
}
