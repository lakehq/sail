use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, plan_err, DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_data_source::resolve_listing_urls;
use sail_delta_lake::table::open_table_with_object_store_and_table_config;
use sail_delta_lake::DeltaSnapshotConfig;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::merge::{MergeCardinalityCheckNode, RowLevelWriteNode};
use sail_physical_plan::file_delete::create_file_delete_physical_plan;
use sail_physical_plan::file_write::create_file_write_physical_plan;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
use sail_physical_plan::row_level_write::create_row_level_write_physical_plan;
use url::Url;

mod optimizer;

pub use optimizer::{lakehouse_optimizer_rules, ExpandRowLevelOp};
use sail_common_datafusion::datasource::is_lakehouse_format;

async fn delta_table_schema(session_state: &SessionState, path: &str) -> Result<DFSchemaRef> {
    let mut urls = resolve_listing_urls(session_state, vec![path.to_string()]).await?;
    let table_url = match (urls.pop(), urls.is_empty()) {
        (Some(url), true) => <ListingTableUrl as AsRef<Url>>::as_ref(&url).clone(),
        _ => return plan_err!("expected a single path for Delta table: {path}"),
    };

    let object_store = session_state
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Only the schema (protocol + metadata) is needed; skip loading file-level actions.
    let table_config = DeltaSnapshotConfig {
        require_files: false,
        ..Default::default()
    };
    let table = open_table_with_object_store_and_table_config(
        table_url,
        object_store,
        Default::default(),
        table_config,
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let arrow_schema = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    arrow_schema.to_dfschema_ref()
}

#[derive(Debug, Default)]
pub struct DeltaExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DeltaExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            if !is_lakehouse_format(node.options().format.as_str()) {
                return Ok(None);
            }
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let [physical_input] = physical_inputs else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            let plan = create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input.clone(),
                node.options().clone(),
            )
            .await?;
            return Ok(Some(plan));
        }

        if let Some(node) = node.as_any().downcast_ref::<FileDeleteNode>() {
            if !is_lakehouse_format(node.options().format.as_str()) {
                return Ok(None);
            }
            // Non-lakehouse DELETE falls through to session planner.
            // Lakehouse DELETE is handled via RowLevelWriteNode after optimizer expansion.
            // This branch catches FileDeleteNode that wasn't expanded (shouldn't normally happen).
            if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
                return internal_err!("FileDeleteNode should have no inputs");
            }

            let schema = delta_table_schema(session_state, &node.options().path).await?;
            let plan = create_file_delete_physical_plan(
                session_state,
                planner,
                schema,
                node.options().clone(),
            )
            .await?;
            return Ok(Some(plan));
        }

        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !is_lakehouse_format(node.target_format()) {
                return Ok(None);
            }

            let plan = create_row_level_write_physical_plan(session_state, planner, node).await?;
            return Ok(Some(plan));
        }

        if let Some(node) = node.as_any().downcast_ref::<MergeCardinalityCheckNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
                    "MergeCardinalityCheckNode requires exactly one physical input"
                );
            };
            let exec = MergeCardinalityCheckExec::new(
                input.clone(),
                node.target_row_id_col(),
                node.target_present_col(),
                node.source_present_col(),
            )?;
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}

pub fn new_lakehouse_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![
        Arc::new(sail_delta_lake::planner::DeltaTablePhysicalPlanner),
        Arc::new(sail_iceberg::IcebergTablePhysicalPlanner),
        Arc::new(DeltaExtensionPlanner),
    ]
}

pub fn new_delta_extension_planner() -> Arc<dyn ExtensionPlanner + Send + Sync> {
    Arc::new(DeltaExtensionPlanner)
}
