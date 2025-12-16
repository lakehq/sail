use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, plan_err, DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_data_source::resolve_listing_urls;
use sail_delta_lake::datasource::schema::DataFusionMixins;
use sail_delta_lake::table::open_table_with_object_store;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::merge::MergeIntoWriteNode;
use sail_physical_plan::file_delete::create_file_delete_physical_plan;
use sail_physical_plan::file_write::create_file_write_physical_plan;
use sail_physical_plan::merge::create_preexpanded_merge_physical_plan;
use url::Url;

mod optimizer;

pub use optimizer::{lakehouse_optimizer_rules, ExpandMerge};

fn is_lakehouse_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("delta")
}

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

    let table = open_table_with_object_store(table_url, object_store, Default::default())
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

        if let Some(node) = node.as_any().downcast_ref::<MergeIntoWriteNode>() {
            if !is_lakehouse_format(&node.options().target.format) {
                return Ok(None);
            }

            let physical_write = planner
                .create_physical_plan(node.input(), session_state)
                .await?;
            let physical_touched = planner
                .create_physical_plan(node.touched_files_plan(), session_state)
                .await?;

            let plan = create_preexpanded_merge_physical_plan(
                session_state,
                &[physical_write, physical_touched],
                node,
            )
            .await?;
            return Ok(Some(plan));
        }

        Ok(None)
    }
}

pub fn new_lakehouse_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![Arc::new(DeltaExtensionPlanner)]
}

pub fn new_delta_extension_planner() -> Arc<dyn ExtensionPlanner + Send + Sync> {
    Arc::new(DeltaExtensionPlanner)
}
