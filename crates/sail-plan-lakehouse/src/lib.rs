use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, plan_err, DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_data_source::resolve_listing_urls;
use sail_delta_lake::physical_plan::{
    compile_data_quality_invariants, DataQualityPolicy, DeltaDataQualityExec, DeltaWriterExec,
};
use sail_delta_lake::table::open_table_with_object_store_and_table_config;
use sail_delta_lake::DeltaTableConfig;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::merge::{MergeCardinalityCheckNode, MergeIntoWriteNode};
use sail_physical_plan::file_delete::create_file_delete_physical_plan;
use sail_physical_plan::file_write::create_file_write_physical_plan;
use sail_physical_plan::merge::create_preexpanded_merge_physical_plan;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
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

    // Only the schema (protocol + metadata) is needed; skip loading file-level actions.
    let table_config = DeltaTableConfig {
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

            // Compile invariants on the driver side and wrap the input if needed.
            // `wrap_with_data_quality` also returns the already-opened table so we can
            // pass it into `DeltaWriterExec` and avoid a redundant open during execution.
            let (physical_input, maybe_table) = self
                .wrap_with_data_quality(session_state, physical_input.clone(), node.options())
                .await?;

            let plan = create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input,
                node.options().clone(),
            )
            .await?;

            // Inject the pre-opened table into `DeltaWriterExec` so execution reuses it.
            let plan = if let Some(table) = maybe_table {
                inject_prefetched_table(plan, Arc::new(table))?
            } else {
                plan
            };
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

impl DeltaExtensionPlanner {
    /// Compile Delta invariants and wrap the input with a `DeltaDataQualityExec` if needed.
    ///
    /// Returns both the (possibly-wrapped) input plan and the `DeltaTable` that was opened to
    /// compile invariants. The caller can inject the table into `DeltaWriterExec` via
    /// `inject_prefetched_table` so that execution does not need to open the table a second time.
    ///
    /// This runs on the driver during physical plan construction so that workers
    /// only evaluate pre-compiled `PhysicalExpr` without needing SQL parsing or session context.
    async fn wrap_with_data_quality(
        &self,
        session_state: &SessionState,
        physical_input: Arc<dyn ExecutionPlan>,
        options: &sail_logical_plan::file_write::FileWriteOptions,
    ) -> Result<(
        Arc<dyn ExecutionPlan>,
        Option<sail_delta_lake::table::DeltaTable>,
    )> {
        let all_options: Vec<std::collections::HashMap<String, String>> = options
            .options
            .iter()
            .map(|set| set.iter().cloned().collect())
            .collect();
        let path = all_options
            .iter()
            .find_map(|set| set.get("path").or_else(|| set.get("location")).cloned());
        let Some(path) = path else {
            return Ok((physical_input, None));
        };

        let table = resolve_and_open_delta_table(session_state, &path)
            .await
            .ok();

        let table_properties: std::collections::HashMap<String, String> =
            options.table_properties.iter().cloned().collect();

        let input_schema = physical_input.schema();
        let invariants = compile_data_quality_invariants(
            session_state,
            table.as_ref(),
            &[], // no pending schema actions at this stage
            &input_schema,
            &table_properties,
        )?;

        if invariants.is_empty() {
            return Ok((physical_input, table));
        }

        let exec =
            DeltaDataQualityExec::new(physical_input, invariants, DataQualityPolicy::Strict)?;
        Ok((Arc::new(exec), table))
    }
}

/// Recursively traverse `plan` and attach `table` to any `DeltaWriterExec` found in the tree,
/// rebuilding ancestor nodes via `with_new_children`.  This lets the executor skip opening the
/// table a second time at task start.
fn inject_prefetched_table(
    plan: Arc<dyn ExecutionPlan>,
    table: Arc<sail_delta_lake::table::DeltaTable>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(writer) = plan.as_any().downcast_ref::<DeltaWriterExec>() {
        return Ok(Arc::new(writer.with_prefetched_table(Arc::clone(&table))?));
    }
    let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .map(|child| inject_prefetched_table(Arc::clone(child), Arc::clone(&table)))
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_children(new_children)
}

async fn resolve_and_open_delta_table(
    session_state: &SessionState,
    path: &str,
) -> Result<sail_delta_lake::table::DeltaTable> {
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

    // Only protocol + metadata are needed for invariant compilation;
    // skip eagerly loading file-level actions.
    let table_config = DeltaTableConfig {
        require_files: false,
        ..Default::default()
    };
    open_table_with_object_store_and_table_config(
        table_url,
        object_store,
        Default::default(),
        table_config,
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn new_lakehouse_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![
        Arc::new(sail_delta_lake::planner::DeltaTablePhysicalPlanner),
        Arc::new(DeltaExtensionPlanner),
    ]
}

pub fn new_delta_extension_planner() -> Arc<dyn ExtensionPlanner + Send + Sync> {
    Arc::new(DeltaExtensionPlanner)
}
