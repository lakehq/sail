use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_datafusion_err, internal_err, Result, ToDFSchema};
use datafusion_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{create_physical_sort_exprs, LexOrdering};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::TableKind;
use sail_common_datafusion::datasource::SourceInfo;
use sail_data_source::default_registry;

use crate::extension::logical::{
    FileDeleteNode, FileWriteNode, MapPartitionsNode, RangeNode, SchemaPivotNode, ShowStringNode,
    SortWithinPartitionsNode,
};
use crate::extension::physical::map_partitions::MapPartitionsExec;
use crate::extension::physical::range::RangeExec;
use crate::extension::physical::schema_pivot::SchemaPivotExec;
use crate::extension::physical::show_string::ShowStringExec;
use crate::extension::physical::{
    create_file_delete_physical_plan, create_file_write_physical_plan,
};
use crate::utils::ItemTaker;

pub(crate) struct ExtensionPhysicalPlanner {}

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let physical_inputs = physical_inputs.to_vec();
        let plan: Arc<dyn ExecutionPlan> = if let Some(node) =
            node.as_any().downcast_ref::<RangeNode>()
        {
            Arc::new(RangeExec::new(
                node.range().clone(),
                node.num_partitions(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
            Arc::new(ShowStringExec::new(
                physical_inputs.one()?,
                node.names().to_vec(),
                node.limit(),
                node.format().clone(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
            Arc::new(MapPartitionsExec::new(
                physical_inputs.one()?,
                node.udf().clone(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
            let expr = create_physical_sort_exprs(
                node.sort_expr(),
                node.schema(),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("SortExec requires at least one sort expression");
            };
            let sort = SortExec::new(ordering, physical_inputs.one()?)
                .with_fetch(node.fetch())
                .with_preserve_partitioning(true);
            Arc::new(sort)
        } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
            Arc::new(SchemaPivotExec::new(
                physical_inputs.one()?,
                node.names().to_vec(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let Ok(physical_input) = physical_inputs.one() else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input,
                node.options().clone(),
            )
            .await?
        } else if let Some(node) = node.as_any().downcast_ref::<FileDeleteNode>() {
            if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
                return internal_err!("FileDeleteNode should have no inputs");
            }
            // Create a dummy logical plan for schema context
            let catalog_manager = session_state
                .config()
                .get_extension::<CatalogManager>()
                .ok_or_else(|| internal_datafusion_err!("CatalogManager extension not found"))?;
            let table_status = catalog_manager
                .get_table_or_view(&node.options().table_name)
                .await
                .map_err(|e| internal_datafusion_err!("Failed to get table: {e}"))?;

            let schema = match &table_status.kind {
                TableKind::Table {
                    columns,
                    format,
                    location,
                    ..
                } if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") => {
                    let Some(location) = location.as_ref() else {
                        return internal_err!("Table for delete has no location");
                    };
                    let source_info = SourceInfo {
                        paths: vec![location.clone()],
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![],
                    };
                    let provider = default_registry()
                        .get_format(format)?
                        .create_provider(session_state, source_info)
                        .await?;
                    Ok(provider.schema().to_dfschema_ref()?)
                }
                TableKind::Table { columns, .. } => {
                    let schema = datafusion::arrow::datatypes::Schema::new(
                        columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
                    );
                    Ok(schema.to_dfschema_ref()?)
                }
                _ => internal_err!("Expected a table for DELETE"),
            }?;
            let dummy_logical_plan = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            });
            create_file_delete_physical_plan(
                session_state,
                planner,
                &dummy_logical_plan,
                node.options().clone(),
            )
            .await?
        } else {
            return internal_err!("Unsupported logical extension node: {:?}", node);
        };
        Ok(Some(plan))
    }
}
