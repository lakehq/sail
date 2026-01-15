use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_datafusion_err, internal_err, DFSchema, ToDFSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{create_physical_sort_exprs, Partitioning};
use sail_catalog::manager::CatalogManager;
use sail_catalog_system::logical_rewriter::RewriteSystemTableSource;
use sail_catalog_system::planner::SystemTablePhysicalPlanner;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use sail_common_datafusion::streaming::event::schema::{
    to_flow_event_field_names, to_flow_event_projection,
};
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::merge::MergeIntoNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::schema_pivot::SchemaPivotNode;
use sail_logical_plan::show_string::ShowStringNode;
use sail_logical_plan::sort::SortWithinPartitionsNode;
use sail_logical_plan::streaming::collector::StreamCollectorNode;
use sail_logical_plan::streaming::limit::StreamLimitNode;
use sail_logical_plan::streaming::source_adapter::StreamSourceAdapterNode;
use sail_logical_plan::streaming::source_wrapper::StreamSourceWrapperNode;
use sail_physical_plan::file_delete::create_file_delete_physical_plan;
use sail_physical_plan::file_write::create_file_write_physical_plan;
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::schema_pivot::SchemaPivotExec;
use sail_physical_plan::show_string::ShowStringExec;
use sail_physical_plan::streaming::collector::StreamCollectorExec;
use sail_physical_plan::streaming::limit::StreamLimitExec;
use sail_physical_plan::streaming::source_adapter::StreamSourceAdapterExec;
use sail_plan_lakehouse::new_lakehouse_extension_planners;

#[derive(Debug)]
pub struct ExtensionQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: show rewriters and the final logical plan in `EXPLAIN`
        let rewriters = vec![RewriteSystemTableSource];
        let mut logical_plan = logical_plan.clone();
        for rewriter in rewriters {
            logical_plan = rewriter.rewrite(logical_plan)?.data
        }
        let mut extension_planners = new_lakehouse_extension_planners();
        extension_planners.push(Arc::new(SystemTablePhysicalPlanner));
        extension_planners.push(Arc::new(ExtensionPhysicalPlanner));
        let planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        planner
            .create_physical_plan(&logical_plan, session_state)
            .await
    }
}

pub struct ExtensionPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> = if let Some(node) =
            node.as_any().downcast_ref::<RangeNode>()
        {
            Arc::new(RangeExec::new(
                node.range().clone(),
                node.num_partitions(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
            let [input] = physical_inputs else {
                return internal_err!("ShowStringExec requires exactly one physical input");
            };
            Arc::new(ShowStringExec::new(
                input.clone(),
                node.names().to_vec(),
                node.limit(),
                node.format().clone(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MapPartitionsExec requires exactly one physical input");
            };
            Arc::new(MapPartitionsExec::new(
                input.clone(),
                node.udf().clone(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SortExec requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                node.sort_expr(),
                UserDefinedLogicalNode::schema(node),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("SortExec requires at least one sort expression");
            };
            let sort = SortExec::new(ordering, input.clone())
                .with_fetch(node.fetch())
                .with_preserve_partitioning(true);
            Arc::new(sort)
        } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SchemaPivotExec requires exactly one physical input");
            };
            Arc::new(SchemaPivotExec::new(
                input.clone(),
                node.names().to_vec(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let [physical_input] = physical_inputs else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input.clone(),
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
                    let registry = session_state.extension::<TableFormatRegistry>()?;
                    let provider = registry
                        .get(format)?
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
            create_file_delete_physical_plan(session_state, planner, schema, node.options().clone())
                .await?
        } else if let Some(node) = node.as_any().downcast_ref::<MergeIntoNode>() {
            let _ = (
                planner,
                logical_inputs,
                physical_inputs,
                session_state,
                node,
            );
            return internal_err!(
                "MERGE planning expects a pre-expanded logical plan (MergeIntoWriteNode). \
Ensure expand_merge is enabled; MERGE is currently only supported for Delta tables."
            );
        } else if let Some(node) = node.as_any().downcast_ref::<ExplicitRepartitionNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
                    "ExplicitRepartitionExec requires exactly one physical input"
                );
            };
            let partitioning = plan_explicit_partitioning(
                planner,
                UserDefinedLogicalNode::schema(node),
                input.as_ref(),
                node.num_partitions(),
                node.partitioning_expressions(),
                session_state,
            )?;
            Arc::new(ExplicitRepartitionExec::new(input.clone(), partitioning))
        } else if node.as_any().is::<StreamSourceAdapterNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamSourceExec requires exactly one physical input");
            };
            Arc::new(StreamSourceAdapterExec::new(input.clone()))
        } else if let Some(node) = node.as_any().downcast_ref::<StreamSourceWrapperNode>() {
            let plan = node
                .source()
                .scan(
                    session_state,
                    node.projection(),
                    node.filters(),
                    node.fetch(),
                )
                .await?;
            match node.names() {
                Some(names) => {
                    let names = to_flow_event_field_names(names);
                    let projection = node.projection().map(|x| to_flow_event_projection(x));
                    rename_projected_physical_plan(plan, &names, projection.as_ref())?
                }
                None => plan,
            }
        } else if let Some(node) = node.as_any().downcast_ref::<StreamLimitNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamLimitExec requires exactly one physical input");
            };
            Arc::new(StreamLimitExec::try_new(
                input.clone(),
                node.skip(),
                node.fetch(),
            )?)
        } else if node.as_any().is::<StreamCollectorNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamCollectorExec requires exactly one physical input");
            };
            Arc::new(StreamCollectorExec::try_new(input.clone())?)
        } else {
            return internal_err!("unsupported logical extension node: {:?}", node);
        };
        Ok(Some(plan))
    }
}

fn plan_explicit_partitioning(
    planner: &dyn PhysicalPlanner,
    schema: &DFSchema,
    input: &dyn ExecutionPlan,
    num_partitions: Option<usize>,
    expressions: &[Expr],
    session_state: &SessionState,
) -> datafusion_common::Result<Partitioning> {
    match (num_partitions, expressions) {
        (Some(0), _) => internal_err!("number of explicit partitions cannot be zero"),
        (Some(1), _) => Ok(Partitioning::UnknownPartitioning(1)),
        (Some(_) | None, expressions) => {
            if expressions.is_empty() {
                return internal_err!(
                    "explicit repartitioning requires at least one partitioning expression"
                );
            }
            let num_partitions = num_partitions
                .unwrap_or_else(|| input.properties().output_partitioning().partition_count());
            let expressions = expressions
                .iter()
                .map(|e| planner.create_physical_expr(e, schema, session_state))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(Partitioning::Hash(expressions, num_partitions))
        }
    }
}

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(ExtensionQueryPlanner {})
}
