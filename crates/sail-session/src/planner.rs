use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, Statistics, internal_datafusion_err, internal_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{Partitioning, create_physical_sort_exprs};
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_catalog_system::planner::SystemTablePhysicalPlanner;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::rename::physical_plan::{
    rename_physical_plan, rename_projected_physical_plan,
};
use sail_common_datafusion::streaming::event::schema::{
    to_flow_event_field_names, to_flow_event_projection,
};
use sail_data_source::formats::console::ConsolePhysicalPlanner;
use sail_data_source::formats::noop::NoopPhysicalPlanner;
use sail_data_source::formats::python::PythonPhysicalPlanner;
use sail_data_source::listing::planner::ListingPhysicalPlanner;
use sail_delta_lake::physical::DeltaPhysicalPlanner;
use sail_iceberg::IcebergPhysicalPlanner;
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::remote_checkpoint::{
    RemoteCheckpointCommandNode, RemoteCheckpointRelationNode,
};
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};
use sail_logical_plan::schema_pivot::SchemaPivotNode;
use sail_logical_plan::show_string::ShowStringNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_logical_plan::streaming::collector::StreamCollectorNode;
use sail_logical_plan::streaming::filter::StreamFilterNode;
use sail_logical_plan::streaming::limit::StreamLimitNode;
use sail_logical_plan::streaming::source_adapter::StreamSourceAdapterNode;
use sail_logical_plan::streaming::source_wrapper::StreamSourceWrapperNode;
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::monotonic_id::MonotonicIdExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::remote_checkpoint::{
    RemoteCheckpointCommitExec, RemoteCheckpointScanExec, RemoteCheckpointWriteExec,
    checkpoint_storage_schema,
};
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::schema_pivot::SchemaPivotExec;
use sail_physical_plan::show_string::ShowStringExec;
use sail_physical_plan::spark_partition_id::SparkPartitionIdExec;
use sail_physical_plan::streaming::collector::StreamCollectorExec;
use sail_physical_plan::streaming::filter::StreamFilterExec;
use sail_physical_plan::streaming::limit::StreamLimitExec;
use sail_physical_plan::streaming::source_adapter::StreamSourceAdapterExec;
use sail_plan::catalog::CatalogCommandNode;

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
        // Note: the rewriter list is currently empty but may be useful for future logical rewrites.
        let rewriters: Vec<Box<dyn LogicalRewriter>> = vec![];
        let mut logical_plan = logical_plan.clone();
        for rewriter in rewriters {
            logical_plan = rewriter.rewrite(logical_plan)?.data
        }
        let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(DeltaPhysicalPlanner),
            Arc::new(IcebergPhysicalPlanner),
            Arc::new(SystemTablePhysicalPlanner),
            Arc::new(ListingPhysicalPlanner),
            Arc::new(ConsolePhysicalPlanner),
            Arc::new(NoopPhysicalPlanner),
            Arc::new(PythonPhysicalPlanner),
            Arc::new(ExtensionPhysicalPlanner),
        ];
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
            node.as_any().downcast_ref::<RemoteCheckpointCommandNode>()
        {
            let [_logical_input] = logical_inputs else {
                return internal_err!("RemoteCheckpointCommand requires exactly one logical input");
            };
            let [input] = physical_inputs else {
                return internal_err!(
                    "RemoteCheckpointCommand requires exactly one physical input"
                );
            };
            let registry = session_state.extension::<RemoteCheckpointRegistry>()?;
            let (object_store_url, prefix) = registry
                .resolve_relation(session_state.runtime_env().as_ref(), node.relation_id())?;
            let storage_schema = checkpoint_storage_schema(node.logical_schema());
            let output_partitioning =
                checkpoint_schema_partitioning(input.output_partitioning(), &storage_schema)?;
            let output_ordering = input
                .output_ordering()
                .map(|ordering| checkpoint_schema_ordering(ordering, &storage_schema))
                .transpose()?;
            let writer: Arc<dyn ExecutionPlan> = Arc::new(RemoteCheckpointWriteExec::try_new(
                Arc::clone(input),
                object_store_url.clone(),
                prefix.clone(),
                Arc::clone(&storage_schema),
            )?);
            let commit_input: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(writer));
            Arc::new(RemoteCheckpointCommitExec::new(
                commit_input,
                node.relation_id().to_string(),
                object_store_url,
                prefix,
                Arc::clone(node.logical_schema()),
                storage_schema,
                output_partitioning,
                output_ordering,
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<RemoteCheckpointRelationNode>() {
            let registry = session_state.extension::<RemoteCheckpointRegistry>()?;
            let descriptor = registry.get(node.relation_id())?.ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(format!(
                    "checkpoint relation is not available: {}",
                    node.relation_id()
                ))
            })?;
            let parquet_options = TableParquetOptions {
                global: session_state.config().options().execution.parquet.clone(),
                ..Default::default()
            };
            let storage_output_ordering = descriptor
                .output_ordering
                .as_ref()
                .map(|ordering| {
                    checkpoint_schema_ordering(ordering, descriptor.storage_schema.as_ref())
                })
                .transpose()?;
            let source = ParquetSource::new(TableSchema::new(
                Arc::clone(&descriptor.storage_schema),
                vec![],
            ))
            .with_table_parquet_options(parquet_options);
            // One file group per source partition, including empty partitions, keeps the saved
            // physical properties sound.
            let file_groups = descriptor
                .partitions
                .iter()
                .map(|partition| {
                    let files = partition
                        .file
                        .as_ref()
                        .map(|file| {
                            let mut statistics =
                                Statistics::new_unknown(descriptor.storage_schema.as_ref());
                            statistics.num_rows = Precision::Exact(
                                usize::try_from(file.row_count).map_err(|_| {
                                    datafusion_common::DataFusionError::Plan(
                                        "checkpoint row count is too large".to_string(),
                                    )
                                })?,
                            );
                            Ok(PartitionedFile::new(file.location.to_string(), file.size)
                                .with_statistics(Arc::new(statistics)))
                        })
                        .into_iter()
                        .collect::<datafusion_common::Result<Vec<_>>>()?;
                    Ok(FileGroup::new(files))
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            let total_rows = descriptor
                .partitions
                .iter()
                .try_fold(0_u64, |total, partition| {
                    total
                        .checked_add(
                            partition
                                .file
                                .as_ref()
                                .map(|file| file.row_count)
                                .unwrap_or(0),
                        )
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Plan(
                                "checkpoint row count overflow".to_string(),
                            )
                        })
                })?;
            let mut statistics = Statistics::new_unknown(descriptor.storage_schema.as_ref());
            statistics.num_rows = Precision::Exact(usize::try_from(total_rows).map_err(|_| {
                datafusion_common::DataFusionError::Plan(
                    "checkpoint row count is too large".to_string(),
                )
            })?);
            let scan =
                FileScanConfigBuilder::new(descriptor.object_store_url.clone(), Arc::new(source))
                    .with_file_groups(file_groups)
                    .with_statistics(statistics)
                    .with_output_ordering(storage_output_ordering.into_iter().collect())
                    .with_preserve_order(true)
                    .with_partitioned_by_file_group(true)
                    .build();
            let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan);
            let names = UserDefinedLogicalNode::schema(node)
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>();
            let scan = if names.is_empty() {
                Arc::new(ProjectionExec::try_new(
                    Vec::<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)>::new(),
                    scan,
                )?) as Arc<dyn ExecutionPlan>
            } else {
                rename_physical_plan(scan, &names)?
            };
            let output_partitioning = checkpoint_schema_partitioning(
                &descriptor.output_partitioning,
                scan.schema().as_ref(),
            )?;
            let output_ordering = descriptor
                .output_ordering
                .as_ref()
                .map(|ordering| checkpoint_schema_ordering(ordering, scan.schema().as_ref()))
                .transpose()?;
            Arc::new(RemoteCheckpointScanExec::new(
                scan,
                output_partitioning,
                output_ordering,
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<RangeNode>() {
            let schema = UserDefinedLogicalNode::schema(node).inner().clone();
            let projection = (0..schema.fields().len()).collect();
            Arc::new(RangeExec::try_new(
                node.range().clone(),
                node.num_partitions(),
                schema,
                projection,
            )?)
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
        } else if let Some(node) = node.as_any().downcast_ref::<MonotonicIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MonotonicIdExec requires exactly one physical input");
            };
            Arc::new(MonotonicIdExec::try_new(
                input.clone(),
                node.column_name().to_string(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<SparkPartitionIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SparkPartitionIdExec requires exactly one physical input");
            };
            Arc::new(SparkPartitionIdExec::try_new(
                input.clone(),
                node.column_name().to_string(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            )?)
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
        } else if let Some(node) = node.as_any().downcast_ref::<RequiredSortNode>() {
            let [input] = physical_inputs else {
                return internal_err!("RequiredSort requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                node.sort_expr(),
                UserDefinedLogicalNode::schema(node),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("RequiredSort requires at least one sort expression");
            };
            let sort = SortExec::new(ordering, input.clone())
                .with_fetch(node.fetch())
                .with_preserve_partitioning(node.preserve_partitioning());
            let requirements = OrderingRequirements::from(sort.expr().clone());
            let distribution = sort.required_input_distribution().swap_remove(0);
            Arc::new(OutputRequirementExec::new(
                Arc::new(sort),
                Some(requirements),
                distribution,
                node.fetch(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SchemaPivotExec requires exactly one physical input");
            };
            Arc::new(SchemaPivotExec::new(
                input.clone(),
                node.names().to_vec(),
                node.schema().inner().clone(),
            ))
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
                node.kind(),
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
        } else if let Some(node) = node.as_any().downcast_ref::<StreamFilterNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("StreamFilterExec requires exactly one logical input");
            };
            let [input] = physical_inputs else {
                return internal_err!("StreamFilterExec requires exactly one physical input");
            };
            let predicate = planner.create_physical_expr(
                node.predicate(),
                logical_input.schema(),
                session_state,
            )?;
            Arc::new(StreamFilterExec::try_new(input.clone(), predicate)?)
        } else if node.as_any().is::<StreamCollectorNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamCollectorExec requires exactly one physical input");
            };
            Arc::new(StreamCollectorExec::try_new(input.clone())?)
        } else if let Some(node) = node.as_any().downcast_ref::<CatalogCommandNode>() {
            let schema = node.schema().inner().clone();
            Arc::new(CatalogCommandExec::new(node.command().clone(), schema))
        } else if let Some(_node) = node.as_any().downcast_ref::<BarrierNode>() {
            let (plan, preconditions) = physical_inputs.split_last().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "{} requires at least one physical input",
                    BarrierExec::static_name()
                ))
            })?;
            if preconditions.is_empty() {
                plan.clone()
            } else {
                Arc::new(BarrierExec::new(preconditions.to_vec(), plan.clone()))
            }
        } else {
            return internal_err!("unsupported logical extension node: {:?}", node);
        };
        Ok(Some(plan))
    }
}

// Column ordinals survive logical/storage renames; names may be duplicated or synthetic.
fn checkpoint_schema_partitioning(
    partitioning: &Partitioning,
    schema: &datafusion::arrow::datatypes::Schema,
) -> datafusion_common::Result<Partitioning> {
    match partitioning {
        Partitioning::RoundRobinBatch(partitions) => Ok(Partitioning::RoundRobinBatch(*partitions)),
        Partitioning::Hash(expressions, partitions) => Ok(Partitioning::Hash(
            expressions
                .iter()
                .map(|expression| checkpoint_schema_expression(Arc::clone(expression), schema))
                .collect::<datafusion_common::Result<Vec<_>>>()?,
            *partitions,
        )),
        Partitioning::UnknownPartitioning(partitions) => {
            Ok(Partitioning::UnknownPartitioning(*partitions))
        }
    }
}

fn checkpoint_schema_ordering(
    ordering: &LexOrdering,
    schema: &datafusion::arrow::datatypes::Schema,
) -> datafusion_common::Result<LexOrdering> {
    let expressions = ordering
        .iter()
        .map(|sort| {
            Ok(PhysicalSortExpr::new(
                checkpoint_schema_expression(Arc::clone(&sort.expr), schema)?,
                sort.options,
            ))
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    LexOrdering::new(expressions)
        .ok_or_else(|| internal_datafusion_err!("checkpoint output ordering cannot be empty"))
}

fn checkpoint_schema_expression(
    expression: Arc<dyn PhysicalExpr>,
    schema: &datafusion::arrow::datatypes::Schema,
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    expression
        .transform_down(|expression| {
            let Some(column) = expression.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expression));
            };
            let field = schema.fields().get(column.index()).ok_or_else(|| {
                internal_datafusion_err!(
                    "checkpoint property references column {} at invalid index {}",
                    column.name(),
                    column.index()
                )
            })?;
            Ok(Transformed::yes(
                Arc::new(Column::new(field.name(), column.index())) as Arc<dyn PhysicalExpr>,
            ))
        })
        .data()
}

/// Plans the explicit repartitioning emitted by the logical planner.
/// Empty expressions keep round-robin explicit here, while hash repartitioning
/// and `UnknownPartitioning(1)` are normalized later by `RewriteExplicitRepartition`.
fn plan_explicit_partitioning(
    planner: &dyn PhysicalPlanner,
    schema: &DFSchema,
    input: &dyn ExecutionPlan,
    num_partitions: Option<usize>,
    kind: ExplicitRepartitionKind,
    expressions: &[Expr],
    session_state: &SessionState,
) -> datafusion_common::Result<Partitioning> {
    let input_partition_count = input.properties().output_partitioning().partition_count();
    match kind {
        ExplicitRepartitionKind::Coalesce => match num_partitions {
            Some(0) => internal_err!("number of explicit partitions cannot be zero"),
            Some(num_partitions) => Ok(Partitioning::UnknownPartitioning(
                num_partitions.min(input_partition_count),
            )),
            None => internal_err!("explicit coalesce requires a target partition count"),
        },
        ExplicitRepartitionKind::RoundRobin => match num_partitions {
            Some(0) => internal_err!("number of explicit partitions cannot be zero"),
            Some(num_partitions) => Ok(Partitioning::RoundRobinBatch(num_partitions)),
            None => {
                internal_err!("explicit round-robin repartition requires a target partition count")
            }
        },
        ExplicitRepartitionKind::Hash => {
            if expressions.is_empty() {
                return internal_err!(
                    "explicit hash repartitioning requires at least one partitioning expression"
                );
            }
            let num_partitions = match num_partitions {
                Some(0) => return internal_err!("number of explicit partitions cannot be zero"),
                Some(num_partitions) => num_partitions,
                None => input_partition_count,
            };
            let num_partitions = num_partitions.max(1);
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

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::col;
    use object_store::memory::InMemory;
    use sail_common::spec;
    use sail_plan::config::PlanConfig;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn checkpoint_properties_map_columns_to_target_schema_by_position() {
        let target_schema = Schema::new(vec![
            Field::new("__sail_checkpoint_col_00000", DataType::Int64, false),
            Field::new("__sail_checkpoint_col_00001", DataType::Int64, false),
        ]);
        let first = Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
        let second = Arc::new(Column::new("value", 1)) as Arc<dyn PhysicalExpr>;
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(first)]).unwrap();

        let partitioning =
            checkpoint_schema_partitioning(&Partitioning::Hash(vec![second], 2), &target_schema)
                .unwrap();
        let ordering = checkpoint_schema_ordering(&ordering, &target_schema).unwrap();

        let Partitioning::Hash(expressions, 2) = partitioning else {
            panic!("expected hash partitioning");
        };
        let partition_column = expressions[0].downcast_ref::<Column>().unwrap();
        assert_eq!(partition_column.name(), "__sail_checkpoint_col_00001");
        assert_eq!(partition_column.index(), 1);
        let ordering_column = ordering.first().expr.downcast_ref::<Column>().unwrap();
        assert_eq!(ordering_column.name(), "__sail_checkpoint_col_00000");
        assert_eq!(ordering_column.index(), 0);
    }

    #[tokio::test]
    async fn checkpoint_command_plans_writer_coalesce_and_commit() -> datafusion_common::Result<()>
    {
        let registry = Arc::new(RemoteCheckpointRegistry::new(Some(
            "memory:///checkpoint".to_string(),
        )));
        let config = SessionConfig::new().with_extension(registry);
        let runtime_env = Arc::new(RuntimeEnv::default());
        let object_store_url =
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://")?;
        runtime_env.register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);
        let command = spec::Plan::Command(spec::CommandPlan::new(
            spec::CommandNode::RemoteCheckpoint {
                relation_id: "relation".to_string(),
                input: Box::new(spec::QueryPlan::new(spec::QueryNode::Range(spec::Range {
                    start: Some(0),
                    end: 10,
                    step: 1,
                    num_partitions: Some(2),
                }))),
            },
        ));

        let (physical, _) =
            sail_plan::resolve_and_execute_plan(&context, Arc::new(PlanConfig::default()), command)
                .await
                .map_err(|error| internal_datafusion_err!("{error}"))?;

        let commit = physical
            .downcast_ref::<RemoteCheckpointCommitExec>()
            .ok_or_else(|| internal_datafusion_err!("checkpoint command did not plan a commit"))?;
        let coalesce = commit
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .ok_or_else(|| internal_datafusion_err!("checkpoint commit input is not coalesced"))?;
        assert!(coalesce.input().is::<RemoteCheckpointWriteExec>());
        Ok(())
    }

    fn plan_partitioning(
        input: &dyn ExecutionPlan,
        schema: &Arc<Schema>,
        num_partitions: Option<usize>,
        kind: ExplicitRepartitionKind,
        expressions: &[Expr],
    ) -> datafusion_common::Result<Partitioning> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![]);
        let session_state = SessionContext::new().state();
        let df_schema = schema.as_ref().clone().to_dfschema()?;

        plan_explicit_partitioning(
            &planner,
            &df_schema,
            input,
            num_partitions,
            kind,
            expressions,
            &session_state,
        )
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_round_robin_without_expressions() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));

        let partitioning = plan_partitioning(
            &input,
            &schema,
            Some(3),
            ExplicitRepartitionKind::RoundRobin,
            &[],
        )
        .unwrap();

        assert_eq!(partitioning, Partitioning::RoundRobinBatch(3));
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_single_round_robin_without_expressions() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));

        let partitioning = plan_partitioning(
            &input,
            &schema,
            Some(1),
            ExplicitRepartitionKind::RoundRobin,
            &[],
        )
        .unwrap();

        assert_eq!(partitioning, Partitioning::RoundRobinBatch(1));
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_error_for_zero_partitions() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));

        let error = plan_partitioning(
            &input,
            &schema,
            Some(0),
            ExplicitRepartitionKind::RoundRobin,
            &[],
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("number of explicit partitions cannot be zero")
        );
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_error_without_partition_count() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));

        let error = plan_partitioning(
            &input,
            &schema,
            None,
            ExplicitRepartitionKind::RoundRobin,
            &[],
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("explicit round-robin repartition requires a target partition count")
        );
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_hash_for_single_partition_with_expressions() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));
        let expressions = [col("id")];

        let partitioning = plan_partitioning(
            &input,
            &schema,
            Some(1),
            ExplicitRepartitionKind::Hash,
            &expressions,
        )
        .unwrap();

        match partitioning {
            Partitioning::Hash(expressions, 1) => assert_eq!(expressions.len(), 1),
            other => panic!("expected hash partitioning, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_explicit_partitioning_returns_hash_for_expressions() {
        let schema = schema();
        let input = EmptyExec::new(Arc::clone(&schema));
        let expressions = [col("id")];

        let partitioning = plan_partitioning(
            &input,
            &schema,
            Some(3),
            ExplicitRepartitionKind::Hash,
            &expressions,
        )
        .unwrap();

        match partitioning {
            Partitioning::Hash(expressions, 3) => assert_eq!(expressions.len(), 1),
            other => panic!("expected hash partitioning, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_explicit_partitioning_uses_input_partition_count_for_hash_without_explicit_count()
    {
        let schema = schema();
        let input = RepartitionExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Partitioning::RoundRobinBatch(4),
        )
        .unwrap();
        let expressions = [col("id")];

        let partitioning = plan_partitioning(
            &input,
            &schema,
            None,
            ExplicitRepartitionKind::Hash,
            &expressions,
        )
        .unwrap();

        match partitioning {
            Partitioning::Hash(expressions, 4) => assert_eq!(expressions.len(), 1),
            other => panic!("expected hash partitioning with inherited count, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_explicit_partitioning_caps_coalesce_to_input_partition_count() {
        let schema = schema();
        let input = RepartitionExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Partitioning::RoundRobinBatch(2),
        )
        .unwrap();

        let partitioning = plan_partitioning(
            &input,
            &schema,
            Some(4),
            ExplicitRepartitionKind::Coalesce,
            &[],
        )
        .unwrap();

        assert!(matches!(partitioning, Partitioning::UnknownPartitioning(2)));
    }
}
