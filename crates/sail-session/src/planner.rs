use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, Statistics, internal_datafusion_err, internal_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::{Partitioning, RangePartitioning, create_physical_sort_exprs};
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_catalog_system::planner::SystemTablePhysicalPlanner;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use sail_common_datafusion::streaming::event::schema::{
    to_flow_event_field_names, to_flow_event_projection,
};
use sail_data_source::formats::console::ConsolePhysicalPlanner;
use sail_data_source::formats::noop::NoopPhysicalPlanner;
use sail_data_source::formats::python::PythonPhysicalPlanner;
use sail_data_source::listing::planner::ListingPhysicalPlanner;
use sail_delta_lake::logical::DeltaMetadataAggregateRewriter;
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
use sail_physical_plan::lake_procedure::LakeProcedureExec;
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::monotonic_id::MonotonicIdExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::remote_checkpoint::{
    CheckpointDataSource, RemoteCheckpointCommitExec, RemoteCheckpointWriteExec,
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
use sail_plan::procedure::LakeProcedureNode;

use crate::scalar_subquery::{
    ensure_scalar_subquery_nullability, share_equivalent_scalar_subqueries,
};

#[derive(Debug)]
pub struct ExtensionQueryPlanner {}

pub struct ExtensionPhysicalPlanner;

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(ExtensionQueryPlanner {})
}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: show rewriters and the final logical plan in `EXPLAIN`.
        let rewriters: Vec<Box<dyn LogicalRewriter>> = vec![
            Box::new(DeltaMetadataAggregateRewriter),
            Box::new(sail_iceberg::logical::IcebergMetadataAggregateRewriter),
        ];
        // Equivalent scalar subqueries are shared before the rewriters, which give each
        // rewritten scan its own source. The rewritten bodies remain equal, so DataFusion
        // still executes them once.
        let mut logical_plan = share_equivalent_scalar_subqueries(logical_plan.clone())?;
        for rewriter in rewriters {
            logical_plan = rewriter.rewrite(logical_plan, session).await?.data;
        }
        logical_plan = sail_plan::function::prepare_async_functions(logical_plan)?;
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
        let plan = planner.create_physical_plan(&logical_plan, session).await?;
        ensure_scalar_subquery_nullability(plan)
    }
}

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
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
            let registry = session.extension::<RemoteCheckpointRegistry>()?;
            let (object_store_url, prefix) =
                registry.resolve_relation(session.runtime_env().as_ref(), node.relation_id())?;
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
                input.output_ordering().cloned(),
            )?);
            Arc::new(RemoteCheckpointCommitExec::new(
                writer,
                node.relation_id().to_string(),
                object_store_url,
                prefix,
                Arc::clone(node.logical_schema()),
                storage_schema,
                output_partitioning,
                output_ordering,
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<RemoteCheckpointRelationNode>() {
            let registry = session.extension::<RemoteCheckpointRegistry>()?;
            let descriptor = registry.get(node.relation_id())?.ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(format!(
                    "checkpoint relation is not available: {}",
                    node.relation_id()
                ))
            })?;
            let logical_schema = UserDefinedLogicalNode::schema(node).inner().clone();
            let schema_matches = descriptor.storage_schema.fields().len()
                == logical_schema.fields().len()
                && descriptor
                    .storage_schema
                    .fields()
                    .iter()
                    .zip(logical_schema.fields())
                    .all(|(storage, logical)| storage.data_type() == logical.data_type());
            if !schema_matches {
                return internal_err!(
                    "checkpoint storage schema does not match the relation schema by position"
                );
            }
            let output_partitioning = checkpoint_schema_partitioning(
                &descriptor.output_partitioning,
                logical_schema.as_ref(),
            )?;
            let storage_output_ordering = descriptor
                .output_ordering
                .as_ref()
                .map(|ordering| {
                    checkpoint_schema_ordering(ordering, descriptor.storage_schema.as_ref())
                })
                .transpose()?;
            let scan: Arc<dyn ExecutionPlan> = if descriptor.storage_schema.fields().is_empty() {
                let partitions = descriptor
                    .partitions
                    .iter()
                    .map(|partition| {
                        let row_count = usize::try_from(partition.row_count).map_err(|_| {
                            datafusion_common::DataFusionError::Plan(
                                "checkpoint row count is too large".to_string(),
                            )
                        })?;
                        let batch = RecordBatch::try_new_with_options(
                            Arc::clone(&descriptor.storage_schema),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(row_count)),
                        )?;
                        Ok(vec![batch])
                    })
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                let source = MemorySourceConfig::try_new(
                    &partitions,
                    Arc::clone(&descriptor.storage_schema),
                    None,
                )?
                .try_with_sort_information(storage_output_ordering.clone().into_iter().collect())?;
                Arc::new(DataSourceExec::new(Arc::new(CheckpointDataSource::new(
                    Arc::new(source),
                    output_partitioning,
                ))))
            } else {
                let parquet_options = TableParquetOptions {
                    global: session.config().options().execution.parquet.clone(),
                    ..Default::default()
                };
                let source =
                    ParquetSource::new(TableSchema::from(Arc::clone(&descriptor.storage_schema)))
                        .with_table_parquet_options(parquet_options);
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
                                    usize::try_from(partition.row_count).map_err(|_| {
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
                let total_rows =
                    descriptor
                        .partitions
                        .iter()
                        .try_fold(0_u64, |total, partition| {
                            total.checked_add(partition.row_count).ok_or_else(|| {
                                datafusion_common::DataFusionError::Plan(
                                    "checkpoint row count overflow".to_string(),
                                )
                            })
                        })?;
                let mut statistics = Statistics::new_unknown(descriptor.storage_schema.as_ref());
                statistics.num_rows =
                    Precision::Exact(usize::try_from(total_rows).map_err(|_| {
                        datafusion_common::DataFusionError::Plan(
                            "checkpoint row count is too large".to_string(),
                        )
                    })?);
                let scan = FileScanConfigBuilder::new(
                    descriptor.object_store_url.clone(),
                    Arc::new(source),
                )
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_output_ordering(storage_output_ordering.into_iter().collect())
                .with_preserve_order(true)
                .build();
                let projection = descriptor
                    .storage_schema
                    .fields()
                    .iter()
                    .zip(logical_schema.fields())
                    .enumerate()
                    .map(|(index, (storage, logical))| {
                        ProjectionExpr::new(
                            Arc::new(Column::new(storage.name(), index)) as Arc<dyn PhysicalExpr>,
                            logical.name(),
                        )
                    })
                    .collect::<ProjectionExprs>();
                let source = scan
                    .try_swapping_with_projection(&projection)?
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "checkpoint Parquet source rejected its column projection"
                        )
                    })?;
                Arc::new(DataSourceExec::new(Arc::new(CheckpointDataSource::new(
                    source,
                    output_partitioning,
                ))))
            };
            scan
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
                session.execution_props(),
                planning_ctx,
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
                session.execution_props(),
                planning_ctx,
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("RequiredSort requires at least one sort expression");
            };
            let sort = SortExec::new(ordering, input.clone())
                .with_fetch(node.fetch())
                .with_preserve_partitioning(node.preserve_partitioning());
            let requirements = OrderingRequirements::from(sort.expr().clone());
            let distribution = sort
                .input_distribution_requirements()
                .into_per_child()
                .swap_remove(0);
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
                session,
                planning_ctx,
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
                .scan(session, node.projection(), node.filters(), node.fetch())
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
                session,
                planning_ctx,
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
        } else if let Some(node) = node.as_any().downcast_ref::<LakeProcedureNode>() {
            let [input] = physical_inputs else {
                return internal_err!("LakeProcedure requires exactly one physical input");
            };
            sail_iceberg::physical_plan::validate_iceberg_procedure_call_identity(
                input,
                node.call(),
            )?;
            let procedure = LakeProcedureExec::try_new(
                node.call().clone(),
                input.clone(),
                node.root_placement(),
            )?;
            Arc::new(procedure)
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
        Partitioning::Range(range) => Ok(Partitioning::Range(RangePartitioning::try_new(
            checkpoint_schema_ordering(range.ordering(), schema)?,
            range.split_points().to_vec(),
        )?)),
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
    session: &dyn Session,
    planning_ctx: &PhysicalPlanningContext,
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
                .map(|e| planner.create_physical_expr(e, schema, session, planning_ctx))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(Partitioning::Hash(expressions, num_partitions))
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, Int64Array};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use datafusion::catalog::TableFunctionArgs;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::{ScalarValue, ToDFSchema};
    use datafusion_expr::{
        AggregateUDF, ColumnarValue, LogicalPlanBuilder, TableSource, Volatility, cast, col,
        create_udf, lit, rollup, scalar_subquery, when,
    };
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use object_store::memory::InMemory;
    use sail_common::spec;
    use sail_common_datafusion::min_max::min_max_agg::Max;
    use sail_plan::config::PlanConfig;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn scalar_subquery_projection_allows_zero_row_null() -> datafusion_common::Result<()> {
        use datafusion::logical_expr::physical_planning_context::{
            ScalarSubqueryResults, SubqueryIndex,
        };
        use datafusion::physical_expr::expressions::BinaryExpr;

        let input_schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));
        let scalar = Arc::new(ScalarSubqueryExpr::new(
            DataType::Int64,
            false,
            SubqueryIndex::new(0),
            ScalarSubqueryResults::new(1),
        ));
        let shifted = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            datafusion_expr::Operator::Plus,
            scalar,
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            [ProjectionExpr::new(shifted, "shifted")],
            input,
        )?);

        assert!(!projection.schema().field(0).is_nullable());
        let projection = ensure_scalar_subquery_nullability(projection)?;
        assert!(projection.schema().field(0).is_nullable());
        Ok(())
    }

    fn memory_source(values: Vec<i64>) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        Ok(provider_as_source(Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch]],
        )?)))
    }

    /// Builds `(SELECT <column> FROM scan WHERE k = 2)` the way Sail resolves a CTE
    /// reference, where both the CTE body and the reference get fresh output names.
    fn renamed_scalar_subquery(
        scan: &LogicalPlan,
        column: &str,
        body_name: &str,
        reference_name: &str,
    ) -> datafusion_common::Result<Expr> {
        let subquery = LogicalPlanBuilder::from(scan.clone())
            .filter(col("k").eq(lit(2_i64)))?
            .project(vec![col(column).alias(body_name)])?
            .alias("cte")?
            .project(vec![
                Expr::Column(datafusion_common::Column::new(Some("cte"), body_name))
                    .alias(reference_name),
            ])?
            .build()?;
        Ok(scalar_subquery(Arc::new(subquery)))
    }

    async fn plan_and_collect(
        plan: LogicalPlan,
    ) -> datafusion_common::Result<(String, RecordBatch)> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);
        let state = context.state();
        let physical = state
            .query_planner()
            .create_physical_plan(&plan, &state)
            .await?;
        let display = displayable(physical.as_ref()).indent(false).to_string();
        let schema = physical.schema();
        let batches = collect(physical, context.task_ctx()).await?;
        Ok((display, concat_batches(&schema, &batches)?))
    }

    /// Builds `(SELECT max(<column>) FROM <scan>)` the way Sail resolves a table reference,
    /// where each reference gets fresh column names and the aggregate is aliased above it.
    fn aggregate_scalar_subquery(
        scan: LogicalPlanBuilder,
        column: Expr,
        input_name: &str,
        output_name: &str,
    ) -> datafusion_common::Result<Expr> {
        let max = AggregateUDF::from(Max::new()).call(vec![col(input_name)]);
        let subquery = scan
            .project(vec![column.alias(input_name)])?
            .aggregate(Vec::<Expr>::new(), vec![max.clone()])?
            .project(vec![max.alias(output_name)])?
            .build()?;
        Ok(scalar_subquery(Arc::new(subquery)))
    }

    #[tokio::test]
    async fn scalar_subqueries_differing_in_generated_names_share_one_execution()
    -> datafusion_common::Result<()> {
        let left = memory_source(vec![10, 20])?;
        let right = memory_source(vec![100, 200])?;
        let scan = |name: &str, source: &Arc<dyn TableSource>| {
            LogicalPlanBuilder::scan(name, Arc::clone(source), None)
        };
        let join = || scan("a", &left)?.cross_join(scan("b", &right)?.build()?);
        // A CTE whose first reference reads the body directly, while later references
        // rename its columns. Both compute the same as reading the table directly.
        let cte = || {
            scan("t", &left)?
                .project(vec![col("t.v").alias("#20")])?
                .alias("cte")
        };
        let renamed_cte = || cte()?.project(vec![col("cte.#20").alias("#30")]);
        // A projection that only renames the columns, and one that also sets field metadata.
        let renamed = |metadata: Option<BTreeMap<String, String>>| {
            scan("t", &left)?.project(vec![
                col("t.k").alias("#40"),
                col("t.v").alias_with_metadata("#41", metadata.map(Into::into)),
            ])
        };
        let metadata = BTreeMap::from([("key".to_string(), "value".to_string())]);
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                aggregate_scalar_subquery(scan("t", &left)?, col("t.v"), "#1", "#2")?.alias("a"),
                // Fresh names and another spelling of the table name are the same computation.
                aggregate_scalar_subquery(scan("T", &left)?, col("T.v"), "#3", "#4")?.alias("b"),
                // A different column is a different computation.
                aggregate_scalar_subquery(scan("t", &left)?, col("t.k"), "#5", "#6")?.alias("c"),
                // The same column of different relations is a different computation.
                aggregate_scalar_subquery(join()?, col("a.v"), "#7", "#8")?.alias("d"),
                aggregate_scalar_subquery(join()?, col("b.v"), "#9", "#10")?.alias("e"),
                aggregate_scalar_subquery(cte()?, col("cte.#20"), "#21", "#22")?.alias("f"),
                aggregate_scalar_subquery(renamed_cte()?, col("#30"), "#31", "#32")?.alias("g"),
                aggregate_scalar_subquery(renamed(None)?, col("#41"), "#42", "#43")?.alias("h"),
                aggregate_scalar_subquery(renamed(Some(metadata))?, col("#41"), "#44", "#45")?
                    .alias("i"),
            ])?
            .build()?;

        let (display, batch) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=5"),
            "{display}"
        );
        let values = batch
            .columns()
            .iter()
            .map(|column| column.as_primitive::<Int64Type>().value(0))
            .collect::<Vec<_>>();
        assert_eq!(values, [20, 20, 2, 20, 200, 20, 20, 20, 20]);
        Ok(())
    }

    #[tokio::test]
    async fn equivalent_scalar_subqueries_share_one_execution() -> datafusion_common::Result<()> {
        let first = LogicalPlanBuilder::scan("t", memory_source(vec![10, 20])?, None)?.build()?;
        // The same table name and schema with different data: only the source differs.
        let second =
            LogicalPlanBuilder::scan("t", memory_source(vec![100, 200])?, None)?.build()?;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                renamed_scalar_subquery(&first, "v", "#1", "#11")?.alias("a"),
                // The name of an unaliased subquery must survive sharing its body.
                renamed_scalar_subquery(&first, "v", "#2", "#12")?,
                renamed_scalar_subquery(&second, "v", "#3", "#13")?.alias("c"),
                // A different column of the same input is a different computation.
                renamed_scalar_subquery(&first, "k", "#4", "#14")?.alias("d"),
            ])?
            .build()?;

        let (display, batch) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=3"),
            "{display}"
        );
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a", "#12", "c", "d"]);
        let values = batch
            .columns()
            .iter()
            .map(|column| column.as_primitive::<Int64Type>().value(0))
            .collect::<Vec<_>>();
        assert_eq!(values, [20, 20, 200, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_with_literals_of_different_time_zones_are_not_shared()
    -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan("t", memory_source(vec![10, 20])?, None)?.build()?;
        // Literal equality ignores the time zone, but the formatted values differ.
        let subquery = |time_zone: Option<&str>, name: &str| -> datafusion_common::Result<Expr> {
            let timestamp = lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_087_000_000_000),
                time_zone.map(Into::into),
            ));
            let subquery = LogicalPlanBuilder::from(scan.clone())
                .filter(col("k").eq(lit(2_i64)))?
                .project(vec![
                    cast(
                        when(col("v").gt(lit(0_i64)), timestamp).end()?,
                        DataType::Utf8,
                    )
                    .alias(name),
                ])?
                .build()?;
            Ok(scalar_subquery(Arc::new(subquery)))
        };
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                subquery(None, "#1")?.alias("a"),
                subquery(Some("+05:30"), "#2")?.alias("b"),
            ])?
            .build()?;

        let (display, batch) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        let values = batch
            .columns()
            .iter()
            .map(|column| column.as_string::<i32>().value(0).to_string())
            .collect::<Vec<_>>();
        assert_ne!(values[0], values[1]);
        Ok(())
    }

    #[tokio::test]
    async fn coerced_scalar_subqueries_share_one_execution() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan("t", memory_source(vec![10, 20])?, None)?.build()?;
        // Type coercion casts the renamed output of each reference.
        let subquery = |body_name: &str, reference_name: &str| -> datafusion_common::Result<Expr> {
            let subquery = LogicalPlanBuilder::from(scan.clone())
                .filter(col("k").eq(lit(2_i64)))?
                .project(vec![col("v").alias(body_name)])?
                .alias("cte")?
                .project(vec![cast(
                    Expr::Column(datafusion_common::Column::new(Some("cte"), body_name))
                        .alias(reference_name),
                    DataType::Float64,
                )])?
                .build()?;
            Ok(scalar_subquery(Arc::new(subquery)))
        };
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                subquery("#1", "#11")?.alias("a"),
                subquery("#2", "#12")?.alias("b"),
            ])?
            .build()?;

        let (display, batch) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=1"),
            "{display}"
        );
        let values = batch
            .columns()
            .iter()
            .map(|column| column.as_primitive::<Float64Type>().value(0))
            .collect::<Vec<_>>();
        assert_eq!(values, [20.0, 20.0]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_in_grouping_sets_are_not_rewritten() -> datafusion_common::Result<()>
    {
        let scan = LogicalPlanBuilder::scan("t", memory_source(vec![10, 20])?, None)?.build()?;
        let plan = LogicalPlanBuilder::empty(true)
            .aggregate(
                vec![rollup(vec![
                    renamed_scalar_subquery(&scan, "v", "#1", "#11")?,
                    renamed_scalar_subquery(&scan, "v", "#2", "#12")?,
                ])],
                Vec::<Expr>::new(),
            )?
            .build()?;

        let (display, _) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        Ok(())
    }

    /// Builds a range the way Sail resolves `spark.range`, which gives each reference a
    /// generated output name.
    fn range(name: &str, end: i64) -> datafusion_common::Result<LogicalPlan> {
        Ok(LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(RangeNode::try_new(name.to_string(), 0, end, 1, 2)?),
        }))
    }

    /// Builds `(SELECT max(<input>) FROM <input>)` with the aggregate aliased above it.
    fn max_scalar_subquery(
        input: LogicalPlan,
        input_name: &str,
        output_name: &str,
    ) -> datafusion_common::Result<Expr> {
        let max = Arc::new(AggregateUDF::new_from_impl(Max::new()));
        let subquery = LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![max.call(vec![col(input_name)])])?
            .build()?;
        let aggregate = subquery.schema().field(0).name().clone();
        let subquery = LogicalPlanBuilder::from(subquery)
            .project(vec![col(aggregate).alias(output_name)])?
            .build()?;
        Ok(scalar_subquery(Arc::new(subquery)))
    }

    async fn plan_two_scalar_subqueries(
        a: Expr,
        b: Expr,
    ) -> datafusion_common::Result<(String, RecordBatch)> {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![a.alias("a"), b.alias("b")])?
            .build()?;
        plan_and_collect(plan).await
    }

    fn int64_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .columns()
            .iter()
            .map(|column| column.as_primitive::<Int64Type>().value(0))
            .collect()
    }

    #[tokio::test]
    async fn scalar_subqueries_over_ranges_are_shared() -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(range("#1", 10)?, "#1", "#2")?,
            max_scalar_subquery(range("#3", 10)?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=1"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 9]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_over_different_ranges_are_not_shared()
    -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(range("#1", 10)?, "#1", "#2")?,
            max_scalar_subquery(range("#3", 20)?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 19]);
        Ok(())
    }

    /// Builds a scan of the `range` table function the way Sail resolves `range(<args>)` in
    /// SQL, which creates a new table source for every reference.
    fn range_table_scan(name: &str, args: &[i64]) -> datafusion_common::Result<LogicalPlan> {
        let function = sail_plan::function::get_built_in_table_function("range")
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let state = SessionContext::new().state();
        let args = args.iter().map(|arg| lit(*arg)).collect::<Vec<_>>();
        let provider =
            function.create_table_provider_with_args(TableFunctionArgs::new(&args, &state))?;
        LogicalPlanBuilder::scan("range", provider_as_source(provider), None)?
            .project(vec![col("id").alias(name)])?
            .build()
    }

    #[tokio::test]
    async fn scalar_subqueries_over_range_table_functions_are_shared()
    -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(range_table_scan("#1", &[10])?, "#1", "#2")?,
            max_scalar_subquery(range_table_scan("#3", &[10])?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=1"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 9]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_over_different_range_table_functions_are_not_shared()
    -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(range_table_scan("#1", &[10])?, "#1", "#2")?,
            max_scalar_subquery(range_table_scan("#3", &[20])?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 19]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_over_range_table_functions_with_different_partitions_are_not_shared()
    -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(range_table_scan("#1", &[0, 10, 1, 2])?, "#1", "#2")?,
            max_scalar_subquery(range_table_scan("#3", &[0, 10, 1, 3])?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 9]);
        Ok(())
    }

    /// Builds `SELECT <name> FROM range(10) SORT BY <name>` with a required sort and a
    /// fetch above it, the way Sail keeps the sort below an ordered aggregate.
    fn sorted_range(name: &str, asc: bool) -> datafusion_common::Result<LogicalPlan> {
        let sort = vec![col(name).sort(asc, true)];
        let sort_within_partitions = LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(SortWithinPartitionsNode::new(
                Arc::new(range(name, 10)?),
                sort.clone(),
                None,
            )),
        });
        Ok(LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(RequiredSortNode::new(
                Arc::new(sort_within_partitions),
                sort,
                Some(1),
                false,
            )),
        }))
    }

    #[tokio::test]
    async fn scalar_subqueries_over_sorts_are_shared() -> datafusion_common::Result<()> {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(sorted_range("#1", false)?, "#1", "#2")?,
            max_scalar_subquery(sorted_range("#3", false)?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=1"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 9]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_over_different_sorts_are_not_shared() -> datafusion_common::Result<()>
    {
        let (display, batch) = plan_two_scalar_subqueries(
            max_scalar_subquery(sorted_range("#1", false)?, "#1", "#2")?,
            max_scalar_subquery(sorted_range("#3", true)?, "#3", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        assert_eq!(int64_values(&batch), vec![9, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_subqueries_over_monotonic_ids_are_not_shared() -> datafusion_common::Result<()>
    {
        let monotonic_id = |input: &str, output: &str| -> datafusion_common::Result<LogicalPlan> {
            Ok(LogicalPlan::Extension(datafusion_expr::Extension {
                node: Arc::new(MonotonicIdNode::try_new(
                    Arc::new(range(input, 10)?),
                    output.to_string(),
                )?),
            }))
        };
        let (display, _) = plan_two_scalar_subqueries(
            max_scalar_subquery(monotonic_id("#1", "#2")?, "#2", "#3")?,
            max_scalar_subquery(monotonic_id("#1", "#2")?, "#2", "#4")?,
        )
        .await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn volatile_scalar_subqueries_are_not_shared() -> datafusion_common::Result<()> {
        let volatile = create_udf(
            "volatile",
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Volatile,
            Arc::new(
                |args: &[ColumnarValue]| -> datafusion_common::Result<ColumnarValue> {
                    Ok(args[0].clone())
                },
            ),
        );
        let subquery = |name: &str| -> datafusion_common::Result<Expr> {
            let subquery = LogicalPlanBuilder::empty(true)
                .project(vec![volatile.call(vec![lit(1_i64)]).alias(name)])?
                .build()?;
            Ok(scalar_subquery(Arc::new(subquery)))
        };
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![subquery("#1")?.alias("a"), subquery("#2")?.alias("b")])?
            .build()?;

        let (display, _) = plan_and_collect(plan).await?;

        assert!(
            display.contains("ScalarSubqueryExec: subqueries=2"),
            "{display}"
        );
        Ok(())
    }

    #[test]
    fn checkpoint_properties_map_columns_to_target_schema_by_position() {
        let target_schema = Schema::new(vec![
            Field::new("_c0", DataType::Int64, false),
            Field::new("_c1", DataType::Int64, false),
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
        assert_eq!(partition_column.name(), "_c1");
        assert_eq!(partition_column.index(), 1);
        let ordering_column = ordering.first().expr.downcast_ref::<Column>().unwrap();
        assert_eq!(ordering_column.name(), "_c0");
        assert_eq!(ordering_column.index(), 0);
    }

    #[tokio::test]
    async fn checkpoint_command_optimizer_coalesces_writer_before_commit()
    -> datafusion_common::Result<()> {
        let registry = Arc::new(RemoteCheckpointRegistry::new(
            Some("memory:///checkpoint".to_string()),
            "session".to_string(),
        ));
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

    #[tokio::test]
    async fn zero_column_checkpoint_scan_preserves_partition_rows() -> datafusion_common::Result<()>
    {
        let registry = Arc::new(RemoteCheckpointRegistry::new(
            Some("memory:///checkpoint".to_string()),
            "session".to_string(),
        ));
        registry.insert(sail_cache::remote_checkpoint::RemoteCheckpointDescriptor {
            relation_id: "zero-columns".to_string(),
            object_store_url: datafusion::execution::object_store::ObjectStoreUrl::parse(
                "memory://",
            )?,
            prefix: object_store::path::Path::from("checkpoint/session/zero-columns"),
            logical_schema: Arc::new(Schema::empty()),
            storage_schema: Arc::new(Schema::empty()),
            output_partitioning: Partitioning::RoundRobinBatch(2),
            output_ordering: None,
            partitions: vec![
                sail_cache::remote_checkpoint::RemoteCheckpointPartition {
                    partition: 0,
                    row_count: 2,
                    file: None,
                },
                sail_cache::remote_checkpoint::RemoteCheckpointPartition {
                    partition: 1,
                    row_count: 3,
                    file: None,
                },
            ],
        })?;
        let config = SessionConfig::new().with_extension(registry);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);
        let logical = LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(RemoteCheckpointRelationNode::try_new(
                "zero-columns".to_string(),
                Arc::new(Schema::empty()),
            )?),
        });
        let state = context.state();
        let physical = state
            .query_planner()
            .create_physical_plan(&logical, &state)
            .await?;

        assert!(physical.is::<DataSourceExec>());
        assert!(matches!(
            physical.output_partitioning(),
            Partitioning::RoundRobinBatch(2)
        ));
        let batches = collect(physical, context.task_ctx()).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_scan_uses_data_source_properties() -> datafusion_common::Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let storage_schema = checkpoint_storage_schema(&logical_schema);
        let storage_key = Arc::new(Column::new("_c0", 0)) as Arc<dyn PhysicalExpr>;
        let registry = Arc::new(RemoteCheckpointRegistry::new(
            Some("memory:///checkpoint".to_string()),
            "session".to_string(),
        ));
        registry.insert(sail_cache::remote_checkpoint::RemoteCheckpointDescriptor {
            relation_id: "relation".to_string(),
            object_store_url: datafusion::execution::object_store::ObjectStoreUrl::parse(
                "memory://",
            )?,
            prefix: object_store::path::Path::from("checkpoint/session/relation"),
            logical_schema: Arc::clone(&logical_schema),
            storage_schema,
            output_partitioning: Partitioning::Hash(vec![Arc::clone(&storage_key)], 2),
            output_ordering: LexOrdering::new([PhysicalSortExpr::new_default(storage_key)]),
            partitions: (0..2)
                .map(
                    |partition| sail_cache::remote_checkpoint::RemoteCheckpointPartition {
                        partition,
                        row_count: 0,
                        file: None,
                    },
                )
                .collect(),
        })?;
        let config = SessionConfig::new().with_extension(registry);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);
        let logical = LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(RemoteCheckpointRelationNode::try_new(
                "relation".to_string(),
                logical_schema,
            )?),
        });
        let state = context.state();
        let physical = state
            .query_planner()
            .create_physical_plan(&logical, &state)
            .await?;

        assert!(physical.is::<DataSourceExec>());
        let Partitioning::Hash(expressions, 2) = physical.output_partitioning() else {
            return internal_err!("checkpoint scan lost its hash partitioning");
        };
        let partition_column = expressions[0]
            .downcast_ref::<Column>()
            .ok_or_else(|| internal_datafusion_err!("checkpoint hash key is not a column"))?;
        assert_eq!(partition_column.name(), "id");
        assert_eq!(partition_column.index(), 0);
        let ordering = physical
            .output_ordering()
            .ok_or_else(|| internal_datafusion_err!("checkpoint scan lost its ordering"))?;
        let ordering_column = ordering[0]
            .expr
            .downcast_ref::<Column>()
            .ok_or_else(|| internal_datafusion_err!("checkpoint ordering key is not a column"))?;
        assert_eq!(ordering_column.name(), "id");
        assert_eq!(ordering_column.index(), 0);

        let projected_logical = LogicalPlanBuilder::from(logical)
            .project(vec![col("id").alias("key")])?
            .build()?;
        let projected = state
            .query_planner()
            .create_physical_plan(&projected_logical, &state)
            .await?;
        let Partitioning::Hash(expressions, 2) = projected.output_partitioning() else {
            return internal_err!("checkpoint projection lost its hash partitioning");
        };
        let partition_column = expressions[0]
            .downcast_ref::<Column>()
            .ok_or_else(|| internal_datafusion_err!("projected hash key is not a column"))?;
        assert_eq!(partition_column.name(), "key");
        assert_eq!(partition_column.index(), 0);
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
        let planning_ctx = PhysicalPlanningContext::default();
        let df_schema = schema.as_ref().clone().to_dfschema()?;

        plan_explicit_partitioning(
            &planner,
            &df_schema,
            input,
            num_partitions,
            kind,
            expressions,
            &session_state,
            &planning_ctx,
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
