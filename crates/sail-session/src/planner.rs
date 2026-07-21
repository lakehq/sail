use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{Partitioning, create_physical_sort_exprs};
use sail_cache::cached_relation::CachedRelationRegistry;
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
use sail_logical_plan::cached_relation::CachedRelationNode;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::range::RangeNode;
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
            node.as_any().downcast_ref::<CachedRelationNode>()
        {
            let registry = session_state.extension::<CachedRelationRegistry>()?;
            let relation = registry.get(node.relation_id())?.ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "No DataFrame with id {} is found",
                    node.relation_id()
                ))
            })?;
            let plan = relation.to_physical_plan(node.relation_id()).await?;
            let names = UserDefinedLogicalNode::schema(node)
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>();
            rename_physical_plan(plan, &names)?
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
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ToDFSchema;
    use datafusion_expr::col;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
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
