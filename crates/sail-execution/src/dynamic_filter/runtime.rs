use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, TryStreamExt};
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;
use tokio_util::task::AbortOnDropHandle;

use super::wire::{snapshot, union_predicates};
use super::{DynamicFilterBinding, DynamicFilterBuildExec};
use crate::driver::r#gen::{DynamicFilterUpdate, ExchangeDynamicFiltersResponse};
use crate::driver::{DriverActor, DriverClient, DriverMessage};
use crate::error::ExecutionResult;
use crate::id::TaskKey;
use crate::proto::{
    RemoteExecutionCodec, decode_remote_physical_expr, encode_remote_physical_expr,
};

#[derive(Clone)]
pub(crate) enum DynamicFilterClient {
    Driver(ActorHandle<DriverActor>),
    Worker(DriverClient),
}

impl DynamicFilterClient {
    async fn exchange(
        &self,
        key: &TaskKey,
        updates: Vec<DynamicFilterUpdate>,
        revision: u64,
    ) -> ExecutionResult<ExchangeDynamicFiltersResponse> {
        match self {
            Self::Worker(client) => {
                client
                    .exchange_dynamic_filters(key, updates, revision)
                    .await
            }
            Self::Driver(driver) => {
                let (tx, rx) = oneshot::channel();
                driver
                    .send(DriverMessage::ExchangeDynamicFilters {
                        key: key.clone(),
                        updates,
                        revision,
                        result: tx,
                    })
                    .await?;
                rx.await?
            }
        }
    }
}

struct Producer {
    expr: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
}

pub(crate) struct TaskDynamicFilters {
    producers: HashMap<u64, Vec<Producer>>,
    consumers: HashMap<u64, DynamicFilterBinding>,
    sent: HashMap<u64, Vec<(u64, bool)>>,
    revision: u64,
}

impl TaskDynamicFilters {
    pub fn prepare(
        plan: Arc<dyn ExecutionPlan>,
        mut consumers: HashMap<u64, DynamicFilterBinding>,
        filter_ids: &[u64],
        context: &TaskContext,
    ) -> Result<(Arc<dyn ExecutionPlan>, Self)> {
        let mut producers = HashMap::<u64, Vec<Producer>>::new();
        let mut consumer_ids = HashSet::new();
        consumers.retain(|id, _| filter_ids.contains(id));
        let plan = plan
            .transform_up(|plan| {
                let produced = plan.dynamic_expressions_produced();
                consumer_ids.extend(super::consumer_filter_ids(plan.as_ref())?);
                let Some(expr) = produced.first() else {
                    return Ok(Transformed::no(plan));
                };
                if !expr.is::<DynamicFilterPhysicalExpr>() {
                    return Ok(Transformed::no(plan));
                }
                if !expr
                    .expression_id()
                    .is_some_and(|id| filter_ids.contains(&id))
                {
                    return Ok(Transformed::no(plan));
                }
                let schema = if let Some(build) = plan.downcast_ref::<DynamicFilterBuildExec>() {
                    build.probe_schema.clone()
                } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
                    sort.input().schema()
                } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
                    aggregate.input().schema()
                } else {
                    return Err(plan_datafusion_err!(
                        "unsupported dynamic filter producer {}",
                        plan.name()
                    ));
                };
                let bytes = encode_remote_physical_expr(&RemoteExecutionCodec, expr)?;
                let expr =
                    decode_remote_physical_expr(context, &RemoteExecutionCodec, &bytes, &schema)?;
                let filter = (expr.clone() as Arc<dyn Any + Send + Sync>)
                    .downcast::<DynamicFilterPhysicalExpr>()
                    .map_err(|_| plan_datafusion_err!("expected a dynamic filter"))?;
                let id = expr
                    .expression_id()
                    .ok_or_else(|| plan_datafusion_err!("dynamic filter has no ID"))?;
                let updated: Arc<dyn ExecutionPlan> =
                    if let Some(build) = plan.downcast_ref::<DynamicFilterBuildExec>() {
                        Arc::new(DynamicFilterBuildExec {
                            filter: filter.clone(),
                            ..build.clone()
                        })
                    } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
                        Arc::new(sort.clone().with_dynamic_filter_expr(filter)?)
                    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
                        Arc::new(aggregate.clone().with_dynamic_filter_expr(filter)?)
                    } else {
                        return Ok(Transformed::no(plan));
                    };
                producers
                    .entry(id)
                    .or_default()
                    .push(Producer { expr, schema });
                Ok(Transformed::yes(updated))
            })
            .data()?;
        consumers.retain(|id, _| consumer_ids.contains(id));
        Ok((
            plan,
            Self {
                producers,
                consumers,
                sent: HashMap::new(),
                revision: 0,
            },
        ))
    }

    pub fn stream(
        mut self,
        input: SendableRecordBatchStream,
        key: TaskKey,
        client: DynamicFilterClient,
        context: Arc<TaskContext>,
    ) -> SendableRecordBatchStream {
        if self.producers.is_empty() && self.consumers.is_empty() {
            return input;
        }
        let schema = input.schema();
        let output = async_stream::try_stream! {
            // Fetch completed build filters before opening a newly scheduled scan.
            let _ = tokio::time::timeout(Duration::from_secs(1), self.exchange(&key, &client, &context, false)).await;
            let (finish, finished) = oneshot::channel();
            let mut transport = AbortOnDropHandle::new(tokio::spawn(self.synchronize(key, client, context, finished)));
            let mut input = input;
            while let Some(batch) = input.try_next().await? { yield batch; }
            let _ = finish.send(());
            // The final update precedes task success, so a short producer cannot
            // disappear before a later consumer has fetched its filter.
            let _ = (&mut transport).await;
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, output))
    }

    async fn synchronize(
        mut self,
        key: TaskKey,
        client: DynamicFilterClient,
        context: Arc<TaskContext>,
        mut finished: oneshot::Receiver<()>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let complete = tokio::select! {
                _ = &mut finished => true,
                _ = interval.tick() => false,
            };
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                self.exchange(&key, &client, &context, complete),
            )
            .await;
            if let Ok(Err(error)) = &result {
                log::debug!("dynamic filter exchange for {key:?}: {error}");
            }
            if complete || (matches!(result, Ok(Ok(()))) && self.is_complete()) {
                break;
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.producers
            .values()
            .flatten()
            .map(|producer| &producer.expr)
            .chain(self.consumers.values().map(|binding| &binding.filter))
            .all(|expr| {
                expr.downcast_ref::<DynamicFilterPhysicalExpr>()
                    .is_some_and(|filter| filter.wait_complete().now_or_never().is_some())
            })
    }

    async fn exchange(
        &mut self,
        key: &TaskKey,
        client: &DynamicFilterClient,
        context: &TaskContext,
        complete: bool,
    ) -> ExecutionResult<()> {
        let mut updates = vec![];
        let mut sent = HashMap::new();
        for (id, producers) in &self.producers {
            let versions = producers
                .iter()
                .map(|producer| {
                    let done = complete
                        || producer
                            .expr
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .is_some_and(|filter| filter.wait_complete().now_or_never().is_some());
                    (producer.expr.snapshot_generation(), done)
                })
                .collect::<Vec<_>>();
            if self.sent.get(id) == Some(&versions) {
                continue;
            }
            let snapshots = producers
                .iter()
                .zip(&versions)
                .map(|(producer, (_, done))| snapshot(&producer.expr, &producer.schema, *done))
                .collect::<Result<Vec<_>>>()?;
            let Some(first) = snapshots.first() else {
                continue;
            };
            let mut update = first.clone();
            if snapshots.len() > 1 {
                update.predicate = union_predicates(&snapshots.iter().collect::<Vec<_>>())?;
            }
            update.generation = versions.iter().map(|(generation, _)| *generation).sum();
            update.complete = versions.iter().all(|(_, done)| *done);
            sent.insert(*id, versions);
            updates.push(update);
        }
        let response = client.exchange(key, updates, self.revision).await?;
        self.sent.extend(sent);
        for update in response.updates {
            if let Some(binding) = self.consumers.get(&update.expression_id) {
                binding.apply(&update, context)?;
            }
        }
        self.revision = response.revision;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::expressions::{Column, lit};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
    use datafusion::prelude::SessionContext;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use prost::Message;

    use super::*;
    use crate::proto::decode::decode_task_plan;
    use crate::proto::encode_remote_physical_plan;

    fn input(partitions: &[Vec<Option<i64>>]) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batches = partitions
            .iter()
            .map(|values| {
                Ok(vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(values.clone()))],
                )?])
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&batches, schema, None)?,
        ))
    }

    #[tokio::test]
    async fn isolated_join_partitions_produce_filters_without_waiting_for_siblings() -> Result<()> {
        let ctx = SessionContext::new().task_ctx();
        for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
            for null_equality in [
                NullEquality::NullEqualsNothing,
                NullEquality::NullEqualsNull,
            ] {
                let build = if mode == PartitionMode::Partitioned {
                    input(&[vec![Some(1), None], vec![Some(3), Some(3)]])?
                } else {
                    input(&[vec![Some(1), Some(3), None]])?
                };
                let key = Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>;
                let filter: Arc<dyn PhysicalExpr> =
                    Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
                let probe = Arc::new(FilterExec::try_new(
                    filter.clone(),
                    input(&[vec![Some(1), Some(2), None], vec![Some(3), Some(4)]])?,
                )?);
                let typed_filter = (filter.clone() as Arc<dyn Any + Send + Sync>)
                    .downcast::<DynamicFilterPhysicalExpr>()
                    .map_err(|_| plan_datafusion_err!("expected dynamic filter"))?;
                let plan: Arc<dyn ExecutionPlan> = Arc::new(
                    HashJoinExecBuilder::new(
                        build,
                        probe,
                        vec![(key.clone(), key)],
                        JoinType::Inner,
                    )
                    .with_partition_mode(mode)
                    .with_null_equality(null_equality)
                    .build()?
                    .with_dynamic_filter_expr(typed_filter)?,
                );
                let id = filter
                    .expression_id()
                    .ok_or_else(|| plan_datafusion_err!("missing filter ID"))?;
                let plan = crate::dynamic_filter::prepare_join_filters(plan)?;
                let bytes = encode_remote_physical_plan(&RemoteExecutionCodec, plan)?;
                let proto = PhysicalPlanNode::decode(bytes.as_slice())
                    .map_err(|e| plan_datafusion_err!("{e}"))?;
                let mut rows = 0;
                for partition in 0..2 {
                    let (decoded, consumers) = decode_task_plan(&ctx, &proto)?;
                    let (plan, filters) =
                        TaskDynamicFilters::prepare(decoded, consumers, &[id], &ctx)?;
                    let batches = tokio::time::timeout(
                        Duration::from_secs(2),
                        plan.execute(partition, ctx.clone())?
                            .try_collect::<Vec<_>>(),
                    )
                    .await
                    .map_err(|_| plan_datafusion_err!("isolated join waited for another task"))??;
                    rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
                    let producer = &filters.producers[&id][0];
                    assert!(producer.expr.snapshot_generation() > 1);
                    assert!(
                        producer
                            .expr
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .is_some_and(|filter| filter.wait_complete().now_or_never().is_some())
                    );
                    // Local build bounds must not leak into the independently shared consumer.
                    let batch = RecordBatch::try_new(
                        producer.schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
                    )?;
                    let values = filters.consumers[&id]
                        .filter
                        .evaluate(&batch)?
                        .into_array(4)?;
                    let values = values
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| plan_datafusion_err!("expected Boolean array"))?;
                    assert_eq!(values.true_count(), values.len());
                }
                let expected = if mode == PartitionMode::Partitioned {
                    3
                } else {
                    2
                } + usize::from(null_equality == NullEquality::NullEqualsNull);
                assert_eq!(rows, expected);
            }
        }
        Ok(())
    }

    struct FilterService {
        available: Arc<std::sync::atomic::AtomicBool>,
        state: tokio::sync::Mutex<crate::dynamic_filter::DynamicFilterState>,
        routes: std::collections::BTreeMap<u64, crate::dynamic_filter::DynamicFilterRoute>,
        attempts: HashMap<(usize, usize), usize>,
    }

    #[tonic::async_trait]
    impl crate::driver::r#gen::driver_service_server::DriverService for FilterService {
        async fn exchange_dynamic_filters(
            &self,
            request: tonic::Request<crate::driver::r#gen::ExchangeDynamicFiltersRequest>,
        ) -> std::result::Result<tonic::Response<ExchangeDynamicFiltersResponse>, tonic::Status>
        {
            if !self.available.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(tonic::Status::unavailable("control plane unavailable"));
            }
            let request = request.into_inner();
            let key = TaskKey {
                job_id: request.job_id.into(),
                stage: request.stage as usize,
                partition: request.partition as usize,
                attempt: request.attempt as usize,
            };
            let response = self
                .state
                .lock()
                .await
                .exchange(
                    &key,
                    &self.routes,
                    &self.attempts,
                    request.updates,
                    request.revision,
                )
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
            Ok(tonic::Response::new(response))
        }
        async fn register_worker(
            &self,
            _: tonic::Request<crate::driver::r#gen::RegisterWorkerRequest>,
        ) -> std::result::Result<
            tonic::Response<crate::driver::r#gen::RegisterWorkerResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn report_worker_heartbeat(
            &self,
            _: tonic::Request<crate::driver::r#gen::ReportWorkerHeartbeatRequest>,
        ) -> std::result::Result<
            tonic::Response<crate::driver::r#gen::ReportWorkerHeartbeatResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn report_worker_known_peers(
            &self,
            _: tonic::Request<crate::driver::r#gen::ReportWorkerKnownPeersRequest>,
        ) -> std::result::Result<
            tonic::Response<crate::driver::r#gen::ReportWorkerKnownPeersResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn report_task_status(
            &self,
            _: tonic::Request<crate::driver::r#gen::ReportTaskStatusRequest>,
        ) -> std::result::Result<
            tonic::Response<crate::driver::r#gen::ReportTaskStatusResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn report_metrics(
            &self,
            _: tonic::Request<crate::driver::r#gen::ReportMetricsRequest>,
        ) -> std::result::Result<
            tonic::Response<crate::driver::r#gen::ReportMetricsResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("unused"))
        }
    }

    async fn run_remote_stage(
        plan: Arc<dyn ExecutionPlan>,
        id: u64,
        stage: usize,
        partition: usize,
        client: DynamicFilterClient,
    ) -> Result<Vec<RecordBatch>> {
        let context = SessionContext::new().task_ctx();
        let proto = PhysicalPlanNode::decode(
            encode_remote_physical_plan(&RemoteExecutionCodec, plan)?.as_slice(),
        )
        .map_err(|e| plan_datafusion_err!("{e}"))?;
        let (plan, bindings) = decode_task_plan(&context, &proto)?;
        let (plan, filters) = TaskDynamicFilters::prepare(plan, bindings, &[id], &context)?;
        let input = plan.execute(partition, context.clone())?;
        filters
            .stream(
                input,
                TaskKey {
                    job_id: 1.into(),
                    stage,
                    partition,
                    attempt: 0,
                },
                client,
                context,
            )
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn remote_build_partitions_prune_an_independently_decoded_consumer()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::BTreeMap;

        use crate::driver::r#gen::driver_service_server::DriverServiceServer;
        use crate::dynamic_filter::{DynamicFilterRoute, DynamicFilterState};
        use crate::rpc::ClientOptions;

        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let filter: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let id = filter.expression_id().ok_or("missing filter ID")?;
        let probe = input(&[(0..128).map(Some).collect()])?;
        let producer: Arc<dyn ExecutionPlan> = Arc::new(DynamicFilterBuildExec {
            input: input(&[vec![Some(1), Some(17)], vec![Some(93)]])?,
            keys: vec![key],
            filter: filter.clone(),
            probe_schema: probe.schema(),
            null_equals_null: false,
            null_aware: false,
        });
        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                Arc::new(Column::new("k", 0)),
                datafusion::logical_expr::Operator::GtEq,
                lit(0_i64),
            )),
            datafusion::logical_expr::Operator::And,
            filter,
        ));
        let consumer: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, probe)?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = DynamicFilterClient::Worker(DriverClient::new(
            1.into(),
            ClientOptions {
                enable_tls: false,
                host: "127.0.0.1".into(),
                port: listener.local_addr()?.port(),
            },
        ));
        let available = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let service = FilterService {
            available: available.clone(),
            state: tokio::sync::Mutex::new(DynamicFilterState::default()),
            routes: BTreeMap::from([(
                id,
                DynamicFilterRoute {
                    producers: BTreeMap::from([(0, 2)]),
                    consumers: [1].into(),
                },
            )]),
            attempts: HashMap::from([((0, 0), 0), ((0, 1), 0), ((1, 0), 0)]),
        };
        let connections = futures::stream::unfold(listener, |listener| async {
            let result = listener.accept().await.map(|(stream, _)| stream);
            Some((result, listener))
        });
        let server = AbortOnDropHandle::new(tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DriverServiceServer::new(service))
                .serve_with_incoming(connections),
        ));
        run_remote_stage(producer.clone(), id, 0, 0, client.clone()).await?;
        let unfiltered = run_remote_stage(consumer.clone(), id, 1, 0, client.clone()).await?;
        assert_eq!(
            unfiltered.iter().map(RecordBatch::num_rows).sum::<usize>(),
            128
        );
        run_remote_stage(producer, id, 0, 1, client.clone()).await?;
        let filtered = run_remote_stage(consumer.clone(), id, 1, 0, client.clone()).await?;
        let values = filtered
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .into_iter()
                    .flat_map(|array| array.values().iter().copied())
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1, 17, 93]);
        // Losing the optional control plane must leave a fresh consumer unfiltered.
        available.store(false, std::sync::atomic::Ordering::SeqCst);
        let unfiltered = run_remote_stage(consumer, id, 1, 0, client).await?;
        assert_eq!(
            unfiltered.iter().map(RecordBatch::num_rows).sum::<usize>(),
            128
        );
        drop(server);
        Ok(())
    }
}
