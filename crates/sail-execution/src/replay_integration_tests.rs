mod gated_store;
mod lifecycle;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use opentelemetry::logs::LoggerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, ShuffleBackend, StorageShuffleBackend, SystemCatalogStore};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::replay::ReplayId;
use sail_common_datafusion::session::job::JobRunner;
use sail_physical_plan::replay::ReplayExec;
use sail_telemetry::events::SystemEventReporter;

use crate::driver::{
    DriverComponents, DriverGateway, DriverGatewayOptions, DriverHandle, DriverOptions,
    DriverRegistryAccessor,
};
use crate::error::ExecutionResult;
use crate::id::DriverId;
use crate::job_runner::ClusterJobRunner;
use crate::worker_manager::LocalWorkerManager;

pub(crate) fn test_event_reporter() -> SystemEventReporter {
    // A private no-export logger avoids process-global initialization and runtime ownership.
    SystemEventReporter::new(SdkLoggerProvider::builder().build().logger("replay-tests"))
}

struct Registry(DriverHandle);
#[tonic::async_trait]
impl DriverRegistryAccessor for Registry {
    async fn get(&self, _id: DriverId) -> ExecutionResult<DriverHandle> {
        Ok(self.0.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_cluster_flight_and_storage_share_volatile_producer_and_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(60), async {
        for storage in [false, true] {
            let mut config = AppConfig::load()?;
            config.cluster.worker_initial_count = 2;
            config.cluster.worker_max_count = 2;
            config.cluster.worker_task_slots = 1;
            config.cluster.driver_external_host = "127.0.0.1".into();
            config.cluster.driver_listen_host = "127.0.0.1".into();
            config.cluster.driver_listen_port = 0;
            config.catalog.system.store = SystemCatalogStore::Memory;
            config.telemetry.export_traces = false;
            config.telemetry.export_metrics = false;
            config.telemetry.export_logs = false;
            if storage {
                config.cluster.shuffle_backend = ShuffleBackend::Storage(StorageShuffleBackend {
                    path: Some("memory:///replay-tests".into()),
                    max_file_size: 1024 * 1024,
                    compression: sail_common::config::ShuffleCompression::None,
                });
            }
            let reporter = test_event_reporter();
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024)))
                .build_arc()?;
            let store = Arc::new(InMemory::new());
            runtime.register_object_store(&url::Url::parse("memory:///")?, store.clone());
            let session = SessionContext::new_with_state(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_runtime_env(runtime.clone())
                    .build(),
            );
            let handle = tokio::runtime::Handle::current();
            let runtime_handle = RuntimeHandle::new(handle.clone(), handle);
            let mut gateway = DriverGateway::try_new(DriverGatewayOptions::new(&config)).await?;
            let options = DriverOptions::try_new(
                &config,
                runtime_handle.clone(),
                format!("replay-cluster-{storage}"),
                DriverId::from(1),
                gateway.port(),
            )?;
            let mut system = ActorSystem::new();
            let runner = ClusterJobRunner::new(
                &mut system,
                options,
                DriverComponents {
                    worker_manager: Box::new(LocalWorkerManager::new(
                        runtime_handle,
                        session.clone(),
                    )),
                    event_reporter: reporter,
                },
            );
            gateway.start(Arc::new(Registry(runner.driver())));
            runner.driver().activate().await?;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
            )?;
            let source = session
                .read_batch(batch)?
                .select(vec![datafusion::functions::math::expr_fn::random()])?
                .create_physical_plan()
                .await?;
            let source = Arc::new(
                datafusion::physical_plan::repartition::RepartitionExec::try_new(
                    source,
                    datafusion::physical_expr::Partitioning::RoundRobinBatch(2),
                )?,
            );
            let named = Arc::new(ReplayExec::try_new(
                ReplayId {
                    scope: Default::default(),
                    ordinal: 0,
                },
                source,
            )?);
            let plan = UnionExec::try_new(vec![named.clone(), named])?;
            let mut previous = None;
            for _ in 0..2 {
                let batches = runner
                    .execute(&session, plan.clone())
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let mut counts = HashMap::new();
                for batch in batches {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or("expected random f64")?;
                    for value in values.values() {
                        *counts.entry(value.to_bits()).or_insert(0) += 1;
                    }
                }
                assert_eq!(counts.len(), 1024);
                assert!(counts.values().all(|&count| count == 2));
                if let Some(previous) = previous {
                    assert_ne!(counts, previous);
                }
                previous = Some(counts);
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        if runtime.disk_manager.used_disk_space() == 0
                            && store.list(None).try_collect::<Vec<_>>().await?.is_empty()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok::<_, object_store::Error>(())
                })
                .await??;
            }
            // Root drop is cancellation, not permission to restart the producer.
            drop(runner.execute(&session, plan).await?);
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    if runtime.disk_manager.used_disk_space() == 0
                        && store.list(None).try_collect::<Vec<_>>().await?.is_empty()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, object_store::Error>(())
            })
            .await??;
            runner.driver().shutdown_and_wait().await?;
            gateway.stop().await;
            system.join().await;
            assert_eq!(runtime.disk_manager.used_disk_space(), 0);
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?
}
