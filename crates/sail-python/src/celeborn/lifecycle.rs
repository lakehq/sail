use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sail_celeborn::common::{CompressionCodec, PartitionSplitMode};
use sail_celeborn::endpoint::EndpointResolver;
use sail_celeborn::error::CelebornError;
use sail_celeborn::lifecycle::{
    LifecycleManager as LifecycleManagerTrait, LifecycleManagerActor, LifecycleManagerOptions,
    LocalLifecycleManager,
};
use sail_celeborn::master::MasterClientOptions;
use sail_common::actor::ActorSystem;
use sail_common::runtime::RuntimeHandle;

use crate::celeborn::endpoint::PyStaticEndpointResolver;
use crate::celeborn::to_py_error;
use crate::globals::GlobalState;

enum LifecycleManagerState {
    Stopped,
    Running {
        system: ActorSystem,
        manager: LocalLifecycleManager,
    },
}

#[pyclass(name = "LifecycleManager")]
pub(super) struct PyLifecycleManager {
    master_host: String,
    master_port: u16,
    application_id: String,
    endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    partition_split_threshold: i64,
    partition_split_mode: PartitionSplitMode,
    compression: CompressionCodec,
    heartbeat_interval_secs: u64,
    runtime: RuntimeHandle,
    state: LifecycleManagerState,
}

#[pymethods]
impl PyLifecycleManager {
    #[expect(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (master_host, master_port, application_id, *, endpoint_resolver=None, partition_split_threshold=1073741824, partition_split_mode="soft".to_string(), compression="lz4".to_string(), heartbeat_interval_secs=10))]
    fn new(
        py: Python<'_>,
        master_host: String,
        master_port: u16,
        application_id: String,
        endpoint_resolver: Option<Py<PyStaticEndpointResolver>>,
        partition_split_threshold: i64,
        partition_split_mode: String,
        compression: String,
        heartbeat_interval_secs: u64,
    ) -> PyResult<Self> {
        let endpoint_resolver = endpoint_resolver.map(|resolver| {
            Arc::new(resolver.bind(py).borrow().clone()) as Arc<dyn EndpointResolver>
        });
        let partition_split_mode = partition_split_mode
            .parse::<PartitionSplitMode>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let compression = compression
            .parse::<CompressionCodec>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            master_host,
            master_port,
            application_id,
            endpoint_resolver,
            partition_split_threshold,
            partition_split_mode,
            compression,
            heartbeat_interval_secs,
            runtime: GlobalState::instance(py)?.runtime.handle(),
            state: LifecycleManagerState::Stopped,
        })
    }

    #[getter]
    fn running(&self) -> bool {
        matches!(&self.state, LifecycleManagerState::Running { .. })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running() {
            return Err(PyRuntimeError::new_err(
                "the lifecycle manager is already started",
            ));
        }
        let runtime = self.runtime.clone();
        let mut options = LifecycleManagerOptions::new(
            self.application_id.clone(),
            MasterClientOptions::new(self.master_host.clone(), self.master_port),
        );
        if let Some(endpoint_resolver) = self.endpoint_resolver.clone() {
            options = options.with_endpoint_resolver(endpoint_resolver);
        }
        options =
            options.with_partition_split(self.partition_split_threshold, self.partition_split_mode);
        options =
            options.with_heartbeat_interval(Duration::from_secs(self.heartbeat_interval_secs));
        let state = py.detach(move || {
            runtime.primary().block_on(async move {
                let mut system = ActorSystem::new();
                let handle = system.spawn::<LifecycleManagerActor>(options);
                let manager = LocalLifecycleManager::new(handle);
                Ok::<_, CelebornError>(LifecycleManagerState::Running { system, manager })
            })
        });
        self.state = state.map_err(to_py_error)?;
        Ok(())
    }

    fn register_shuffle(
        &self,
        py: Python<'_>,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> PyResult<Vec<String>> {
        let manager = self.manager()?;
        let runtime = self.runtime.clone();
        let response = py.detach(move || {
            runtime.primary().block_on(manager.register_shuffle(
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
            ))
        });
        let response = response.map_err(to_py_error)?;
        Ok(response
            .worker_ids
            .into_iter()
            .map(|worker| worker.to_string())
            .collect())
    }

    fn unregister_shuffle(&self, py: Python<'_>, shuffle_id: i32) -> PyResult<()> {
        let manager = self.manager()?;
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(manager.unregister_shuffle(shuffle_id))
        })
        .map_err(to_py_error)
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        let (system, manager) =
            match std::mem::replace(&mut self.state, LifecycleManagerState::Stopped) {
                LifecycleManagerState::Stopped => {
                    return Err(PyRuntimeError::new_err(
                        "the lifecycle manager is not started",
                    ));
                }
                LifecycleManagerState::Running { system, manager } => (system, manager),
            };
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(async move {
                let mut system = system;
                manager.stop().await?;
                system.join().await;
                Ok::<_, CelebornError>(())
            })
        })
        .map_err(to_py_error)
    }

    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.start(py)?;
        Ok(slf)
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.stop(py)?;
        Ok(false)
    }
}

impl PyLifecycleManager {
    pub(super) fn application_id(&self) -> &str {
        &self.application_id
    }

    pub(super) fn endpoint_resolver(&self) -> Option<Arc<dyn EndpointResolver>> {
        self.endpoint_resolver.clone()
    }

    pub(super) fn compression(&self) -> CompressionCodec {
        self.compression
    }

    pub(super) fn manager(&self) -> PyResult<LocalLifecycleManager> {
        match &self.state {
            LifecycleManagerState::Stopped => Err(PyRuntimeError::new_err(
                "the lifecycle manager is not started",
            )),
            LifecycleManagerState::Running { manager, .. } => Ok(manager.clone()),
        }
    }
}
