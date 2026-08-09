use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sail_celeborn::error::CelebornError;
use sail_celeborn::shuffle::{ShuffleClient, ShuffleClientActor, ShuffleClientOptions};
use sail_common::actor::ActorSystem;
use sail_common::runtime::RuntimeHandle;

use crate::celeborn::lifecycle::PyLifecycleManager;
use crate::celeborn::to_py_error;
use crate::globals::GlobalState;

enum ShuffleClientState {
    Stopped,
    Running {
        system: ActorSystem,
        client: ShuffleClient,
    },
}

#[pyclass(name = "ShuffleClient")]
pub(super) struct PyShuffleClient {
    lifecycle_manager: Py<PyLifecycleManager>,
    runtime: RuntimeHandle,
    state: ShuffleClientState,
}

#[pymethods]
impl PyShuffleClient {
    #[new]
    #[pyo3(signature = (lifecycle_manager, /))]
    fn new(py: Python<'_>, lifecycle_manager: Py<PyLifecycleManager>) -> PyResult<Self> {
        Ok(Self {
            lifecycle_manager,
            runtime: GlobalState::instance(py)?.runtime.handle(),
            state: ShuffleClientState::Stopped,
        })
    }

    #[getter]
    fn running(&self) -> bool {
        matches!(&self.state, ShuffleClientState::Running { .. })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running() {
            return Err(PyRuntimeError::new_err(
                "the shuffle client is already started",
            ));
        }
        let runtime = self.runtime.clone();
        let (application_id, lifecycle_manager, endpoint_resolver) = {
            let lifecycle_manager = self.lifecycle_manager.bind(py).borrow();
            (
                lifecycle_manager.application_id().to_string(),
                lifecycle_manager.manager()?,
                lifecycle_manager.endpoint_resolver(),
            )
        };
        let state = py.detach(move || {
            runtime.primary().block_on(async move {
                let mut system = ActorSystem::new();
                let client = ShuffleClient::new(system.spawn::<ShuffleClientActor>(
                    ShuffleClientOptions::new(
                        application_id,
                        Arc::new(lifecycle_manager),
                        endpoint_resolver,
                    ),
                ));
                Ok::<_, CelebornError>(ShuffleClientState::Running { system, client })
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
        let client = self.client()?;
        let runtime = self.runtime.clone();
        let response = py.detach(move || {
            runtime.primary().block_on(client.register_shuffle(
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
            ))
        });
        Ok(response.map_err(to_py_error)?.worker_ids)
    }

    fn push_data(
        &self,
        py: Python<'_>,
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Vec<u8>,
    ) -> PyResult<usize> {
        let client = self.client()?;
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(client.push_data(
                shuffle_id,
                partition_id,
                map_id,
                attempt_id,
                data,
            ))
        })
        .map_err(to_py_error)
    }

    fn mapper_end(
        &self,
        py: Python<'_>,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> PyResult<()> {
        let client = self.client()?;
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(client.mapper_end(
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
            ))
        })
        .map_err(to_py_error)
    }

    fn read_partition(
        &self,
        py: Python<'_>,
        shuffle_id: i32,
        partition_id: i32,
    ) -> PyResult<Vec<u8>> {
        let client = self.client()?;
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime
                .primary()
                .block_on(client.read_partition(shuffle_id, partition_id))
        })
        .map_err(to_py_error)
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        let (system, client) = match std::mem::replace(&mut self.state, ShuffleClientState::Stopped)
        {
            ShuffleClientState::Stopped => {
                return Err(PyRuntimeError::new_err("the shuffle client is not started"));
            }
            ShuffleClientState::Running { system, client } => (system, client),
        };
        let runtime = self.runtime.clone();
        py.detach(move || {
            runtime.primary().block_on(async move {
                let mut system = system;
                client.stop().await?;
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

impl PyShuffleClient {
    fn client(&self) -> PyResult<ShuffleClient> {
        match &self.state {
            ShuffleClientState::Stopped => {
                Err(PyRuntimeError::new_err("the shuffle client is not started"))
            }
            ShuffleClientState::Running { client, .. } => Ok(client.clone()),
        }
    }
}
