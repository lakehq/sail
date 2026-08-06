use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sail_celeborn::error::CelebornError;
use sail_celeborn::lifecycle::{
    LifecycleManager as LifecycleManagerTrait, LifecycleManagerActor, LifecycleManagerOptions,
    LocalLifecycleManager,
};
use sail_celeborn::master::MasterClientOptions;
use sail_common::actor::ActorSystem;
use sail_common::runtime::RuntimeHandle;

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
    runtime: RuntimeHandle,
    state: LifecycleManagerState,
}

#[pymethods]
impl PyLifecycleManager {
    #[new]
    #[pyo3(signature = (master_host, master_port, application_id, /))]
    fn new(
        py: Python<'_>,
        master_host: String,
        master_port: u16,
        application_id: String,
    ) -> PyResult<Self> {
        Ok(Self {
            master_host,
            master_port,
            application_id,
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
        let options = LifecycleManagerOptions::new(
            self.application_id.clone(),
            MasterClientOptions::new(self.master_host.clone(), self.master_port),
        );
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

    fn request_slots(
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
            runtime.primary().block_on(manager.request_slots(
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
            ))
        });
        let response = response.map_err(to_py_error)?;
        Ok(response.worker_ids)
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
    fn manager(&self) -> PyResult<LocalLifecycleManager> {
        match &self.state {
            LifecycleManagerState::Stopped => Err(PyRuntimeError::new_err(
                "the lifecycle manager is not started",
            )),
            LifecycleManagerState::Running { manager, .. } => Ok(manager.clone()),
        }
    }
}

fn to_py_error(error: CelebornError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
