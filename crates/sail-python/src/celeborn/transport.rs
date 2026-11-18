use std::time::Duration;

use pyo3::prelude::*;
use sail_celeborn::protocol::TransportConnection;
use sail_common::runtime::RuntimeHandle;

use crate::celeborn::to_py_error;
use crate::globals::GlobalState;

/// A serialized RPC connection with caller-assigned request IDs.
#[pyclass(name = "RpcConnection")]
pub(super) struct PyRpcConnection {
    connection: TransportConnection,
    runtime: RuntimeHandle,
}

#[pymethods]
impl PyRpcConnection {
    #[new]
    fn new(py: Python<'_>, host: String, port: u16) -> PyResult<Self> {
        Ok(Self {
            connection: TransportConnection::new(host, port, Duration::from_secs(5)),
            runtime: GlobalState::instance(py)?.runtime.handle(),
        })
    }

    fn send_rpc(&self, py: Python<'_>, request_id: i64, body: Vec<u8>) -> PyResult<Vec<u8>> {
        py.detach(|| {
            self.runtime
                .primary()
                .block_on(self.connection.send_rpc_with_id(request_id, body))
        })
        .map_err(to_py_error)
    }
}
