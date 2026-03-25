use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use log::{error, info};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::serve;
use tokio::net::TcpListener;

/// Handles graceful shutdown by waiting for a `SIGINT` signal in [tokio].
///
/// The `SIGINT` signal is captured by Python if the `_signal` module is imported [1].
/// To prevent this, we would need to run Python code like the following [2].
/// ```python
/// import signal
/// signal.signal(signal.SIGINT, signal.SIG_DFL)
/// ```
/// The workaround above is not necessary if we use this function to handle the signal.
///
/// References:
///   - [1] https://github.com/PyO3/pyo3/issues/2576
///   - [2] https://github.com/PyO3/pyo3/issues/3218
async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Spark Connect server...");
}

pub(super) mod telemetry {
    use sail_common::config::AppConfig;
    use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

    pub struct TelemetryGuard {
        /// A marker to prevent struct creation without calling [`TelemetryGuard::try_new()`].
        _marker: (),
    }

    impl TelemetryGuard {
        pub fn try_new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
            let resource = ResourceOptions { kind: "server" };
            init_telemetry(&config.telemetry, resource)?;
            Ok(Self { _marker: () })
        }
    }

    impl Drop for TelemetryGuard {
        fn drop(&mut self) {
            shutdown_telemetry();
        }
    }
}

/// A user-facing error for the Spark Connect server.
/// This does not wrap the underlying error but only tracks the error message,
/// so that it can be `Send` from the server task.
#[derive(Debug)]
pub struct ServerError(String);

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error: {}", self.0)
    }
}

impl std::error::Error for ServerError {}

/// Starts a Spark Connect server and runs the given workload with the server address.
/// This function should be called only once in the entire process since it initializes
/// the telemetry and shuts down the telemetry when the server stops.
pub(super) fn with_spark_connect_server<S, W, F>(
    address: (IpAddr, u16),
    signal: S,
    workload: W,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: Future<Output = ()> + Send + 'static,
    W: FnOnce(SocketAddr) -> F,
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    let _telemetry = runtime
        .handle()
        .primary()
        .block_on(async { telemetry::TelemetryGuard::try_new(&config) })?;

    let handle = runtime.handle();
    let (server_address, server_task) = runtime.handle().primary().block_on(async {
        // A secure connection can be handled by a gateway in production.
        let listener = TcpListener::bind(address).await?;
        let server_address = listener.local_addr()?;
        let server_task = async move {
            info!("Starting the Spark Connect server on {server_address}...");
            match serve(listener, signal, config, handle).await {
                Ok(()) => {
                    info!("The Spark Connect server has stopped.");
                    Ok(())
                }
                Err(e) => {
                    error!("{e}");
                    Err(ServerError(e.to_string()))
                }
            }
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((server_address, server_task))
    })?;

    let server_task = runtime.handle().primary().spawn(server_task);

    runtime.handle().primary().block_on(async move {
        let result = workload(server_address).await;
        let server_result = server_task.await;
        match (result, server_result) {
            (Err(e), _) => Err(e),
            (Ok(()), Ok(Ok(()))) => Ok(()),
            (Ok(()), Ok(Err(e))) => Err(Box::new(e) as Box<dyn std::error::Error>),
            (Ok(()), Err(e)) => Err(Box::new(e) as Box<dyn std::error::Error>),
        }
    })
}

pub fn run_spark_connect_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    with_spark_connect_server((ip, port), shutdown(), |_| async { Ok(()) })
}
