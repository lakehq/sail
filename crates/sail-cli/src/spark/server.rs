use std::net::IpAddr;
use std::sync::Arc;

use log::info;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::{serve, SessionManagerOptions};
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry};
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

pub fn run_spark_connect_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime
        .handle()
        .primary()
        .block_on(async { init_telemetry(&config.telemetry) })?;

    let options = SessionManagerOptions {
        config: Arc::clone(&config),
        runtime: runtime.handle(),
    };

    runtime.handle().primary().block_on(async {
        // A secure connection can be handled by a gateway in production.
        let listener = TcpListener::bind((ip, port)).await?;
        info!(
            "Starting the Spark Connect server on {}...",
            listener.local_addr()?
        );
        serve(listener, shutdown(), options).await?;
        info!("The Spark Connect server has stopped.");
        <Result<(), Box<dyn std::error::Error>>>::Ok(())
    })?;

    shutdown_telemetry();

    Ok(())
}
