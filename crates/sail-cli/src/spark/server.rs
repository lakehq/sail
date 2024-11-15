use std::net::IpAddr;

use log::info;
use sail_spark_connect::entrypoint::serve;
use sail_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;

const SERVER_STACK_SIZE: usize = 1024 * 1024 * 8;

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
    init_telemetry()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(SERVER_STACK_SIZE)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        // A secure connection can be handled by a gateway in production.
        let listener = TcpListener::bind((ip, port)).await?;
        info!(
            "Starting the Spark Connect server on {}...",
            listener.local_addr()?
        );
        serve(listener, shutdown()).await?;
        info!("The Spark Connect server has stopped.");
        <Result<(), Box<dyn std::error::Error>>>::Ok(())
    })?;

    fastrace::flush();

    Ok(())
}
