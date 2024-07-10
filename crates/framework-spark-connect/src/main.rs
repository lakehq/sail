use framework_spark_connect::entrypoint::serve;
use framework_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;
use tracing::info;

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
    _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Spark Connect server...");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    // A secure connection can be handled by a gateway in production.
    let listener = TcpListener::bind("0.0.0.0:50051").await?;
    info!(
        "Starting the Spark Connect server on {}...",
        listener.local_addr()?
    );
    serve(listener, Some(shutdown())).await?;
    info!("The Spark Connect server has stopped.");
    Ok(())
}
