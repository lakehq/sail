use sail_spark_connect::entrypoint::serve;
use sail_telemetry::telemetry::init_telemetry;
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run())?;
    // Shutdown the runtime with a timeout.
    // When the timeout is reached, the `main()` function returns and
    // the process exits immediately (though the exit code is still zero).
    // TODO: understand why some tasks are still running after the DataFusion stream is dropped.
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    Ok(())
}
