use std::net::IpAddr;

use clap::Parser;
use log::info;
use sail_spark_connect::entrypoint::serve;
use sail_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;

const SERVER_STACK_SIZE: usize = 1024 * 1024 * 8;
const SERVER_SHUTDOWN_TIMEOUT_SECONDS: u64 = 5;

#[derive(Parser)]
struct Args {
    /// The IP address that the server binds to
    #[clap(long, default_value = "127.0.0.1")]
    ip: IpAddr,
    /// The port number that the server listens on
    #[clap(long, default_value = "50051")]
    port: u16,
    /// The directory to change to before starting the server
    #[clap(short = 'C', long)]
    directory: Option<String>,
}

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

async fn run(ip: &IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    // A secure connection can be handled by a gateway in production.
    let listener = TcpListener::bind(format!("{ip}:{port}")).await?;
    info!(
        "Starting the Spark Connect server on {}...",
        listener.local_addr()?
    );
    serve(listener, Some(shutdown())).await?;
    info!("The Spark Connect server has stopped.");

    fastrace::flush();

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Some(directory) = args.directory {
        std::env::set_current_dir(directory)?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(SERVER_STACK_SIZE)
        .enable_all()
        .build()?;
    runtime.block_on(run(&args.ip, args.port))?;
    // Shutdown the runtime with a timeout.
    // When the timeout is reached, the `main()` function returns and
    // the process exits immediately (though the exit code is still zero).
    // TODO: understand why some tasks are still running after the DataFusion stream is dropped.
    runtime.shutdown_timeout(std::time::Duration::from_secs(
        SERVER_SHUTDOWN_TIMEOUT_SECONDS,
    ));
    Ok(())
}
