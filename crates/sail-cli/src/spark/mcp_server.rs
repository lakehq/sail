use std::fmt;
use std::fmt::Formatter;
use std::net::Ipv4Addr;

use clap::ValueEnum;
use log::info;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_spark_connect::entrypoint::serve;
use sail_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use crate::python::Modules;

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Spark Connect server...");
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum McpTransport {
    Stdio,
    Sse,
}

impl fmt::Display for McpTransport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            McpTransport::Stdio => write!(f, "stdio"),
            McpTransport::Sse => write!(f, "sse"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct McpSettings {
    pub transport: McpTransport,
    pub host: String,
    pub port: u16,
    pub spark_remote: Option<String>,
}

fn run_spark_connect_server(runtime: &Runtime) -> Result<String, Box<dyn std::error::Error>> {
    let (server_port, server_task) = runtime.block_on(async move {
        // Listen on only the loopback interface for security.
        let listener = TcpListener::bind((Ipv4Addr::new(127, 0, 0, 1), 0)).await?;
        let port = listener.local_addr()?.port();
        let task = async move {
            info!("Starting the Spark Connect server on port {port}...");
            let _ = serve(listener, shutdown()).await;
            info!("The Spark Connect server has stopped.");
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((port, task))
    })?;
    runtime.spawn(server_task);
    Ok(format!("sc://127.0.0.1:{server_port}"))
}

pub fn run_spark_mcp_server(settings: McpSettings) -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let spark_remote = match settings.spark_remote {
        None => run_spark_connect_server(&runtime)?,
        Some(x) => x,
    };

    Python::with_gil(|py| -> PyResult<_> {
        let _ = Modules::NATIVE_LOGGING.load(py)?;
        let server = Modules::SPARK_MCP_SERVER.load(py)?;
        server.getattr("configure_logging")?.call0()?;
        server.getattr("configure_fastmcp_log_level")?.call0()?;
        let mcp = server.getattr("create_spark_mcp_server")?.call1((
            settings.host,
            settings.port,
            spark_remote,
        ))?;
        server.getattr("override_default_logging_config")?.call0()?;
        mcp.getattr("run")?
            .call1((settings.transport.to_string(),))?;
        Ok(())
    })?;
    Ok(())
}
