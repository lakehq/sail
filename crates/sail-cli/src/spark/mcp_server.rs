use std::ffi::CString;
use std::fmt;
use std::fmt::Formatter;
use std::net::Ipv4Addr;

use clap::ValueEnum;
use log::info;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{PyResult, Python};
use sail_spark_connect::entrypoint::serve;
use sail_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;

const MCP_SERVER_SOURCE_CODE: &str = include_str!("mcp_server.py");

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
}

pub fn run_spark_mcp_server(settings: McpSettings) -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
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
    Python::with_gil(|py| -> PyResult<_> {
        let shell = PyModule::from_code(
            py,
            CString::new(MCP_SERVER_SOURCE_CODE)?.as_c_str(),
            CString::new("spark_mcp_server.py")?.as_c_str(),
            CString::new("spark_mcp_server")?.as_c_str(),
        )?;
        shell.getattr("run_spark_mcp_server")?.call(
            (
                settings.transport.to_string(),
                settings.host,
                settings.port,
                server_port,
            ),
            None,
        )?;
        Ok(())
    })?;
    Ok(())
}
