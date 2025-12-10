use std::fmt;
use std::fmt::Formatter;
use std::net::Ipv4Addr;
use std::sync::Arc;

use clap::ValueEnum;
use log::info;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::{serve, SessionManagerOptions};
use sail_telemetry::telemetry::{init_telemetry, ResourceOptions};
use tokio::net::TcpListener;

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

fn run_spark_connect_server(
    options: SessionManagerOptions,
) -> Result<String, Box<dyn std::error::Error>> {
    let handle = options.runtime.primary().clone();
    let (server_port, server_task) = handle.block_on(async move {
        // Listen on only the loopback interface for security.
        let listener = TcpListener::bind((Ipv4Addr::new(127, 0, 0, 1), 0)).await?;
        let port = listener.local_addr()?.port();
        let task = async move {
            info!("Starting the Spark Connect server on port {port}...");
            let _ = serve(listener, shutdown(), options).await;
            info!("The Spark Connect server has stopped.");
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((port, task))
    })?;
    handle.spawn(server_task);
    Ok(format!("sc://127.0.0.1:{server_port}"))
}

pub fn run_spark_mcp_server(settings: McpSettings) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime.handle().primary().block_on(async {
        let resource = ResourceOptions { kind: "server" };
        init_telemetry(&config.telemetry, resource)
    })?;

    let spark_remote = match settings.spark_remote {
        None => {
            let options = SessionManagerOptions {
                config: Arc::clone(&config),
                runtime: runtime.handle(),
            };
            run_spark_connect_server(options)?
        }
        Some(x) => x,
    };

    Python::attach(|py| -> PyResult<_> {
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
