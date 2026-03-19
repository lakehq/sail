use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use clap::ValueEnum;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use tokio::sync::oneshot;

use crate::python::Modules;
use crate::spark::server::{telemetry, with_spark_connect_server};

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

pub fn run_spark_mcp_server(
    settings: McpSettings,
    spark_remote: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match spark_remote {
        None => {
            // We follow the same setup as `run_pyspark_shell`.
            // Please refer to the comments in that function for details.
            let (tx, rx) = oneshot::channel::<()>();
            let address = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
            let shutdown = async {
                let _ = rx.await;
            };
            with_spark_connect_server(address, shutdown, |addr| async move {
                let _tx = tx;
                _run_mcp_server(settings, format!("sc://127.0.0.1:{}", addr.port()))
            })
        }
        Some(x) => {
            let config = Arc::new(AppConfig::load()?);
            let runtime = RuntimeManager::try_new(&config.runtime)?;

            let _telemetry = runtime
                .handle()
                .primary()
                .block_on(async { telemetry::TelemetryGuard::try_new(&config) })?;

            _run_mcp_server(settings, x.clone())
        }
    }
}

fn _run_mcp_server(
    settings: McpSettings,
    spark_remote: String,
) -> Result<(), Box<dyn std::error::Error>> {
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
