use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use log::info;
use sail_execution::remote_plugin::{RemotePluginClient, RemoteScalarUdf};

/// Manages discovery and registration of remote Arrow Flight plugin UDFs.
///
/// Connects to a remote plugin endpoint, discovers available UDFs via `ListActions`,
/// and registers them as `ScalarUDF` on a `SessionContext`.
pub struct RemotePluginManager {
    endpoint: String,
    scalar_udfs: Vec<Arc<ScalarUDF>>,
}

impl RemotePluginManager {
    /// Connect to a remote plugin and discover its functions.
    ///
    /// This calls `ListActions` on the plugin to discover available UDFs,
    /// then creates a `RemoteScalarUdf` wrapper for each one.
    pub async fn connect(endpoint: &str, runtime: tokio::runtime::Handle) -> Result<Self> {
        let client = RemotePluginClient::new(endpoint)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let functions = client
            .discover_functions()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let mut scalar_udfs = Vec::with_capacity(functions.len());
        for func in &functions {
            let udf = RemoteScalarUdf::new(
                func.name.clone(),
                endpoint.to_string(),
                client.clone(),
                func.volatility,
                runtime.clone(),
            );
            scalar_udfs.push(Arc::new(ScalarUDF::new_from_impl(udf)));
        }

        let names: Vec<&str> = scalar_udfs.iter().map(|u| u.name()).collect();
        info!(
            "Connected to remote plugin {}: {} scalar UDFs {:?}",
            endpoint,
            scalar_udfs.len(),
            names,
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            scalar_udfs,
        })
    }

    /// Register all discovered UDFs on the given session context.
    pub fn register_on(&self, ctx: &SessionContext) {
        for udf in &self.scalar_udfs {
            ctx.register_udf(udf.as_ref().clone());
        }
    }

    /// The endpoint URL this manager is connected to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
