use std::sync::Arc;

use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[derive(Debug)]
pub struct RemoteExecutionCodec {}

impl RemoteExecutionCodec {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalExtensionCodec for RemoteExecutionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "unknown execution plan".to_string(),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, _buf: &mut Vec<u8>) -> Result<()> {
        Err(DataFusionError::Internal(format!(
            "unknown execution plan: {:?}",
            node
        )))
    }
}
