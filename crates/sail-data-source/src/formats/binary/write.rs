use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, Result};

use crate::listing::source::{ListingSinkInput, WriteFormat};

#[derive(Debug, Default, Clone)]
pub struct BinaryWriteFormat;

#[async_trait]
impl WriteFormat for BinaryWriteFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        _input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Binary file format does not support writing")
    }
}
