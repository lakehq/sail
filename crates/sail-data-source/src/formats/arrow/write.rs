use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::{ListingSinkInput, WriteFormat};

#[derive(Debug, Default, Clone)]
pub struct ArrowWriteFormat;

#[async_trait]
impl WriteFormat for ArrowWriteFormat {
    async fn sink(
        &self,
        ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        input.sink.file_extension = ArrowFormat.get_ext();
        ArrowFormat
            .create_writer_physical_plan(input.input, ctx, input.sink, input.sort_order)
            .await
    }
}
