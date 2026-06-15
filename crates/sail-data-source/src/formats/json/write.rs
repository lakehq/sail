use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::gen::JsonWriteOptions;

#[derive(Debug, Clone)]
pub struct JsonWriteFormat {
    pub(super) options: JsonWriteOptions,
}

#[async_trait]
impl WriteFormat for JsonWriteFormat {
    async fn sink(
        &self,
        ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let format = JsonFormat::default().with_options(options);
        input.sink.file_extension = if let Some(file_compression_type) = format.compression_type() {
            format.get_ext_with_compression(&file_compression_type)?
        } else {
            format.get_ext()
        };
        format
            .create_writer_physical_plan(input.input, ctx, input.sink, input.sort_order)
            .await
    }
}
