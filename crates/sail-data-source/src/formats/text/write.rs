use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, GetExt, Result, not_impl_err};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::sink::DataSinkExec;

use crate::formats::text::DEFAULT_TEXT_EXTENSION;
use crate::formats::text::writer::{TextSink, TextWriterOptions};
use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::r#gen::TextWriteOptions;

#[derive(Debug, Clone)]
pub struct TextWriteFormat {
    pub(super) options: TextWriteOptions,
}

#[async_trait]
impl WriteFormat for TextWriteFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if input.sink.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Text files");
        }
        let file_compression_type = FileCompressionType::from(options.compression);
        input.sink.file_extension = format!(
            "{}{}",
            &DEFAULT_TEXT_EXTENSION[1..],
            file_compression_type.get_ext()
        );
        let writer_options = TextWriterOptions::try_from(&options)?;
        let sink = Arc::new(TextSink::new(input.sink, writer_options));
        Ok(Arc::new(DataSinkExec::new(
            input.input,
            sink,
            input.sort_order,
        )))
    }
}
