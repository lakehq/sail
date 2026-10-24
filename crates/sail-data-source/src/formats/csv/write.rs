use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::csv::writer::Terminator;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::{CsvFormat, CsvSink};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::{DataFusionError, Result, not_impl_err};
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_expr::dml::InsertOp;

use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::r#gen::CsvWriteOptions;

#[derive(Debug, Clone)]
pub struct CsvWriteFormat {
    pub(super) options: CsvWriteOptions,
}

#[async_trait]
impl WriteFormat for CsvWriteFormat {
    async fn sink(
        &self,
        ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let terminator = self
            .options
            .line_terminator()
            .map_err(DataFusionError::from)?;
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let format = CsvFormat::default().with_options(options);
        input.sink.file_extension = if let Some(file_compression_type) = format.compression_type() {
            format.get_ext_with_compression(&file_compression_type)?
        } else {
            format.get_ext()
        };
        if matches!(terminator, Terminator::Any(b'\n')) {
            return format
                .create_writer_physical_plan(input.input, ctx, input.sink, input.sort_order)
                .await;
        }
        // `CsvWriterOptions` built from `CsvOptions` always uses `\n` as the line terminator,
        // so the sink is created here with the line terminator set, as
        // `CsvFormat::create_writer_physical_plan` does otherwise.
        if input.sink.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for CSV");
        }
        let has_header = format
            .options()
            .has_header
            .unwrap_or_else(|| ctx.config_options().catalog.has_header);
        let options = format.options().clone().with_has_header(has_header);
        let mut writer_options = CsvWriterOptions::try_from(&options)?;
        writer_options.writer_options = writer_options
            .writer_options
            .with_line_terminator(terminator);
        let sink = Arc::new(CsvSink::new(input.sink, writer_options));
        Ok(Arc::new(DataSinkExec::new(
            input.input,
            sink,
            input.sort_order,
        )))
    }
}
