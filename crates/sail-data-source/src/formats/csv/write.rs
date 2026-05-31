use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource_csv::file_format::CsvSink;

use crate::listing::source::WriteFormat;
use crate::options::gen::CsvWriteOptions;

#[derive(Debug, Clone)]
pub struct CsvWriteFormat {
    pub(super) options: CsvWriteOptions,
}

impl WriteFormat for CsvWriteFormat {
    fn sink(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for CSV");
        }

        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;

        let has_header = options
            .has_header
            .unwrap_or_else(|| ctx.config_options().catalog.has_header);
        let newlines_in_values = options
            .newlines_in_values
            .unwrap_or_else(|| ctx.config_options().catalog.newlines_in_values);
        let options = options
            .with_has_header(has_header)
            .with_newlines_in_values(newlines_in_values);

        let writer_options = CsvWriterOptions::try_from(&options)?;
        let sink = Arc::new(CsvSink::new(conf, writer_options));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            order_requirements,
        )) as _)
    }
}
