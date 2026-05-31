use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSinkExec;

use crate::formats::text::writer::TextWriterOptions;
use crate::formats::text::writer::TextSink;
use crate::listing::source::WriteFormat;
use crate::options::gen::TextWriteOptions;

#[derive(Debug, Clone)]
pub struct TextWriteFormat {
    pub(super) options: TextWriteOptions,
}

impl WriteFormat for TextWriteFormat {
    fn sink(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _ctx: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Text files");
        }

        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let writer_options = TextWriterOptions::try_from(&options)?;
        let sink = Arc::new(TextSink::new(conf, writer_options));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            order_requirements,
        )) as _)
    }
}
