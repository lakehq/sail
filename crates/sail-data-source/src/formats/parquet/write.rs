use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource_parquet::file_format::ParquetSink;

use crate::listing::source::WriteFormat;
use crate::options::gen::ParquetWriteOptions;

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    pub(super) options: ParquetWriteOptions,
}

impl WriteFormat for ParquetWriteFormat {
    fn sink(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _ctx: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Parquet");
        }

        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let sink = Arc::new(ParquetSink::new(conf, options));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            order_requirements,
        )) as _)
    }
}
