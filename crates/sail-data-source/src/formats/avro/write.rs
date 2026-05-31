use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, Result};
use datafusion_datasource::file_sink_config::FileSinkConfig;

use crate::listing::source::WriteFormat;

#[derive(Debug, Default, Clone)]
pub struct AvroWriteFormat;

impl WriteFormat for AvroWriteFormat {
    fn sink(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _ctx: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Avro file format writing is not implemented yet in Sail")
    }
}
