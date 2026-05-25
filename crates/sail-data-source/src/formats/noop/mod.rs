use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::formats::noop::writer::NoopSinkExec;

mod writer;

#[derive(Debug, Default)]
pub struct NoopTableFormat;

#[async_trait]
impl TableFormat for NoopTableFormat {
    fn name(&self) -> &str {
        "noop"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("noop format does not support reading")
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            partition_by,
            bucket_by,
            ..
        } = info;

        if is_flow_event_schema(&input.schema()) {
            return plan_err!("cannot write streaming data to noop format");
        }

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for noop write format");
        }

        if !partition_by.is_empty() {
            return not_impl_err!("partitioning for noop write format");
        }

        Ok(Arc::new(NoopSinkExec::new(input)))
    }
}
