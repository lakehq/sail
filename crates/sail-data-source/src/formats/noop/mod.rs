use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::LogicalPlan;
use datafusion::logical_expr::TableSource;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

mod writer;
mod write_node;

pub use writer::NoopSinkExec;
pub use write_node::NoopWriteNode;

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
    ) -> Result<LogicalPlan> {
        if is_flow_event_schema(info.input.schema().inner()) {
            return plan_err!("cannot write streaming data to noop format");
        }

        if info.bucket_by.is_some() {
            return not_impl_err!("bucketing for noop write format");
        }

        if !info.partition_by.is_empty() {
            return not_impl_err!("partitioning for noop write format");
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(NoopWriteNode::new(Arc::new(info.input))),
        }))
    }
}
