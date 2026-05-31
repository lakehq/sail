mod options;
mod writer;
mod write_node;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::LogicalPlan;
use datafusion::logical_expr::TableSource;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SinkMode, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

pub use crate::formats::console::writer::ConsoleSinkExec;
use crate::options::gen::ConsoleWriteOptions;
use crate::options::ResolveOptions;
pub use write_node::ConsoleWriteNode;

/// Write data to stdout for testing purposes.
#[derive(Debug)]
pub struct ConsoleTableFormat;

#[async_trait]
impl TableFormat for ConsoleTableFormat {
    fn name(&self) -> &str {
        "console"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("console table format does not support reading")
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<LogicalPlan> {
        if !is_flow_event_schema(info.input.schema().inner()) {
            return plan_err!("the console table format only supports streaming data");
        }
        if !matches!(info.mode, SinkMode::Append) {
            return not_impl_err!("the console table format only supports append mode");
        }
        if !info.partition_by.is_empty() {
            return not_impl_err!("the console table format does not support partitioning");
        }
        if info.bucket_by.is_some() || !info.sort_order.is_empty() {
            return not_impl_err!("the console table format does not support bucketing");
        }

        let ConsoleWriteOptions {} = ConsoleWriteOptions::resolve(ctx, info.options)?;

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ConsoleWriteNode::new(Arc::new(info.input))),
        }))
    }
}
