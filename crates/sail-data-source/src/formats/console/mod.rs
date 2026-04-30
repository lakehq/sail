mod options;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::formats::console::options::resolve_console_write_options;
pub use crate::formats::console::writer::ConsoleSinkExec;
use crate::options::gen::ConsoleWriteOptions;

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
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            logical_schema: _,
        } = info;
        if !is_flow_event_schema(&input.schema()) {
            return plan_err!("the console table format only supports streaming data");
        }
        if !matches!(mode, PhysicalSinkMode::Append) {
            return not_impl_err!("the console table format only supports append mode");
        }
        if !partition_by.is_empty() {
            return not_impl_err!("the console table format does not support partitioning");
        }
        if bucket_by.is_some() || sort_order.is_some() {
            return not_impl_err!("the console table format does not support bucketing");
        }
        let ConsoleWriteOptions {} = resolve_console_write_options(options)?;
        Ok(Arc::new(ConsoleSinkExec::new(input)))
    }
}
