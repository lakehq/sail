mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{
    OptionLayer, PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

pub use crate::formats::console::writer::ConsoleSinkExec;

/// Write data to stdout for testing purposes.
#[derive(Debug)]
pub struct ConsoleTableFormat;

#[async_trait]
impl TableFormat for ConsoleTableFormat {
    fn name(&self) -> &str {
        "console"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        not_impl_err!("console table format does not support reading")
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let path = info.path();
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            table_properties: _,
            options,
            logical_schema: _,
        } = info;
        if !is_flow_event_schema(&input.schema()) {
            return plan_err!("the console table format only supports streaming data");
        }
        if !path.is_empty() {
            return plan_err!("the console table format does not support path");
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
        if options.iter().any(|layer| match layer {
            OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                !items.is_empty()
            }
            _ => true,
        }) {
            return not_impl_err!("the console table format does not support options");
        }
        Ok(Arc::new(ConsoleSinkExec::new(input)))
    }
}
