mod options;
mod write_node;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::logical_expr::TableSource;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource::{SinkInfo, SinkMode, SourceInfo, TableFormat};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;
pub use write_node::ConsoleWriteNode;

pub use crate::formats::console::writer::ConsoleSinkExec;
use crate::options::gen::ConsoleWriteOptions;
use crate::options::ResolveOptions;

fn is_streaming_table_provider(provider: &dyn TableProvider) -> bool {
    if provider
        .as_any()
        .downcast_ref::<StreamSourceTableProvider>()
        .is_some()
    {
        true
    } else if let Some(rename) = provider.as_any().downcast_ref::<RenameTableProvider>() {
        is_streaming_table_provider(rename.inner().as_ref())
    } else {
        false
    }
}

fn is_streaming_logical_plan(plan: &LogicalPlan) -> bool {
    plan.exists(|plan| {
        if let LogicalPlan::TableScan(scan) = plan {
            Ok(source_as_provider(&scan.source)
                .is_ok_and(|p| is_streaming_table_provider(p.as_ref())))
        } else {
            Ok(false)
        }
    })
    .unwrap_or(false)
}

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

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        if !is_flow_event_schema(info.input.schema().inner())
            && !is_streaming_logical_plan(&info.input)
        {
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
