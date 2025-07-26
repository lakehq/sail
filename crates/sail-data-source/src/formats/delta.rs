use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use deltalake::protocol::SaveMode;
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_format::DeltaDataSink;

#[derive(Debug, Default)]
pub struct DeltaTableFormat;

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema: _,
            options,
        } = info;
        let [table_uri] = paths.as_slice() else {
            return plan_err!("Must provide a single path for a Delta table");
        };
        // TODO: schema is ignored for now
        create_delta_provider(ctx, table_uri, &options).await
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            path,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;

        if !partition_by.is_empty() {
            return not_impl_err!("partitioning for Delta format");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        let mode = match mode {
            PhysicalSinkMode::ErrorIfExists => SaveMode::ErrorIfExists,
            PhysicalSinkMode::IgnoreIfExists => SaveMode::Ignore,
            PhysicalSinkMode::Append => SaveMode::Append,
            PhysicalSinkMode::Overwrite => SaveMode::Overwrite,
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
        };
        let sink = Arc::new(DeltaDataSink::new(mode, path, options, input.schema()));

        Ok(Arc::new(DataSinkExec::new(input, sink, sort_order)))
    }
}
