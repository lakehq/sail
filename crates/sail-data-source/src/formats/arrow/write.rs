use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::IpcWriteOptions;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion_common::config::ArrowOptions;
use datafusion_common::{Result, internal_datafusion_err, not_impl_err};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::{ListingSinkInput, WriteFormat};

#[derive(Debug, Default, Clone)]
pub struct ArrowWriteFormat;

#[async_trait]
impl WriteFormat for ArrowWriteFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let format = ArrowFormat::default();
        input.sink.file_extension = format.get_ext();
        let sink = ArrowSink::try_new(input.sink, format.options().clone())?;
        Ok(Arc::new(DataSinkExec::new(
            input.input,
            Arc::new(sink),
            input.sort_order,
        )))
    }
}

/// Serializable configuration for DataFusion's private Arrow file sink.
/// All demultiplexing, file writing and task cleanup remain upstream.
#[derive(Debug)]
pub struct ArrowSink {
    config: FileSinkConfig,
    options: ArrowOptions,
}

impl ArrowSink {
    pub fn try_new(config: FileSinkConfig, options: ArrowOptions) -> Result<Self> {
        if config.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Arrow format");
        }
        IpcWriteOptions::try_new(options.alignment, false, arrow::ipc::MetadataVersion::V5)?
            .try_with_compression(options.compression.into())?
            .try_with_compression_level(options.compression_level)?;
        Ok(Self { config, options })
    }

    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    pub fn options(&self) -> &ArrowOptions {
        &self.options
    }
}

impl DisplayAs for ArrowSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ArrowFileSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: arrow")?;
                write!(f, "file={}", self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl DataSink for ArrowSink {
    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        // The public factory is async and requires a Session, although Arrow does
        // not use it. The placeholder input is never executed: only its sink is used.
        let state = SessionStateBuilder::new()
            .with_config(context.session_config().clone())
            .with_runtime_env(context.runtime_env())
            .build();
        let plan = ArrowFormat::default()
            .with_options(self.options.clone())
            .create_writer_physical_plan(
                Arc::new(EmptyExec::new(Arc::clone(self.schema()))),
                &state,
                self.config.clone(),
                None,
            )
            .await?;
        let exec = plan.downcast_ref::<DataSinkExec>().ok_or_else(|| {
            internal_datafusion_err!("Arrow writer factory did not return a DataSinkExec")
        })?;
        exec.sink().write_all(data, context).await
    }
}
