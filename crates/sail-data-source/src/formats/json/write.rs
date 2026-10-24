use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::json::LineDelimitedWriter;
use datafusion::catalog::Session;
use datafusion::common::runtime::SpawnedTask;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result, not_impl_err};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::BatchSerializer;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
use datafusion_expr::dml::InsertOp;
use object_store::ObjectStore;

use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::r#gen::JsonWriteOptions;

#[derive(Debug, Clone)]
pub struct JsonWriteFormat {
    pub(super) options: JsonWriteOptions,
}

#[async_trait]
impl WriteFormat for JsonWriteFormat {
    async fn sink(
        &self,
        ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let line_sep = self.options.line_sep.clone();
        if line_sep.is_empty() {
            return Err(DataFusionError::Plan(
                "'lineSep' cannot be an empty string".to_string(),
            ));
        }
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let format = JsonFormat::default().with_options(options);
        input.sink.file_extension = if let Some(file_compression_type) = format.compression_type() {
            format.get_ext_with_compression(&file_compression_type)?
        } else {
            format.get_ext()
        };
        if line_sep == "\n" {
            return format
                .create_writer_physical_plan(input.input, ctx, input.sink, input.sort_order)
                .await;
        }
        // The JSON sink of DataFusion always ends each record with a newline.
        if input.sink.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for JSON");
        }
        let sink = Arc::new(JsonSink {
            config: input.sink,
            line_sep: Bytes::from(line_sep.into_bytes()),
            compression: format.options().compression,
        });
        Ok(Arc::new(DataSinkExec::new(
            input.input,
            sink,
            input.sort_order,
        )))
    }
}

/// Serializes each row as a JSON object followed by the line separator.
struct JsonSerializer {
    line_sep: Bytes,
}

impl BatchSerializer for JsonSerializer {
    fn serialize(&self, batch: RecordBatch, _initial: bool) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(4096);
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch)?;
        writer.finish()?;
        // Each row is written on its own line, and a newline within a value is escaped,
        // so every newline in the output ends a row.
        let mut output = Vec::with_capacity(buffer.len());
        for row in buffer.split_inclusive(|b| *b == b'\n') {
            output.extend_from_slice(row.strip_suffix(b"\n").unwrap_or(row));
            output.extend_from_slice(&self.line_sep);
        }
        Ok(Bytes::from(output))
    }
}

/// A JSON sink that ends each record with a custom line separator.
struct JsonSink {
    config: FileSinkConfig,
    line_sep: Bytes,
    compression: CompressionTypeVariant,
}

impl Debug for JsonSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonSink").finish()
    }
}

impl DisplayAs for JsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "JsonSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: json")?;
                write!(f, "file={}", self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl FileSink for JsonSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let serializer = Arc::new(JsonSerializer {
            line_sep: self.line_sep.clone(),
        }) as _;
        spawn_writer_tasks_and_join(
            context,
            serializer,
            self.compression.into(),
            None,
            object_store,
            demux_task,
            file_stream_rx,
        )
        .await
    }
}

#[async_trait]
impl DataSink for JsonSink {
    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}
