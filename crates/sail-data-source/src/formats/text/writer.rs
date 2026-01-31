use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
use datafusion_datasource::write::BatchSerializer;
use object_store::ObjectStore;

use crate::formats::text::TableTextOptions;
use crate::utils::char_to_u8;

#[derive(Clone, Debug)]
pub struct TextWriterOptions {
    pub line_sep: u8,
    pub compression: CompressionTypeVariant,
}

impl TextWriterOptions {
    pub fn new(line_sep: u8, compression: CompressionTypeVariant) -> Self {
        Self {
            line_sep,
            compression,
        }
    }
}

impl TryFrom<&TableTextOptions> for TextWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: &TableTextOptions) -> Result<Self> {
        let line_sep = if let Some(line_sep) = value.line_sep {
            char_to_u8(line_sep, "line_sep")?
        } else {
            b'\n'
        };
        Ok(Self {
            line_sep,
            compression: value.compression,
        })
    }
}

struct TextWriter<W: Write> {
    writer: W,
    line_sep: u8,
}

impl<W: Write> TextWriter<W> {
    fn new(writer: W, line_sep: u8) -> Self {
        Self { writer, line_sep }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Text data source supports only a single column, and you have {} columns.",
                batch.num_columns()
            )));
        }

        let column = batch.column(0);
        let string_array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ArrowError::CastError("Failed to cast column to StringArray".to_string())
            })?;

        // BufWriter uses a buffer size of 8KB, so double this and flush once we have more than 8KB
        let mut buffer = Vec::with_capacity(16 * 1024);
        for row_idx in 0..batch.num_rows() {
            if !string_array.is_null(row_idx) {
                buffer.extend_from_slice(string_array.value(row_idx).as_bytes());
            }
            if buffer.len() > 8 * 1024 {
                self.writer.write_all(&buffer)?;
                buffer.clear();
            }
            buffer.write_all(&[self.line_sep])?;
        }

        if !buffer.is_empty() {
            self.writer.write_all(&buffer)?;
        }

        Ok(())
    }

    #[allow(unused)]
    fn into_inner(self) -> W {
        self.writer
    }
}

pub struct TextSerializer {
    line_sep: u8,
}

impl TextSerializer {
    pub fn new(line_sep: u8) -> Self {
        Self { line_sep }
    }
}

impl BatchSerializer for TextSerializer {
    fn serialize(&self, batch: RecordBatch, _initial: bool) -> Result<Bytes> {
        // Text files should have exactly one column named "value"
        if batch.num_columns() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Text data source supports only a single column, and you have {} columns.",
                batch.num_columns()
            )));
        }
        let mut buffer = Vec::with_capacity(4096);
        let mut writer = TextWriter::new(&mut buffer, self.line_sep);
        writer.write(&batch)?;
        Ok(Bytes::from(buffer))
    }
}

pub struct TextSink {
    config: FileSinkConfig,
    writer_options: TextWriterOptions,
}

impl Debug for TextSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextSink").finish()
    }
}

impl DisplayAs for TextSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TextSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: txt")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

impl TextSink {
    pub fn new(config: FileSinkConfig, writer_options: TextWriterOptions) -> Self {
        Self {
            config,
            writer_options,
        }
    }

    pub fn writer_options(&self) -> &TextWriterOptions {
        &self.writer_options
    }
}

#[async_trait::async_trait]
impl FileSink for TextSink {
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
        let serializer = Arc::new(TextSerializer::new(self.writer_options.line_sep)) as _;
        spawn_writer_tasks_and_join(
            context,
            serializer,
            self.writer_options.compression.into(),
            None,
            object_store,
            demux_task,
            file_stream_rx,
        )
        .await
    }
}

#[async_trait::async_trait]
impl DataSink for TextSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
