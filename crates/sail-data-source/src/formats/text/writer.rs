use std::fmt;
use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArrayType};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::BatchSerializer;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
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
        match column.data_type() {
            DataType::Utf8 => self.write_strings(column.as_string::<i32>()),
            DataType::LargeUtf8 => self.write_strings(column.as_string::<i64>()),
            DataType::Utf8View => self.write_strings(column.as_string_view()),
            data_type => Err(ArrowError::CastError(format!(
                "Text data source requires a string column, got {data_type}"
            ))
            .into()),
        }
    }

    fn write_strings<'a, S>(&mut self, string_array: &'a S) -> Result<()>
    where
        &'a S: StringArrayType<'a>,
    {
        // BufWriter uses a buffer size of 8KB, so double this and flush once we have more than 8KB
        let mut buffer = Vec::with_capacity(16 * 1024);
        for value in string_array.iter() {
            if let Some(value) = value {
                buffer.extend_from_slice(value.as_bytes());
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

    #[cfg_attr(not(test), expect(unused))]
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
                write!(f, "file={}", self.config.original_url)
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn text_writer_accepts_all_spark_string_arrays() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("short"),
                None,
                Some("long value"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("short"),
                None,
                Some("long value"),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("short"),
                None,
                Some("long value"),
            ])),
        ];

        for array in arrays {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let mut writer = TextWriter::new(Vec::new(), b'\n');
            writer.write(&batch).unwrap();
            assert_eq!(writer.into_inner(), b"short\n\nlong value\n");
        }
    }
}
