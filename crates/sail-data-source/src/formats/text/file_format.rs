use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use crate::utils::char_to_u8;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion_datasource::decoder::{deserialize_stream, Decoder, DecoderDeserializer};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{
    as_file_source, calculate_range, FileRange, ListingTableUrl, PartitionedFile, RangeCalculation,
};

use crate::formats::text::source::TextSource;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::Session;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, DataFusionError, GetExt, Result, Statistics};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
use datafusion_datasource::write::BatchSerializer;
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{delimited::newline_delimited_stream, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

pub const DEFAULT_TEXT_EXTENSION: &str = ".txt";

#[derive(Debug, Clone, PartialEq)]
pub struct TableTextOptions {
    pub whole_text: bool,
    pub line_sep: Option<char>,
    pub compression: CompressionTypeVariant,
}

impl Default for TableTextOptions {
    fn default() -> Self {
        Self {
            whole_text: false,
            line_sep: None,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }
}

#[derive(Debug)]
pub struct TextFileFormat {
    options: TableTextOptions,
}

impl TextFileFormat {
    pub fn new(table_text_options: TableTextOptions) -> Self {
        Self {
            options: table_text_options,
        }
    }

    pub fn options(&self) -> &TableTextOptions {
        &self.options
    }

    pub fn with_whole_text(mut self, enable: bool) -> Self {
        self.options.whole_text = enable;
        self
    }

    pub fn whole_text(&self) -> bool {
        self.options.whole_text
    }

    pub fn with_line_sep(mut self, line_sep: char) -> Self {
        self.options.line_sep = Some(line_sep);
        self
    }

    pub fn line_sep(&self) -> Option<char> {
        self.options.line_sep
    }

    pub fn with_compression(mut self, compression: CompressionTypeVariant) -> Self {
        self.options.compression = compression;
        self
    }

    pub fn compression(&self) -> CompressionTypeVariant {
        self.options.compression
    }
}

#[async_trait::async_trait]
impl FileFormat for TextFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        DEFAULT_TEXT_EXTENSION[1..].to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{ext}{}", file_compression_type.get_ext()))
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(self.options.compression.into())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // TODO: Should we infer if the DataType should be Utf8 or LargeUtf8?
        let schema = SchemaRef::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            ),
        ]));
        Ok(schema)
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let line_sep = self
            .options
            .line_sep
            .map(|line_sep| char_to_u8(line_sep, "line_sep"))
            .transpose()?;
        let source = Arc::new(TextSource::new(self.options.whole_text, line_sep));
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(self.options.compression))
            .with_source(source)
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Text files");
        }
        let writer_options = TextWriterOptions::try_from(&self.options)?;
        let sink = Arc::new(TextSink::new(conf, writer_options));
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(TextSource::default())
    }
}

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
        let mut buffer = Vec::with_capacity(4096);

        // Text files should have exactly one column named "value"
        if batch.num_columns() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Text files must have exactly 1 column, found {}",
                batch.num_columns()
            )));
        }

        // Ok(Bytes::from(buffer))
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
        // CHECK HERE: WRITER BUILDER?
        //         let builder = self.writer_options.writer_options.clone();
        let serializer = Arc::new(TextSerializer::new(self.writer_options.line_sep)) as _;
        spawn_writer_tasks_and_join(
            context,
            serializer,
            self.writer_options.compression.into(),
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

#[derive(Debug)]
pub struct TextDecoder {
    inner: ,
}

impl TextDecoder {
    pub fn new(decoder: ) -> Self {
        Self { inner: decoder }
    }
}

impl Decoder for TextDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.inner.decode(buf)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        self.inner.capacity() == 0
    }
}
