use std::any::Any;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_datasource::decoder::{deserialize_stream, Decoder, DecoderDeserializer};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::{calculate_range, PartitionedFile, RangeCalculation, TableSchema};
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};

use crate::formats::text;
use crate::formats::text::reader::{Format, ReaderBuilder};

#[derive(Debug, Clone, Default)]
pub struct TextSource {
    whole_text: bool,
    line_sep: Option<u8>,
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    file_projection: Option<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl TextSource {
    pub fn new(whole_text: bool, line_sep: Option<u8>) -> Self {
        Self {
            whole_text,
            line_sep,
            ..Self::default()
        }
    }

    pub fn whole_text(&self) -> bool {
        self.whole_text
    }

    pub fn line_sep(&self) -> Option<u8> {
        self.line_sep
    }

    pub fn with_whole_text(mut self, whole_text: bool) -> Self {
        self.whole_text = whole_text;
        self
    }

    pub fn with_line_sep(mut self, line_sep: Option<u8>) -> Self {
        self.line_sep = line_sep;
        self
    }

    fn open<R: Read>(&self, reader: R) -> Result<text::reader::Reader<R>> {
        Ok(self.builder()?.build(reader)?)
    }

    fn builder(&self) -> Result<ReaderBuilder> {
        let batch_size = self.batch_size.ok_or_else(|| {
            DataFusionError::Internal("batch_size must be set before calling builder()".to_string())
        })?;
        let schema = if let Some(schema) = &self.file_schema {
            Arc::clone(schema)
        } else {
            return Err(DataFusionError::Internal(
                "Schema must be set before calling builder()".to_string(),
            ));
        };
        let mut format = Format::default().with_whole_text(self.whole_text);
        if let Some(line_sep) = self.line_sep {
            format = format.with_line_sep(line_sep);
        }
        let mut builder = ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .with_format(format);
        if let Some(file_projection) = &self.file_projection {
            builder = builder.with_projection(file_projection.clone());
        }
        Ok(builder)
    }
}

impl From<TextSource> for Arc<dyn FileSource> {
    fn from(source: TextSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for TextSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(TextOpener::new(
            Arc::new(self.clone()),
            base_config.file_compression_type,
            object_store,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_schema = Some(schema.file_schema().clone());
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_projection = config.file_column_projection_indices();
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        statistics.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "projected_statistics must be set before calling statistics()".to_string(),
            )
        })
    }

    fn file_type(&self) -> &str {
        "text"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", whole_text={}", self.whole_text)?;
                if let Some(sep) = self.line_sep {
                    write!(f, ", line_sep={:?}", sep as char)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

pub struct TextOpener {
    config: Arc<TextSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl TextOpener {
    pub fn new(
        config: Arc<TextSource>,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            config,
            file_compression_type,
            object_store,
        }
    }
}

impl FileOpener for TextOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let file_compression_type = self.file_compression_type.to_owned();
        if file.range.is_some() && file_compression_type.is_compressed() {
            return Err(DataFusionError::Internal(
                "Reading compressed .txt in parallel is not supported".to_string(),
            ));
        }

        let store = Arc::clone(&self.object_store);
        let line_sep = self.config.line_sep;
        let config = self.config.clone();

        Ok(Box::pin(async move {
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)
            let calculated_range = calculate_range(&file, &store, line_sep).await?;
            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed())
                }
            };
            let options = GetOptions {
                range,
                ..Default::default()
            };
            let result = store.get_opts(&file.object_meta.location, options).await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut local_file, _path) => {
                    let is_whole_file_scanned = file.range.is_none();
                    let decoder = if is_whole_file_scanned {
                        // Don't seek if no range as breaks FIFO files
                        file_compression_type.convert_read(local_file)?
                    } else {
                        local_file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type
                            .convert_read(local_file.take(result.range.end - result.range.start))?
                    };

                    Ok(futures::stream::iter(config.open(decoder)?)
                        .map_err(DataFusionError::from)
                        .boxed())
                }
                GetResultPayload::Stream(s) => {
                    let decoder = config.builder()?.build_decoder();
                    let s = s.map_err(DataFusionError::from);
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    Ok(deserialize_stream(
                        input,
                        DecoderDeserializer::new(TextDecoder::new(decoder)),
                    )
                    .map_err(DataFusionError::from)
                    .boxed())
                }
            }
        }))
    }
}

#[derive(Debug)]
pub struct TextDecoder {
    inner: text::reader::Decoder,
}

impl TextDecoder {
    pub fn new(decoder: text::reader::Decoder) -> Self {
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
