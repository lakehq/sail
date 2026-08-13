use std::fmt;
use std::io::Read;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, apply_expression_roots};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::boundary_stream::AlignedBoundaryStream;
use datafusion_datasource::decoder::{Decoder, DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};

use crate::formats::text;
use crate::formats::text::reader::{Format, ReaderBuilder};

#[derive(Debug, Clone)]
pub struct TextSource {
    table_schema: TableSchema,
    whole_text: bool,
    line_sep: Option<u8>,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl TextSource {
    pub fn new(
        table_schema: impl Into<TableSchema>,
        whole_text: bool,
        line_sep: Option<u8>,
    ) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            whole_text,
            line_sep,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn table_schema(&self) -> &TableSchema {
        &self.table_schema
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
        let schema = Arc::clone(self.table_schema.file_schema());
        let mut format = Format::default().with_whole_text(self.whole_text);
        if let Some(line_sep) = self.line_sep {
            format = format.with_line_sep(line_sep);
        }
        let builder = ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .with_format(format)
            .with_projection(self.projection.file_indices.clone());
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
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(TextOpener::new(
            Arc::new(self.clone()),
            base_config.file_compression_type,
            object_store,
        )) as Arc<dyn FileOpener>;

        let opener = ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
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
            let file_size = file.object_meta.size;
            let location = file.object_meta.location;

            if let Some(file_range) = file.range.as_ref() {
                let raw_start = u64::try_from(file_range.start).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "text file range start does not fit in u64: {}",
                        file_range.start
                    ))
                })?;
                let raw_end = u64::try_from(file_range.end).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "text file range end does not fit in u64: {}",
                        file_range.end
                    ))
                })?;
                let stream = AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    location.clone(),
                    raw_start,
                    raw_end,
                    file_size,
                    line_sep.unwrap_or(b'\n'),
                )
                .await?
                .map_err(DataFusionError::from);
                let decoder = config.builder()?.build_decoder();
                let input = file_compression_type.convert_stream(stream.boxed())?.fuse();
                return Ok(deserialize_stream(
                    input,
                    DecoderDeserializer::new(TextDecoder::new(decoder)),
                )
                .map_err(DataFusionError::from)
                .boxed());
            }

            let result = store.get_opts(&location, GetOptions::default()).await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(local_file, _path) => {
                    let decoder = file_compression_type.convert_read(local_file)?;

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
