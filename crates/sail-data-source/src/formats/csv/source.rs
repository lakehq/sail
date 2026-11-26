// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::io::{BufReader, Read};
use std::sync::Arc;

use bytes::Bytes;
use csv_core::{ReadRecordResult, ReaderBuilder, Terminator};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
#[cfg(not(target_arch = "wasm32"))]
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::file_format::csv::CsvDecoder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{DisplayFormatType, apply_expression_roots};
use datafusion_common::config::CsvOptions;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use datafusion_datasource::boundary_stream::AlignedBoundaryStream;
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{FileRange, PartitionedFile, TableSchema};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use sail_object_store::GetResultExt;

use super::decoder::{CsvInputDecoder, CsvInputReader, decode_csv_input_stream};
use super::projected::{DecoderBatchReader, ProjectedCsvDecoder, ProjectedCsvOptions};

type CsvBatchReader = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;

#[cfg(not(target_arch = "wasm32"))]
fn local_csv_chunks(reader: Box<dyn Read + Send>) -> BoxStream<'static, Result<Bytes>> {
    stream::try_unfold(reader, |mut reader| async move {
        SpawnedTask::spawn_blocking(move || {
            let mut bytes = Vec::new();
            (&mut reader).take(64 * 1024).read_to_end(&mut bytes)?;
            Ok((!bytes.is_empty()).then(|| (Bytes::from(bytes), reader)))
        })
        .await
        .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?
    })
    .boxed()
}

/// Count the fields in the first logical record. Reopen files whose leading comments or
/// first record exceed the replay buffer instead of retaining an unbounded prefix.
async fn multiline_file_width(
    input: BoxStream<'static, Result<Bytes>>,
    options: &CsvOptions,
) -> Result<(usize, Option<BoxStream<'static, Result<Bytes>>>)> {
    const REPLAY_LIMIT: usize = 64 * 1024;
    let mut input = input.fuse();
    let mut parser = ReaderBuilder::new();
    parser
        .delimiter(options.delimiter)
        .quote(options.quote)
        .escape(options.escape)
        .comment(options.comment);
    if let Some(terminator) = options.terminator {
        parser.terminator(Terminator::Any(terminator));
    }
    let mut parser = parser.build();
    let mut prefix = Some(Vec::new());
    let mut retained = 0;
    let mut width = 0;
    let mut field_bytes = [0; 4096];
    let mut field_ends = [0; 64];
    loop {
        let Some(bytes) = input.try_next().await? else {
            let (_, _, _, fields) = parser.read_record(&[], &mut field_bytes, &mut field_ends);
            width += fields;
            break;
        };
        let mut chunk = bytes.as_ref();
        let mut complete = false;
        while !chunk.is_empty() {
            let (result, read, _, fields) =
                parser.read_record(chunk, &mut field_bytes, &mut field_ends);
            width += fields;
            chunk = &chunk[read..];
            if result == ReadRecordResult::Record {
                complete = true;
                break;
            }
        }
        if let Some(chunks) = &mut prefix {
            retained += bytes.len();
            if complete || retained <= REPLAY_LIMIT {
                chunks.push(bytes);
            } else {
                prefix = None;
            }
        }
        if complete {
            break;
        }
    }
    Ok((
        width,
        prefix.map(|chunks| stream::iter(chunks).map(Ok).chain(input).boxed()),
    ))
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/0d1f2ebe2cc97c91b736bc0a160b5b73cf40437a/datafusion/datasource-csv/src/source.rs
#[derive(Debug, Clone)]
pub struct CsvSource {
    options: CsvOptions,
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl CsvSource {
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            options: CsvOptions::default(),
            batch_size: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn with_csv_options(mut self, options: CsvOptions) -> Self {
        self.options = options;
        self
    }

    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }

    fn truncate_rows(&self) -> bool {
        self.options.truncated_rows.unwrap_or(false)
    }

    fn permissive_multiline(&self) -> bool {
        self.options.newlines_in_values == Some(true) && self.truncate_rows()
    }

    fn all_string_fields(&self) -> bool {
        self.table_schema
            .file_schema()
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8)
    }

    fn delimiter(&self) -> u8 {
        self.options.delimiter
    }

    fn quote(&self) -> u8 {
        self.options.quote
    }

    fn terminator(&self) -> Option<u8> {
        self.options.terminator
    }

    fn comment(&self) -> Option<u8> {
        self.options.comment
    }

    fn escape(&self) -> Option<u8> {
        self.options.escape
    }

    fn open<R: Read + Send + 'static>(&self, reader: R) -> Result<CsvBatchReader> {
        let schema = self.table_schema.file_schema();
        match self.projected_decoder()? {
            Some(decoder) => Ok(Box::new(DecoderBatchReader::new(
                BufReader::new(reader),
                decoder,
            ))),
            None => Ok(Box::new(self.builder(schema)?.build(reader)?)),
        }
    }

    fn batch_size(&self) -> Result<usize> {
        self.batch_size.ok_or_else(|| {
            DataFusionError::Internal("batch_size must be set before calling builder()".to_string())
        })
    }

    /// The projected decoder covers all-string file schemas (the Spark default
    /// `inferSchema=false`); typed columns keep the arrow decoder.
    fn projected_decoder(&self) -> Result<Option<ProjectedCsvDecoder>> {
        if !self.all_string_fields() {
            return Ok(None);
        }
        let decoder = ProjectedCsvDecoder::try_new(ProjectedCsvOptions {
            schema: Arc::clone(self.table_schema.file_schema()),
            projection: self.projection.file_indices.clone(),
            delimiter: self.delimiter(),
            quote: self.quote(),
            escape: self.escape(),
            comment: self.comment(),
            terminator: self.terminator(),
            has_header: self.has_header(),
            truncated_rows: self.truncate_rows(),
            ignore_extra_fields: self.permissive_multiline(),
            batch_size: self.batch_size()?,
        })?;
        Ok(Some(decoder))
    }

    fn builder(&self, schema: &SchemaRef) -> Result<csv::ReaderBuilder> {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(schema))
            .with_delimiter(self.delimiter())
            .with_batch_size(self.batch_size()?)
            .with_header(self.has_header())
            .with_quote(self.quote())
            .with_truncated_rows(self.truncate_rows())
            .with_projection(self.projection.file_indices.clone());

        if let Some(terminator) = self.terminator() {
            builder = builder.with_terminator(terminator);
        }
        if let Some(escape) = self.escape() {
            builder = builder.with_escape(escape);
        }
        if let Some(comment) = self.comment() {
            builder = builder.with_comment(comment);
        }

        Ok(builder)
    }
}

impl From<CsvSource> for Arc<dyn FileSource> {
    fn from(source: CsvSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for CsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(CsvOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
            partition,
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
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
        "csv"
    }

    fn supports_repartitioning(&self) -> bool {
        !self.options.newlines_in_values.unwrap_or(false)
    }

    fn fmt_extra(&self, format: DisplayFormatType, output: &mut fmt::Formatter) -> fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(output, ", has_header={}", self.has_header())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

struct CsvOpener {
    config: Arc<CsvSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    partition: usize,
}

impl FileOpener for CsvOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let mut has_header = self.config.has_header();
        if let Some(FileRange { start, .. }) = partitioned_file.range
            && start != 0
        {
            has_header = false;
        }

        let mut config = (*self.config).clone();
        config.options.has_header = Some(has_header);
        config.options.truncated_rows = Some(config.truncate_rows());

        let file_compression_type = self.file_compression_type;
        if partitioned_file.range.is_some() && file_compression_type.is_compressed() {
            return Err(DataFusionError::Internal(
                "Reading compressed .csv in parallel is not supported".to_string(),
            ));
        }

        let object_store = Arc::clone(&self.object_store);
        let terminator = self.config.terminator();
        let baseline_metrics = BaselineMetrics::new(&self.config.metrics, self.partition);

        Ok(Box::pin(async move {
            let file_size = partitioned_file.object_meta.size;
            let location = partitioned_file.object_meta.location;

            if let Some(file_range) = partitioned_file.range.as_ref() {
                let raw_start: u64 = file_range.start.try_into().map_err(|_| {
                    exec_datafusion_err!(
                        "Expected start range to fit in u64, got {}",
                        file_range.start
                    )
                })?;
                let raw_end: u64 = file_range.end.try_into().map_err(|_| {
                    exec_datafusion_err!("Expected end range to fit in u64, got {}", file_range.end)
                })?;

                let aligned_stream = AlignedBoundaryStream::new(
                    Arc::clone(&object_store),
                    location.clone(),
                    raw_start,
                    raw_end,
                    file_size,
                    terminator.unwrap_or(b'\n'),
                )
                .await?
                .map_err(DataFusionError::from);
                let decoder = config
                    .builder(config.table_schema.file_schema())?
                    .build_decoder();
                let input = file_compression_type.convert_stream(aligned_stream.boxed())?;
                let input =
                    decode_csv_input_stream(input, CsvInputDecoder::new(&config.options)).fuse();
                let stream =
                    deserialize_stream(input, DecoderDeserializer::new(CsvDecoder::new(decoder)));
                return Ok(stream.map_err(DataFusionError::from).boxed());
            }

            let result = object_store
                .get_opts(&location, GetOptions::default())
                .await?;

            let multiline_width = config.permissive_multiline() && !config.all_string_fields();
            let reopen = GetOptions {
                if_match: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
                ..Default::default()
            };
            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) if !multiline_width => {
                    let decompressed = file_compression_type.convert_read(file)?;
                    let decoder = CsvInputDecoder::new(&config.options);
                    let mut reader = config.open(CsvInputReader::new(decompressed, decoder))?;
                    let iterator = std::iter::from_fn(move || {
                        let mut timer = baseline_metrics.elapsed_compute().timer();
                        let result = reader.next();
                        timer.stop();
                        result
                    });

                    return Ok(futures::stream::iter(iterator)
                        .map_err(DataFusionError::from)
                        .boxed());
                }
                _ => {}
            }
            let decode = |result: object_store::GetResult| -> Result<_> {
                let input = match result.payload {
                    // Local decoders read concatenated bzip2, XZ, and Zstandard streams.
                    #[cfg(not(target_arch = "wasm32"))]
                    GetResultPayload::File(file, _) if file_compression_type.is_compressed() => {
                        local_csv_chunks(file_compression_type.convert_read(file)?)
                    }
                    _ => {
                        let stream = result.read_stream().map_err(DataFusionError::from).boxed();
                        file_compression_type.convert_stream(stream)?
                    }
                };
                Ok(decode_csv_input_stream(
                    input,
                    CsvInputDecoder::new(&config.options),
                ))
            };
            let input = decode(result)?;
            let mut schema = Arc::clone(config.table_schema.file_schema());
            let input = if multiline_width {
                let (width, input) = multiline_file_width(input, &config.options).await?;
                if width == 0 {
                    return Ok(stream::empty().boxed());
                }
                if width > schema.fields.len() {
                    // Spark takes column names from the first input and ignores trailing fields.
                    // Arrow validates record width before applying the logical projection.
                    let mut fields = schema.fields.to_vec();
                    fields.resize(width, Arc::new(Field::new("", DataType::Utf8, true)));
                    schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata.clone()));
                }
                match input {
                    Some(input) => input,
                    None => decode(object_store.get_opts(&location, reopen).await?)?,
                }
            } else {
                input
            };
            let input = input.fuse();
            let stream = match config.projected_decoder()? {
                Some(decoder) => deserialize_stream(input, DecoderDeserializer::new(decoder)),
                None => {
                    let decoder = config.builder(&schema)?.build_decoder();
                    deserialize_stream(input, DecoderDeserializer::new(CsvDecoder::new(decoder)))
                }
            };
            Ok(stream.map_err(DataFusionError::from).boxed())
        }))
    }
}
// SPDX-SnippetEnd
