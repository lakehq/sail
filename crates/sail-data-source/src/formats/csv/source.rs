use std::fmt;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use csv_core::{ReadRecordResult, Reader, ReaderBuilder, Terminator};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::file_format::csv::CsvDecoder;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_common::config::CsvOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{
    FileRange, PartitionedFile, RangeCalculation, TableSchema, calculate_range,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};

use super::decoder::{LossyUtf8Reader, decode_utf8_lossy_stream};
use super::projected::{DecoderBatchReader, ProjectedCsvDecoder, ProjectedCsvOptions};

type CsvBatchReader = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;

struct CsvHeaderValidator {
    reader: Reader,
    output: Vec<u8>,
    output_len: usize,
    ends: Vec<usize>,
    ends_len: usize,
    expected: Vec<String>,
    path: String,
    complete: bool,
}

impl CsvHeaderValidator {
    /// Creates a header validator configured with the same dialect as the CSV reader.
    fn new(config: &CsvSource, path: &str) -> Self {
        let mut builder = ReaderBuilder::new();
        builder.escape(config.escape());
        builder.comment(config.comment());
        builder.delimiter(config.delimiter());
        builder.quote(config.quote());
        if let Some(terminator) = config.terminator() {
            builder.terminator(Terminator::Any(terminator));
        }

        Self {
            reader: builder.build(),
            output: vec![0; 1024],
            output_len: 0,
            ends: vec![0; config.table_schema.file_schema().fields().len() + 1],
            ends_len: 0,
            expected: config
                .table_schema
                .file_schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            path: path.to_string(),
            complete: false,
        }
    }

    /// Feeds CSV bytes until the first non-comment record is available for validation.
    fn feed(&mut self, input: &[u8]) -> Result<bool> {
        if self.complete {
            return Ok(true);
        }

        let mut consumed = 0;
        loop {
            let (result, read, written, ends) = self.reader.read_record(
                &input[consumed..],
                &mut self.output[self.output_len..],
                &mut self.ends[self.ends_len..],
            );
            consumed += read;
            self.output_len += written;
            self.ends_len += ends;

            match result {
                ReadRecordResult::InputEmpty => return Ok(false),
                ReadRecordResult::OutputFull => {
                    self.output.resize(self.output.len() * 2, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    self.ends.resize(self.ends.len() * 2, 0);
                }
                ReadRecordResult::Record => {
                    self.validate()?;
                    self.complete = true;
                    return Ok(true);
                }
                ReadRecordResult::End => return Ok(false),
            }
        }
    }

    /// Finishes parsing and validates a final header without a record terminator.
    fn finish(&mut self) -> Result<()> {
        while !self.complete {
            if !self.feed(&[])? {
                break;
            }
        }
        Ok(())
    }

    /// Compares parsed header names with schema fields by position.
    fn validate(&self) -> Result<()> {
        let mut start = 0;
        let actual = self.ends[..self.ends_len]
            .iter()
            .map(|end| {
                let field = String::from_utf8_lossy(&self.output[start..*end]).into_owned();
                start = *end;
                field
            })
            .collect::<Vec<_>>();

        if actual == self.expected {
            return Ok(());
        }

        Err(DataFusionError::Execution(format!(
            "CSV header does not conform to the schema.\nHeader: [{}]\nSchema: [{}]\nExpected: {} but found: {}\nCSV file: {}",
            actual.join(", "),
            self.expected.join(", "),
            self.expected
                .iter()
                .zip(actual.iter())
                .find_map(|(expected, actual)| (expected != actual).then_some(expected.as_str()))
                .unwrap_or_else(|| self
                    .expected
                    .get(actual.len())
                    .map_or("<missing>", String::as_str)),
            self.expected
                .iter()
                .zip(actual.iter())
                .find_map(|(expected, actual)| (expected != actual).then_some(actual.as_str()))
                .unwrap_or_else(|| actual
                    .get(self.expected.len())
                    .map_or("<missing>", String::as_str)),
            self.path,
        )))
    }
}

/// Validates a reader's CSV header and returns every byte consumed while doing so.
fn validate_reader_header<R: Read>(
    reader: &mut R,
    config: &CsvSource,
    path: &str,
) -> Result<Vec<u8>> {
    let mut validator = CsvHeaderValidator::new(config, path);
    let mut prefix = Vec::new();
    let mut buffer = [0; 8 * 1024];

    while !validator.complete {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            validator.finish()?;
            break;
        }
        prefix.extend_from_slice(&buffer[..read]);
        validator.feed(&buffer[..read])?;
    }
    Ok(prefix)
}

/// Validates a stream's CSV header and replays the inspected byte chunks.
async fn validate_stream_header(
    mut input: futures::stream::BoxStream<'static, Result<Bytes>>,
    config: &CsvSource,
    path: &str,
) -> Result<futures::stream::BoxStream<'static, Result<Bytes>>> {
    let mut validator = CsvHeaderValidator::new(config, path);
    let mut prefix = Vec::new();

    while !validator.complete {
        match input.try_next().await? {
            Some(bytes) => {
                validator.feed(&bytes)?;
                prefix.push(bytes);
            }
            None => {
                validator.finish()?;
                break;
            }
        }
    }

    Ok(futures::stream::iter(prefix.into_iter().map(Ok))
        .chain(input)
        .boxed())
}

#[derive(Debug, Clone)]
pub struct CsvSource {
    options: CsvOptions,
    enforce_schema: bool,
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
            enforce_schema: true,
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

    /// Sets whether the configured schema is applied without validating CSV headers.
    pub fn with_enforce_schema(mut self, enforce_schema: bool) -> Self {
        self.enforce_schema = enforce_schema;
        self
    }

    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    /// Returns whether the configured schema is applied without validating CSV headers.
    pub fn enforce_schema(&self) -> bool {
        self.enforce_schema
    }

    fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }

    /// Returns whether this source should validate the first CSV record as a header.
    fn should_validate_header(&self) -> bool {
        self.has_header() && !self.enforce_schema
    }

    fn truncate_rows(&self) -> bool {
        self.options.truncated_rows.unwrap_or(false)
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
        match self.projected_decoder()? {
            Some(decoder) => Ok(Box::new(DecoderBatchReader::new(
                BufReader::new(reader),
                decoder,
            ))),
            None => Ok(Box::new(self.builder()?.build(reader)?)),
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
        let schema = self.table_schema.file_schema();
        let all_strings = schema
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8);
        if !all_strings {
            return Ok(None);
        }
        let decoder = ProjectedCsvDecoder::try_new(ProjectedCsvOptions {
            schema: Arc::clone(schema),
            projection: self.projection.file_indices.clone(),
            delimiter: self.delimiter(),
            quote: self.quote(),
            escape: self.escape(),
            comment: self.comment(),
            terminator: self.terminator(),
            has_header: self.has_header(),
            truncated_rows: self.truncate_rows(),
            batch_size: self.batch_size()?,
        })?;
        Ok(Some(decoder))
    }

    fn builder(&self) -> Result<csv::ReaderBuilder> {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(self.table_schema.file_schema()))
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
        let path = partitioned_file.object_meta.location.to_string();

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range(&partitioned_file, &object_store, terminator).await?;
            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed());
                }
            };
            let result = object_store
                .get_opts(
                    &partitioned_file.object_meta.location,
                    GetOptions {
                        range,
                        ..Default::default()
                    },
                )
                .await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let decompressed = if partitioned_file.range.is_none() {
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type.convert_read(
                            file.take((result.range.end - result.range.start) as u64),
                        )?
                    };
                    let mut decoded = LossyUtf8Reader::new(decompressed);
                    let prefix = if config.should_validate_header() {
                        validate_reader_header(&mut decoded, &config, &path)?
                    } else {
                        Vec::new()
                    };
                    let mut reader = config.open(Cursor::new(prefix).chain(decoded))?;
                    let iterator = std::iter::from_fn(move || {
                        let mut timer = baseline_metrics.elapsed_compute().timer();
                        let result = reader.next();
                        timer.stop();
                        result
                    });

                    Ok(futures::stream::iter(iterator)
                        .map_err(DataFusionError::from)
                        .boxed())
                }
                GetResultPayload::Stream(stream) => {
                    let input = stream.map_err(DataFusionError::from).boxed();
                    let input = file_compression_type.convert_stream(input)?;
                    let input = decode_utf8_lossy_stream(input);
                    let input = if config.should_validate_header() {
                        validate_stream_header(input, &config, &path).await?
                    } else {
                        input
                    };
                    let input = input.fuse();
                    let stream = match config.projected_decoder()? {
                        Some(decoder) => {
                            deserialize_stream(input, DecoderDeserializer::new(decoder))
                        }
                        None => {
                            let decoder = config.builder()?.build_decoder();
                            deserialize_stream(
                                input,
                                DecoderDeserializer::new(CsvDecoder::new(decoder)),
                            )
                        }
                    };

                    Ok(stream.map_err(DataFusionError::from).boxed())
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    /// Creates a validating CSV source with the requested string columns.
    fn source_with_fields(names: &[&str]) -> CsvSource {
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let options = CsvOptions {
            has_header: Some(true),
            ..Default::default()
        };

        let mut source = CsvSource::new(schema)
            .with_csv_options(options)
            .with_enforce_schema(false);
        source.batch_size = Some(1024);
        source
    }

    /// Verifies that validated bytes are replayed to the ordinary CSV decoder.
    #[test]
    fn matching_reader_header_is_replayed() -> Result<()> {
        let source = source_with_fields(&["f1", "f2"]);
        let mut input = Cursor::new(b"f1,f2\n1,2\n".to_vec());
        let prefix = validate_reader_header(&mut input, &source, "matching.csv")?;
        let reader = Cursor::new(prefix).chain(input);
        let batches = source
            .open(reader)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        Ok(())
    }

    /// Verifies that header fields are checked against schema fields by position.
    #[test]
    fn mismatched_reader_header_returns_error() -> Result<()> {
        let source = source_with_fields(&["f2", "f1"]);
        let mut input = Cursor::new(b"f1,f2\n1,2\n".to_vec());
        let error = validate_reader_header(&mut input, &source, "mismatched.csv")
            .err()
            .ok_or_else(|| {
                DataFusionError::Execution("reversed header was accepted".to_string())
            })?;

        assert!(
            error
                .to_string()
                .contains("CSV header does not conform to the schema")
        );
        assert!(error.to_string().contains("Expected: f2 but found: f1"));
        assert!(error.to_string().contains("mismatched.csv"));
        Ok(())
    }

    /// Verifies that streaming validation handles quoted fields split across chunks.
    #[tokio::test]
    async fn matching_stream_header_is_replayed() -> Result<()> {
        let source = source_with_fields(&["first,name", "f2"]);
        let chunks = vec![
            Ok(Bytes::from_static(b"\"first")),
            Ok(Bytes::from_static(b",name\",f2\n1")),
            Ok(Bytes::from_static(b",2\n")),
        ];
        let input = futures::stream::iter(chunks).boxed();
        let output = validate_stream_header(input, &source, "stream.csv")
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(output.concat(), b"\"first,name\",f2\n1,2\n".as_slice());
        Ok(())
    }
}
