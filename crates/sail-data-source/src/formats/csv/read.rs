use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int64Type, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::config::CsvOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::delimited::newline_delimited_stream;
use object_store::{Error as ObjectStoreError, ObjectMeta, ObjectStoreExt};

use super::decoder::decode_utf8_lossy_stream;
use super::source::CsvSource;
use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::{infer_listing_compression, list_all_files};
use crate::options::r#gen::CsvReadOptions;
use crate::url::PathGlobFilter;

#[derive(Debug, Clone)]
pub struct CsvReadFormat {
    pub(super) options: CsvReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for CsvReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if options.compression != CompressionTypeVariant::UNCOMPRESSED {
            return Ok(options.compression);
        }
        Ok(infer_listing_compression(files)?.unwrap_or(CompressionTypeVariant::UNCOMPRESSED))
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let mut options = self.options.clone().into_table_options()?;
        options.compression = compression;

        let csv_format = CsvFormat::default().with_options(options.clone());

        let mut schemas: Vec<Schema> = vec![];
        let Some(mut records_to_read) = options.schema_infer_max_rec else {
            // `into_table_options` always sets `schema_infer_max_records` to `Some`
            unreachable!();
        };

        'outer: for group in files {
            for object in &group.objects {
                let stream = group.store.get(&object.location).await?;
                let stream: BoxStream<'static, Result<Bytes>> = stream
                    .into_stream()
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
                    .boxed();

                let stream =
                    FileCompressionType::from(options.compression).convert_stream(stream)?;
                let stream = decode_utf8_lossy_stream(stream);
                let stream = newline_delimited_stream(stream.map_err(|error| match error {
                    DataFusionError::ObjectStore(error) => *error,
                    error => ObjectStoreError::Generic {
                        store: "CSV schema inference",
                        source: Box::new(error),
                    },
                }))
                .map_err(DataFusionError::from)
                .boxed();
                let (schema, records_read) = csv_format
                    .infer_schema_from_stream(ctx, records_to_read, stream)
                    .await
                    .map_err(|err| {
                        DataFusionError::Context(
                            format!("Error when processing CSV file {}", object.location),
                            Box::new(err),
                        )
                    })?;

                records_to_read = records_to_read.saturating_sub(records_read);
                schemas.push(schema);
                if records_to_read == 0 {
                    break 'outer;
                }
            }
        }

        let mut schema = Schema::try_merge(schemas)?;
        if !self.options.infer_schema {
            schema = super::convert_string_columns(schema);
        } else {
            schema = self
                .narrow_integer_columns(ctx, files, &options, schema)
                .await?;
            // A column whose every value is null has no type to infer, and `toStructFields` reads
            // that as a STRING (`CSVInferSchema.scala:105-109`). Arrow infers `Null`, which is a
            // `void` column: it is not Spark's type and it breaks a write or a join downstream.
            schema = Schema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if field.data_type().is_null() {
                            Arc::new(Field::new(field.name(), DataType::Utf8, true))
                        } else {
                            Arc::clone(field)
                        }
                    })
                    .collect::<Fields>(),
                schema.metadata().clone(),
            );
        }
        schema = super::rename_default_csv_columns(schema);

        Ok(Arc::new(schema))
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        options.compression = input.compression;

        // Consult configuration options for default values
        let has_header = options
            .has_header
            .unwrap_or_else(|| ctx.config_options().catalog.has_header);
        let newlines_in_values = options
            .newlines_in_values
            .unwrap_or_else(|| ctx.config_options().catalog.newlines_in_values);

        options.has_header = Some(has_header);
        options.newlines_in_values = Some(newlines_in_values);

        let source = CsvSource::new(input.schema).with_csv_options(options.clone());

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_file_compression_type(FileCompressionType::from(options.compression))
            .with_preserve_order(input.preserve_order)
            .with_output_partitioning(input.output_partitioning)
            .build();

        Ok(config)
    }

    fn path_glob_filter(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}

impl CsvReadFormat {
    /// Spark infers an integer column as INT when every value fits one, and as BIGINT only past
    /// that (`CSVInferSchema.tryParseInteger`, `CSVInferSchema.scala:138-139,159`); DataFusion goes
    /// straight to BIGINT. The type is not cosmetic -- `DATE + column` takes an INT and refuses a
    /// BIGINT -- so each BIGINT column whose values all fit is narrowed.
    ///
    /// Spark's `inferSchema` reads every row of every file (`samplingRatio` 1.0), and so does this:
    /// the type sample above is a prefix, and narrowing on a prefix would fail the scan on a later
    /// value past an INT. Only the BIGINT columns are parsed, the files are streamed, and the pass
    /// stops as soon as no column can be narrowed any more. A file the pass cannot read keeps the
    /// schema as DataFusion inferred it.
    async fn narrow_integer_columns(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        options: &CsvOptions,
        schema: Schema,
    ) -> Result<Schema> {
        let mut fits: Vec<bool> = schema
            .fields()
            .iter()
            .map(|field| field.data_type() == &DataType::Int64)
            .collect();
        if !fits.contains(&true) {
            return Ok(schema);
        }
        let path_glob_filter = self
            .path_glob_filter()
            .map(PathGlobFilter::parse)
            .transpose()?;
        for group in files {
            let objects: Vec<ObjectMeta> = list_all_files(
                group.url,
                ctx,
                group.store.as_ref(),
                path_glob_filter.as_ref(),
            )
            .await?
            .try_filter(|meta| futures::future::ready(meta.size > 0))
            .try_collect()
            .await?;
            for object in objects {
                let Ok(still_fits) = self
                    .integer_columns_fit(ctx, group, &object, options, &schema, &fits)
                    .await
                else {
                    return Ok(schema);
                };
                fits = still_fits;
                if !fits.contains(&true) {
                    return Ok(schema);
                }
            }
        }
        let fields = schema
            .fields()
            .iter()
            .zip(&fits)
            .map(|(field, fits)| {
                if *fits {
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Int32))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }

    /// Streams one file and clears `fits` for every column holding a value past an INT.
    async fn integer_columns_fit(
        &self,
        ctx: &dyn Session,
        group: &ListingFileSample<'_>,
        object: &ObjectMeta,
        options: &CsvOptions,
        schema: &Schema,
        fits: &[bool],
    ) -> Result<Vec<bool>> {
        let mut fits = fits.to_vec();
        let columns: Vec<usize> = (0..fits.len()).filter(|index| fits[*index]).collect();
        // The columns still in question are parsed as numbers; the rest only split, as text.
        let read_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let data_type = if fits[index] {
                        DataType::Int64
                    } else {
                        DataType::Utf8
                    };
                    Field::new(field.name(), data_type, true)
                })
                .collect::<Vec<_>>(),
        ));
        let has_header = options
            .has_header
            .unwrap_or_else(|| ctx.config_options().catalog.has_header);
        let mut builder = ReaderBuilder::new(read_schema)
            .with_header(has_header)
            .with_delimiter(options.delimiter)
            .with_quote(options.quote)
            .with_truncated_rows(options.truncated_rows.unwrap_or(false))
            .with_projection(columns.clone());
        if let Some(escape) = options.escape {
            builder = builder.with_escape(escape);
        }
        if let Some(comment) = options.comment {
            builder = builder.with_comment(comment);
        }
        if let Some(null_regex) = &options.null_regex {
            let regex = regex::Regex::new(null_regex)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            builder = builder.with_null_regex(regex);
        }
        let mut decoder = builder.build_decoder();

        let stream = group.store.get(&object.location).await?;
        let stream: BoxStream<'static, Result<Bytes>> = stream
            .into_stream()
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
            .boxed();
        let stream = FileCompressionType::from(options.compression).convert_stream(stream)?;
        let mut stream = decode_utf8_lossy_stream(stream);

        let mut buffered = Bytes::new();
        let mut exhausted = false;
        loop {
            if buffered.is_empty() && !exhausted {
                match stream.next().await {
                    Some(chunk) => buffered = chunk?,
                    None => exhausted = true,
                }
            }
            let decoded = decoder.decode(buffered.as_ref())?;
            let _ = buffered.split_to(decoded);
            if decoded == 0 || decoder.capacity() == 0 {
                let Some(batch) = decoder.flush()? else {
                    break;
                };
                // The projection keeps `columns` in order, one batch column each.
                for (position, &index) in columns.iter().enumerate() {
                    if fits[index]
                        && batch
                            .column(position)
                            .as_primitive::<Int64Type>()
                            .iter()
                            .flatten()
                            .any(|value| i32::try_from(value).is_err())
                    {
                        fits[index] = false;
                    }
                }
                if columns.iter().all(|index| !fits[*index]) {
                    break;
                }
            }
        }
        Ok(fits)
    }
}
