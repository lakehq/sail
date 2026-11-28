// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::delimited::newline_delimited_stream;
use object_store::{Error as ObjectStoreError, ObjectStoreExt};
use sail_common_datafusion::type_coercion::binary_type_coercion::type_union_coercion;
use sail_object_store::GetResultExt;

use super::decoder::{CsvInputDecoder, decode_csv_input_stream};
use super::source::CsvSource;
use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::{
    NanosecondTimestamps, SchemaSampling, infer_listing_compression, try_merge_normalized,
};
use crate::options::r#gen::CsvReadOptions;

#[derive(Debug, Clone)]
pub struct CsvReadFormat {
    pub(super) options: CsvReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for CsvReadFormat {
    fn configure_schema_inference(
        &mut self,
        _ctx: &dyn Session,
        hadoop_paths: bool,
        _case_sensitive: bool,
    ) -> Result<SchemaSampling> {
        Ok(if hadoop_paths && self.options.multi_line {
            SchemaSampling::MultilineCsv
        } else {
            SchemaSampling::Sample
        })
    }

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

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/0d1f2ebe2cc97c91b736bc0a160b5b73cf40437a/datafusion/datasource-csv/src/file_format.rs
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
                    .read_stream()
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
                    .boxed();

                let stream =
                    FileCompressionType::from(options.compression).convert_stream(stream)?;
                let stream = decode_csv_input_stream(stream, CsvInputDecoder::new(&options));
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

                // Blank or comment-only files do not supply the first multiline CSV header.
                if self.options.multi_line && schema.fields.is_empty() {
                    continue;
                }
                records_to_read = records_to_read.saturating_sub(records_read);
                schemas.push(schema);
                if records_to_read == 0 {
                    break 'outer;
                }
            }
        }

        // Spark takes multiline CSV field names from the first header and combines
        // the types inferred from each file by position.
        if self.options.multi_line && schemas.len() > 1 {
            let mut files = schemas.into_iter();
            let mut first = files.next().unwrap_or_else(Schema::empty);
            let mut fields = first.fields.to_vec();
            for file in files {
                for (field, other) in fields.iter_mut().zip(file.fields()) {
                    let (a, b) = (field.data_type(), other.data_type());
                    if a != b {
                        let ty = if a.is_string() || b.is_string() {
                            DataType::Utf8
                        } else {
                            type_union_coercion(a, b).unwrap_or(DataType::Utf8)
                        };
                        *field = Arc::new(field.as_ref().clone().with_data_type(ty));
                    }
                }
            }
            first.fields = fields.into();
            schemas = vec![first];
        }
        let mut schema = try_merge_normalized(schemas, NanosecondTimestamps::WidenToMicrosecond)?;
        if !self.options.infer_schema {
            schema = super::convert_string_columns(schema);
        }
        schema = super::rename_default_csv_columns(schema);

        Ok(Arc::new(schema))
    }
    // SPDX-SnippetEnd

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/0d1f2ebe2cc97c91b736bc0a160b5b73cf40437a/datafusion/datasource-csv/src/file_format.rs
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
    // SPDX-SnippetEnd

    fn path_glob_filter(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}
