use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::physical_plan::CsvSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;

use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::infer_listing_compression;
use crate::options::r#gen::CsvReadOptions;

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

                let stream = csv_format
                    .read_to_delimited_chunks_from_stream(stream)
                    .await;
                let (schema, records_read) = csv_format
                    .infer_schema_from_stream(ctx, records_to_read, stream)
                    .await
                    .map_err(|err| {
                        DataFusionError::Context(
                            format!("Error when processing CSV file {}", &object.location),
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
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}
