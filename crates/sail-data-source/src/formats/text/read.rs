use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};

use crate::formats::text::source::TextSource;
use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::infer_listing_compression;
use crate::options::r#gen::TextReadOptions;

#[derive(Debug, Clone)]
pub struct TextReadFormat {
    pub(super) options: TextReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for TextReadFormat {
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
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            true,
        )])))
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        options.compression = input.compression;

        let line_sep = options.line_sep.map(|c| c as u8);
        let file_source = Arc::new(TextSource::new(input.schema, options.whole_text, line_sep));

        let config = FileScanConfigBuilder::new(input.object_store_url, file_source)
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

    fn path_glob_filter(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}
