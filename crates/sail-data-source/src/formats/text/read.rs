use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};

use crate::formats::text::file_format::TextFileFormat;
use crate::formats::text::source::TextSource;
use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};
use crate::options::gen::TextReadOptions;

#[derive(Debug, Clone)]
pub struct TextReadFormat {
    pub(super) options: TextReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for TextReadFormat {
    fn create_read_format(
        &self,
        compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(TextFileFormat::new(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(crate::listing::source::DefaultSchemaInfer)
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        mut input: ListingScanInput,
    ) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if let Some(compression) = input.compression.take() {
            options.compression = compression;
        }

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
}
