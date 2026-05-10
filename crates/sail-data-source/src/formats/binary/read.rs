use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};

use crate::formats::binary::file_format::BinaryFileFormat;
use crate::formats::binary::source::BinarySource;
use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};
use crate::options::gen::BinaryReadOptions;

#[derive(Debug, Clone)]
pub struct BinaryReadFormat {
    pub(super) options: BinaryReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for BinaryReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(BinaryFileFormat::new(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(crate::listing::source::DefaultSchemaInfer)
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let options = self.options.clone().into_table_options();

        let file_source = Arc::new(BinarySource::new(
            input.schema,
            options.path_glob_filter.clone(),
        ));

        let config = FileScanConfigBuilder::new(input.object_store_url, file_source)
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}
