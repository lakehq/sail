use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion_common::Result;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};

use crate::formats::binary::source::BinarySource;
use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::options::r#gen::BinaryReadOptions;

#[derive(Debug, Clone)]
pub struct BinaryReadFormat {
    pub(super) options: BinaryReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for BinaryReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let tz = Arc::from(
            ctx.config()
                .options()
                .execution
                .time_zone
                .clone()
                .unwrap_or_else(|| "UTC".to_string()),
        );
        Ok(super::read_schema(tz))
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

    fn input_file_name_glob(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}
