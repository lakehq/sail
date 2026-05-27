use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};

#[derive(Debug, Default, Clone)]
pub struct ArrowReadFormat;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];

async fn is_object_in_arrow_ipc_file_format(
    store: Arc<dyn ObjectStore>,
    object_location: &Path,
) -> Result<bool> {
    let get_opts = GetOptions {
        range: Some(GetRange::Bounded(0..6)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(object_location, get_opts)
        .await?
        .bytes()
        .await?;
    Ok(bytes.len() >= 6 && bytes[0..6] == ARROW_MAGIC)
}

#[async_trait::async_trait]
impl ReadFormat for ArrowReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(crate::listing::source::DefaultSchemaInfer)
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let object_store = ctx.runtime_env().object_store(&input.object_store_url)?;
        let object_location = &input
            .file_groups
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .files()
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .object_meta
            .location;

        let table_schema = input.schema;
        let source =
            match is_object_in_arrow_ipc_file_format(Arc::clone(&object_store), object_location)
                .await?
            {
                true => ArrowSource::new_file_source(table_schema.clone()),
                false => ArrowSource::new_stream_file_source(table_schema),
            };

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
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
