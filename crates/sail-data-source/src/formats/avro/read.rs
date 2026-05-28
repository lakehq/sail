use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::AvroSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource_avro::avro_to_arrow::read_avro_schema_from_reader;
use object_store::{GetResultPayload, ObjectStoreExt};

use crate::listing::source::{ListingScanInput, ReadFormat};

#[derive(Debug, Default, Clone)]
pub struct AvroReadFormat;

#[async_trait::async_trait]
impl ReadFormat for AvroReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    async fn infer_schema(
        &self,
        _ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in files {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => read_avro_schema_from_reader(&mut file)?,
                GetResultPayload::Stream(_) => {
                    // Fetching entire file to get schema is potentially wasteful but required for
                    // stream payloads.
                    let data = r.bytes().await?;
                    read_avro_schema_from_reader(&mut data.as_ref())?
                }
            };
            schemas.push(schema);
        }
        Ok(Arc::new(Schema::try_merge(schemas)?))
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let source = AvroSource::new(input.schema);

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
