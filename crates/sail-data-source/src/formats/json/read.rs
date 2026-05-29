use std::io::BufReader;
use std::sync::Arc;

use bytes::Buf;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::reader::{infer_json_schema_from_iterator, ValueIter};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::JsonSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource_json::utils::JsonArrayToNdjsonReader;
use object_store::{GetResultPayload, ObjectStoreExt};

use crate::listing::source::{ListingScanInput, ReadFormat};
use crate::options::gen::JsonReadOptions;

#[derive(Debug, Clone)]
pub struct JsonReadFormat {
    pub(super) options: JsonReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for JsonReadFormat {
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
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    async fn infer_schema(
        &self,
        _ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        objects: &[object_store::ObjectMeta],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<Schema> = vec![];
        let mut records_to_read = self.options.schema_infer_max_records;
        let file_compression_type = FileCompressionType::from(compression);
        let newline_delimited = true;

        for object in objects {
            if records_to_read == 0 {
                break;
            }

            let r = store.as_ref().get(&object.location).await?;

            let (schema, records_consumed) = match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let reader = BufReader::new(decoder);
                    if newline_delimited {
                        let iter = ValueIter::new(reader, None);
                        let mut count = 0;
                        let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
                            let should_take = count < records_to_read;
                            if should_take {
                                count += 1;
                            }
                            should_take
                        }))?;
                        (schema, count)
                    } else {
                        infer_schema_from_json_array(reader, records_to_read)?
                    }
                }
                GetResultPayload::Stream(_) => {
                    // Fetching entire file is potentially wasteful but required for stream payloads.
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let reader = BufReader::new(decoder);
                    if newline_delimited {
                        let iter = ValueIter::new(reader, None);
                        let mut count = 0;
                        let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
                            let should_take = count < records_to_read;
                            if should_take {
                                count += 1;
                            }
                            should_take
                        }))?;
                        (schema, count)
                    } else {
                        infer_schema_from_json_array(reader, records_to_read)?
                    }
                }
            };

            schemas.push(schema);
            records_to_read = records_to_read.saturating_sub(records_consumed);
        }

        Ok(Arc::new(Schema::try_merge(schemas)?))
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

        let source =
            JsonSource::new(input.schema).with_newline_delimited(options.newline_delimited);

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

/// A tuple of (Schema, records_consumed) where records_consumed is the number of records that were
/// processed for schema inference.
fn infer_schema_from_json_array<R: std::io::Read>(
    reader: R,
    max_records: usize,
) -> Result<(Schema, usize)> {
    let ndjson_reader = JsonArrayToNdjsonReader::new(reader);
    let iter = ValueIter::new(ndjson_reader, None);
    let mut count = 0;
    let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
        let should_take = count < max_records;
        if should_take {
            count += 1;
        }
        should_take
    }))?;
    Ok((schema, count))
}
