use std::io::{BufReader, Read};
use std::sync::Arc;

use bytes::Buf;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::JsonSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource_json::utils::JsonArrayToNdjsonReader;
use object_store::{GetResultPayload, ObjectStoreExt};
use serde_json::Value;

use super::records::{JsonRecord, JsonRecordOptions, ParseMode};
use super::source::{SparkJsonSource, has_corrupt_record_column};
use crate::listing::source::{ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::{
    NanosecondTimestamps, infer_listing_compression, try_merge_normalized,
};
use crate::options::r#gen::JsonReadOptions;

#[derive(Debug, Clone)]
pub struct JsonReadFormat {
    pub(super) options: JsonReadOptions,
}

#[async_trait::async_trait]
impl ReadFormat for JsonReadFormat {
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
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let record_options = JsonRecordOptions::try_new(&self.options)?;
        let schema = if JsonRecordOptions::is_required(&self.options) {
            self.infer_record_schema(files, compression, &record_options)
                .await?
        } else {
            match self
                .infer_newline_delimited_schema(files, compression)
                .await
            {
                Ok(schema) => schema,
                // Like Spark, malformed records are inferred as the corrupt record column.
                Err(_) if record_options.mode == ParseMode::Permissive => {
                    self.infer_record_schema(files, compression, &record_options)
                        .await?
                }
                Err(e) => return Err(e),
            }
        };
        let schema = if self.options.primitives_as_string {
            primitives_as_string(schema)
        } else {
            schema
        };
        let schema = sort_inferred_fields(schema);
        let schema = if self.options.drop_field_if_all_null {
            drop_all_null_fields(schema)
        } else {
            schema
        };
        Ok(Arc::new(schema))
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        options.compression = input.compression;

        let record_options = JsonRecordOptions::try_new(&self.options)?;
        let source: Arc<dyn FileSource> = if JsonRecordOptions::is_required(&self.options)
            || has_corrupt_record_column(input.schema.file_schema(), &record_options)
        {
            Arc::new(SparkJsonSource::new(
                input.schema,
                record_options,
                self.options.primitives_as_string,
            ))
        } else {
            Arc::new(
                JsonSource::new(input.schema).with_newline_delimited(options.newline_delimited),
            )
        };

        let config = FileScanConfigBuilder::new(input.object_store_url, source)
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

impl JsonReadFormat {
    /// Infers the schema of newline-delimited JSON files.
    async fn infer_newline_delimited_schema(
        &self,
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
    ) -> Result<Schema> {
        let mut schemas: Vec<Schema> = vec![];
        let mut records_to_read = self.options.schema_infer_max_records;
        let file_compression_type = FileCompressionType::from(compression);
        let newline_delimited = true;

        'outer: for group in files {
            for object in &group.objects {
                if records_to_read == 0 {
                    break 'outer;
                }

                let r = group.store.as_ref().get(&object.location).await?;
                let decoder = match r.payload {
                    #[cfg(not(target_arch = "wasm32"))]
                    GetResultPayload::File(file, _) => file_compression_type.convert_read(file)?,
                    GetResultPayload::Stream(_) => {
                        // Fetching entire file is potentially wasteful but required for stream payloads.
                        let data = r.bytes().await?;
                        file_compression_type.convert_read(data.reader())?
                    }
                };
                let reader = BufReader::new(decoder);
                let (schema, records_consumed) = if newline_delimited {
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
                };

                schemas.push(schema);
                records_to_read = records_to_read.saturating_sub(records_consumed);
            }
        }

        // Spark's JSON reader behaves like its CSV reader: with `inferTimestamp`
        // it infers a timestamp from a string and truncates to microseconds.
        // Nothing observable depends on this today, because `arrow-json` never
        // infers a timestamp from a string, so this path cannot produce one; the
        // policy is stated so that the day it can, it matches Spark rather than
        // inheriting Parquet's.
        try_merge_normalized(schemas, NanosecondTimestamps::WidenToMicrosecond)
    }

    /// Infers the schema from the records found as Spark does, with the corrupt record
    /// column when a record is malformed in the `PERMISSIVE` mode.
    async fn infer_record_schema(
        &self,
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
        record_options: &JsonRecordOptions,
    ) -> Result<Schema> {
        let file_compression_type = FileCompressionType::from(compression);
        let mut values = vec![];
        let mut has_corrupt_records = false;
        'outer: for group in files {
            for object in &group.objects {
                let data = group.store.get(&object.location).await?.bytes().await?;
                let mut content = vec![];
                file_compression_type
                    .convert_read(data.reader())?
                    .read_to_end(&mut content)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                for record in record_options.read_records(&content)? {
                    if values.len() >= self.options.schema_infer_max_records {
                        break 'outer;
                    }
                    match record {
                        JsonRecord::Valid(object) => values.push(Value::Object(object)),
                        JsonRecord::Corrupt(text) => match record_options.mode {
                            ParseMode::FailFast => {
                                return Err(record_options.malformed_record_error(&text));
                            }
                            ParseMode::DropMalformed => {}
                            ParseMode::Permissive => has_corrupt_records = true,
                        },
                    }
                }
            }
        }
        let schema = infer_json_schema_from_iterator(values.into_iter().map(Ok))?;
        let schema = try_merge_normalized(vec![schema], NanosecondTimestamps::WidenToMicrosecond)?;
        if !has_corrupt_records
            || schema
                .field_with_name(&record_options.column_name_of_corrupt_record)
                .is_ok()
        {
            return Ok(schema);
        }
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            &record_options.column_name_of_corrupt_record,
            DataType::Utf8,
            true,
        )));
        Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }
}

/// Infers every primitive value as a string, as Spark's `primitivesAsString` option does.
fn primitives_as_string(schema: Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| primitive_field_as_string(field))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn primitive_field_as_string(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::List(element) => DataType::List(Arc::new(primitive_field_as_string(element))),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| primitive_field_as_string(field))
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Null => DataType::Null,
        _ => DataType::Utf8,
    };
    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(field.metadata().clone())
}

/// Sorts the inferred fields by name at every level, as Spark's JSON schema inference does.
fn sort_inferred_fields(schema: Schema) -> Schema {
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| sort_inferred_field(field))
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name().cmp(right.name()));
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn sort_inferred_field(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::List(element) => DataType::List(Arc::new(sort_inferred_field(element))),
        DataType::Struct(fields) => {
            let mut fields = fields
                .iter()
                .map(|field| sort_inferred_field(field))
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.name().cmp(right.name()));
            DataType::Struct(fields.into())
        }
        data_type => data_type.clone(),
    };
    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(field.metadata().clone())
}

/// Removes fields whose inferred JSON type contains no concrete value.
fn drop_all_null_fields(schema: Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .filter_map(|field| canonicalize_inferred_field(field))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Canonicalizes one field using Spark's `dropFieldIfAllNull` behavior.
fn canonicalize_inferred_field(field: &Field) -> Option<Field> {
    let data_type = match field.data_type() {
        DataType::Null => return None,
        DataType::List(element) => {
            let element = canonicalize_inferred_field(element)?;
            DataType::List(Arc::new(element))
        }
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .filter_map(|field| canonicalize_inferred_field(field))
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return None;
            }
            DataType::Struct(fields.into())
        }
        data_type => data_type.clone(),
    };
    Some(
        Field::new(field.name(), data_type, field.is_nullable())
            .with_metadata(field.metadata().clone()),
    )
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
