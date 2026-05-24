use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;
use sail_common::spec;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{
    FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::{ParquetReadOptions, ParquetWriteOptions};
use crate::options::ResolveOptions;

mod options;

pub type ParquetTableFormat = ListingTableFormat<ParquetFormatFactory>;

#[derive(Debug, Default)]
pub struct ParquetFormatFactory;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    options: ParquetReadOptions,
}

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    options: ParquetWriteOptions,
}

impl FormatFactory for ParquetFormatFactory {
    type Read = ParquetReadFormat;
    type Write = ParquetWriteFormat;

    fn name() -> &'static str {
        "parquet"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = ParquetReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetWriteFormat { options })
    }
}

impl ReadFormat for ParquetReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(Some(self.options.extension.clone()))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(ParquetSchemaInfer {
            options: self.options.clone(),
        })
    }
}

impl WriteFormat for ParquetWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let compression = options.global.compression.clone();
        Ok((
            Arc::new(ParquetFormat::default().with_options(options)),
            compression,
        ))
    }
}

#[derive(Debug)]
struct ParquetSchemaInfer {
    options: ParquetReadOptions,
}

#[async_trait::async_trait]
impl SchemaInfer for ParquetSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        _list_options: &datafusion::datasource::listing::ListingOptions,
    ) -> Result<Schema> {
        let base_options = self.options.clone().into_table_options();
        let base_schema = ParquetFormat::default()
            .with_options(base_options.clone())
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();

        if !base_options.global.skip_metadata {
            return Ok(base_schema);
        }

        let mut metadata_options = base_options;
        metadata_options.global.skip_metadata = false;
        let metadata_schema = ParquetFormat::default()
            .with_options(metadata_options)
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();

        Ok(restore_spark_metadata_in_schema(
            base_schema,
            &metadata_schema,
        ))
    }
}

fn restore_spark_metadata_in_schema(base: Schema, metadata: &Schema) -> Schema {
    let fields = restore_spark_metadata_in_fields(base.fields(), metadata.fields());
    Schema::new_with_metadata(fields, spark_metadata(metadata.metadata()))
}

fn restore_spark_metadata_in_fields(base: &Fields, metadata: &Fields) -> Vec<Field> {
    base.iter()
        .enumerate()
        .map(|(index, base)| {
            if let Some(metadata) = metadata.iter().nth(index) {
                restore_spark_metadata_in_field(base, metadata)
            } else {
                base.as_ref().clone()
            }
        })
        .collect()
}

fn restore_spark_metadata_in_field(base: &Field, metadata: &Field) -> Field {
    let data_type = restore_spark_metadata_in_type(base.data_type(), metadata.data_type());
    let mut field = base.as_ref().clone().with_data_type(data_type);
    field.set_metadata(spark_metadata(metadata.metadata()));
    field
}

fn restore_spark_metadata_in_type(base: &DataType, metadata: &DataType) -> DataType {
    match (base, metadata) {
        (DataType::List(base), DataType::List(metadata)) => {
            DataType::List(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::LargeList(base), DataType::LargeList(metadata)) => {
            DataType::LargeList(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::FixedSizeList(base, size), DataType::FixedSizeList(metadata, _)) => {
            DataType::FixedSizeList(
                Arc::new(restore_spark_metadata_in_field(base, metadata)),
                *size,
            )
        }
        (DataType::ListView(base), DataType::ListView(metadata)) => {
            DataType::ListView(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::LargeListView(base), DataType::LargeListView(metadata)) => {
            DataType::LargeListView(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::Struct(base), DataType::Struct(metadata)) => DataType::Struct(Fields::from(
            restore_spark_metadata_in_fields(base, metadata),
        )),
        (DataType::Map(base, sorted), DataType::Map(metadata, _)) => DataType::Map(
            Arc::new(restore_spark_metadata_in_field(base, metadata)),
            *sorted,
        ),
        _ => base.clone(),
    }
}

fn spark_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    [
        spec::SPARK_METADATA_JSON_KEY,
        spec::SAIL_SPARK_UDT_METADATA_KEY,
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY,
    ]
    .into_iter()
    .filter_map(|key| {
        metadata
            .get(key)
            .map(|value| (key.to_string(), value.clone()))
    })
    .collect()
}
