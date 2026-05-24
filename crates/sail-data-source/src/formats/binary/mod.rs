use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::Session;
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::binary::file_format::BinaryFileFormat;
use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::BinaryReadOptions;
use crate::options::ResolveOptions;

pub mod file_format;
pub mod options;
pub mod reader;
pub mod source;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct TableBinaryOptions {
    pub path_glob_filter: Option<String>,
}

pub type BinaryTableFormat = ListingTableFormat<BinaryFormatFactory>;

#[derive(Debug, Default)]
pub struct BinaryFormatFactory;

#[derive(Debug, Clone)]
pub struct BinaryReadFormat {
    options: BinaryReadOptions,
}

#[derive(Debug, Default, Clone)]
pub struct BinaryWriteFormat;

impl FormatFactory for BinaryFormatFactory {
    type Read = BinaryReadFormat;
    type Write = BinaryWriteFormat;

    fn name() -> &'static str {
        "binaryFile"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(BinaryReadFormat {
            options: BinaryReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?,
        })
    }

    fn write(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(BinaryWriteFormat)
    }
}

impl ReadFormat for BinaryReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(BinaryFileFormat::new(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for BinaryWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        not_impl_err!("Binary file format does not support writing")
    }
}

fn read_schema(tz: Arc<str>) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "modificationTime",
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
            false,
        ),
        Field::new("length", DataType::Int64, false),
        Field::new("content", DataType::Binary, false),
    ]))
}

fn time_zone_from_read_schema(schema: &Schema) -> Result<Arc<str>> {
    let Ok(field) = schema.field_with_name("modificationTime") else {
        return internal_err!("schema must contain a 'modificationTime' field");
    };
    if let DataType::Timestamp(_, Some(tz)) = field.data_type() {
        Ok(tz.clone())
    } else {
        internal_err!("'modificationTime' field must be of type Timestamp with time zone")
    }
}
