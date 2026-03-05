use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::Session;
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, not_impl_err, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::formats::binary::file_format::BinaryFileFormat;
use crate::formats::binary::options::resolve_binary_read_options;
use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};

pub mod file_format;
pub mod options;
pub mod reader;
pub mod source;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct TableBinaryOptions {
    pub path_glob_filter: Option<String>,
}

pub type BinaryTableFormat = ListingTableFormat<BinaryListingFormat>;

#[derive(Debug, Default)]
pub struct BinaryListingFormat;

impl ListingFormat for BinaryListingFormat {
    fn name(&self) -> &'static str {
        "binaryFile"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(BinaryFileFormat::new(
            resolve_binary_read_options(options)?,
        )))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        not_impl_err!("Binary file format does not support writing")
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
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
