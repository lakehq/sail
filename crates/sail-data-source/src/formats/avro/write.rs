use std::sync::Arc;

use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;

#[derive(Debug, Default, Clone)]
pub struct AvroWriteFormat;

impl WriteFormat for AvroWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(AvroFormat), None))
    }
}
