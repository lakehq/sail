use std::sync::Arc;

use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;

#[derive(Debug, Default, Clone)]
pub struct ArrowWriteFormat;

impl WriteFormat for ArrowWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(ArrowFormat), None))
    }
}
