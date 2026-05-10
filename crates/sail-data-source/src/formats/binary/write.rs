use std::sync::Arc;

use datafusion_common::{not_impl_err, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;

#[derive(Debug, Default, Clone)]
pub struct BinaryWriteFormat;

impl WriteFormat for BinaryWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        not_impl_err!("Binary file format does not support writing")
    }
}
