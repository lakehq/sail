use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::formats::text::file_format::TextFileFormat;
use crate::listing::source::WriteFormat;
use crate::options::gen::TextWriteOptions;

#[derive(Debug, Clone)]
pub struct TextWriteFormat {
    pub(super) options: TextWriteOptions,
}

impl WriteFormat for TextWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        Ok((Arc::new(TextFileFormat::new(options)), None))
    }
}
