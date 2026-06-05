use std::sync::Arc;

use datafusion::datasource::file_format::json::JsonFormat;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;
use crate::options::gen::JsonWriteOptions;

#[derive(Debug, Clone)]
pub struct JsonWriteFormat {
    pub(super) options: JsonWriteOptions,
}

impl WriteFormat for JsonWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        Ok((Arc::new(JsonFormat::default().with_options(options)), None))
    }
}
