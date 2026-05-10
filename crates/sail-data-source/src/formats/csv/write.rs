use std::sync::Arc;

use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;
use crate::options::gen::CsvWriteOptions;

#[derive(Debug, Clone)]
pub struct CsvWriteFormat {
    pub(super) options: CsvWriteOptions,
}

impl WriteFormat for CsvWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        Ok((Arc::new(CsvFormat::default().with_options(options)), None))
    }
}
