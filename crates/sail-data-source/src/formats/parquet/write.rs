use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::WriteFormat;
use crate::options::gen::ParquetWriteOptions;

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    pub(super) options: ParquetWriteOptions,
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
