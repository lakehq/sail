use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;

use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::gen::ParquetWriteOptions;
use crate::utils::split_parquet_compression_string;

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    pub(super) options: ParquetWriteOptions,
}

#[async_trait]
impl WriteFormat for ParquetWriteFormat {
    async fn sink(
        &self,
        ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let format = ParquetFormat::default().with_options(options);
        input.sink.file_extension = self.file_extension()?;
        format
            .create_writer_physical_plan(input.input, ctx, input.sink, input.sort_order)
            .await
    }
}

impl ParquetWriteFormat {
    fn file_extension(&self) -> Result<String> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let compression = options.global.compression.clone();
        let format = ParquetFormat::default().with_options(options);
        if let Some(file_compression_type) = format.compression_type() {
            return match format.get_ext_with_compression(&file_compression_type) {
                Ok(ext) => Ok(ext),
                Err(_) => Ok(format.get_ext()),
            };
        }
        let ext = format.get_ext();
        let Some(compression) = compression else {
            return Ok(ext);
        };
        if !matches!(ext.as_str(), ".parquet" | "parquet") {
            return Ok(ext);
        }
        let ext = ext.strip_prefix('.').unwrap_or(&ext);
        let compression = compression.strip_prefix('.').unwrap_or(&compression);
        let (compression, _level) = split_parquet_compression_string(&compression.to_lowercase())?;
        let file_compression_type = FileCompressionType::from_str(compression.as_str());
        let compression = match file_compression_type {
            Ok(compression) => compression.get_ext(),
            Err(_) => compression,
        };
        let compression = compression.strip_prefix('.').unwrap_or(&compression);
        Ok(format!("{compression}.{ext}"))
    }
}
