use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, plan_datafusion_err};
use sail_parquet::{ParquetWriteExecutionOptions, ParquetWriterExec};

use crate::listing::source::{ListingSinkInput, WriteFormat};
use crate::options::r#gen::ParquetWriteOptions;
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
        input.sink.file_extension = parquet_file_extension(&options)?;
        let mut execution_options =
            ParquetWriteExecutionOptions::from(&ctx.config_options().execution);
        execution_options.max_records_per_file = self.max_records_per_file()?;
        Ok(Arc::new(ParquetWriterExec::try_new(
            input.input,
            input.sink,
            options,
            execution_options,
            input.sort_order,
        )?))
    }
}

impl ParquetWriteFormat {
    fn max_records_per_file(&self) -> Result<Option<NonZeroUsize>> {
        if self.options.max_records_per_file <= 0 {
            return Ok(None);
        }
        let value = usize::try_from(self.options.max_records_per_file)
            .map_err(|_| plan_datafusion_err!("maxRecordsPerFile is too large"))?;
        Ok(NonZeroUsize::new(value))
    }
}

fn parquet_file_extension(options: &TableParquetOptions) -> Result<String> {
    let compression = options.global.compression.as_deref().unwrap_or("snappy");
    let (codec, _level) = split_parquet_compression_string(&compression.to_lowercase())?;
    let suffix = match codec.as_str() {
        "" | "none" | "uncompressed" => "",
        "snappy" => "snappy",
        "gzip" => "gz",
        "lzo" => "lzo",
        "brotli" => "br",
        "lz4" => "lz4hadoop",
        "lz4_raw" | "lz4raw" => "lz4raw",
        "zstd" => "zstd",
        _ => {
            return Err(plan_datafusion_err!(
                "unsupported Parquet compression codec: {codec}"
            ));
        }
    };
    Ok(if suffix.is_empty() {
        "parquet".to_string()
    } else {
        format!("{suffix}.parquet")
    })
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::TableParquetOptions;

    use super::parquet_file_extension;

    #[test]
    fn uses_spark_parquet_compression_suffixes() -> datafusion_common::Result<()> {
        for (compression, extension) in [
            (None, "snappy.parquet"),
            (Some("none"), "parquet"),
            (Some("uncompressed"), "parquet"),
            (Some("snappy"), "snappy.parquet"),
            (Some("gzip(4)"), "gz.parquet"),
            (Some("brotli(4)"), "br.parquet"),
            (Some("lz4"), "lz4hadoop.parquet"),
            (Some("lz4_raw"), "lz4raw.parquet"),
            (Some("zstd(4)"), "zstd.parquet"),
        ] {
            let mut options = TableParquetOptions::default();
            options.global.compression = compression.map(str::to_string);
            assert_eq!(parquet_file_extension(&options)?, extension);
        }
        Ok(())
    }
}
