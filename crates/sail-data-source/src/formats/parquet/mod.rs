use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileMeta};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::TableSchema;
use datafusion_session::Session as DataFusionSession;
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::{ParquetReadOptions, ParquetWriteOptions};
use crate::options::ResolveOptions;

mod options;

pub type ParquetTableFormat = ListingTableFormat<ParquetFormatFactory>;

#[derive(Debug, Default)]
pub struct ParquetFormatFactory;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    options: ParquetReadOptions,
}

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    options: ParquetWriteOptions,
}

impl FormatFactory for ParquetFormatFactory {
    type Read = ParquetReadFormat;
    type Write = ParquetWriteFormat;

    fn name() -> &'static str {
        "parquet"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = ParquetReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetWriteFormat { options })
    }
}

impl ReadFormat for ParquetReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(SanitizedStatisticsFileFormat::new(Arc::new(
            ParquetFormat::default().with_options(options),
        ))))
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(Some(self.options.extension.clone()))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
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

#[derive(Debug)]
struct SanitizedStatisticsFileFormat {
    inner: Arc<dyn FileFormat>,
}

impl SanitizedStatisticsFileFormat {
    fn new(inner: Arc<dyn FileFormat>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl FileFormat for SanitizedStatisticsFileFormat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn DataFusionSession,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn DataFusionSession,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let statistics = self
            .inner
            .infer_stats(state, store, table_schema, object)
            .await?;
        Ok(sanitize_statistics(statistics))
    }

    async fn infer_ordering(
        &self,
        state: &dyn DataFusionSession,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn DataFusionSession,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        let mut meta = self
            .inner
            .infer_stats_and_ordering(state, store, table_schema, object)
            .await?;
        meta.statistics = sanitize_statistics(meta.statistics);
        Ok(meta)
    }

    async fn create_physical_plan(
        &self,
        state: &dyn DataFusionSession,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let statistics = sanitize_statistics(conf.statistics());
        let conf = FileScanConfigBuilder::from(conf)
            .with_statistics(statistics)
            .build();
        self.inner.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn DataFusionSession,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

fn sanitize_statistics(mut statistics: Statistics) -> Statistics {
    for column_statistics in &mut statistics.column_statistics {
        sanitize_column_statistics(column_statistics);
    }
    statistics
}

fn sanitize_column_statistics(column_statistics: &mut ColumnStatistics) {
    let invalid_min_max = match (
        column_statistics.min_value.get_value(),
        column_statistics.max_value.get_value(),
    ) {
        (Some(min), Some(max)) => min > max,
        _ => false,
    };

    if invalid_min_max {
        column_statistics.min_value = Precision::Absent;
        column_statistics.max_value = Precision::Absent;
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_sanitize_statistics_removes_invalid_min_max() {
        let statistics = Statistics {
            column_statistics: vec![ColumnStatistics {
                min_value: Precision::Exact(ScalarValue::Utf8(Some("3".to_string()))),
                max_value: Precision::Exact(ScalarValue::Utf8(Some("22".to_string()))),
                ..ColumnStatistics::new_unknown()
            }],
            ..Default::default()
        };

        let statistics = sanitize_statistics(statistics);

        assert_eq!(statistics.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(statistics.column_statistics[0].max_value, Precision::Absent);
    }

    #[test]
    fn test_sanitize_statistics_keeps_valid_min_max() {
        let statistics = Statistics {
            column_statistics: vec![ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Utf8(Some("2".to_string()))),
                max_value: Precision::Inexact(ScalarValue::Utf8(Some("3".to_string()))),
                ..ColumnStatistics::new_unknown()
            }],
            ..Default::default()
        };

        let statistics = sanitize_statistics(statistics);

        assert_eq!(
            statistics.column_statistics[0].min_value,
            Precision::Inexact(ScalarValue::Utf8(Some("2".to_string())))
        );
        assert_eq!(
            statistics.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Utf8(Some("3".to_string())))
        );
    }
}
