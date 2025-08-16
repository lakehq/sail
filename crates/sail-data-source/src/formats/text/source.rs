use crate::utils::char_to_u8;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion_datasource::decoder::{deserialize_stream, DecoderDeserializer};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{
    as_file_source, calculate_range, FileRange, ListingTableUrl, PartitionedFile, RangeCalculation,
};
use std::any::Any;
use std::fmt;
use std::io::{Seek, SeekFrom};
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, DataFusionError, GetExt, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{
    delimited::newline_delimited_stream, GetOptions, GetResultPayload, ObjectMeta, ObjectStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct TextSource {
    whole_text: bool,
    line_sep: Option<u8>,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl TextSource {
    pub fn new(whole_text: bool, line_sep: Option<u8>) -> Self {
        Self {
            whole_text,
            line_sep,
            ..Self::default()
        }
    }

    pub fn whole_text(&self) -> bool {
        self.whole_text
    }

    pub fn line_sep(&self) -> Option<u8> {
        self.line_sep
    }

    pub fn with_whole_text(mut self, whole_text: bool) -> Self {
        self.whole_text = whole_text;
        self
    }

    pub fn with_line_sep(mut self, line_sep: Option<u8>) -> Self {
        self.line_sep = line_sep;
        self
    }
}

pub struct TextOpener {
    config: Arc<TextSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl TextOpener {
    pub fn new(
        config: Arc<TextSource>,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            config,
            file_compression_type,
            object_store,
        }
    }
}

impl From<TextSource> for Arc<dyn FileSource> {
    fn from(source: TextSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for TextSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(TextOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        statistics.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "projected_statistics must be set before calling statistics()".to_string(),
            )
        })
    }

    fn file_type(&self) -> &str {
        "txt"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", whole_text={}", self.whole_text)?;
                if let Some(sep) = self.line_sep {
                    write!(f, ", line_sep={:?}", sep as char)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

impl FileOpener for TextOpener {
    fn open(&self, file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {}
}

pub async fn plan_to_text(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
}
