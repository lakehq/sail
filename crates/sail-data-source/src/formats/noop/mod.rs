use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::file_format::FileFormat;
use datafusion::physical_expr_common::sort_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::TableSchema;
use datafusion_session::Session;
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::noop::writer::NoopSinkExec;
use crate::listing::source::{
    FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};

mod writer;

pub type NoopTableFormat = ListingTableFormat<NoopFormatFactory>;

#[derive(Debug, Default)]
pub struct NoopFormatFactory;

#[derive(Debug, Clone)]
pub struct NoopReadFormat;

#[derive(Debug, Clone)]
pub struct NoopWriteFormat;

#[derive(Debug)]
pub struct NoopFileFormat;

impl FormatFactory for NoopFormatFactory {
    type Read = NoopReadFormat;
    type Write = NoopWriteFormat;

    fn name() -> &'static str {
        "noop"
    }

    fn read(
        _ctx: &dyn datafusion::catalog::Session,
        _options: Vec<OptionLayer>,
    ) -> Result<Self::Read> {
        Ok(NoopReadFormat)
    }

    fn write(
        _ctx: &dyn datafusion::catalog::Session,
        _options: Vec<OptionLayer>,
    ) -> Result<Self::Write> {
        Ok(NoopWriteFormat)
    }
}

impl ReadFormat for NoopReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NoopFileFormat))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(NoopSchemaInfer)
    }
}

impl WriteFormat for NoopWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(NoopFileFormat), None))
    }
}

#[derive(Debug)]
struct NoopSchemaInfer;

#[async_trait::async_trait]
impl SchemaInfer for NoopSchemaInfer {
    async fn get_schema(
        &self,
        _ctx: &dyn datafusion::catalog::Session,
        _store: &Arc<dyn ObjectStore>,
        _files: &[ObjectMeta],
        _list_options: &datafusion::datasource::listing::ListingOptions,
    ) -> Result<Schema> {
        Ok(Schema::empty())
    }
}

#[async_trait::async_trait]
impl FileFormat for NoopFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "noop".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(self.get_ext())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::empty()))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Noop file format does not support reading")
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NoopSinkExec::new(input)))
    }

    fn file_source(&self, _table_schema: TableSchema) -> Arc<dyn FileSource> {
        unimplemented!("Noop file format does not support reading")
    }
}
