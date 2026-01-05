use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr_common::sort_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, DataFusionError, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;
use datafusion_session::Session;
use object_store::{ObjectMeta, ObjectStore};

use crate::formats::binary::source::BinarySource;
use crate::formats::binary::{read_schema, TableBinaryOptions};

#[derive(Debug)]
pub struct BinaryFileFormat {
    options: TableBinaryOptions,
}

impl BinaryFileFormat {
    pub fn new(table_binary_options: TableBinaryOptions) -> Self {
        Self {
            options: table_binary_options,
        }
    }

    #[allow(unused)]
    pub fn options(&self) -> &TableBinaryOptions {
        &self.options
    }
}

#[async_trait::async_trait]
impl FileFormat for BinaryFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "".to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => Err(DataFusionError::Internal(
                "Binary FileFormat does not support compression.".into(),
            )),
        }
    }
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let tz = Arc::from(
            state
                .config()
                .options()
                .execution
                .time_zone
                .clone()
                .unwrap_or_else(|| "UTC".to_string()),
        );
        Ok(read_schema(tz))
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
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let conf = FileScanConfigBuilder::from(conf).build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Binary file format does not support writing")
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(BinarySource::new(
            table_schema,
            self.options.path_glob_filter.clone(),
        ))
    }
}
