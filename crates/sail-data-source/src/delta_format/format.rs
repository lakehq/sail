use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, Result, Statistics};
use object_store::{ObjectMeta, ObjectStore};

use super::sink::DeltaDataSink;

/// Delta Lake file format implementation
#[derive(Debug, Default)]
pub struct DeltaFileFormat {
    options: HashMap<String, String>,
}

impl DeltaFileFormat {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }
}

#[async_trait]
impl FileFormat for DeltaFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "delta".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok("delta".to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        not_impl_err!("Delta Lake schema inference not implemented for write operations")
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: datafusion::arrow::datatypes::SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        not_impl_err!("Delta Lake statistics inference not implemented for write operations")
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Delta Lake read operations should use read_table method")
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Delta Lake format");
        }

        let sink = Arc::new(DeltaDataSink::new(
            self.options.clone(),
            conf.table_paths.clone(),
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        // TODO: implement Delta Lake's FileSource
        // return a placeholder, this method is usually not called for write operations
        unimplemented!("Delta Lake file source not implemented for write operations")
    }
}
