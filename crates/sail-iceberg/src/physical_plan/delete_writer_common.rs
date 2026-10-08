use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::physical_plan::write_location;
use crate::spec::DataFile;
use crate::spec::types::values::Literal;

pub(crate) fn store_context(context: &TaskContext, location_url: &Url) -> Result<StoreContext> {
    let object_store = context
        .runtime_env()
        .object_store_registry
        .get_store(location_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    StoreContext::new(object_store, location_url)
}

#[derive(Debug, Clone)]
pub(crate) struct IcebergDeleteWriterConfig {
    table_url: Url,
    table_properties: Vec<(String, String)>,
    write_data_path: Option<String>,
    write_folder_storage_path: Option<String>,
}

impl IcebergDeleteWriterConfig {
    pub(crate) fn new(
        table_url: Url,
        table_properties: Vec<(String, String)>,
        write_data_path: Option<String>,
        write_folder_storage_path: Option<String>,
    ) -> Self {
        Self {
            table_url,
            table_properties,
            write_data_path,
            write_folder_storage_path,
        }
    }

    pub(crate) fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub(crate) fn table_properties(&self) -> &[(String, String)] {
        &self.table_properties
    }

    pub(crate) fn write_data_path(&self) -> Option<&str> {
        self.write_data_path.as_deref()
    }

    pub(crate) fn write_folder_storage_path(&self) -> Option<&str> {
        self.write_folder_storage_path.as_deref()
    }
}

pub(crate) async fn write_delete_parquet_file(
    data_store_ctx: &StoreContext,
    data_url: &Url,
    file_prefix: &str,
    writer: ArrowParquetWriter,
    partition_spec_id: i32,
    partition: Vec<Option<Literal>>,
) -> Result<DataFile> {
    let (bytes, meta) = writer.close().await.map_err(DataFusionError::Execution)?;

    let relative_path = write_location::parquet_file_name(file_prefix);
    let path = ObjectPath::from(relative_path.as_str());
    data_store_ctx
        .prefixed
        .put(&path, object_store::PutPayload::from(bytes))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let delete_file_path = write_location::manifest_file_path(data_url, &relative_path);

    DataFileWriter::new(partition_spec_id, delete_file_path, partition)
        .finish_without_bounds(meta)
        .map(|outcome| outcome.data_file)
        .map_err(DataFusionError::Execution)
}
