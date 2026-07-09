use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::physical_plan::write_location;
use crate::spec::types::values::Literal;
use crate::spec::{DataFile, FormatVersion, TableMetadata};
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_location_to_object_path_string,
};
use crate::table_format::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
};

pub(crate) fn store_context(context: &TaskContext, table_url: &Url) -> Result<StoreContext> {
    let object_store = context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    StoreContext::new(object_store, table_url)
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

    pub(crate) async fn load_current_table_metadata(
        &self,
        store_ctx: &StoreContext,
    ) -> Result<TableMetadata> {
        load_current_table_metadata(store_ctx, &self.table_url, &self.table_properties).await
    }

    pub(crate) fn resolve_data_dir(&self, table_meta: &TableMetadata) -> String {
        write_location::resolve_data_dir_from_options_and_properties(
            self.write_data_path(),
            self.write_folder_storage_path(),
            &table_meta.properties,
            &self.table_url,
        )
    }
}

pub(crate) async fn load_current_table_metadata(
    store_ctx: &StoreContext,
    table_url: &Url,
    table_properties: &[(String, String)],
) -> Result<TableMetadata> {
    let metadata_file = if catalog_managed_iceberg_from_properties(table_properties) {
        match metadata_location_from_properties(table_properties) {
            Some(location) => metadata_location_to_object_path_string(&location)?,
            None => crate::table::find_latest_metadata_file(&store_ctx.base, table_url).await?,
        }
    } else {
        crate::table::find_latest_metadata_file(&store_ctx.base, table_url).await?
    };
    let bytes = load_metadata_file_bytes(&store_ctx.base, &metadata_file).await?;
    TableMetadata::from_json(&bytes).map_err(|e| DataFusionError::External(Box::new(e)))
}

pub(crate) fn ensure_position_delete_file_writes(table_meta: &TableMetadata) -> Result<()> {
    if table_meta.format_version < FormatVersion::V2 {
        return Err(DataFusionError::Plan(
            "Iceberg position delete writes require table format-version 2".to_string(),
        ));
    }
    if table_meta.format_version >= FormatVersion::V3 {
        return Err(DataFusionError::NotImplemented(
            "Iceberg v3 MERGE MOR position delete writes are not supported; v3 requires deletion vectors".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_equality_delete_writes(table_meta: &TableMetadata) -> Result<()> {
    if table_meta.format_version < FormatVersion::V2 {
        return Err(DataFusionError::Plan(
            "Iceberg equality delete writes require table format-version 2 or higher".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn write_delete_parquet_file(
    store_ctx: &StoreContext,
    table_url: &Url,
    data_dir: &str,
    file_prefix: &str,
    writer: ArrowParquetWriter,
    partition_spec_id: i32,
    partition: Vec<Option<Literal>>,
) -> Result<DataFile> {
    let (bytes, meta) = writer.close().await.map_err(DataFusionError::Execution)?;

    let rel = write_location::parquet_file_path(data_dir, file_prefix);
    let path = ObjectPath::from(rel.as_str());
    store_ctx
        .prefixed
        .put(&path, object_store::PutPayload::from(bytes))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let delete_file_path = crate::utils::join_table_uri(
        table_url.as_str(),
        &rel,
        &crate::utils::WritePathMode::Absolute,
    );

    DataFileWriter::new(partition_spec_id, delete_file_path, partition)
        .finish(meta)
        .map(|outcome| outcome.data_file)
        .map_err(DataFusionError::Execution)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_common::DataFusionError;

    use super::*;

    fn table_metadata_with_format_version(format_version: FormatVersion) -> TableMetadata {
        TableMetadata {
            format_version,
            table_uuid: None,
            location: "file:///tmp/table".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: None,
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::new(),
            statistics: vec![],
            partition_statistics: vec![],
        }
    }

    #[test]
    fn position_delete_file_writes_require_v2_and_reject_v3() {
        let v1 = table_metadata_with_format_version(FormatVersion::V1);
        assert!(matches!(
            ensure_position_delete_file_writes(&v1),
            Err(DataFusionError::Plan(_))
        ));
        assert!(
            ensure_position_delete_file_writes(&v1)
                .is_err_and(|err| err.to_string().contains("position delete writes"))
        );

        let v2 = table_metadata_with_format_version(FormatVersion::V2);
        assert!(ensure_position_delete_file_writes(&v2).is_ok());

        let v3 = table_metadata_with_format_version(FormatVersion::V3);
        assert!(matches!(
            ensure_position_delete_file_writes(&v3),
            Err(DataFusionError::NotImplemented(_))
        ));
        assert!(
            ensure_position_delete_file_writes(&v3)
                .is_err_and(|err| err.to_string().contains("requires deletion vectors"))
        );
    }

    #[test]
    fn equality_delete_writes_require_v2_or_higher() {
        let v1 = table_metadata_with_format_version(FormatVersion::V1);
        assert!(matches!(
            ensure_equality_delete_writes(&v1),
            Err(DataFusionError::Plan(_))
        ));
        assert!(
            ensure_equality_delete_writes(&v1)
                .is_err_and(|err| err.to_string().contains("equality delete writes"))
        );

        let v2 = table_metadata_with_format_version(FormatVersion::V2);
        assert!(ensure_equality_delete_writes(&v2).is_ok());

        let v3 = table_metadata_with_format_version(FormatVersion::V3);
        assert!(ensure_equality_delete_writes(&v3).is_ok());
    }
}
