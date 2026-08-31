use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo};

use crate::catalog_support::commit::{CatalogTableInfo, IcebergCatalogCommitCoordinator};
use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_properties, metadata_location_from_properties,
    resolve_iceberg_metadata_location, validate_iceberg_lakehouse_storage_access,
};
use crate::spec::TableMetadata;
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_location_to_object_path_string,
};

pub(super) struct ProcedureTable {
    pub(super) table_url: url::Url,
    pub(super) table_properties: Vec<(String, String)>,
    pub(super) lakehouse_table: Option<LakehouseExecutionContext>,
}

impl ProcedureTable {
    pub(super) async fn from_source_info(info: SourceInfo) -> Result<Self> {
        validate_iceberg_lakehouse_storage_access(info.lakehouse_table.as_ref())?;
        let table_url = IcebergLakeSource::parse_table_url(info.paths).await?;
        let table_properties = info
            .options
            .iter()
            .filter_map(|layer| match layer {
                OptionLayer::TablePropertyList { items } => Some(items.as_slice()),
                _ => None,
            })
            .flatten()
            .cloned()
            .collect();
        Ok(Self {
            table_url,
            table_properties,
            lakehouse_table: info.lakehouse_table,
        })
    }
}

pub(super) async fn load_current_metadata(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<TableMetadata> {
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let catalog_table = lakehouse_table.map(|context| context.catalog_table());
    let catalog_info = match catalog_table {
        Some(table) => IcebergCatalogCommitCoordinator::load_table_info(ctx, table).await?,
        None => CatalogTableInfo::default(),
    };
    let recorded_metadata_location = catalog_info
        .metadata_location
        .or_else(|| metadata_location_from_properties(table_properties));
    let metadata_location = resolve_iceberg_metadata_location(
        lakehouse_table,
        recorded_metadata_location,
        catalog_info.is_catalog_managed_iceberg_table
            || catalog_managed_iceberg_from_properties(table_properties),
    )?;
    let metadata_file = match metadata_location {
        Some(location) => metadata_location_to_object_path_string(&location)?,
        None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
    };
    let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
    TableMetadata::from_json(&bytes).map_err(|error| DataFusionError::External(Box::new(error)))
}
