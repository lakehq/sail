use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo};
use sail_common_datafusion::lakeprocedure::{LakeProcedureCall, LakeProcedureExecutionTarget};
use serde::{Deserialize, Serialize};

use crate::catalog_support::commit::{CatalogTableInfo, IcebergCatalogCommitCoordinator};
use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_properties, metadata_location_from_properties,
    resolve_iceberg_metadata_location, validate_iceberg_lakehouse_storage_access,
};
use crate::spec::TableMetadata;
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_location_to_object_path_string,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub(crate) struct ProcedureTable {
    pub(super) table_location: String,
    pub(super) table_properties: Vec<(String, String)>,
    pub(super) lakehouse_table: Option<LakehouseExecutionContext>,
}

impl ProcedureTable {
    pub(crate) async fn from_execution_target(
        target: LakeProcedureExecutionTarget,
    ) -> Result<Self> {
        let LakeProcedureExecutionTarget::Table(info) = target else {
            return plan_err!("Iceberg system procedures require a table target");
        };
        Self::from_source_info(*info).await
    }

    pub(super) async fn from_source_info(info: SourceInfo) -> Result<Self> {
        validate_iceberg_lakehouse_storage_access(info.lakehouse_table.as_ref())?;
        let table_location = match info.paths.as_slice() {
            [path] => path.clone(),
            paths => {
                return plan_err!(
                    "Iceberg table requires exactly one path, got {}",
                    paths.len()
                );
            }
        };
        IcebergLakeSource::parse_table_url(info.paths).await?;
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
            table_location,
            table_properties,
            lakehouse_table: info.lakehouse_table,
        })
    }

    pub(crate) fn validate_for_call(&self, call: &LakeProcedureCall) -> Result<()> {
        call.validate()?;
        let Some(target) = call.target.as_ref() else {
            return plan_err!("Iceberg procedure table is missing its bound call target");
        };
        let Some(context) = self.lakehouse_table.as_ref() else {
            return plan_err!("Iceberg procedure table is missing its lakehouse context");
        };
        if !target.binding.matches_access_context(context) {
            return plan_err!("Iceberg procedure table does not match its bound call target");
        }
        if let Some(identity_location) = context.table_identity.table_uri.as_deref()
            && identity_location != self.table_location
        {
            return plan_err!(
                "Iceberg procedure table location does not match its bound table identity"
            );
        }
        validate_iceberg_lakehouse_storage_access(Some(context))
    }

    pub(crate) async fn table_url(&self) -> Result<url::Url> {
        IcebergLakeSource::parse_table_url(vec![self.table_location.clone()]).await
    }

    pub(crate) fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
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
