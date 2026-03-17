use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableColumnStatus, TableHandle};
use sail_common_datafusion::datasource::TableFormatRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<Option<TableHandle>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let handle = match manager.open_table_handle(table.parts()).await {
            Ok(handle) => handle,
            Err(CatalogError::NotFound(_, _)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(self.hydrate_table_handle(table, handle).await?))
    }

    pub(super) async fn require_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<TableHandle> {
        self.resolve_table_handle(table)
            .await?
            .ok_or_else(|| PlanError::invalid(format!("table does not exist: {table:?}")))
    }

    async fn hydrate_table_handle(
        &self,
        table: &spec::ObjectName,
        handle: TableHandle,
    ) -> PlanResult<TableHandle> {
        if !handle.columns.is_empty() {
            return Ok(handle);
        }

        let registry = self
            .ctx
            .extension::<TableFormatRegistry>()
            .map_err(|error| {
                PlanError::invalid(format!(
                    "failed to access table format registry for table `{table:?}`: {error}",
                ))
            })?;
        let table_format = registry.get(&handle.format).map_err(|error| {
            PlanError::invalid(format!(
                "failed to resolve table format `{}` for table `{table:?}`: {error}",
                handle.format
            ))
        })?;
        let provider = table_format
            .create_provider(
                &self.ctx.state(),
                handle.to_source_info(None, Default::default(), vec![]),
            )
            .await
            .map_err(|error| {
                PlanError::invalid(format!(
                    "failed to infer schema for table `{table:?}` from format `{}`: {error}",
                    handle.format
                ))
            })?;
        let columns = provider
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let is_partition = handle
                    .partition_by
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(field.name()));
                let is_bucket = handle.bucket_by.as_ref().is_some_and(|bucket| {
                    bucket
                        .columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(field.name()))
                });
                TableColumnStatus {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    is_partition,
                    is_bucket,
                    is_cluster: false,
                }
            })
            .collect();
        Ok(handle.with_columns(columns))
    }
}
