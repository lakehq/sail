// SPDX-License-Identifier: Apache-2.0

use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, LakehouseResolvedTable, ResolveLakehouseTableRequest,
    TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_lakehouse_table_context(
        &self,
        table: &[String],
        operation: LakehouseOperation,
        requested_format: Option<&str>,
        options: Vec<(String, String)>,
    ) -> PlanResult<LakehouseExecutionContext> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let resolved = manager
            .resolve_lakehouse_table(
                self.ctx,
                table,
                ResolveLakehouseTableRequest {
                    catalog_table: table.to_vec(),
                    operation,
                    requested_format: requested_format.map(LakehouseFormat::from_format_name),
                    options,
                },
            )
            .await?;
        self.begin_lakehouse_table_access(table, resolved, table_access_purpose(operation))
            .await
    }

    /// Begins access to a table that the catalog has already resolved.
    pub(super) async fn begin_lakehouse_table_access(
        &self,
        table: &[String],
        resolved: LakehouseResolvedTable,
        access_purpose: TableAccessPurpose,
    ) -> PlanResult<LakehouseExecutionContext> {
        let execution = resolved.execution;
        let manager = self.ctx.extension::<CatalogManager>()?;
        match manager
            .begin_table_access(
                table,
                BeginTableAccessRequest {
                    context: execution.clone(),
                    purpose: access_purpose,
                },
            )
            .await
        {
            Ok(session) => Ok(session.context),
            Err(CatalogError::NotSupported(_) | CatalogError::UnsupportedCapability(_)) => {
                Ok(execution)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn table_access_purpose(operation: LakehouseOperation) -> TableAccessPurpose {
    match operation {
        LakehouseOperation::Read => TableAccessPurpose::DataRead,
        LakehouseOperation::Write
        | LakehouseOperation::WritePrecondition
        | LakehouseOperation::Create
        | LakehouseOperation::Register => TableAccessPurpose::DataWrite,
        LakehouseOperation::Alter | LakehouseOperation::Maintenance => {
            TableAccessPurpose::MetadataRead
        }
    }
}
