// SPDX-License-Identifier: Apache-2.0

use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, LakehouseResolvedTable, ResolveLakehouseTableRequest,
    TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::{LakehouseExecutionContext, LakehouseOperation};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::{CatalogTableLookup, CatalogTableLookupKey, PlanResolverState};

impl PlanResolver<'_> {
    /// Looks up a catalog table together with its lakehouse resolution, or a view when
    /// `views` is set, reusing the successful lookups of the statement. Every reference
    /// to a table in a statement, to read it or to write it, then has the same state of
    /// the table, and the catalog is asked once.
    pub(super) async fn lookup_catalog_table(
        &self,
        manager: &CatalogManager,
        reference: &[String],
        views: bool,
        state: &mut PlanResolverState,
    ) -> Result<CatalogTableLookup, CatalogError> {
        let key = CatalogTableLookupKey {
            reference: reference.to_vec(),
            views,
        };
        if let Some(lookup) = state.get_catalog_table_lookup(&key) {
            return Ok(lookup);
        }
        // A lookup with views that found the catalog table is the lookup without views.
        if !views
            && let Some(lookup @ (_, Some(_))) =
                state.get_catalog_table_lookup(&CatalogTableLookupKey {
                    reference: reference.to_vec(),
                    views: true,
                })
        {
            return Ok(lookup);
        }
        let request = ResolveLakehouseTableRequest {
            catalog_table: reference.to_vec(),
            operation: LakehouseOperation::Read,
            requested_format: None,
            options: vec![],
        };
        let lookup = if views {
            manager
                .get_lakehouse_table_or_view(self.ctx, reference, request)
                .await?
        } else {
            let (status, resolved) = manager
                .get_lakehouse_table(self.ctx, reference, request)
                .await?;
            (status, Some(resolved))
        };
        state.insert_catalog_table_lookup(key, lookup.clone());
        Ok(lookup)
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
