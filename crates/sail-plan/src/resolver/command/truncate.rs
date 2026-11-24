// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_logical_plan::barrier::BarrierNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::{CatalogTableLookupKey, PlanResolverState};

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_truncate(
        &self,
        table: spec::ObjectName,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let registry = self.ctx.extension::<DataSourceRegistry>()?;
        let path_format = match table.parts() {
            [format, _] => registry
                .get_lake_source(&format.as_ref().to_ascii_lowercase())
                .ok(),
            _ => None,
        };
        // Resolve views before format paths, including temporary views shadowing tables.
        let manager = self.ctx.extension::<CatalogManager>()?;
        let mut table = table;
        // The target is looked up once in the statement.
        let reference: Vec<String> = table.clone().into();
        let transactional = match self
            .lookup_catalog_table(&manager, &reference, true, state)
            .await
        {
            Ok(lookup) => match &lookup.0.kind {
                TableKind::Table { format, .. } => {
                    let transactional = registry
                        .get_lake_source(&format.to_ascii_lowercase())
                        .is_ok();
                    // A namespace named delta/iceberg is not a format path. Preserve
                    // the catalog resolution for both target metadata and its read plan,
                    // which resolve the name of the table in the catalog with this lookup.
                    if path_format.is_some() {
                        let status = &lookup.0;
                        let name = status
                            .catalog
                            .iter()
                            .chain(&status.database)
                            .chain(std::iter::once(&status.name))
                            .cloned()
                            .collect::<Vec<_>>();
                        state.insert_catalog_table_lookup(
                            CatalogTableLookupKey {
                                reference: name.clone(),
                                views: true,
                            },
                            lookup.clone(),
                        );
                        table = spec::ObjectName::from(name);
                    }
                    transactional
                }
                _ => {
                    return Err(PlanError::unsupported(format!(
                        "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'TRUNCATE TABLE' expects a table but `{}` is a view.",
                        lookup.0.name
                    )));
                }
            },
            Err(CatalogError::NotFound(_, _)) if path_format.is_some() => true,
            Err(error) => return Err(error.into()),
        };
        if !transactional {
            return self.resolve_catalog_command(CatalogCommand::TruncateTable {
                table: table.into(),
            });
        }
        let target = self.resolve_row_level_target(&table, state).await?;
        let source = registry.get_lake_source(&target.format)?;
        let target_plan = Arc::new(self.resolve_row_level_table_plan(table, state).await?);
        let truncate = source
            .plan_row_level_operation(
                &self.ctx.state(),
                RowLevelOperation::Truncate {
                    target_plan,
                    target: Box::new(target),
                },
            )
            .await?;
        // Drain the transaction before returning an empty command result. LIMIT 0
        // would allow the optimizer to eliminate the mutation entirely.
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(BarrierNode::new(
                vec![Arc::new(truncate)],
                Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
                })),
            )),
        }))
    }
}
