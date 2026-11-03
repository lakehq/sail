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
use crate::resolver::state::PlanResolverState;

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
        let transactional = match manager.get_table_or_view(table.parts()).await {
            Ok(status) => match status.kind {
                TableKind::Table { format, .. } => {
                    // A namespace named delta/iceberg is not a format path. Preserve
                    // the catalog resolution for both target metadata and its read plan.
                    let name = status
                        .catalog
                        .into_iter()
                        .chain(status.database)
                        .chain(std::iter::once(status.name))
                        .collect::<Vec<_>>();
                    table = spec::ObjectName::from(name);
                    registry
                        .get_lake_source(&format.to_ascii_lowercase())
                        .is_ok()
                }
                _ => {
                    return Err(PlanError::unsupported(format!(
                        "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'TRUNCATE TABLE' expects a table but `{}` is a view.",
                        status.name
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
        let target = self.resolve_row_level_target(&table).await?;
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
