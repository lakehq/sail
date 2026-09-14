use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{Expr, LogicalPlan, SubqueryAlias};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{
    DataSourceRegistry, OptionLayer, RowLevelTarget, SourceInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) fn validate_row_level_condition(
        &self,
        command: &str,
        condition: &Expr,
    ) -> PlanResult<()> {
        if condition.is_volatile() {
            return Err(PlanError::AnalysisError(format!(
                "Non-deterministic expressions are not allowed in {command} conditions"
            )));
        }
        Ok(())
    }

    pub(super) async fn resolve_row_level_table_plan(
        &self,
        name: spec::ObjectName,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let read = spec::ReadNamedTable {
            name,
            temporal: None,
            sample: None,
            options: vec![],
        };
        let plan = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(read)),
            is_streaming: false,
        });
        self.resolve_query_plan(plan, state).await
    }

    pub(super) fn apply_row_level_table_alias(
        &self,
        plan: LogicalPlan,
        alias: &str,
    ) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            TableReference::Bare {
                table: Arc::from(alias.to_string()),
            },
        )?))
    }

    pub(super) async fn resolve_row_level_target(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<RowLevelTarget> {
        if let [format, path] = table.parts() {
            let format = format.as_ref().to_ascii_lowercase();
            let registry = self.ctx.extension::<DataSourceRegistry>()?;
            if let Ok(lake_source) = registry.get_lake_source(&format) {
                let location = path.as_ref().to_string();
                let metadata = lake_source
                    .infer_metadata(
                        &self.ctx.state(),
                        SourceInfo {
                            paths: vec![location.clone()],
                            lakehouse_table: None,
                            schema: None,
                            constraints: Default::default(),
                            partition_by: vec![],
                            bucket_by: None,
                            sort_order: vec![],
                            options: vec![],
                            read_case_sensitive: self.config.case_sensitive,
                        },
                    )
                    .await?;
                return Ok(RowLevelTarget {
                    table_name: table.clone().into(),
                    format,
                    location,
                    partition_by: vec![],
                    options: vec![OptionLayer::TablePropertyList {
                        items: metadata.properties,
                    }],
                    lakehouse_table: None,
                });
            }
        }

        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        match status.kind {
            TableKind::Table {
                location,
                format,
                partition_by,
                properties,
                ..
            } => {
                let normalized_format = format.to_ascii_lowercase();
                let location = location.ok_or_else(|| {
                    PlanError::invalid(format!("table does not have a location: {table:?}"))
                })?;
                let table_name: Vec<String> = table.clone().into();
                let lakehouse_table = self
                    .resolve_lakehouse_table_context(
                        &table_name,
                        LakehouseOperation::Write,
                        Some(&normalized_format),
                        vec![],
                    )
                    .await?;
                Ok(RowLevelTarget {
                    table_name,
                    format: normalized_format,
                    location,
                    partition_by: partition_by.into_iter().map(|field| field.column).collect(),
                    options: vec![OptionLayer::TablePropertyList { items: properties }],
                    lakehouse_table: Some(lakehouse_table),
                })
            }
            _ => Err(PlanError::unsupported(
                "row-level operations are only supported against tables",
            )),
        }
    }
}
