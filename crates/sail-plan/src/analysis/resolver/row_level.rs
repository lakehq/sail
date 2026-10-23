use datafusion_expr::Volatility;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{
    DataSourceRegistry, OptionLayer, RowLevelTarget, SourceInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::Resolver;
use crate::analysis::command::LogicalCommand;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::row_level::{Condition, RowAction, RowOperation};
use crate::error::{PlanError, PlanResult};
impl Resolver<'_, '_> {
    pub(super) async fn resolve_command_delete(
        &mut self,
        request: spec::Delete,
    ) -> PlanResult<LogicalCommand> {
        let target = self.resolve_row_level_target(&request.table).await?;
        let mut input = self.resolve_row_level_table_plan(request.table).await?;
        if let Some(alias) = request.table_alias {
            self.row_level_alias(&mut input, alias.as_ref())?;
        }
        let condition = self
            .row_level_condition(
                "DELETE",
                request.condition,
                input.schema.resolved("DELETE target")?,
            )
            .await?;
        Ok(LogicalCommand::RowOperation(Box::new(RowOperation {
            input,
            target,
            condition,
            case_sensitive: self.context.config.case_sensitive,
            action: RowAction::Delete,
        })))
    }

    pub(super) async fn row_level_condition(
        &mut self,
        command: &str,
        request: Option<spec::ExprWithSource>,
        schema: &PlanSchema,
    ) -> PlanResult<Option<Condition>> {
        let Some(request) = request else {
            return Ok(None);
        };
        let expression = self.expression(request.expr, schema).await?.expression;
        if expression.any(&|node| match node {
            ExpressionNode::ScalarFunction { function, .. } => {
                function.signature().volatility == Volatility::Volatile
            }
            ExpressionNode::AggregateFunction { function, .. } => {
                function.signature().volatility == Volatility::Volatile
            }
            ExpressionNode::HigherOrderFunction { function, .. } => {
                function.signature().volatility == Volatility::Volatile
            }
            _ => false,
        })? {
            return Err(PlanError::analysis(format!(
                "Non-deterministic expressions are not allowed in {command} conditions"
            )));
        }
        let expression = self.cast(expression, arrow_schema::DataType::Boolean)?;
        Ok(Some(Condition {
            expression,
            source: request.source,
        }))
    }

    pub(super) async fn resolve_row_level_table_plan(
        &mut self,
        name: spec::ObjectName,
    ) -> PlanResult<LogicalPlan> {
        self.query(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name,
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        }))
        .await
    }

    pub(super) fn row_level_alias(&self, plan: &mut LogicalPlan, alias: &str) -> PlanResult<()> {
        let mut schema = plan.schema.resolved("row-level alias")?.clone();
        for (index, attribute) in schema.attributes.iter_mut().enumerate() {
            attribute.qualifier = vec![alias.to_string()];
            attribute.qualified_position = index;
        }
        plan.schema = Deferred::Resolved(schema);
        Ok(())
    }
    pub(super) async fn resolve_row_level_target(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<RowLevelTarget> {
        if let [format, path] = table.parts() {
            let format = format.as_ref().to_ascii_lowercase();
            let registry = self.context.session.extension::<DataSourceRegistry>()?;
            if let Ok(lake_source) = registry.get_lake_source(&format) {
                let location = path.as_ref().to_string();
                let metadata = lake_source
                    .infer_metadata(
                        &self.context.session.state(),
                        SourceInfo {
                            paths: vec![location.clone()],
                            lakehouse_table: None,
                            schema: None,
                            constraints: Default::default(),
                            partition_by: vec![],
                            bucket_by: None,
                            sort_order: vec![],
                            options: vec![],
                            read_case_sensitive: self.context.config.case_sensitive,
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

        let catalog_manager = self.context.session.extension::<CatalogManager>()?;
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
