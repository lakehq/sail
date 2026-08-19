use datafusion_expr::LogicalPlan;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{OptimizeInfo, OptionLayer, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_optimize(
        &self,
        optimize: spec::Optimize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let table_name: Vec<String> = optimize.table.clone().into();
        let status = self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(optimize.table.parts())
            .await?;
        let TableKind::Table {
            location,
            format,
            partition_by,
            properties,
            ..
        } = status.kind
        else {
            return Err(PlanError::unsupported(
                "OPTIMIZE is only supported on tables",
            ));
        };
        if !format.eq_ignore_ascii_case("delta") {
            return Err(PlanError::unsupported(format!(
                "OPTIMIZE is not supported for {format} tables"
            )));
        }
        let path = location
            .ok_or_else(|| PlanError::unsupported("OPTIMIZE on tables without location"))?;
        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                &table_name,
                LakehouseOperation::Maintenance,
                Some(&format),
                vec![],
            )
            .await?;
        let read = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: optimize.table,
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        });
        let input = self.resolve_query_plan(read, state).await?;
        let fields = Self::get_field_names(input.schema(), state)?;
        let input = rename_logical_plan(input, &fields)?;

        self.ctx
            .extension::<TableFormatRegistry>()?
            .get(&format)?
            .create_optimizer(
                &self.ctx.state(),
                OptimizeInfo {
                    input,
                    path: path.clone(),
                    partition_by,
                    options: vec![
                        OptionLayer::TablePropertyList { items: properties },
                        OptionLayer::OptionList {
                            items: vec![("path".to_string(), path)],
                        },
                    ],
                    lakehouse_table: Some(lakehouse_table),
                },
            )
            .await
            .map_err(PlanError::from)
    }
}
