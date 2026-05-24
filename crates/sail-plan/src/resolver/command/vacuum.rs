use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_vacuum::{FileVacuumNode, FileVacuumOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

type VacuumTableInfo = (String, String, Vec<(String, String)>);

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_vacuum(
        &self,
        target: spec::VacuumTarget,
        retention_hours: Option<u64>,
        dry_run: bool,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let (table_name, path, format, properties) = match target {
            spec::VacuumTarget::Table(name) => {
                let catalog_manager = self.ctx.extension::<CatalogManager>()?;
                let table_status = catalog_manager
                    .get_table_or_view(name.parts())
                    .await
                    .map_err(PlanError::from)?;
                let (path, format, properties) = Self::extract_vacuum_table_info(&table_status)?;
                (name.into(), path, format, properties)
            }
            spec::VacuumTarget::Path(path) => (vec![], path, "DELTA".to_string(), vec![]),
        };

        let options = FileVacuumOptions {
            table_name,
            path,
            format,
            retention_hours,
            dry_run,
            options: vec![OptionLayer::TablePropertyList { items: properties }],
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileVacuumNode::new(options)),
        }))
    }

    fn extract_vacuum_table_info(
        table_status: &TableStatus,
    ) -> PlanResult<VacuumTableInfo> {
        let (location, format, properties) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                properties,
                ..
            } => (location.clone(), format.clone(), properties.clone()),
            _ => {
                return Err(PlanError::unsupported(
                    "VACUUM is only supported on tables, not views",
                ));
            }
        };

        let location = location
            .ok_or_else(|| PlanError::unsupported("VACUUM on tables without a location"))?;

        Ok((location, format, properties))
    }
}
