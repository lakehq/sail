use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateDatabaseOptions;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::validate_location_identifier;
use crate::config::{qualify_absolute_database_location, qualify_database_location};
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_catalog_create_database(
        &self,
        database: spec::ObjectName,
        definition: spec::DatabaseDefinition,
    ) -> PlanResult<LogicalPlan> {
        let spec::DatabaseDefinition {
            if_not_exists,
            comment,
            location,
            properties,
        } = definition;
        let database_parts = database.parts();
        let database_name = database_parts
            .last()
            .cloned()
            .ok_or_else(|| PlanError::invalid("missing database name"))?;
        let database_name: String = database_name.into();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let provider = catalog_manager.database_provider(database_parts)?;
        if provider.uses_spark_default_database_location() {
            validate_location_identifier(&database_name, "database")?;
        }
        let location = if provider.uses_spark_default_database_location() {
            Some(qualify_database_location(
                location.as_deref(),
                &database_name,
                &self.config.default_warehouse_directory,
            ))
        } else {
            location.as_deref().map(qualify_absolute_database_location)
        };
        let command = CatalogCommand::CreateDatabase {
            database: database.into(),
            options: CreateDatabaseOptions {
                if_not_exists,
                comment,
                location,
                properties,
            },
        };
        self.resolve_catalog_command(command)
    }
}
