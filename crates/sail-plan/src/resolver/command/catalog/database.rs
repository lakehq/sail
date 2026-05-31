use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateDatabaseOptions;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::validate_location_identifier;
use crate::config::qualify_database_location;
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
        let location = if location.is_some() || provider.uses_spark_default_database_location() {
            Some(qualify_database_location(
                location.as_deref(),
                &database_name,
                &self.config.default_warehouse_directory,
            ))
        } else {
            None
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use sail_catalog::command::CatalogCommand;
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::{
        AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
        CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };
    use sail_common::spec;
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
    use sail_common_datafusion::session::plan::PlanService;

    use crate::catalog::{CatalogCommandNode, SparkCatalogObjectDisplay};
    use crate::error::{PlanError, PlanResult};
    use crate::formatter::SparkPlanFormatter;
    use crate::resolver::PlanResolver;
    use crate::PlanConfig;

    struct NativeNamespaceProvider;

    #[async_trait::async_trait]
    impl CatalogProvider for NativeNamespaceProvider {
        fn get_name(&self) -> &str {
            "native"
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> sail_catalog::error::CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn get_database(
            &self,
            database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "native".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            prefix: Option<&Namespace>,
        ) -> sail_catalog::error::CatalogResult<Vec<DatabaseStatus>> {
            Ok(vec![DatabaseStatus {
                catalog: "native".to_string(),
                database: prefix
                    .cloned()
                    .map(Into::into)
                    .unwrap_or_else(|| vec!["default".to_string()]),
                comment: None,
                location: None,
                properties: vec![],
            }])
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_tables(
            &self,
            _database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_view(
            &self,
            _database: &Namespace,
            _view: &str,
        ) -> sail_catalog::error::CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_views(
            &self,
            _database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> sail_catalog::error::CatalogResult<()> {
            unreachable!()
        }
    }

    fn create_session(default_catalog: &str) -> PlanResult<SessionContext> {
        let mut state = SessionStateBuilder::new().build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([
                (
                    "sail".to_string(),
                    Arc::new(sail_catalog_memory::MemoryCatalogProvider::new(
                        "sail".to_string(),
                        vec![Arc::from("default")].try_into()?,
                        None,
                        None,
                    )) as Arc<dyn CatalogProvider>,
                ),
                (
                    "native".to_string(),
                    Arc::new(NativeNamespaceProvider) as Arc<dyn CatalogProvider>,
                ),
            ]),
            default_catalog: default_catalog.to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })?;
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        state.config_mut().set_extension(Arc::new(catalog_manager));
        state.config_mut().set_extension(Arc::new(plan_service));
        Ok(SessionContext::new_with_state(state))
    }

    fn database_definition(location: Option<&str>) -> spec::DatabaseDefinition {
        spec::DatabaseDefinition {
            if_not_exists: false,
            comment: None,
            location: location.map(ToString::to_string),
            properties: vec![],
        }
    }

    fn resolved_create_database_options(plan: LogicalPlan) -> PlanResult<CreateDatabaseOptions> {
        let LogicalPlan::Extension(extension) = plan else {
            return Err(PlanError::internal("expected extension logical plan"));
        };
        let command = extension
            .node
            .as_any()
            .downcast_ref::<CatalogCommandNode>()
            .ok_or_else(|| PlanError::internal("expected catalog command node"))?
            .command()
            .clone();
        let CatalogCommand::CreateDatabase { options, .. } = command else {
            return Err(PlanError::internal("expected create database command"));
        };
        Ok(options)
    }

    #[test]
    fn test_create_database_rejects_invalid_name_for_spark_default_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("../escaped"),
            database_definition(None),
        ) {
            Ok(plan) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {plan:?}"
                )))
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid database name"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_create_database_rejects_invalid_name_for_spark_explicit_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("../escaped"),
            database_definition(Some("relative/db")),
        ) {
            Ok(plan) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {plan:?}"
                )))
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid database name"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_native_create_database_preserves_catalog_owned_default_location() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let plan = resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("../escaped"),
            database_definition(None),
        )?;
        let options = resolved_create_database_options(plan)?;

        assert_eq!(options.location, None);
        Ok(())
    }
}
