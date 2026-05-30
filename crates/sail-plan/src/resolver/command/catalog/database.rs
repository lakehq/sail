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
        let [.., last] = database.parts() else {
            return Err(PlanError::invalid("missing database name"));
        };
        let db_name: String = last.clone().into();
        validate_location_identifier(&db_name, "database")?;
        let location = match location.as_deref() {
            Some(location) => Some(qualify_database_location(
                Some(location),
                &db_name,
                &self.config.default_warehouse_directory,
            )),
            None => {
                let catalog_manager = self.ctx.extension::<CatalogManager>()?;
                let provider = catalog_manager.database_provider(database.parts())?;
                // Spark-managed metastore catalogs synthesize a default database
                // location, while native namespace catalogs should only persist a
                // location when the user explicitly supplied one.
                provider.uses_spark_default_database_location().then(|| {
                    qualify_database_location(
                        None,
                        &db_name,
                        &self.config.default_warehouse_directory,
                    )
                })
            }
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
    use crate::config::{qualify_database_location, PlanConfig};
    use crate::error::{PlanError, PlanResult};
    use crate::formatter::SparkPlanFormatter;
    use crate::resolver::PlanResolver;

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
            _database: &Namespace,
        ) -> sail_catalog::error::CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> sail_catalog::error::CatalogResult<Vec<DatabaseStatus>> {
            unreachable!()
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

    fn resolved_command(plan: LogicalPlan) -> PlanResult<sail_catalog::command::CatalogCommand> {
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
        Ok(command)
    }

    #[test]
    fn test_create_database_defaults_location_for_spark_managed_catalogs() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let config = Arc::new(PlanConfig::new()?);
        let resolver = PlanResolver::new(&ctx, config.clone());
        let database = spec::ObjectName::bare("managed_db");

        let plan = resolver.resolve_catalog_create_database(
            database,
            spec::DatabaseDefinition {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )?;

        let sail_catalog::command::CatalogCommand::CreateDatabase { options, .. } =
            resolved_command(plan)?
        else {
            return Err(PlanError::internal("expected create database command"));
        };
        assert_eq!(
            options.location,
            Some(qualify_database_location(
                None,
                "managed_db",
                &config.default_warehouse_directory
            ))
        );
        Ok(())
    }

    #[test]
    fn test_create_database_leaves_location_unset_for_native_namespace_catalogs() -> PlanResult<()>
    {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let plan = resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("native_db"),
            spec::DatabaseDefinition {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )?;

        let sail_catalog::command::CatalogCommand::CreateDatabase { options, .. } =
            resolved_command(plan)?
        else {
            return Err(PlanError::internal("expected create database command"));
        };
        assert_eq!(options.location, None);
        Ok(())
    }

    #[test]
    fn test_create_database_qualifies_explicit_relative_location_for_all_catalogs() -> PlanResult<()>
    {
        let ctx = create_session("native")?;
        let config = Arc::new(PlanConfig::new()?);
        let resolver = PlanResolver::new(&ctx, config.clone());

        let plan = resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("native_db"),
            spec::DatabaseDefinition {
                if_not_exists: false,
                comment: None,
                location: Some("relative/db".to_string()),
                properties: vec![],
            },
        )?;

        let sail_catalog::command::CatalogCommand::CreateDatabase { options, .. } =
            resolved_command(plan)?
        else {
            return Err(PlanError::internal("expected create database command"));
        };
        assert_eq!(
            options.location,
            Some(qualify_database_location(
                Some("relative/db"),
                "native_db",
                &config.default_warehouse_directory
            ))
        );
        Ok(())
    }

    #[test]
    fn test_create_database_rejects_invalid_name_for_default_location() -> PlanResult<()> {
        let ctx = create_session("sail")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("../escaped"),
            spec::DatabaseDefinition {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        ) {
            Ok(plan) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {plan:?}"
                )))
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_create_database_rejects_invalid_name_for_explicit_location() -> PlanResult<()> {
        let ctx = create_session("native")?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let err = match resolver.resolve_catalog_create_database(
            spec::ObjectName::bare("nested/db"),
            spec::DatabaseDefinition {
                if_not_exists: false,
                comment: None,
                location: Some("relative/db".to_string()),
                properties: vec![],
            },
        ) {
            Ok(plan) => {
                return Err(PlanError::internal(format!(
                    "expected error, got: {plan:?}"
                )))
            }
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid"),
            "unexpected error: {err}"
        );
        Ok(())
    }
}
