use std::sync::Arc;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::utils::match_pattern;

impl CatalogManager {
    pub fn default_catalog(&self) -> CatalogResult<Arc<str>> {
        Ok(self.state()?.default_catalog.clone())
    }

    /// Sets the default catalog for the current session.
    /// An error is returned if the catalog does not exist.
    /// Switching to a different catalog resets the default database to that catalog's
    /// default namespace, as in Spark. Setting the current catalog again keeps the
    /// default database.
    pub fn set_default_catalog(&self, catalog: impl Into<Arc<str>>) -> CatalogResult<()> {
        let catalog = catalog.into();
        let mut state = self.state()?;
        let Some(provider) = state.catalogs.get(&catalog) else {
            return Err(CatalogError::NotFound(
                CatalogObject::Catalog,
                catalog.to_string(),
            ));
        };
        if catalog != state.default_catalog {
            let database = provider.default_namespace();
            state.default_database = database;
            state.default_catalog = catalog;
        }
        Ok(())
    }

    pub fn list_catalogs(&self, pattern: Option<&str>) -> CatalogResult<Vec<Arc<str>>> {
        Ok(self
            .state()?
            .catalogs
            .keys()
            .filter(|name| match_pattern(name.as_ref(), pattern))
            .cloned()
            .collect::<Vec<_>>())
    }

    /// Resolves an explicit or default catalog name and verifies that it exists.
    pub fn resolve_catalog_reference(&self, catalog: Option<&str>) -> CatalogResult<Arc<str>> {
        let state = self.state()?;
        let catalog = catalog
            .map(Arc::<str>::from)
            .unwrap_or_else(|| state.default_catalog.clone());
        state.get_catalog(&catalog)?;
        Ok(catalog)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

    use crate::error::{CatalogError, CatalogResult};
    use crate::manager::{CatalogManager, CatalogManagerOptions};
    use crate::provider::{
        CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
        DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };

    struct TestProvider {
        name: &'static str,
        default_namespace: &'static str,
    }

    fn unsupported<T>() -> CatalogResult<T> {
        Err(CatalogError::NotSupported("unused in test".to_string()))
    }

    #[async_trait]
    impl CatalogProvider for TestProvider {
        fn get_name(&self) -> &str {
            self.name
        }

        fn default_namespace(&self) -> Namespace {
            Namespace {
                head: self.default_namespace.into(),
                tail: vec![],
            }
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unsupported()
        }

        async fn get_database(&self, _database: &Namespace) -> CatalogResult<DatabaseStatus> {
            unsupported()
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unsupported()
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unsupported()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<TableStatus> {
            unsupported()
        }

        async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unsupported()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: crate::provider::AlterTableOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unsupported()
        }

        async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
            unsupported()
        }

        async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unsupported()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }
    }

    /// Two catalogs as in #2787: `first` defaults to `default`, `second` to `home`.
    fn manager() -> CatalogResult<CatalogManager> {
        let catalogs = [("first", "default"), ("second", "home")]
            .into_iter()
            .map(|(name, default_namespace)| {
                let provider: Arc<dyn CatalogProvider> = Arc::new(TestProvider {
                    name,
                    default_namespace,
                });
                (name.to_string(), provider)
            })
            .collect::<HashMap<_, _>>();
        CatalogManager::try_new(CatalogManagerOptions {
            catalogs,
            default_catalog: "first".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })
    }

    /// Selects `custom` in the current catalog without checking that it exists,
    /// as `set_default_database` would after asking the provider.
    fn select_custom(manager: &CatalogManager) -> CatalogResult<()> {
        manager.state()?.default_database = vec!["custom"].try_into()?;
        Ok(())
    }

    fn current(manager: &CatalogManager) -> CatalogResult<(String, Vec<String>)> {
        let catalog = manager.default_catalog()?.to_string();
        let database = Vec::<Arc<str>>::from(manager.default_database()?)
            .iter()
            .map(|x| x.to_string())
            .collect();
        Ok((catalog, database))
    }

    fn state(catalog: &str, database: &str) -> (String, Vec<String>) {
        (catalog.to_string(), vec![database.to_string()])
    }

    #[test]
    fn switching_catalog_resets_database_to_target_default() -> CatalogResult<()> {
        let manager = manager()?;
        select_custom(&manager)?;
        manager.set_default_catalog("second")?;
        assert_eq!(current(&manager)?, state("second", "home"));
        Ok(())
    }

    #[test]
    fn switching_back_uses_default_not_previous_selection() -> CatalogResult<()> {
        let manager = manager()?;
        select_custom(&manager)?;
        manager.set_default_catalog("second")?;
        manager.set_default_catalog("first")?;
        assert_eq!(current(&manager)?, state("first", "default"));
        Ok(())
    }

    #[test]
    fn setting_same_catalog_keeps_database() -> CatalogResult<()> {
        let manager = manager()?;
        select_custom(&manager)?;
        manager.set_default_catalog("first")?;
        assert_eq!(current(&manager)?, state("first", "custom"));
        Ok(())
    }

    #[test]
    fn unknown_catalog_leaves_state_unchanged() -> CatalogResult<()> {
        let manager = manager()?;
        select_custom(&manager)?;
        let result = manager.set_default_catalog("missing");
        assert!(matches!(result, Err(CatalogError::NotFound(_, _))));
        assert_eq!(current(&manager)?, state("first", "custom"));
        Ok(())
    }

    #[test]
    fn unqualified_name_resolves_in_target_default_after_switch() -> CatalogResult<()> {
        let manager = manager()?;
        select_custom(&manager)?;
        manager.set_default_catalog("second")?;
        assert_eq!(
            manager.resolve_fully_qualified_object_name(&["t"])?,
            vec!["second", "home", "t"]
        );
        Ok(())
    }
}
