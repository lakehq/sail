use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::catalog::FunctionStatus;
use sail_common_datafusion::session::plan::FunctionRegistry;

use crate::command::CatalogTableFunction;
use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;

impl CatalogManager {
    fn canonical_function_name(name: &str) -> Arc<str> {
        name.to_ascii_lowercase().into()
    }

    pub fn register_function(&self, udf: ScalarUDF) -> CatalogResult<()> {
        let mut state = self.state()?;
        let name = Self::canonical_function_name(udf.name());
        state.functions.insert(name, udf);
        Ok(())
    }

    pub fn get_function<T: AsRef<str>>(&self, name: T) -> CatalogResult<Option<ScalarUDF>> {
        let state = self.state()?;
        let name = Self::canonical_function_name(name.as_ref());
        Ok(state.functions.get(&name).cloned())
    }

    pub fn register_table_function(
        &self,
        _name: String,
        udtf: CatalogTableFunction,
    ) -> CatalogResult<()> {
        let _function: Arc<dyn TableFunctionImpl> = match udtf {};
        #[expect(unreachable_code)]
        Ok(())
    }

    pub async fn deregister_function<T: AsRef<str>>(
        &self,
        function: &[T],
        if_exists: bool,
        _is_temporary: bool,
    ) -> CatalogResult<()> {
        let [name] = function else {
            return Err(CatalogError::NotSupported(
                "qualified function name".to_string(),
            ));
        };
        let mut state = self.state()?;
        let name = Self::canonical_function_name(name.as_ref());
        let found = state.functions.remove(&name).is_some();
        if !found && !if_exists {
            return Err(CatalogError::NotFound(
                CatalogObject::Function,
                name.to_string(),
            ));
        }
        Ok(())
    }

    /// Returns a [`FunctionStatus`] for a session-registered (temporary) function
    /// with the given unqualified name, if one exists.
    fn get_temporary_function_status(&self, name: &str) -> CatalogResult<Option<FunctionStatus>> {
        let Some(udf) = self.get_function(name)? else {
            return Ok(None);
        };
        let name = Self::canonical_function_name(udf.name());
        Ok(Some(FunctionStatus {
            catalog: None,
            namespace: None,
            name: name.to_string(),
            description: None,
            class_name: String::new(),
            is_temporary: true,
        }))
    }

    /// Resolves a function reference and returns its metadata.
    ///
    /// The lookup order follows Spark semantics:
    /// 1. If the reference is a single name, check session-registered (temporary) functions.
    /// 2. If the reference is a single name, check the built-in function registry.
    /// 3. Otherwise, delegate to the catalog provider for a persistent function lookup.
    ///
    /// If the provider does not support persistent functions, the reference is treated as
    /// not found.
    pub async fn get_catalog_function<T: AsRef<str>>(
        &self,
        function: &[T],
        registry: &dyn FunctionRegistry,
    ) -> CatalogResult<FunctionStatus> {
        if let [name] = function {
            if let Some(status) = self.get_temporary_function_status(name.as_ref())? {
                return Ok(status);
            }
            if let Some(status) = registry.get_function(name.as_ref()) {
                return Ok(status);
            }
        }
        let (provider, database, name) = self.resolve_object(function)?;
        match provider.get_function(&database, &name).await {
            Ok(status) => Ok(status),
            Err(CatalogError::NotSupported(_)) => Err(CatalogError::NotFound(
                CatalogObject::Function,
                name.to_string(),
            )),
            Err(e) => Err(e),
        }
    }

    /// Returns `true` if a function with the given reference exists.
    ///
    /// The lookup order follows Spark semantics (see [`Self::get_catalog_function`]).
    /// A [`CatalogError::NotFound`] error from any layer is interpreted as `false`.
    pub async fn function_exists<T: AsRef<str>>(
        &self,
        function: &[T],
        registry: &dyn FunctionRegistry,
    ) -> CatalogResult<bool> {
        match self.get_catalog_function(function, registry).await {
            Ok(_) => Ok(true),
            Err(CatalogError::NotFound(_, _)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Lists all functions visible from the given database qualifier.
    ///
    /// Consistent with Spark semantics for `SHOW FUNCTIONS` / `catalog.listFunctions()`:
    /// - Persistent functions from the resolved database are listed first.
    ///   If the provider does not support persistent functions, this layer is skipped.
    ///   Pattern filtering is applied at this level because the `CatalogProvider` trait does
    ///   not accept a pattern argument (consistent with how `list_tables` is handled).
    /// - Built-in functions from the `registry` are included, filtered by `pattern`.
    /// - Session-registered temporary functions are included, filtered by `pattern`.
    ///
    /// There is no deduplication: if a name is shadowed at a higher precedence level,
    /// all entries are still returned (consistent with Spark).
    pub async fn list_all_functions<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
        registry: &dyn FunctionRegistry,
    ) -> CatalogResult<Vec<FunctionStatus>> {
        use crate::utils::match_pattern;

        // 1. Persistent functions from the catalog provider.
        let (provider, db_namespace) = self.resolve_database_by_qualifier(database)?;
        let mut functions: Vec<FunctionStatus> = match provider.list_functions(&db_namespace).await
        {
            Ok(funcs) => funcs
                .into_iter()
                .filter(|f| match_pattern(&f.name, pattern))
                .collect(),
            // Provider does not support persistent functions — skip silently.
            Err(CatalogError::NotSupported(_)) => vec![],
            Err(e) => return Err(e),
        };

        // 2. Built-in functions from the session function registry.
        functions.extend(registry.list_functions(pattern));

        // 3. Session-registered temporary functions.
        let temp_names: Vec<Arc<str>> = {
            let state = self.state()?;
            state.functions.keys().cloned().collect()
        };
        for name in temp_names {
            if match_pattern(&name, pattern) {
                functions.push(FunctionStatus {
                    catalog: None,
                    namespace: None,
                    name: name.to_string(),
                    description: None,
                    class_name: String::new(),
                    is_temporary: true,
                });
            }
        }

        Ok(functions)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

    use super::*;
    use crate::manager::CatalogManagerOptions;
    use crate::provider::{
        AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
        CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };

    #[tokio::test]
    async fn test_temporary_function_status_uses_canonical_name() -> CatalogResult<()> {
        let manager = test_catalog_manager()?;
        manager.register_function(test_udf("MiXeD"))?;

        let status = manager
            .get_catalog_function(&["mixed"], &TestFunctionRegistry::default())
            .await?;
        assert_eq!(status.name, "mixed");
        assert!(status.is_temporary);
        Ok(())
    }

    #[tokio::test]
    async fn test_temporary_function_shadows_built_in() -> CatalogResult<()> {
        let manager = test_catalog_manager()?;
        manager.register_function(test_udf("count"))?;

        let status = manager
            .get_catalog_function(&["count"], &TestFunctionRegistry::with_builtin("count"))
            .await?;
        assert_eq!(status.name, "count");
        assert_eq!(status.class_name, "");
        assert!(status.is_temporary);
        Ok(())
    }

    #[tokio::test]
    async fn test_qualified_function_does_not_consult_built_in_registry() -> CatalogResult<()> {
        let manager = test_catalog_manager()?;

        let err = manager
            .get_catalog_function(
                &["default", "count"],
                &TestFunctionRegistry::with_builtin("count"),
            )
            .await;

        assert!(
            matches!(
                &err,
                Err(CatalogError::NotFound(CatalogObject::Function, name)) if name == "count"
            ),
            "expected NotFound for 'count', got: {err:?}"
        );
        Ok(())
    }

    fn test_catalog_manager() -> CatalogResult<CatalogManager> {
        let provider: Arc<dyn CatalogProvider> = Arc::new(TestCatalogProvider);
        let catalogs = HashMap::from([(String::from("sail"), provider)]);
        CatalogManager::try_new(CatalogManagerOptions {
            catalogs,
            default_catalog: "sail".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })
    }

    #[derive(Debug)]
    struct TestCatalogProvider;

    #[async_trait::async_trait]
    impl CatalogProvider for TestCatalogProvider {
        fn get_name(&self) -> &str {
            "sail"
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<sail_common_datafusion::catalog::DatabaseStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn get_database(
            &self,
            _database: &Namespace,
        ) -> CatalogResult<sail_common_datafusion::catalog::DatabaseStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<sail_common_datafusion::catalog::DatabaseStatus>> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<sail_common_datafusion::catalog::TableStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<sail_common_datafusion::catalog::TableStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn list_tables(
            &self,
            _database: &Namespace,
        ) -> CatalogResult<Vec<sail_common_datafusion::catalog::TableStatus>> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<sail_common_datafusion::catalog::TableStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn get_view(
            &self,
            _database: &Namespace,
            _view: &str,
        ) -> CatalogResult<sail_common_datafusion::catalog::TableStatus> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn list_views(
            &self,
            _database: &Namespace,
        ) -> CatalogResult<Vec<sail_common_datafusion::catalog::TableStatus>> {
            Err(CatalogError::NotSupported("test".to_string()))
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            Err(CatalogError::NotSupported("test".to_string()))
        }
    }

    #[derive(Default)]
    struct TestFunctionRegistry {
        builtins: Vec<String>,
    }

    impl TestFunctionRegistry {
        fn with_builtin(name: &str) -> Self {
            Self {
                builtins: vec![name.to_ascii_lowercase()],
            }
        }
    }

    impl FunctionRegistry for TestFunctionRegistry {
        fn contains_function(&self, name: &str) -> bool {
            self.builtins
                .iter()
                .any(|builtin| builtin == &name.to_ascii_lowercase())
        }

        fn get_function(&self, name: &str) -> Option<FunctionStatus> {
            self.contains_function(name).then(|| FunctionStatus {
                catalog: None,
                namespace: None,
                name: name.to_ascii_lowercase(),
                description: Some("built-in".to_string()),
                class_name: "builtin".to_string(),
                is_temporary: true,
            })
        }

        fn list_functions(&self, _pattern: Option<&str>) -> Vec<FunctionStatus> {
            self.builtins
                .iter()
                .map(|name| FunctionStatus {
                    catalog: None,
                    namespace: None,
                    name: name.clone(),
                    description: Some("built-in".to_string()),
                    class_name: "builtin".to_string(),
                    is_temporary: true,
                })
                .collect()
        }
    }

    fn test_udf(name: &str) -> ScalarUDF {
        create_udf(
            name,
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Immutable,
            Arc::new(|_| -> datafusion_common::Result<ColumnarValue> {
                Err(datafusion_common::DataFusionError::Execution(
                    "not used in tests".to_string(),
                ))
            }),
        )
    }
}
