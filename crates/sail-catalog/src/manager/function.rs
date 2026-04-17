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
        Ok(Some(FunctionStatus {
            catalog: None,
            namespace: None,
            name: udf.name().to_string(),
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
}
