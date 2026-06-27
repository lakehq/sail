use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::catalog::FunctionStatus;

use crate::command::CatalogTableFunction;
use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::utils::match_pattern;

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

    pub async fn list_functions<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
        system_functions: &[FunctionStatus],
        show_user_functions: bool,
        show_system_functions: bool,
    ) -> CatalogResult<Vec<FunctionStatus>> {
        let _ = self.get_database_by_qualifier(database).await?;
        let state = self.state()?;
        let mut functions = if show_system_functions {
            system_functions
                .iter()
                .filter(|status| match_pattern(&status.name, pattern))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        if show_user_functions {
            functions.extend(
                state
                    .functions
                    .keys()
                    .filter(|name| match_pattern(name.as_ref(), pattern))
                    .map(|name| FunctionStatus::temporary(name.to_string())),
            );
        }
        functions.sort_by(|left, right| left.name.cmp(&right.name));
        functions.dedup_by(|left, right| left.name == right.name);
        Ok(functions)
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
}
