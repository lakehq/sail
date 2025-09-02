use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::prelude::SessionContext;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::ScalarUDF;

use crate::command::CatalogTableFunction;
use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;

impl CatalogManager {
    pub fn register_function(&self, ctx: &SessionContext, udf: ScalarUDF) -> CatalogResult<()> {
        ctx.register_udf(udf);
        Ok(())
    }

    pub fn register_table_function(
        &self,
        _ctx: &SessionContext,
        _name: String,
        udtf: CatalogTableFunction,
    ) -> CatalogResult<()> {
        let _function: Arc<dyn TableFunctionImpl> = match udtf {};
        #[allow(unreachable_code)]
        {
            _ctx.register_udtf(_name.as_str(), _function);
            Ok(())
        }
    }

    pub async fn deregister_function<T: AsRef<str>>(
        &self,
        ctx: &SessionContext,
        function: &[T],
        if_exists: bool,
        _is_temporary: bool,
    ) -> CatalogResult<()> {
        let [name] = function else {
            return Err(CatalogError::NotSupported(
                "qualified function name".to_string(),
            ));
        };
        let found = ctx
            .state_ref()
            .write()
            .deregister_udf(name.as_ref())
            .map_err(|e| CatalogError::Internal(e.to_string()))?
            .is_some();
        if !found && !if_exists {
            return Err(CatalogError::NotFound(
                "function",
                name.as_ref().to_string(),
            ));
        }
        Ok(())
    }
}
