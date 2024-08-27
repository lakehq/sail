use std::sync::Arc;

use datafusion::datasource::function::TableFunctionImpl;
use datafusion_common::Result;
use datafusion_expr::ScalarUDF;
use serde::{Deserialize, Serialize};

use crate::catalog::CatalogManager;
use crate::extension::logical::CatalogTableFunction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FunctionMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) class_name: String,
    pub(crate) is_temporary: bool,
}

impl<'a> CatalogManager<'a> {
    pub(crate) fn register_function(&self, udf: ScalarUDF) -> Result<()> {
        self.ctx.register_udf(udf);
        Ok(())
    }

    pub(crate) fn register_table_function(
        &self,
        name: String,
        udtf: CatalogTableFunction,
    ) -> Result<()> {
        let f: Arc<dyn TableFunctionImpl> = match udtf {
            CatalogTableFunction::PySparkUDTF(x) => Arc::new(x),
        };
        self.ctx.register_udtf(name.as_str(), f);
        Ok(())
    }
}
