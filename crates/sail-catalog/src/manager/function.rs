use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{DdlStatement, DropFunction, LogicalPlan, ScalarUDF};
use serde::{Deserialize, Serialize};

use crate::command::CatalogTableFunction;
use crate::manager::CatalogManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetadata {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Option<Vec<String>>,
    pub description: Option<String>,
    pub class_name: String,
    pub is_temporary: bool,
}

impl CatalogManager<'_> {
    pub fn register_function(&self, udf: ScalarUDF) -> Result<()> {
        self.ctx.register_udf(udf);
        Ok(())
    }

    pub fn register_table_function(&self, _name: String, udtf: CatalogTableFunction) -> Result<()> {
        let _function: Arc<dyn TableFunctionImpl> = match udtf {};
        #[allow(unreachable_code)]
        {
            self.ctx.register_udtf(_name.as_str(), _function);
            Ok(())
        }
    }

    pub async fn drop_function(
        &self,
        function: TableReference,
        if_exists: bool,
        _is_temporary: bool,
    ) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropFunction(DropFunction {
            name: function.to_string(),
            if_exists,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
