use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateDatabaseOptions, CreateTableOptions, CreateTemporaryViewOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropTemporaryViewOptions, DropViewOptions,
};
use crate::utils::quote_namespace_if_needed;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum CatalogCommand {
    CurrentCatalog,
    SetCurrentCatalog {
        catalog: String,
    },
    ListCatalogs {
        pattern: Option<String>,
    },
    CurrentDatabase,
    SetCurrentDatabase {
        database: Vec<String>,
    },
    CreateDatabase {
        database: Vec<String>,
        options: CreateDatabaseOptions,
    },
    DatabaseExists {
        database: Vec<String>,
    },
    GetDatabase {
        database: Vec<String>,
    },
    ListDatabases {
        qualifier: Vec<String>,
        pattern: Option<String>,
    },
    DropDatabase {
        database: Vec<String>,
        options: DropDatabaseOptions,
    },
    CreateTable {
        table: Vec<String>,
        options: CreateTableOptions,
    },
    TableExists {
        table: Vec<String>,
    },
    GetTable {
        table: Vec<String>,
    },
    ListTables {
        database: Vec<String>,
        pattern: Option<String>,
    },
    ListViews {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropTable {
        table: Vec<String>,
        options: DropTableOptions,
    },
    ListColumns {
        table: Vec<String>,
    },
    FunctionExists {
        function: Vec<String>,
    },
    GetFunction {
        function: Vec<String>,
    },
    ListFunctions {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropFunction {
        function: Vec<String>,
        if_exists: bool,
        is_temporary: bool,
    },
    RegisterFunction {
        udf: ScalarUDF,
    },
    #[allow(unused)]
    RegisterTableFunction {
        name: String,
        // We have to be explicit about the UDTF types we support.
        // We cannot use `Arc<dyn TableFunctionImpl>` because it does not implement `Eq` and `Hash`.
        udtf: CatalogTableFunction,
    },
    DropTemporaryView {
        view: String,
        is_global: bool,
        options: DropTemporaryViewOptions,
    },
    DropView {
        view: Vec<String>,
        options: DropViewOptions,
    },
    CreateTemporaryView {
        view: String,
        is_global: bool,
        options: CreateTemporaryViewOptions,
    },
    CreateView {
        view: Vec<String>,
        options: CreateViewOptions,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub enum CatalogTableFunction {
    // We do not support any kind of table functions yet.
    // PySpark UDTF is registered as a scalar UDF.
}

impl CatalogCommand {
    pub fn name(&self) -> &str {
        match self {
            CatalogCommand::CurrentCatalog => "CurrentCatalog",
            CatalogCommand::SetCurrentCatalog { .. } => "SetCurrentCatalog",
            CatalogCommand::ListCatalogs { .. } => "ListCatalogs",
            CatalogCommand::CurrentDatabase => "CurrentDatabase",
            CatalogCommand::SetCurrentDatabase { .. } => "SetCurrentDatabase",
            CatalogCommand::CreateDatabase { .. } => "CreateDatabase",
            CatalogCommand::DatabaseExists { .. } => "DatabaseExists",
            CatalogCommand::GetDatabase { .. } => "GetDatabase",
            CatalogCommand::ListDatabases { .. } => "ListDatabases",
            CatalogCommand::DropDatabase { .. } => "DropDatabase",
            CatalogCommand::CreateTable { .. } => "CreateTable",
            CatalogCommand::TableExists { .. } => "TableExists",
            CatalogCommand::GetTable { .. } => "GetTable",
            CatalogCommand::ListTables { .. } => "ListTables",
            CatalogCommand::ListViews { .. } => "ListViews",
            CatalogCommand::DropTable { .. } => "DropTable",
            CatalogCommand::ListColumns { .. } => "ListColumns",
            CatalogCommand::FunctionExists { .. } => "FunctionExists",
            CatalogCommand::GetFunction { .. } => "GetFunction",
            CatalogCommand::ListFunctions { .. } => "ListFunctions",
            CatalogCommand::RegisterFunction { .. } => "RegisterFunction",
            CatalogCommand::RegisterTableFunction { .. } => "RegisterTableFunction",
            CatalogCommand::DropFunction { .. } => "DropFunction",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropView { .. } => "DropView",
            CatalogCommand::CreateTemporaryView { .. } => "CreateTemporaryView",
            CatalogCommand::CreateView { .. } => "CreateView",
        }
    }

    pub fn schema(&self, ctx: &SessionContext) -> CatalogResult<SchemaRef> {
        let service = ctx.extension::<PlanService>()?;
        let display = service.catalog_display();
        let schema = match self {
            CatalogCommand::ListCatalogs { .. } => display.catalogs().schema()?,
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                display.databases().schema()?
            }
            CatalogCommand::GetTable { .. }
            | CatalogCommand::ListTables { .. }
            | CatalogCommand::ListViews { .. } => display.tables().schema()?,
            CatalogCommand::ListColumns { .. } => display.table_columns().schema()?,
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                display.functions().schema()?
            }
            CatalogCommand::SetCurrentCatalog { .. }
            | CatalogCommand::SetCurrentDatabase { .. }
            | CatalogCommand::RegisterFunction { .. }
            | CatalogCommand::RegisterTableFunction { .. } => display.empty().schema()?,
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                display.strings().schema()?
            }
            CatalogCommand::DatabaseExists { .. }
            | CatalogCommand::TableExists { .. }
            | CatalogCommand::FunctionExists { .. }
            | CatalogCommand::CreateDatabase { .. }
            | CatalogCommand::CreateTable { .. }
            | CatalogCommand::CreateTemporaryView { .. }
            | CatalogCommand::CreateView { .. }
            | CatalogCommand::DropDatabase { .. }
            | CatalogCommand::DropTable { .. }
            | CatalogCommand::DropFunction { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropView { .. } => display.bools().schema()?,
        };
        Ok(schema)
    }

    pub async fn execute(
        self,
        ctx: &SessionContext,
        manager: &CatalogManager,
    ) -> CatalogResult<RecordBatch> {
        // TODO: make sure we return the same schema as Spark for each command
        let service = ctx.extension::<PlanService>()?;
        let display = service.catalog_display();
        let batch = match self {
            CatalogCommand::CurrentCatalog => {
                let value = manager.default_catalog()?;
                display.strings().to_record_batch(vec![value.to_string()])?
            }
            CatalogCommand::SetCurrentCatalog { catalog } => {
                manager.set_default_catalog(catalog)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::ListCatalogs { pattern } => {
                let rows = manager
                    .list_catalogs(pattern.as_deref())?
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();
                display.catalogs().to_record_batch(rows)?
            }
            CatalogCommand::CurrentDatabase => {
                let value = manager.default_database()?;
                let value = quote_namespace_if_needed(&value);
                display.strings().to_record_batch(vec![value])?
            }
            CatalogCommand::SetCurrentDatabase { database } => {
                manager.set_default_database(database).await?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::CreateDatabase { database, options } => {
                manager.create_database(&database, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = match manager.get_database(&database).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                display.bools().to_record_batch(vec![value])?
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match manager.get_database(&database).await {
                    Ok(x) => vec![x],
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                display.databases().to_record_batch(rows)?
            }
            CatalogCommand::ListDatabases { qualifier, pattern } => {
                let rows = match manager.list_databases(&qualifier, pattern.as_deref()).await {
                    Ok(rows) => rows,
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                display.databases().to_record_batch(rows)?
            }
            CatalogCommand::DropDatabase { database, options } => {
                manager.drop_database(&database, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateTable { table, options } => {
                manager.create_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::TableExists { table } => {
                let value = match manager.get_table_or_view(&table).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                display.bools().to_record_batch(vec![value])?
            }
            CatalogCommand::GetTable { table } => {
                // We are supposed to return an error if the table or view does not exist.
                let table = manager.get_table_or_view(&table).await?;
                display.tables().to_record_batch(vec![table])?
            }
            CatalogCommand::ListTables { database, pattern } => {
                let rows = manager
                    .list_tables_and_temporary_views(&database, pattern.as_deref())
                    .await?;
                display.tables().to_record_batch(rows)?
            }
            CatalogCommand::ListViews { database, pattern } => {
                let rows = manager
                    .list_views_and_temporary_views(&database, pattern.as_deref())
                    .await?;
                display.tables().to_record_batch(rows)?
            }
            CatalogCommand::DropTable { table, options } => {
                manager.drop_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::ListColumns { table } => {
                let rows = manager.get_table_or_view(&table).await?.kind.columns();
                display.table_columns().to_record_batch(rows)?
            }
            CatalogCommand::FunctionExists { .. } => {
                return Err(CatalogError::NotSupported("function exists".to_string()));
            }
            CatalogCommand::GetFunction { .. } => {
                return Err(CatalogError::NotSupported("get function".to_string()));
            }
            CatalogCommand::ListFunctions { .. } => {
                return Err(CatalogError::NotSupported("list functions".to_string()));
            }
            // TODO: `ctx` will not be needed if `CatalogManager` manages functions internally.
            CatalogCommand::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => {
                manager
                    .deregister_function(ctx, &function, if_exists, is_temporary)
                    .await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::RegisterFunction { udf } => {
                manager.register_function(ctx, udf)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::RegisterTableFunction { name, udtf } => {
                manager.register_table_function(ctx, name, udtf)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::DropTemporaryView {
                view,
                is_global,
                options,
            } => {
                if is_global {
                    manager.drop_global_temporary_view(&view, options).await?;
                } else {
                    manager.drop_temporary_view(&view, options).await?;
                }
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::DropView { view, options } => {
                manager.drop_maybe_temporary_view(&view, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateTemporaryView {
                view,
                is_global,
                options,
            } => {
                if is_global {
                    manager.create_global_temporary_view(&view, options).await?;
                } else {
                    manager.create_temporary_view(&view, options).await?;
                }
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateView { view, options } => {
                manager.create_view(&view, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
        };
        Ok(batch)
    }
}
