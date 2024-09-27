use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::{
    exec_datafusion_err, not_impl_err, Constraints, DFSchema, SchemaReference, TableReference,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{ScalarUDF, TableScan, UNNAMED_TABLE};
use sail_python_udf::udf::pyspark_udtf::PySparkUDTF;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

use crate::catalog::catalog::CatalogMetadata;
use crate::catalog::column::TableColumnMetadata;
use crate::catalog::database::DatabaseMetadata;
use crate::catalog::function::FunctionMetadata;
use crate::catalog::table::TableMetadata;
use crate::catalog::{CatalogManager, EmptyMetadata, SingleValueMetadata};
use crate::config::PlanConfig;
use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CatalogCommandNode {
    name: String,
    schema: DFSchemaRef,
    command: CatalogCommand,
    config: Arc<PlanConfig>,
}

impl CatalogCommandNode {
    pub(crate) fn try_new(command: CatalogCommand, config: Arc<PlanConfig>) -> Result<Self> {
        let schema = command.schema()?;
        Ok(Self {
            name: format!("CatalogCommand: {}", command.name()),
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
            command,
            config,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum CatalogTableFunction {
    PySparkUDTF(PySparkUDTF),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum CatalogCommand {
    CurrentCatalog,
    SetCurrentCatalog {
        catalog_name: String,
    },
    ListCatalogs {
        catalog_pattern: Option<String>,
    },
    CurrentDatabase,
    SetCurrentDatabase {
        database_name: String,
    },
    CreateDatabase {
        database: SchemaReference,
        if_not_exists: bool,
        comment: Option<String>,
        location: Option<String>,
        properties: Vec<(String, String)>,
    },
    DatabaseExists {
        database: SchemaReference,
    },
    GetDatabase {
        database: SchemaReference,
    },
    ListDatabases {
        catalog: Option<String>,
        database_pattern: Option<String>,
    },
    DropDatabase {
        database: SchemaReference,
        if_exists: bool,
        cascade: bool,
    },
    CreateTable {
        table: TableReference,
        schema: DFSchemaRef,
        comment: Option<String>,
        column_defaults: Vec<(String, Expr)>,
        constraints: Constraints,
        location: String,
        file_format: String,
        table_partition_cols: Vec<String>,
        file_sort_order: Vec<Vec<Sort>>,
        if_not_exists: bool,
        or_replace: bool,
        unbounded: bool,
        options: Vec<(String, String)>,
        definition: Option<String>,
        copy_to_plan: Option<Arc<LogicalPlan>>,
    },
    TableExists {
        table: TableReference,
    },
    GetTable {
        table: TableReference,
    },
    ListTables {
        database: Option<SchemaReference>,
        table_pattern: Option<String>,
    },
    DropTable {
        table: TableReference,
        if_exists: bool,
        purge: bool,
    },
    ListColumns {
        table: TableReference,
    },
    FunctionExists {
        function: TableReference,
    },
    GetFunction {
        function: TableReference,
    },
    ListFunctions {
        database: Option<SchemaReference>,
        function_pattern: Option<String>,
    },
    DropFunction {
        function: TableReference,
        if_exists: bool,
        is_temporary: bool,
    },
    RegisterFunction {
        udf: ScalarUDF,
    },
    RegisterTableFunction {
        name: String,
        // We have to be explicit about the UDTF types we support.
        // We cannot use `Arc<dyn TableFunctionImpl>` because it does not implement `Eq` and `Hash`.
        udtf: CatalogTableFunction,
    },
    DropTemporaryView {
        view_name: String,
        is_global: bool,
        if_exists: bool,
    },
    DropView {
        view: TableReference,
        if_exists: bool,
    },
    CreateTemporaryView {
        input: Arc<LogicalPlan>,
        view_name: String,
        is_global: bool,
        replace: bool,
        definition: Option<String>,
    },
    CreateView {
        input: Arc<LogicalPlan>,
        view: TableReference,
        replace: bool,
        definition: Option<String>,
    },
}

fn build_record_batch<T: Serialize>(schema: SchemaRef, items: &[T]) -> Result<RecordBatch> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let arrays = to_arrow(&fields, items)
        .map_err(|e| exec_datafusion_err!("failed to create record batch: {}", e))?;
    // We must specify the row count if the schema has no fields.
    let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
    Ok(RecordBatch::try_new_with_options(schema, arrays, &options)?)
}

impl CatalogCommand {
    pub(crate) fn name(&self) -> &str {
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

    pub(crate) fn schema(&self) -> Result<SchemaRef> {
        // TODO: make sure we return the same schema as Spark for each command
        let fields = match self {
            CatalogCommand::ListCatalogs { .. } => {
                Vec::<FieldRef>::from_type::<CatalogMetadata>(TracingOptions::default())
            }
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                Vec::<FieldRef>::from_type::<DatabaseMetadata>(TracingOptions::default())
            }
            CatalogCommand::GetTable { .. } | CatalogCommand::ListTables { .. } => {
                Vec::<FieldRef>::from_type::<TableMetadata>(TracingOptions::default())
            }
            CatalogCommand::ListColumns { .. } => {
                Vec::<FieldRef>::from_type::<TableColumnMetadata>(TracingOptions::default())
            }
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                Vec::<FieldRef>::from_type::<FunctionMetadata>(TracingOptions::default())
            }
            CatalogCommand::SetCurrentCatalog { .. }
            | CatalogCommand::SetCurrentDatabase { .. }
            | CatalogCommand::RegisterFunction { .. }
            | CatalogCommand::RegisterTableFunction { .. } => {
                Vec::<FieldRef>::from_type::<EmptyMetadata>(TracingOptions::default())
            }
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<String>>(TracingOptions::default())
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
            | CatalogCommand::DropView { .. } => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<bool>>(TracingOptions::default())
            }
        }
        .map_err(|e| exec_datafusion_err!("failed to build catalog command schema: {}", e))?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(crate) async fn execute(self, manager: CatalogManager<'_>) -> Result<LogicalPlan> {
        let command_schema = self.schema()?;
        let batch = match self {
            CatalogCommand::CurrentCatalog => {
                let value = manager.default_catalog()?;
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::SetCurrentCatalog { catalog_name } => {
                manager.set_default_catalog(catalog_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::ListCatalogs { catalog_pattern } => {
                let rows = manager.list_catalogs(catalog_pattern.as_deref())?;
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::CurrentDatabase => {
                let value = manager.default_database()?;
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::SetCurrentDatabase { database_name } => {
                manager.set_default_database(database_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::CreateDatabase {
                database,
                if_not_exists,
                comment,
                location,
                properties,
            } => {
                manager
                    .create_database(database, if_not_exists, comment, location, properties)
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = manager.get_database(database)?.is_some();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match manager.get_database(database)? {
                    Some(x) => vec![x],
                    None => vec![],
                };
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::ListDatabases {
                catalog,
                database_pattern,
            } => {
                let rows = manager.list_databases(catalog, database_pattern.as_deref())?;
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::DropDatabase {
                database,
                if_exists,
                cascade,
            } => {
                manager.drop_database(database, if_exists, cascade).await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::CreateTable {
                table,
                schema,
                comment,
                column_defaults,
                constraints,
                location,
                file_format,
                table_partition_cols,
                file_sort_order,
                if_not_exists,
                or_replace,
                unbounded,
                options,
                definition,
                copy_to_plan,
            } => {
                manager
                    .create_table(CatalogCommand::CreateTable {
                        table,
                        schema,
                        comment,
                        column_defaults,
                        constraints,
                        location,
                        file_format,
                        table_partition_cols,
                        file_sort_order,
                        if_not_exists,
                        or_replace,
                        unbounded,
                        options,
                        definition,
                        copy_to_plan,
                    })
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::TableExists { table } => {
                let value = manager.get_table(table).await?.is_some();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::GetTable { table } => {
                let rows = match manager.get_table(table).await? {
                    Some(x) => vec![x],
                    None => vec![],
                };
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::ListTables {
                database,
                table_pattern,
            } => {
                let rows = manager
                    .list_tables(database, table_pattern.as_deref())
                    .await?;
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::DropTable {
                table,
                if_exists,
                purge,
            } => {
                manager.drop_table(table, if_exists, purge).await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::ListColumns { table } => {
                let rows = manager.list_table_columns(table).await?;
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::FunctionExists { .. } => return not_impl_err!("function exists"),
            CatalogCommand::GetFunction { .. } => return not_impl_err!("get function"),
            CatalogCommand::ListFunctions { .. } => return not_impl_err!("list functions"),
            CatalogCommand::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => {
                manager
                    .drop_function(function, if_exists, is_temporary)
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::RegisterFunction { udf } => {
                manager.register_function(udf)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::RegisterTableFunction { name, udtf } => {
                manager.register_table_function(name, udtf)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::DropTemporaryView {
                view_name,
                is_global,
                if_exists,
            } => {
                manager
                    .drop_temporary_view(&view_name, is_global, if_exists)
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::DropView { view, if_exists } => {
                manager.drop_view(view, if_exists).await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::CreateTemporaryView {
                input,
                view_name,
                is_global,
                replace,
                definition: _,
            } => {
                manager
                    .create_temporary_view(input, &view_name, is_global, replace)
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::CreateView {
                input,
                view,
                replace,
                definition,
            } => {
                manager
                    .create_view(input, view, replace, definition)
                    .await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
        };
        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        Ok(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(provider)),
            None,
            vec![],
            None,
        )?))
    }
}

impl CatalogCommandNode {
    pub(crate) async fn execute(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let manager = CatalogManager::new(ctx, self.config.clone());
        self.command.clone().execute(manager).await
    }
}

impl UserDefinedLogicalNodeCore for CatalogCommandNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}
