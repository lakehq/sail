use std::cmp::Ordering;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::{
    exec_datafusion_err, not_impl_err, plan_err, Constraints, DFSchemaRef, Result, SchemaReference,
    TableReference,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF, TableScan, UNNAMED_TABLE};
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

use crate::manager::catalog::CatalogMetadata;
use crate::manager::column::TableColumnMetadata;
use crate::manager::database::DatabaseMetadata;
use crate::manager::function::FunctionMetadata;
use crate::manager::table::TableMetadata;
use crate::manager::{CatalogManager, EmptyMetadata, SingleValueMetadata};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub enum CatalogCommand {
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
        definition: CatalogTableDefinition,
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
    ListViews {
        database: Option<SchemaReference>,
        view_pattern: Option<String>,
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
    #[allow(unused)]
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum CatalogTableFunction {
    // We do not support any kind of table functions yet.
    // PySpark UDTF is registered as a scalar UDF.
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CatalogTableDefinition {
    pub schema: DFSchemaRef,
    pub comment: Option<String>,
    pub column_defaults: Vec<(String, Expr)>,
    pub constraints: Constraints,
    pub location: String,
    pub file_format: String,
    pub table_partition_cols: Vec<String>,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub unbounded: bool,
    pub options: Vec<(String, String)>,
    pub definition: Option<String>,
}

#[derive(PartialEq, PartialOrd)]
struct CatalogTableDefinitionOrd<'a> {
    comment: &'a Option<String>,
    column_defaults: &'a Vec<(String, Expr)>,
    constraints: &'a Constraints,
    location: &'a String,
    file_format: &'a String,
    table_partition_cols: &'a Vec<String>,
    file_sort_order: &'a Vec<Vec<Sort>>,
    if_not_exists: bool,
    or_replace: bool,
    unbounded: bool,
    options: &'a Vec<(String, String)>,
    definition: &'a Option<String>,
}

impl<'a> From<&'a CatalogTableDefinition> for CatalogTableDefinitionOrd<'a> {
    fn from(definition: &'a CatalogTableDefinition) -> Self {
        let CatalogTableDefinition {
            // ignore schema in comparison
            schema: _,
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
        } = definition;
        CatalogTableDefinitionOrd {
            comment,
            column_defaults,
            constraints,
            location,
            file_format,
            table_partition_cols,
            file_sort_order,
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
            unbounded: *unbounded,
            options,
            definition,
        }
    }
}

impl PartialOrd for CatalogTableDefinition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        CatalogTableDefinitionOrd::from(self).partial_cmp(&other.into())
    }
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

    pub fn schema(&self) -> Result<SchemaRef> {
        // TODO: make sure we return the same schema as Spark for each command
        let fields = match self {
            CatalogCommand::ListCatalogs { .. } => {
                Vec::<FieldRef>::from_type::<CatalogMetadata>(TracingOptions::default())
            }
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                Vec::<FieldRef>::from_type::<DatabaseMetadata>(TracingOptions::default())
            }
            CatalogCommand::GetTable { .. }
            | CatalogCommand::ListTables { .. }
            | CatalogCommand::ListViews { .. } => {
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

    pub async fn execute(self, manager: CatalogManager<'_>) -> Result<LogicalPlan> {
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
            CatalogCommand::CreateTable { table, definition } => {
                manager.create_table(table, definition).await?;
                let rows = vec![SingleValueMetadata { value: true }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::TableExists { table } => {
                let value = manager.get_table(table).await?.is_some();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(command_schema, &rows)?
            }
            CatalogCommand::GetTable { table } => {
                let Some(table) = manager.get_table(table.clone()).await? else {
                    return plan_err!("table not found: {table}");
                };
                build_record_batch(command_schema, &[table])?
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
            CatalogCommand::ListViews {
                database,
                view_pattern,
            } => {
                let rows = manager
                    .list_views(database, view_pattern.as_deref())
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

fn build_record_batch<T: Serialize>(schema: SchemaRef, items: &[T]) -> Result<RecordBatch> {
    let arrays = to_arrow(schema.fields().deref(), items)
        .map_err(|e| exec_datafusion_err!("failed to create record batch: {}", e))?;
    // We must specify the row count if the schema has no fields.
    let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
    Ok(RecordBatch::try_new_with_options(schema, arrays, &options)?)
}
