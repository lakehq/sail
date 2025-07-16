use std::cmp::Ordering;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use datafusion_common::Constraints;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF};
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

use crate::descriptor::{DescriptorFactory, EmptyDescriptor, SingleValueDescriptor};
use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateNamespaceOptions, CreateTableOptions, CreateViewOptions, DeleteNamespaceOptions,
    DeleteTableOptions, DeleteViewOptions,
};
use crate::utils::quote_namespace_if_needed;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
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
        definition: CatalogDatabaseDefinition,
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
        if_exists: bool,
        cascade: bool,
    },
    CreateTable {
        table: Vec<String>,
        definition: CatalogTableDefinition,
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
        if_exists: bool,
        purge: bool,
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
        if_exists: bool,
    },
    DropView {
        view: Vec<String>,
        if_exists: bool,
    },
    CreateTemporaryView {
        view: String,
        input: Arc<LogicalPlan>,
        is_global: bool,
        replace: bool,
    },
    CreateView {
        view: Vec<String>,
        definition: CatalogViewDefinition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum CatalogTableFunction {
    // We do not support any kind of table functions yet.
    // PySpark UDTF is registered as a scalar UDF.
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct CatalogDatabaseDefinition {
    pub if_not_exists: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CatalogTableDefinition {
    pub schema: SchemaRef,
    pub comment: Option<String>,
    pub column_defaults: Vec<(String, Expr)>,
    pub constraints: Constraints,
    pub location: String,
    pub file_format: String,
    pub table_partition_cols: Vec<String>,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub if_not_exists: bool,
    pub or_replace: bool,
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CatalogViewDefinition {
    pub schema: SchemaRef,
    pub definition: String,
    pub replace: bool,
}

#[derive(PartialEq, PartialOrd)]
struct CatalogViewDefinitionOrd<'a> {
    definition: &'a String,
    replace: bool,
}

impl<'a> From<&'a CatalogViewDefinition> for CatalogViewDefinitionOrd<'a> {
    fn from(definition: &'a CatalogViewDefinition) -> Self {
        let CatalogViewDefinition {
            // ignore schema in comparison
            schema: _,
            definition,
            replace,
        } = definition;
        CatalogViewDefinitionOrd {
            definition,
            replace: *replace,
        }
    }
}

impl PartialOrd for CatalogViewDefinition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        CatalogViewDefinitionOrd::from(self).partial_cmp(&other.into())
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

    pub fn schema<F: DescriptorFactory>(&self) -> CatalogResult<SchemaRef> {
        // TODO: make sure we return the same schema as Spark for each command
        let fields = match self {
            CatalogCommand::ListCatalogs { .. } => {
                Vec::<FieldRef>::from_type::<F::Catalog>(TracingOptions::default())
            }
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                Vec::<FieldRef>::from_type::<F::Database>(TracingOptions::default())
            }
            CatalogCommand::GetTable { .. }
            | CatalogCommand::ListTables { .. }
            | CatalogCommand::ListViews { .. } => {
                Vec::<FieldRef>::from_type::<F::Table>(TracingOptions::default())
            }
            CatalogCommand::ListColumns { .. } => {
                Vec::<FieldRef>::from_type::<F::TableColumn>(TracingOptions::default())
            }
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                Vec::<FieldRef>::from_type::<F::Function>(TracingOptions::default())
            }
            CatalogCommand::SetCurrentCatalog { .. }
            | CatalogCommand::SetCurrentDatabase { .. }
            | CatalogCommand::RegisterFunction { .. }
            | CatalogCommand::RegisterTableFunction { .. } => {
                Vec::<FieldRef>::from_type::<EmptyDescriptor>(TracingOptions::default())
            }
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                Vec::<FieldRef>::from_type::<SingleValueDescriptor<String>>(
                    TracingOptions::default(),
                )
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
                Vec::<FieldRef>::from_type::<SingleValueDescriptor<bool>>(TracingOptions::default())
            }
        }
        .map_err(|e| {
            CatalogError::Internal(format!("failed to build catalog command schema: {e}"))
        })?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub async fn execute<F: DescriptorFactory>(
        self,
        ctx: &SessionContext,
        manager: &CatalogManager,
    ) -> CatalogResult<RecordBatch> {
        // TODO: `ctx` will not be needed if `CatalogManager` manages functions internally.
        let schema = self.schema::<F>()?;
        match self {
            CatalogCommand::CurrentCatalog => {
                let value = manager.default_catalog()?;
                let rows = vec![SingleValueDescriptor { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::SetCurrentCatalog { catalog } => {
                manager.set_default_catalog(catalog)?;
                let rows: Vec<EmptyDescriptor> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListCatalogs { pattern } => {
                let rows = manager
                    .list_catalogs(pattern.as_deref())?
                    .into_iter()
                    .map(|x| F::catalog(x.to_string()))
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CurrentDatabase => {
                let value = manager.default_database()?;
                let value = quote_namespace_if_needed(&value);
                let rows = vec![SingleValueDescriptor { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::SetCurrentDatabase { database } => {
                manager.set_default_database(database)?;
                let rows: Vec<EmptyDescriptor> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateDatabase {
                database,
                definition:
                    CatalogDatabaseDefinition {
                        if_not_exists,
                        comment,
                        location,
                        properties,
                    },
            } => {
                let options = CreateNamespaceOptions {
                    if_not_exists,
                    comment,
                    location,
                    properties,
                };
                manager.create_database(&database, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = match manager.get_database(&database).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_)) => false,
                    Err(e) => return Err(e),
                };
                let rows = vec![SingleValueDescriptor { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match manager.get_database(&database).await {
                    Ok(x) => vec![F::database(x)],
                    Err(CatalogError::NotFound(_)) => vec![],
                    Err(e) => return Err(e),
                };
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListDatabases { qualifier, pattern } => {
                let rows = match manager.list_databases(&qualifier, pattern.as_deref()).await {
                    Ok(rows) => rows.into_iter().map(F::database).collect::<Vec<_>>(),
                    Err(CatalogError::NotFound(_)) => vec![],
                    Err(e) => return Err(e),
                };
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropDatabase {
                database,
                if_exists,
                cascade,
            } => {
                let options = DeleteNamespaceOptions { if_exists, cascade };
                manager.delete_database(&database, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateTable {
                table,
                definition:
                    CatalogTableDefinition {
                        schema: table_schema,
                        comment,
                        column_defaults,
                        constraints,
                        location,
                        file_format,
                        table_partition_cols,
                        file_sort_order,
                        if_not_exists,
                        or_replace,
                        options,
                        definition,
                    },
            } => {
                let options = CreateTableOptions {
                    schema: table_schema,
                    file_format,
                    if_not_exists,
                    or_replace,
                    comment,
                    location: Some(location),
                    column_defaults,
                    constraints,
                    table_partition_cols,
                    file_sort_order,
                    definition,
                    properties: options,
                };
                manager.create_table(&table, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::TableExists { table } => {
                let value = match manager.get_table(&table).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_)) => false,
                    Err(e) => return Err(e),
                };
                let rows = vec![SingleValueDescriptor { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::GetTable { table } => {
                // We are supposed to return an error if the table does not exist.
                let table = manager.get_table(&table).await?;
                let table = F::table(table);
                build_record_batch(schema, &[table])
            }
            CatalogCommand::ListTables { database, pattern } => {
                let rows = manager
                    .list_tables_and_temporary_views(&database, pattern.as_deref())
                    .await?
                    .into_iter()
                    .map(F::table)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListViews { database, pattern } => {
                let rows = manager
                    .list_views_and_temporary_views(&database, pattern.as_deref())
                    .await?
                    .into_iter()
                    .map(F::table)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropTable {
                table,
                if_exists,
                purge,
            } => {
                let options = DeleteTableOptions { if_exists, purge };
                manager.delete_table(&table, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListColumns { table } => {
                let rows = manager
                    .list_table_columns(&table)
                    .await?
                    .into_iter()
                    .map(F::table_column)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::FunctionExists { .. } => {
                Err(CatalogError::NotSupported("function exists".to_string()))
            }
            CatalogCommand::GetFunction { .. } => {
                Err(CatalogError::NotSupported("get function".to_string()))
            }
            CatalogCommand::ListFunctions { .. } => {
                Err(CatalogError::NotSupported("list functions".to_string()))
            }
            CatalogCommand::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => {
                manager
                    .deregister_function(ctx, &function, if_exists, is_temporary)
                    .await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::RegisterFunction { udf } => {
                manager.register_function(ctx, udf)?;
                let rows: Vec<EmptyDescriptor> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::RegisterTableFunction { name, udtf } => {
                manager.register_table_function(ctx, name, udtf)?;
                let rows: Vec<EmptyDescriptor> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => {
                if is_global {
                } else {
                    manager.delete_temporary_view(&view, if_exists).await?;
                }
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropView { view, if_exists } => {
                let options = DeleteViewOptions { if_exists };
                manager.delete_maybe_temporary_view(&view, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateTemporaryView {
                input,
                view,
                is_global,
                replace,
            } => {
                if is_global {
                    manager
                        .create_global_temporary_view(input, &view, replace)
                        .await?;
                } else {
                    manager.create_temporary_view(input, &view, replace).await?;
                }
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateView {
                view,
                definition:
                    CatalogViewDefinition {
                        definition,
                        schema: view_schema,
                        replace,
                    },
            } => {
                let options = CreateViewOptions {
                    definition,
                    schema: view_schema,
                    replace,
                    comment: None,
                    properties: vec![],
                };
                manager.create_view(&view, options).await?;
                let rows = vec![SingleValueDescriptor { value: true }];
                build_record_batch(schema, &rows)
            }
        }
    }
}

fn build_record_batch<T: Serialize>(schema: SchemaRef, items: &[T]) -> CatalogResult<RecordBatch> {
    let arrays = to_arrow(schema.fields().deref(), items)
        .map_err(|e| CatalogError::Internal(format!("failed to create record batch: {e}")))?;
    // We must specify the row count if the schema has no fields.
    let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
    let batch = RecordBatch::try_new_with_options(schema, arrays, &options)
        .map_err(|e| CatalogError::Internal(format!("failed to create record batch: {e}")))?;
    Ok(batch)
}
