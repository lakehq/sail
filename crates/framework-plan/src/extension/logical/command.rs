use arrow::array::{RecordBatch, RecordBatchOptions};
use std::fmt::Formatter;
use std::sync::Arc;

use crate::catalog::catalog::CatalogMetadata;
use crate::catalog::column::TableColumnMetadata;
use crate::catalog::database::DatabaseMetadata;
use crate::catalog::function::FunctionMetadata;
use crate::catalog::table::TableMetadata;
use crate::catalog::{CatalogContext, EmptyMetadata, SingleValueMetadata};
use crate::SqlEngine;
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_err, not_impl_err, DFSchema, SchemaReference, TableReference};
use datafusion_expr::{TableScan, UNNAMED_TABLE};
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CatalogCommandNode {
    name: String,
    schema: DFSchemaRef,
    command: CatalogCommand,
}

impl CatalogCommandNode {
    pub(crate) fn try_new(command: CatalogCommand) -> Result<Self> {
        let schema = command.schema()?;
        Ok(Self {
            name: format!("CatalogCommand: {}", command.name()),
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
            command,
        })
    }
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
        plan: Arc<LogicalPlan>,
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
    DropTemporaryView {
        view: TableReference,
        is_global: bool,
        if_exists: bool,
    },
    DropView {
        view: TableReference,
        if_exists: bool,
    },
}

fn build_record_batch<T: Serialize>(schema: SchemaRef, items: &[T]) -> Result<RecordBatch> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let arrays =
        to_arrow(&fields, items).or_else(|e| exec_err!("failed to create record batch: {}", e))?;
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
            CatalogCommand::DropFunction { .. } => "DropFunction",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropView { .. } => "DropView",
        }
    }

    pub(crate) fn schema(&self) -> Result<SchemaRef> {
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
            | CatalogCommand::CreateDatabase { .. }
            | CatalogCommand::CreateTable { .. } => {
                Vec::<FieldRef>::from_type::<EmptyMetadata>(TracingOptions::default())
            }
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<String>>(TracingOptions::default())
            }
            CatalogCommand::DatabaseExists { .. }
            | CatalogCommand::TableExists { .. }
            | CatalogCommand::FunctionExists { .. }
            | CatalogCommand::DropDatabase { .. }
            | CatalogCommand::DropTable { .. }
            | CatalogCommand::DropFunction { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropView { .. } => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<bool>>(TracingOptions::default())
            }
        }
        .or_else(|e| exec_err!("failed to build catalog command schema: {}", e))?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(crate) async fn execute<S: SqlEngine>(
        self,
        ctx: &SessionContext,
        engine: &S,
    ) -> Result<LogicalPlan> {
        let schema = self.schema()?;
        let ctx = CatalogContext::new(ctx, engine);
        let batch = match self {
            CatalogCommand::CurrentCatalog => {
                let value = ctx.default_catalog()?;
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::SetCurrentCatalog { catalog_name } => {
                ctx.set_default_catalog(catalog_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::ListCatalogs { catalog_pattern } => {
                let rows = ctx.list_catalogs(catalog_pattern.as_deref())?;
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::CurrentDatabase => {
                let value = ctx.default_database()?;
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::SetCurrentDatabase { database_name } => {
                ctx.set_default_database(database_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::CreateDatabase {
                database,
                if_not_exists,
                comment,
                location,
                properties,
            } => {
                let value = ctx
                    .create_database(database, if_not_exists, comment, location, properties)
                    .await
                    .is_ok();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = ctx.get_database(database)?.is_some();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match ctx.get_database(database)? {
                    Some(x) => vec![x],
                    None => vec![],
                };
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::ListDatabases {
                catalog,
                database_pattern,
            } => {
                let rows = ctx.list_databases(catalog, database_pattern.as_deref())?;
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::DropDatabase {
                database,
                if_exists,
                cascade,
            } => {
                let value = ctx
                    .drop_database(database, if_exists, cascade)
                    .await
                    .is_ok();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::CreateTable { table, plan } => {
                // TODO: we should probably create external table here
                ctx.create_memory_table(table, plan).await?;
                let rows: Vec<EmptyMetadata> = vec![];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::TableExists { table } => {
                let value = ctx.get_table(table).await?.is_some();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::GetTable { table } => {
                let rows = match ctx.get_table(table).await? {
                    Some(x) => vec![x],
                    None => vec![],
                };
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::ListTables {
                database,
                table_pattern,
            } => {
                let rows = ctx.list_tables(database, table_pattern.as_deref()).await?;
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::DropTable {
                table,
                if_exists,
                purge,
            } => {
                let value = ctx.drop_table(table, if_exists, purge).await.is_ok();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::ListColumns { table } => {
                let rows = ctx.list_table_columns(table).await?;
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::FunctionExists { .. } => return not_impl_err!("function exists"),
            CatalogCommand::GetFunction { .. } => return not_impl_err!("get function"),
            CatalogCommand::ListFunctions { .. } => return not_impl_err!("list functions"),
            CatalogCommand::DropFunction { .. } => return not_impl_err!("drop function"),
            CatalogCommand::DropTemporaryView {
                view,
                is_global: _,
                if_exists,
            } => {
                // TODO: use the correct catalog and database for global temporary views
                let value = ctx.drop_view(view, if_exists).await.is_ok();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
            }
            CatalogCommand::DropView { view, if_exists } => {
                let value = ctx.drop_view(view, if_exists).await.is_ok();
                let rows = vec![SingleValueMetadata { value }];
                build_record_batch(schema, &rows)?
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
    pub(crate) async fn execute<S: SqlEngine>(
        &self,
        ctx: &SessionContext,
        engine: &S,
    ) -> Result<LogicalPlan> {
        self.command.clone().execute(ctx, engine).await
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

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
