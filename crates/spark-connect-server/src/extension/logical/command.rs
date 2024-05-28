use std::fmt::Formatter;
use std::sync::Arc;

use crate::sql::session_catalog::catalog::CatalogMetadata;
use crate::sql::session_catalog::column::TableColumnMetadata;
use crate::sql::session_catalog::database::DatabaseMetadata;
use crate::sql::session_catalog::function::FunctionMetadata;
use crate::sql::session_catalog::table::TableMetadata;
use crate::sql::session_catalog::{EmptyMetadata, SessionCatalogContext, SingleValueMetadata};
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_err, not_impl_err, DFSchema};
use datafusion_expr::{TableScan, UNNAMED_TABLE};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_record_batch;

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
        pattern: Option<String>,
    },
    CurrentDatabase,
    SetCurrentDatabase {
        database_name: String,
    },
    DatabaseExists {
        database_name: String,
    },
    GetDatabase {
        database_name: String,
    },
    ListDatabases {
        pattern: Option<String>,
    },
    CreateTable {
        table_name: String,
        plan: Arc<LogicalPlan>,
    },
    TableExists {
        table_name: String,
        database_name: Option<String>,
    },
    GetTable {
        table_name: String,
        database_name: Option<String>,
    },
    ListTables {
        database_name: Option<String>,
        pattern: Option<String>,
    },
    ListColumns {
        table_name: String,
        database_name: Option<String>,
    },
    FunctionExists {
        function_name: String,
        database_name: Option<String>,
    },
    GetFunction {
        function_name: String,
        database_name: Option<String>,
    },
    ListFunctions {
        database_name: Option<String>,
        pattern: Option<String>,
    },
    DropTemporaryView {
        view_name: String,
    },
    DropGlobalTemporaryView {
        view_name: String,
    },
}

impl CatalogCommand {
    pub(crate) fn name(&self) -> &str {
        match self {
            CatalogCommand::CurrentCatalog => "CurrentCatalog",
            CatalogCommand::SetCurrentCatalog { .. } => "SetCurrentCatalog",
            CatalogCommand::ListCatalogs { .. } => "ListCatalogs",
            CatalogCommand::CurrentDatabase => "CurrentDatabase",
            CatalogCommand::SetCurrentDatabase { .. } => "SetCurrentDatabase",
            CatalogCommand::DatabaseExists { .. } => "DatabaseExists",
            CatalogCommand::GetDatabase { .. } => "GetDatabase",
            CatalogCommand::ListDatabases { .. } => "ListDatabases",
            CatalogCommand::CreateTable { .. } => "CreateTable",
            CatalogCommand::TableExists { .. } => "TableExists",
            CatalogCommand::GetTable { .. } => "GetTable",
            CatalogCommand::ListTables { .. } => "ListTables",
            CatalogCommand::ListColumns { .. } => "ListColumns",
            CatalogCommand::FunctionExists { .. } => "FunctionExists",
            CatalogCommand::GetFunction { .. } => "GetFunction",
            CatalogCommand::ListFunctions { .. } => "ListFunctions",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropGlobalTemporaryView { .. } => "DropGlobalTemporaryView",
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
            | CatalogCommand::CreateTable { .. } => {
                Vec::<FieldRef>::from_type::<EmptyMetadata>(TracingOptions::default())
            }
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<String>>(TracingOptions::default())
            }
            CatalogCommand::DatabaseExists { .. }
            | CatalogCommand::TableExists { .. }
            | CatalogCommand::FunctionExists { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropGlobalTemporaryView { .. } => {
                Vec::<FieldRef>::from_type::<SingleValueMetadata<bool>>(TracingOptions::default())
            }
        }
        .or_else(|e| exec_err!("failed to build catalog command schema: {}", e))?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(crate) async fn execute(self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let schema = self.schema()?;
        let fields = schema.fields().as_ref();
        let ctx = SessionCatalogContext::new(ctx);
        let batch = match self {
            CatalogCommand::CurrentCatalog => {
                let value = ctx.default_catalog()?;
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::SetCurrentCatalog { catalog_name } => {
                ctx.set_default_catalog(catalog_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::ListCatalogs { pattern } => {
                let rows = ctx.list_catalogs(pattern.as_deref())?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::CurrentDatabase => {
                let value = ctx.default_database()?;
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::SetCurrentDatabase { database_name } => {
                ctx.set_default_database(database_name)?;
                let rows: Vec<EmptyMetadata> = vec![];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::DatabaseExists { database_name } => {
                let databases = ctx.list_databases(
                    Some(ctx.default_catalog()?.as_str()),
                    Some(database_name.as_str()),
                )?;
                let value = !databases.is_empty();
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::GetDatabase { database_name } => {
                let rows = ctx.list_databases(
                    Some(ctx.default_catalog()?.as_str()),
                    Some(database_name.as_str()),
                )?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::ListDatabases { pattern } => {
                let rows =
                    ctx.list_databases(Some(ctx.default_catalog()?.as_str()), pattern.as_deref())?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::CreateTable { table_name, plan } => {
                // TODO: we should probably create external table here
                ctx.create_memory_table(table_name.as_str(), plan).await?;
                let rows: Vec<EmptyMetadata> = vec![];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::TableExists {
                table_name,
                database_name,
            } => {
                let tables = ctx
                    .list_tables(
                        Some(ctx.default_catalog()?.as_str()),
                        Some(ctx.infer_database(database_name)?.as_str()),
                        Some(table_name.as_str()),
                    )
                    .await?;
                let value = !tables.is_empty();
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::GetTable {
                table_name,
                database_name,
            } => {
                let rows = ctx
                    .list_tables(
                        Some(ctx.default_catalog()?.as_str()),
                        database_name.as_deref(),
                        Some(table_name.as_str()),
                    )
                    .await?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::ListTables {
                database_name,
                pattern,
            } => {
                let rows = ctx
                    .list_tables(
                        Some(ctx.default_catalog()?.as_str()),
                        Some(ctx.infer_database(database_name)?.as_str()),
                        pattern.as_deref(),
                    )
                    .await?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::ListColumns {
                table_name,
                database_name,
            } => {
                let rows = ctx
                    .list_table_columns(
                        Some(ctx.default_catalog()?.as_str()),
                        Some(ctx.infer_database(database_name)?.as_str()),
                        table_name.as_str(),
                    )
                    .await?;
                to_record_batch(fields, &rows)
            }
            CatalogCommand::FunctionExists { .. } => return not_impl_err!("function exists"),
            CatalogCommand::GetFunction { .. } => return not_impl_err!("get function"),
            CatalogCommand::ListFunctions { .. } => return not_impl_err!("list functions"),
            CatalogCommand::DropTemporaryView { view_name } => {
                let value = ctx.drop_temporary_view(&view_name).await.is_ok();
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
            CatalogCommand::DropGlobalTemporaryView { view_name } => {
                // TODO: use the correct catalog and database
                let value = ctx.drop_temporary_view(&view_name).await.is_ok();
                let rows = vec![SingleValueMetadata { value }];
                to_record_batch(fields, &rows)
            }
        }
        .or_else(|e| exec_err!("failed to execute catalog command: {}", e))?;
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
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
        self.command.clone().execute(ctx).await
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
