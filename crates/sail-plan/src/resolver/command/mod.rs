use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{DropDatabaseOptions, DropTableOptions};
use sail_common::spec;

use crate::catalog::CatalogCommandNode;
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

mod catalog;
mod delete;
mod explain;
mod function;
mod insert;
mod merge;
mod show;
mod variable;
mod write;
mod write_stream;
mod write_v1;
mod write_v2;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_plan(
        &self,
        plan: spec::CommandPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::CommandNode;

        match plan.node {
            CommandNode::ShowString(show) => self.resolve_command_show_string(show, state).await,
            CommandNode::HtmlString(html) => self.resolve_command_html_string(html, state).await,
            CommandNode::CurrentDatabase => {
                self.resolve_catalog_command(CatalogCommand::CurrentDatabase)
            }
            CommandNode::SetCurrentDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentDatabase {
                    database: database.into(),
                })
            }
            CommandNode::ListDatabases { qualifier, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListDatabases {
                    qualifier: qualifier.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListViews { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListViews {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListFunctions { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListFunctions {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: table.into(),
                })
            }
            CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: database.into(),
                })
            }
            CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: table.into(),
                })
            }
            CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: function.into(),
                })
            }
            CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: database.into(),
                })
            }
            CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: table.into(),
                })
            }
            CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: function.into(),
                })
            }
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition, state)
                    .await
            }
            CommandNode::CreateTableAsSelect {
                table,
                definition,
                query,
            } => {
                self.resolve_catalog_create_table_as_select(table, definition, *query, state)
                    .await
            }
            CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_drop_view(view, if_exists).await
            }
            CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => {
                self.resolve_catalog_drop_temporary_view(view, is_global, if_exists)
                    .await
            }
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: database.into(),
                options: DropDatabaseOptions { if_exists, cascade },
            }),
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: function.into(),
                if_exists,
                is_temporary,
            }),
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: table.into(),
                options: DropTableOptions { if_exists, purge },
            }),
            CommandNode::RecoverPartitions { .. } => {
                Err(PlanError::todo("PlanNode::RecoverPartitions"))
            }
            CommandNode::IsCached { .. } => Err(PlanError::todo("PlanNode::IsCached")),
            CommandNode::CacheTable { .. } => Err(PlanError::todo("PlanNode::CacheTable")),
            CommandNode::UncacheTable { .. } => Err(PlanError::todo("PlanNode::UncacheTable")),
            CommandNode::ClearCache => Err(PlanError::todo("PlanNode::ClearCache")),
            CommandNode::RefreshTable { .. } => Err(PlanError::todo("PlanNode::RefreshTable")),
            CommandNode::RefreshByPath { .. } => Err(PlanError::todo("PlanNode::RefreshByPath")),
            CommandNode::CurrentCatalog => {
                self.resolve_catalog_command(CatalogCommand::CurrentCatalog)
            }
            CommandNode::SetCurrentCatalog { catalog } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentCatalog {
                    catalog: catalog.into(),
                })
            }
            CommandNode::ListCatalogs { pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListCatalogs { pattern })
            }
            CommandNode::CreateCatalog { .. } => Err(PlanError::todo("create catalog")),
            CommandNode::CreateDatabase {
                database,
                definition,
            } => self.resolve_catalog_create_database(database, definition),
            CommandNode::RegisterFunction(function) => {
                self.resolve_catalog_register_function(function, state)
            }
            CommandNode::RegisterTableFunction(function) => {
                self.resolve_catalog_register_table_function(function, state)
            }
            CommandNode::RefreshFunction { .. } => {
                Err(PlanError::todo("CommandNode::RefreshFunction"))
            }
            CommandNode::CreateView { view, definition } => {
                self.resolve_catalog_create_view(view, definition, state)
                    .await
            }
            CommandNode::CreateTemporaryView {
                view,
                is_global,
                definition,
            } => {
                self.resolve_catalog_create_temporary_view(view, is_global, definition, state)
                    .await
            }
            CommandNode::Write(write) => self.resolve_command_write(write, state).await,
            CommandNode::WriteTo(write_to) => self.resolve_command_write_to(write_to, state).await,
            CommandNode::WriteStream(write_stream) => {
                self.resolve_command_write_stream(write_stream, state).await
            }
            CommandNode::Explain { mode, input } => {
                self.resolve_command_explain(*input, mode, state).await
            }
            CommandNode::InsertOverwriteDirectory {
                input,
                local,
                location,
                file_format,
                row_format,
                options,
            } => {
                self.resolve_command_insert_overwrite_directory(
                    *input,
                    local,
                    location,
                    file_format,
                    row_format,
                    options,
                    state,
                )
                .await
            }
            CommandNode::InsertInto {
                input,
                table,
                mode,
                partition,
                if_not_exists,
            } => {
                self.resolve_command_insert_into(
                    *input,
                    table,
                    mode,
                    partition,
                    if_not_exists,
                    state,
                )
                .await
            }
            CommandNode::MergeInto(merge) => self.resolve_command_merge_into(merge, state).await,
            CommandNode::SetVariable { variable, value } => {
                self.resolve_command_set_variable(variable, value).await
            }
            CommandNode::Update { .. } => Err(PlanError::todo("CommandNode::Update")),
            CommandNode::Delete {
                table,
                table_alias,
                condition,
            } => {
                let delete = spec::Delete {
                    table,
                    table_alias,
                    condition,
                };
                self.resolve_command_delete(delete, state).await
            }
            CommandNode::AlterTable { .. } => Err(PlanError::todo("CommandNode::AlterTable")),
            CommandNode::AlterView { .. } => Err(PlanError::todo("CommandNode::AlterView")),
            CommandNode::LoadData { .. } => Err(PlanError::todo("CommandNode::LoadData")),
            CommandNode::AnalyzeTable { .. } => Err(PlanError::todo("CommandNode::AnalyzeTable")),
            CommandNode::AnalyzeTables { .. } => Err(PlanError::todo("CommandNode::AnalyzeTables")),
            CommandNode::DescribeQuery { .. } => Err(PlanError::todo("CommandNode::DescribeQuery")),
            CommandNode::DescribeFunction { .. } => {
                Err(PlanError::todo("CommandNode::DescribeFunction"))
            }
            CommandNode::DescribeCatalog { .. } => {
                Err(PlanError::todo("CommandNode::DescribeCatalog"))
            }
            CommandNode::DescribeDatabase { .. } => {
                Err(PlanError::todo("CommandNode::DescribeDatabase"))
            }
            CommandNode::DescribeTable { .. } => Err(PlanError::todo("CommandNode::DescribeTable")),
            CommandNode::CommentOnCatalog { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnCatalog"))
            }
            CommandNode::CommentOnDatabase { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnDatabase"))
            }
            CommandNode::CommentOnTable { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnTable"))
            }
            CommandNode::CommentOnColumn { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnColumn"))
            }
        }
    }

    fn resolve_catalog_command(&self, command: CatalogCommand) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(self.ctx, command)?),
        }))
    }
}
