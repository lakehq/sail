use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::DFSchema;
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{DropDatabaseOptions, DropTableOptions};
use sail_common::spec;

use crate::catalog::CatalogCommandNode;
use crate::error::{PlanError, PlanResult};
use crate::function::list_built_in_function_statuses;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

mod catalog;
mod checkpoint;
mod delete;
mod delta;
mod explain;
mod function;
mod insert;
mod merge;
mod procedure;
mod row_level;
mod show;
mod table_properties;
mod truncate;
mod update;
mod variable;
mod with_relations;
mod write;
mod write_stream;
mod write_v1;
mod write_v2;

impl PlanResolver<'_> {
    /// Resolves a command plan into a logical plan.
    #[async_recursion]
    pub(super) async fn resolve_command_plan(
        &self,
        plan: spec::CommandPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::CommandNode;

        match plan.node {
            CommandNode::ShowString(show) => self.resolve_command_show_string(show, state).await,
            CommandNode::HtmlString(html) => self.resolve_command_html_string(html, state).await,
            CommandNode::WithRelations { root, references } => {
                self.resolve_command_with_relations(*root, references, state)
                    .await
            }
            CommandNode::CallProcedure {
                procedure,
                arguments,
                named_arguments,
            } => {
                self.resolve_command_call_procedure(procedure, arguments, named_arguments, state)
                    .await
            }
            CommandNode::CallTableProcedure {
                format,
                procedure,
                target,
                arguments,
            } => {
                self.resolve_command_call_table_procedure(
                    format, procedure, target, arguments, state,
                )
                .await
            }
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
            CommandNode::ShowTableProperties { table, property } => {
                self.resolve_show_table_properties(table, property).await
            }
            CommandNode::ShowTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ShowTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ShowTableExtended { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ShowTableExtended {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ShowFunctions {
                database,
                pattern,
                show_user_functions,
                show_system_functions,
            } => self.resolve_catalog_command(CatalogCommand::ShowFunctions {
                database: database.map(|x| x.into()).unwrap_or_default(),
                pattern,
                system_functions: list_built_in_function_statuses(),
                show_user_functions,
                show_system_functions,
            }),
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
                    system_functions: list_built_in_function_statuses(),
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
                    system_functions: list_built_in_function_statuses(),
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
            CommandNode::RecoverPartitions { table } => {
                self.resolve_catalog_recover_partitions(table).await
            }
            CommandNode::IsCached { table } => {
                self.resolve_catalog_command(CatalogCommand::IsCached {
                    table: table.into(),
                })
            }
            CommandNode::CacheTable {
                table,
                lazy: _,
                storage_level: _,
                query,
            } => {
                if query.is_some() {
                    return Err(PlanError::todo("CACHE TABLE ... AS SELECT"));
                }
                self.resolve_catalog_command(CatalogCommand::CacheTable {
                    table: table.into(),
                })
            }
            CommandNode::UncacheTable { table, if_exists } => {
                self.resolve_catalog_command(CatalogCommand::UncacheTable {
                    table: table.into(),
                    if_exists,
                })
            }
            CommandNode::ClearCache => self.resolve_catalog_command(CatalogCommand::ClearCache),
            CommandNode::RefreshTable { table } => {
                self.resolve_catalog_command(CatalogCommand::RefreshTable {
                    table: table.into(),
                })
            }
            // No data is cached by path, so there is nothing to refresh.
            CommandNode::RefreshByPath { path: _ } => {
                Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
                }))
            }
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
            CommandNode::RemoteCheckpoint { relation_id, input } => {
                self.resolve_command_remote_checkpoint(relation_id, *input, state)
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
            CommandNode::ResetVariable {
                variable: Some(variable),
            } => Ok(self.resolve_command_reset_variable(variable)),
            CommandNode::ResetVariable { variable: None } => {
                Err(PlanError::unsupported("RESET requires a Spark session"))
            }
            CommandNode::Update {
                table,
                table_alias,
                assignments,
                condition,
            } => {
                self.resolve_command_update(table, table_alias, assignments, condition, state)
                    .await
            }
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
            CommandNode::TruncateTable { table } => {
                self.resolve_command_truncate(table, state).await
            }
            CommandNode::AlterTable {
                table,
                if_exists,
                operation,
            } => {
                self.resolve_delta_alter_table_or_catalog(table, if_exists, operation, state)
                    .await
            }
            CommandNode::AlterView { .. } => Err(PlanError::todo("CommandNode::AlterView")),
            CommandNode::LoadData { .. } => Err(PlanError::todo("CommandNode::LoadData")),
            CommandNode::AnalyzeTable { .. } => Err(PlanError::todo("CommandNode::AnalyzeTable")),
            CommandNode::AnalyzeTables { .. } => Err(PlanError::todo("CommandNode::AnalyzeTables")),
            CommandNode::DescribeQuery { .. } => Err(PlanError::todo("CommandNode::DescribeQuery")),
            CommandNode::DescribeFunction { function, extended } => {
                self.resolve_catalog_command(CatalogCommand::DescribeFunction {
                    function: function.into(),
                    extended,
                    system_functions: list_built_in_function_statuses(),
                })
            }
            CommandNode::DescribeCatalog { .. } => {
                Err(PlanError::todo("CommandNode::DescribeCatalog"))
            }
            CommandNode::DescribeDatabase { database, extended } => {
                self.resolve_catalog_command(CatalogCommand::DescribeDatabase {
                    database: database.into(),
                    extended,
                })
            }
            CommandNode::DescribeTable {
                table,
                extended,
                partition,
                column,
            } => {
                if !partition.is_empty() {
                    return Err(PlanError::todo("DESCRIBE TABLE with partition spec"));
                }
                if column.is_some() {
                    return Err(PlanError::todo("DESCRIBE TABLE with column"));
                }
                self.resolve_catalog_command(CatalogCommand::DescribeTable {
                    table: table.into(),
                    extended,
                })
            }
            CommandNode::CommentOnCatalog { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnCatalog"))
            }
            CommandNode::CommentOnDatabase { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnDatabase"))
            }
            CommandNode::CommentOnTable { table, value } => {
                self.resolve_comment_on_table(table, value).await
            }
            CommandNode::CommentOnColumn { column, value } => {
                self.resolve_comment_on_column(column, value).await
            }
        }
    }

    fn resolve_catalog_command(&self, command: CatalogCommand) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(self.ctx, command)?),
        }))
    }
}
