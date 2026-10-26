use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{
    CreateDatabaseOptions, DropDatabaseOptions, DropTableOptions, DropTemporaryViewOptions,
    DropViewOptions,
};
use sail_common::spec;
use sail_logical_plan::show_string::ShowStringStyle;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::list_built_in_function_statuses;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn command_query(
        &mut self,
        request: spec::QueryPlan,
    ) -> PlanResult<Query> {
        self.complete_query(Query::unresolved(request)).await
    }

    pub(in crate::resolver) fn resolve_catalog_command(
        &self,
        command: CatalogCommand,
    ) -> PlanResult<CommandKind> {
        Ok(CommandKind::Catalog(command))
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_command_plan(
        &mut self,
        request: spec::CommandPlan,
    ) -> PlanResult<CommandKind> {
        match request.node {
            spec::CommandNode::CurrentDatabase => {
                self.resolve_catalog_command(CatalogCommand::CurrentDatabase)
            }
            spec::CommandNode::SetCurrentDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentDatabase {
                    database: database.into(),
                })
            }
            spec::CommandNode::ListDatabases { qualifier, pattern } => self
                .resolve_catalog_command(CatalogCommand::ListDatabases {
                    qualifier: qualifier.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                }),
            spec::CommandNode::ShowTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ShowTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            spec::CommandNode::ShowTableExtended { database, pattern } => self
                .resolve_catalog_command(CatalogCommand::ShowTableExtended {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                }),
            spec::CommandNode::ShowFunctions {
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
            spec::CommandNode::ListTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            spec::CommandNode::ListViews { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListViews {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            spec::CommandNode::ListFunctions { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListFunctions {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                    system_functions: list_built_in_function_statuses(),
                })
            }
            spec::CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: table.into(),
                })
            }
            spec::CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: database.into(),
                })
            }
            spec::CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: table.into(),
                })
            }
            spec::CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: function.into(),
                })
            }
            spec::CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: database.into(),
                })
            }
            spec::CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: table.into(),
                })
            }
            spec::CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: function.into(),
                    system_functions: list_built_in_function_statuses(),
                })
            }
            spec::CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: database.into(),
                options: DropDatabaseOptions { if_exists, cascade },
            }),
            spec::CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: function.into(),
                if_exists,
                is_temporary,
            }),
            spec::CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: table.into(),
                options: DropTableOptions { if_exists, purge },
            }),
            spec::CommandNode::RecoverPartitions { .. } => {
                Err(PlanError::todo("spec::CommandNode::RecoverPartitions"))
            }
            spec::CommandNode::IsCached { .. } => {
                Err(PlanError::todo("spec::CommandNode::IsCached"))
            }
            spec::CommandNode::CacheTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::CacheTable"))
            }
            spec::CommandNode::UncacheTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::UncacheTable"))
            }
            spec::CommandNode::ClearCache => Err(PlanError::todo("spec::CommandNode::ClearCache")),
            spec::CommandNode::RefreshTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::RefreshTable"))
            }
            spec::CommandNode::RefreshByPath { .. } => {
                Err(PlanError::todo("spec::CommandNode::RefreshByPath"))
            }
            spec::CommandNode::CurrentCatalog => {
                self.resolve_catalog_command(CatalogCommand::CurrentCatalog)
            }
            spec::CommandNode::SetCurrentCatalog { catalog } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentCatalog {
                    catalog: catalog.into(),
                })
            }
            spec::CommandNode::ListCatalogs { pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListCatalogs { pattern })
            }
            spec::CommandNode::CreateCatalog { .. } => Err(PlanError::todo("create catalog")),
            spec::CommandNode::RefreshFunction { .. } => {
                Err(PlanError::todo("spec::CommandNode::RefreshFunction"))
            }
            spec::CommandNode::AlterView { .. } => {
                Err(PlanError::todo("spec::CommandNode::AlterView"))
            }
            spec::CommandNode::LoadData { .. } => {
                Err(PlanError::todo("spec::CommandNode::LoadData"))
            }
            spec::CommandNode::AnalyzeTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::AnalyzeTable"))
            }
            spec::CommandNode::AnalyzeTables { .. } => {
                Err(PlanError::todo("spec::CommandNode::AnalyzeTables"))
            }
            spec::CommandNode::DescribeQuery { .. } => {
                Err(PlanError::todo("spec::CommandNode::DescribeQuery"))
            }
            spec::CommandNode::DescribeFunction { function, extended } => self
                .resolve_catalog_command(CatalogCommand::DescribeFunction {
                    function: function.into(),
                    extended,
                    system_functions: list_built_in_function_statuses(),
                }),
            spec::CommandNode::DescribeCatalog { .. } => {
                Err(PlanError::todo("spec::CommandNode::DescribeCatalog"))
            }
            spec::CommandNode::DescribeDatabase { database, extended } => self
                .resolve_catalog_command(CatalogCommand::DescribeDatabase {
                    database: database.into(),
                    extended,
                }),
            spec::CommandNode::DescribeTable {
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
            spec::CommandNode::CommentOnCatalog { .. } => {
                Err(PlanError::todo("spec::CommandNode::CommentOnCatalog"))
            }
            spec::CommandNode::CommentOnDatabase { .. } => {
                Err(PlanError::todo("spec::CommandNode::CommentOnDatabase"))
            }
            spec::CommandNode::CommentOnTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::CommentOnTable"))
            }
            spec::CommandNode::CommentOnColumn { .. } => {
                Err(PlanError::todo("spec::CommandNode::CommentOnColumn"))
            }
            spec::CommandNode::ShowString(spec::ShowString {
                input,
                num_rows,
                truncate,
                vertical,
            }) => {
                self.show(
                    *input,
                    num_rows,
                    truncate,
                    if vertical {
                        ShowStringStyle::Vertical
                    } else {
                        ShowStringStyle::Default
                    },
                    "show_string",
                )
                .await
            }
            spec::CommandNode::HtmlString(spec::HtmlString {
                input,
                num_rows,
                truncate,
            }) => {
                self.show(
                    *input,
                    num_rows,
                    truncate,
                    ShowStringStyle::Html,
                    "html_string",
                )
                .await
            }
            spec::CommandNode::RemoteCheckpoint { relation_id, input } => {
                Ok(CommandKind::Checkpoint {
                    relation_id,
                    input: self.command_query(*input).await?,
                })
            }
            spec::CommandNode::Explain { input, mode } => Ok(CommandKind::Explain {
                input: Box::new(self.resolve_plan((*input).into()).await?),
                mode,
            }),
            spec::CommandNode::SetVariable { variable, value } => Ok(CommandKind::SetVariable {
                variable: variable_name(variable),
                value: sail_sql_analyzer::parser::parse_native_config_value(&value)
                    .unwrap_or(value),
            }),
            spec::CommandNode::ResetVariable {
                variable: Some(variable),
            } => Ok(CommandKind::ResetVariable {
                variable: variable_name(variable),
            }),
            spec::CommandNode::ResetVariable { variable: None } => {
                Err(PlanError::unsupported("RESET requires a Spark session"))
            }
            spec::CommandNode::CreateDatabase {
                database,
                definition,
            } => self.resolve_catalog_command(CatalogCommand::CreateDatabase {
                database: database.into(),
                options: CreateDatabaseOptions {
                    if_not_exists: definition.if_not_exists,
                    comment: definition.comment,
                    location: definition.location,
                    properties: definition.properties,
                },
            }),
            spec::CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_command(CatalogCommand::DropView {
                    view: view.into(),
                    options: DropViewOptions { if_exists },
                })
            }
            spec::CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => self.resolve_catalog_command(CatalogCommand::DropTemporaryView {
                view: view.into(),
                is_global,
                options: DropTemporaryViewOptions { if_exists },
            }),
            spec::CommandNode::ShowTableProperties { table, property } => {
                self.resolve_show_table_properties(table, property).await
            }
            spec::CommandNode::CallProcedure {
                procedure,
                arguments,
                named_arguments,
            } => {
                self.resolve_command_call_procedure(procedure, arguments, named_arguments)
                    .await
            }
            spec::CommandNode::AlterTable {
                table,
                if_exists,
                operation,
            } => {
                self.resolve_delta_alter_table_or_catalog(table, if_exists, operation)
                    .await
            }
            spec::CommandNode::MergeInto(request) => self.resolve_command_merge_into(request).await,
            spec::CommandNode::Update {
                table,
                table_alias,
                assignments,
                condition,
            } => {
                self.resolve_command_update(table, table_alias, assignments, condition)
                    .await
            }
            spec::CommandNode::Delete {
                table,
                table_alias,
                condition,
            } => {
                self.resolve_command_delete(spec::Delete {
                    table,
                    table_alias,
                    condition,
                })
                .await
            }
            spec::CommandNode::WriteStream(request) => {
                self.resolve_command_write_stream(request).await
            }
            spec::CommandNode::InsertInto {
                input,
                table,
                mode,
                partition,
                if_not_exists,
            } => {
                self.resolve_command_insert_into(*input, table, mode, partition, if_not_exists)
                    .await
            }
            spec::CommandNode::InsertOverwriteDirectory {
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
                )
                .await
            }
            spec::CommandNode::CreateTableAsSelect {
                table,
                definition,
                query,
            } => {
                self.resolve_catalog_create_table_as_select(table, definition, *query)
                    .await
            }
            spec::CommandNode::Write(request) => self.resolve_command_write(request).await,
            spec::CommandNode::WriteTo(request) => self.resolve_command_write_to(request).await,
            spec::CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition).await
            }
            spec::CommandNode::RegisterTableFunction(function) => {
                let spec::TableFunctionDefinition::PythonUdtf {
                    return_type,
                    eval_type,
                    command,
                    python_version,
                } = function.function;
                let types = crate::resolver::data_type::DataTypeResolver {
                    config: &self.context.config,
                    allow_large_var_types: true,
                };
                let output_type = return_type
                    .map(|t| types.resolve_data_type(&t))
                    .transpose()?;
                let name: String = function.function_name.into();
                Ok(CommandKind::RegisterFunction(
                    PySparkUnresolvedUDF::new(
                        name.to_ascii_lowercase(),
                        python_version,
                        eval_type,
                        command,
                        output_type,
                        function.deterministic,
                    )
                    .into(),
                ))
            }
            spec::CommandNode::CreateView { view, definition } => {
                self.resolve_command_create_view(view, definition).await
            }
            spec::CommandNode::RegisterFunction(function) => {
                let definition = self.python_definition(function.function)?;
                let name: String = function.function_name.into();
                Ok(CommandKind::RegisterFunction(
                    PySparkUnresolvedUDF::new(
                        name.to_ascii_lowercase(),
                        definition.python_version,
                        definition.eval_type,
                        definition.command,
                        Some(definition.output_type),
                        function.deterministic,
                    )
                    .into(),
                ))
            }
            spec::CommandNode::WithRelations { root, references } => {
                let relations = self.relations.clone();
                let ctes = self.ctes.clone();
                let result = self.resolve_command_with_relations(*root, references).await;
                self.relations = relations;
                self.ctes = ctes;
                result
            }
            spec::CommandNode::CreateTemporaryView {
                view,
                is_global,
                definition,
            } => {
                self.resolve_command_create_temporary_view(view, is_global, definition)
                    .await
            }
        }
    }
}

fn variable_name(name: String) -> String {
    if name.eq_ignore_ascii_case("timezone") || name.eq_ignore_ascii_case("time.zone") {
        "datafusion.execution.time_zone".into()
    } else {
        name
    }
}

pub(in crate::resolver) mod catalog;
pub(in crate::resolver) mod insert;
pub(in crate::resolver) mod merge;
pub(in crate::resolver) mod procedure;
pub(in crate::resolver) mod row_level;
pub(in crate::resolver) mod show;
pub(in crate::resolver) mod table_properties;
pub(in crate::resolver) mod update;
pub(in crate::resolver) mod with_relations;
pub(in crate::resolver) mod write;
pub(in crate::resolver) mod write_constraints;
pub(in crate::resolver) mod write_defaults;
pub(in crate::resolver) mod write_input;
pub(in crate::resolver) mod write_stream;
pub(in crate::resolver) mod write_v1;
pub(in crate::resolver) mod write_v2;
