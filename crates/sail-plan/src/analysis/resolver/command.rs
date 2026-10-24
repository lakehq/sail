use std::sync::Arc;

use datafusion_common::ScalarValue;
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{
    CreateDatabaseOptions, CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions,
    DropTableOptions, DropTemporaryViewOptions, DropViewOptions,
};
use sail_common::spec;
use sail_logical_plan::show_string::{ShowStringFormat, ShowStringStyle};
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use super::{CteDefinition, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::list_built_in_function_statuses;

impl Resolver<'_, '_> {
    pub(super) async fn command_query(
        &mut self,
        request: spec::QueryPlan,
    ) -> PlanResult<QueryNode> {
        self.complete_query(QueryNode::unresolved(request)).await
    }

    pub(super) fn resolve_catalog_command(
        &self,
        command: CatalogCommand,
    ) -> PlanResult<CommandKind> {
        Ok(CommandKind::Catalog(command))
    }

    #[async_recursion::async_recursion]
    pub(super) async fn command(&mut self, request: spec::CommandPlan) -> PlanResult<CommandKind> {
        use spec::CommandNode;
        match request.node {
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
                Err(PlanError::todo("spec::CommandNode::RecoverPartitions"))
            }
            CommandNode::IsCached { .. } => Err(PlanError::todo("spec::CommandNode::IsCached")),
            CommandNode::CacheTable { .. } => Err(PlanError::todo("spec::CommandNode::CacheTable")),
            CommandNode::UncacheTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::UncacheTable"))
            }
            CommandNode::ClearCache => Err(PlanError::todo("spec::CommandNode::ClearCache")),
            CommandNode::RefreshTable { .. } => {
                Err(PlanError::todo("spec::CommandNode::RefreshTable"))
            }
            CommandNode::RefreshByPath { .. } => {
                Err(PlanError::todo("spec::CommandNode::RefreshByPath"))
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
            CommandNode::RefreshFunction { .. } => {
                Err(PlanError::todo("CommandNode::RefreshFunction"))
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
            CommandNode::CommentOnTable { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnTable"))
            }
            CommandNode::CommentOnColumn { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnColumn"))
            }
            CommandNode::ShowString(spec::ShowString {
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
            CommandNode::HtmlString(spec::HtmlString {
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
            CommandNode::RemoteCheckpoint { relation_id, input } => Ok(CommandKind::Checkpoint {
                relation_id,
                input: self.command_query(*input).await?,
            }),
            CommandNode::Explain { input, mode } => Ok(CommandKind::Explain {
                input: Box::new(self.native_plan((*input).into()).await?),
                mode,
            }),
            CommandNode::SetVariable { variable, value } => Ok(CommandKind::SetVariable {
                variable: variable_name(variable),
                value: sail_sql_analyzer::parser::parse_native_config_value(&value)
                    .unwrap_or(value),
            }),
            CommandNode::ResetVariable {
                variable: Some(variable),
            } => Ok(CommandKind::ResetVariable {
                variable: variable_name(variable),
            }),
            CommandNode::ResetVariable { variable: None } => {
                Err(PlanError::unsupported("RESET requires a Spark session"))
            }
            CommandNode::CreateDatabase {
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
            CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_command(CatalogCommand::DropView {
                    view: view.into(),
                    options: DropViewOptions { if_exists },
                })
            }
            CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => self.resolve_catalog_command(CatalogCommand::DropTemporaryView {
                view: view.into(),
                is_global,
                options: DropTemporaryViewOptions { if_exists },
            }),
            CommandNode::ShowTableProperties { table, property } => {
                self.resolve_show_table_properties(table, property).await
            }
            CommandNode::CallProcedure {
                procedure,
                arguments,
                named_arguments,
            } => {
                self.resolve_command_call_procedure(procedure, arguments, named_arguments)
                    .await
            }
            CommandNode::AlterTable {
                table,
                if_exists,
                operation,
            } => {
                self.resolve_delta_alter_table_or_catalog(table, if_exists, operation)
                    .await
            }
            CommandNode::MergeInto(request) => self.resolve_command_merge_into(request).await,
            CommandNode::Update {
                table,
                table_alias,
                assignments,
                condition,
            } => {
                self.resolve_command_update(table, table_alias, assignments, condition)
                    .await
            }
            CommandNode::Delete {
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
            CommandNode::WriteStream(request) => self.resolve_command_write_stream(request).await,
            CommandNode::InsertInto {
                input,
                table,
                mode,
                partition,
                if_not_exists,
            } => {
                self.resolve_command_insert_into(*input, table, mode, partition, if_not_exists)
                    .await
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
                )
                .await
            }
            CommandNode::CreateTableAsSelect {
                table,
                definition,
                query,
            } => {
                self.resolve_catalog_create_table_as_select(table, definition, *query)
                    .await
            }
            CommandNode::Write(request) => self.resolve_command_write(request).await,
            CommandNode::WriteTo(request) => self.resolve_command_write_to(request).await,
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition).await
            }
            CommandNode::RegisterTableFunction(function) => {
                let spec::TableFunctionDefinition::PythonUdtf {
                    return_type,
                    eval_type,
                    command,
                    python_version,
                } = function.function;
                let types = crate::analysis::data_type::DataTypeResolver {
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
            CommandNode::CreateView { view, definition } => {
                self.create_view(view, definition).await
            }
            CommandNode::RegisterFunction(function) => {
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
            CommandNode::WithRelations { root, references } => {
                let relations = self.relations.clone();
                let ctes = self.ctes.clone();
                let result = self.command_with_relations(*root, references).await;
                self.relations = relations;
                self.ctes = ctes;
                result
            }
            CommandNode::CreateTemporaryView {
                view,
                is_global,
                definition,
            } => {
                self.create_temporary_view(view, is_global, definition)
                    .await
            }
        }
    }

    async fn show(
        &mut self,
        input: spec::QueryPlan,
        rows: usize,
        truncate: usize,
        style: ShowStringStyle,
        name: &str,
    ) -> PlanResult<CommandKind> {
        let input = self.command_query(input).await?;
        // Spark clients request the complete result with i32::MAX. Do not turn
        // that sentinel into a TopK allocation of billions of entries. Show's
        // execution operator still enforces the requested display row count.
        let input = if rows == i32::MAX as usize {
            input
        } else {
            let schema = input.schema.clone();
            let fetch = i64::try_from(rows)
                .ok()
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| PlanError::invalid("show row count is too large"))?;
            QueryNode {
                kind: Deferred::Resolved(QueryKind::Limit {
                    input: Box::new(input),
                    skip: Some(Box::new(self.literal(ScalarValue::Int64(Some(0))))),
                    fetch: Some(Box::new(self.literal(ScalarValue::Int64(Some(fetch))))),
                }),
                schema,
            }
        };
        Ok(CommandKind::Show {
            input,
            rows,
            format: ShowStringFormat::new(style, truncate),
            name: name.into(),
        })
    }

    async fn create_temporary_view(
        &mut self,
        view: spec::Identifier,
        is_global: bool,
        definition: spec::TemporaryViewDefinition,
    ) -> PlanResult<CommandKind> {
        use sail_catalog::provider::{
            CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions,
        };
        use sail_common_datafusion::catalog::TemporaryViewSource;
        let source = match &definition.input.node {
            spec::QueryNode::Read {
                read_type: spec::ReadType::DataSource(source),
                ..
            } => source.format.clone().map(|format| TemporaryViewSource {
                format,
                options: source.options.clone(),
            }),
            _ => None,
        };
        let input = self.command_query(*definition.input).await?;
        let mut input = self.visible_output(input)?;
        let columns = if let Some(columns) = definition.columns {
            input = self.rename(
                input,
                columns.iter().map(|c| c.name.clone().into()).collect(),
            )?;
            columns
                .into_iter()
                .map(|c| CreateTemporaryViewColumnOptions { comment: c.comment })
                .collect()
        } else {
            vec![]
        };
        Ok(CommandKind::CreateTemporaryView {
            view: view.into(),
            is_global,
            options: CreateTemporaryViewOptions {
                input: Arc::new(input),
                columns,
                source,
                if_not_exists: definition.if_not_exists,
                replace: definition.replace,
                comment: definition.comment,
                properties: definition.properties,
            },
        })
    }

    async fn create_view(
        &mut self,
        view: spec::ObjectName,
        definition: spec::ViewDefinition,
    ) -> PlanResult<CommandKind> {
        let input = self.command_query(*definition.input).await?;
        let schema = input.schema.resolved("view schema")?;
        if definition
            .columns
            .as_ref()
            .is_some_and(|columns| columns.len() != schema.attributes.len())
        {
            return Err(PlanError::analysis(
                "CREATE VIEW column count does not match query",
            ));
        }
        let mut requested = definition.columns.map(Vec::into_iter);
        let columns = schema
            .attributes
            .iter()
            .map(|attribute| {
                let field = attribute.result_type.resolved("view field")?;
                let column = requested.as_mut().and_then(Iterator::next);
                Ok(CreateViewColumnOptions {
                    name: column
                        .as_ref()
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| attribute.name.clone()),
                    data_type: field.data_type.clone(),
                    nullable: field.nullable,
                    comment: column.and_then(|c| c.comment),
                })
            })
            .collect::<PlanResult<_>>()?;
        self.resolve_catalog_command(CatalogCommand::CreateView {
            view: view.into(),
            options: CreateViewOptions {
                definition: definition.definition,
                columns,
                comment: definition.comment,
                if_not_exists: definition.if_not_exists,
                replace: definition.replace,
                properties: definition.properties,
            },
        })
    }

    #[async_recursion::async_recursion]
    async fn command_with_relations(
        &mut self,
        root: spec::CommandPlan,
        references: Vec<spec::QueryPlan>,
    ) -> PlanResult<CommandKind> {
        for reference in references {
            let id = reference
                .plan_id
                .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
            if let spec::QueryNode::SubqueryAlias { alias, .. } = &reference.node {
                let plan = self.query(reference.clone()).await?;
                self.ctes.insert(
                    self.name_key(alias.as_ref()),
                    Arc::new(CteDefinition {
                        plan,
                        parameter: true,
                    }),
                );
            }
            if self.relations.insert(id, reference).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicate subquery reference for plan_id {id}"
                )));
            }
        }
        self.command(root).await
    }
}

fn variable_name(name: String) -> String {
    if name.eq_ignore_ascii_case("timezone") || name.eq_ignore_ascii_case("time.zone") {
        "datafusion.execution.time_zone".into()
    } else {
        name
    }
}
