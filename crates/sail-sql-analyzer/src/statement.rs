use std::iter::once;

use either::Either;
use sail_common::spec;
use sail_common::spec::QueryPlan;
use sail_common::utils::datetime::get_system_timezone;
use sail_sql_parser::ast::expression::{BooleanLiteral, Expr, IntervalExpr, OrderDirection};
use sail_sql_parser::ast::identifier::{Ident, ObjectName};
use sail_sql_parser::ast::keywords::{Cascade, Overwrite, Restrict};
use sail_sql_parser::ast::literal::{ConfigValue, IntegerLiteral, NumberLiteral, StringLiteral};
use sail_sql_parser::ast::operator::{Minus, Plus};
use sail_sql_parser::ast::query::{IdentList, WhereClause};
use sail_sql_parser::ast::statement::{
    AlterColumnOperation, AlterTableOperation, AlterViewOperation, AnalyzeTableModifier,
    AsQueryClause, Assignment, AssignmentList, ColumnAlteration, ColumnAlterationList,
    ColumnAlterationOption, ColumnDefinition, ColumnDefinitionList, ColumnDefinitionOption,
    ColumnPosition, ColumnTypeDefinition, CommentValue, CreateDatabaseClause, CreateTableClause,
    CreateViewClause, CreateViewDefinition, DeleteTableAlias, DescribeFunctionName, DescribeItem,
    ExplainFormat, FileFormat, InsertDirectoryDestination, MergeMatchClause, MergeMatchedAction,
    MergeNotMatchedBySourceAction, MergeNotMatchedByTargetAction, MergeSource, PartitionByItem,
    PartitionByList, PartitionClause, PartitionValue, PartitionValueList, PropertyKey,
    PropertyKeyList, PropertyKeyValue, PropertyList, PropertyValue, RowFormat,
    RowFormatDelimitedClause, SetClause, SetPropertyKeyValue, SetPropertyValue, ShowFunctionScope,
    ShowFunctionsClause, ShowFunctionsPattern, SortColumn, SortColumnClause, SortColumnList,
    Statement, TableColumnIdentityOption, TableColumnIdentityOptions, TimeZoneValue,
    UpdateTableAlias, ViewColumn, ViewColumnList, ViewUsingClause,
};
use sail_sql_parser::tree::TreeText;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::expression::{
    expr_with_default_column_values, from_ast_expression, from_ast_identifier_list,
    from_ast_object_name,
};
use crate::literal::interval::{IntervalValue, from_ast_signed_interval, multi_unit_interval_days};
use crate::literal::utils::Signed;
use crate::query::from_ast_query;
use crate::value::from_ast_string;

fn from_ast_show_function_scope(scope: Option<ShowFunctionScope>) -> (bool, bool) {
    match scope {
        None | Some(ShowFunctionScope::All(_)) => (true, true),
        Some(ShowFunctionScope::User(_)) => (true, false),
        Some(ShowFunctionScope::System(_)) => (false, true),
    }
}

fn from_ast_show_functions_clause(
    clause: Option<ShowFunctionsClause>,
) -> SqlResult<(Option<spec::ObjectName>, Option<String>)> {
    let Some(clause) = clause else {
        return Ok((None, None));
    };
    match clause {
        ShowFunctionsClause::NamespacePattern(_, database, _, pattern) => Ok((
            Some(from_ast_object_name(database)?),
            Some(from_ast_string(pattern)?),
        )),
        ShowFunctionsClause::Namespace(_, database) => {
            Ok((Some(from_ast_object_name(database)?), None))
        }
        ShowFunctionsClause::Pattern(_, ShowFunctionsPattern::String(pattern)) => {
            Ok((None, Some(from_ast_string(pattern)?)))
        }
        ShowFunctionsClause::Pattern(_, ShowFunctionsPattern::Name(pattern)) => {
            let mut parts: Vec<String> = from_ast_object_name(pattern)?.into();
            let Some(pattern) = parts.pop() else {
                return Err(SqlError::invalid("SHOW FUNCTIONS with empty pattern"));
            };
            Ok((None, Some(pattern)))
        }
    }
}

fn from_ast_describe_function_name(name: DescribeFunctionName) -> SqlResult<spec::ObjectName> {
    let name = match name {
        DescribeFunctionName::Name(name) => return from_ast_object_name(name),
        DescribeFunctionName::String(name) => from_ast_string(name)?,
        DescribeFunctionName::TripleGreaterThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::DoubleVerticalBar(name) => name.text().trim().to_string(),
        DescribeFunctionName::DoubleGreaterThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::DoubleLessThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::GreaterThanEquals(name) => name.text().trim().to_string(),
        DescribeFunctionName::LessThanEquals(name) => name.text().trim().to_string(),
        DescribeFunctionName::LessThanGreaterThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::Spaceship(name) => name.text().trim().to_string(),
        DescribeFunctionName::NotEquals(name) => name.text().trim().to_string(),
        DescribeFunctionName::DoubleEquals(name) => name.text().trim().to_string(),
        DescribeFunctionName::ExclamationMark(name) => name.text().trim().to_string(),
        DescribeFunctionName::GreaterThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::LessThan(name) => name.text().trim().to_string(),
        DescribeFunctionName::Plus(name) => name.text().trim().to_string(),
        DescribeFunctionName::Minus(name) => name.text().trim().to_string(),
        DescribeFunctionName::Asterisk(name) => name.text().trim().to_string(),
        DescribeFunctionName::Slash(name) => name.text().trim().to_string(),
        DescribeFunctionName::Percent(name) => name.text().trim().to_string(),
        DescribeFunctionName::Ampersand(name) => name.text().trim().to_string(),
        DescribeFunctionName::VerticalBar(name) => name.text().trim().to_string(),
        DescribeFunctionName::Caret(name) => name.text().trim().to_string(),
        DescribeFunctionName::Tilde(name) => name.text().trim().to_string(),
        DescribeFunctionName::Equals(name) => name.text().trim().to_string(),
    };
    Ok(spec::ObjectName::bare(name))
}

/// Converts a parsed SQL AST statement into a spec plan (either a query or a command).
pub fn from_ast_statement(statement: Statement) -> SqlResult<spec::Plan> {
    match statement {
        Statement::Query(query) => {
            let plan = from_ast_query(query)?;
            Ok(spec::Plan::Query(plan))
        }
        Statement::SetCatalog {
            set: _,
            catalog: _,
            name,
        } => {
            let name = match name {
                Either::Left(x) => x.value,
                Either::Right(x) => from_ast_string(x)?,
            };
            let node = spec::CommandNode::SetCurrentCatalog {
                catalog: name.into(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::UseCatalog {
            r#use: _,
            catalog: _,
            name,
        } => {
            let node = spec::CommandNode::SetCurrentCatalog {
                catalog: name.value.into(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::UseDatabase {
            r#use: _,
            database: _,
            name,
        } => {
            let node = spec::CommandNode::SetCurrentDatabase {
                database: from_ast_object_name(name)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateDatabase {
            create: _,
            database: _,
            name,
            if_not_exists,
            clauses,
        } => {
            let CreateDatabaseClauses {
                comment,
                location,
                properties,
            } = clauses.try_into()?;
            let node = spec::CommandNode::CreateDatabase {
                database: from_ast_object_name(name)?,
                definition: spec::DatabaseDefinition {
                    if_not_exists: if_not_exists.is_some(),
                    comment: comment.map(from_ast_string).transpose()?,
                    location: location.map(from_ast_string).transpose()?,
                    properties: properties
                        .map(from_ast_property_list)
                        .transpose()?
                        .unwrap_or_default(),
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AlterDatabase { .. } => Err(SqlError::todo("ALTER DATABASE")),
        Statement::DropDatabase {
            drop: _,
            database: _,
            if_exists,
            name,
            specifier,
        } => {
            let cascade = match specifier {
                Some(Either::Left(Restrict { .. })) => {
                    return Err(SqlError::todo("RESTRICT in DROP DATABASE"));
                }
                Some(Either::Right(Cascade { .. })) => true,
                None => false,
            };
            let node = spec::CommandNode::DropDatabase {
                database: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                cascade,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowDatabases {
            show: _,
            databases: _,
            from,
            like,
        } => {
            let qualifier = from
                .map(|(_, name)| from_ast_object_name(name))
                .transpose()?;
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            let node = spec::CommandNode::ListDatabases { qualifier, pattern };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowCatalogs {
            show: _,
            catalogs: _,
            like,
        } => {
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            let node = spec::CommandNode::ListCatalogs { pattern };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateTable {
            create: _,
            or_replace,
            temporary: _, // TODO: handle temporary tables
            external,
            table: _,
            if_not_exists,
            name,
            columns,
            like,
            using,
            clauses,
            r#as,
        } => {
            if like.is_some() {
                return Err(SqlError::todo("LIKE in CREATE TABLE"));
            }
            let definition = TableDefinition {
                external: external.is_some(),
                replace: false,
                or_replace: or_replace.is_some(),
                if_not_exists: if_not_exists.is_some(),
                using: using.map(|(_, x)| x),
                columns,
                clauses: clauses.try_into()?,
                query: r#as,
            };
            let table = from_ast_object_name(name)?;
            let (definition, query) = from_ast_table_definition(definition)?;
            let node = if let Some(query) = query {
                spec::CommandNode::CreateTableAsSelect {
                    table,
                    definition,
                    query,
                }
            } else {
                spec::CommandNode::CreateTable { table, definition }
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ReplaceTable {
            replace: _,
            external,
            table: _,
            name,
            columns,
            using,
            clauses,
            r#as,
        } => {
            let definition = TableDefinition {
                external: external.is_some(),
                replace: true,
                or_replace: false,
                if_not_exists: false,
                using: using.map(|(_, x)| x),
                columns,
                clauses: clauses.try_into()?,
                query: r#as,
            };
            let table = from_ast_object_name(name)?;
            let (definition, query) = from_ast_table_definition(definition)?;
            let node = if let Some(query) = query {
                spec::CommandNode::CreateTableAsSelect {
                    table,
                    definition,
                    query,
                }
            } else {
                spec::CommandNode::CreateTable { table, definition }
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::RefreshTable {
            refresh: _,
            table: _,
            name,
        } => {
            let node = spec::CommandNode::RefreshTable {
                table: from_ast_object_name(name)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AlterTable {
            alter: _,
            table: _,
            name,
            operation,
        } => {
            let node = spec::CommandNode::AlterTable {
                table: from_ast_object_name(name)?,
                if_exists: false,
                operation: from_ast_alter_table_operation(operation)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::DropTable {
            drop: _,
            table: _,
            if_exists,
            name,
            purge,
        } => {
            let node = spec::CommandNode::DropTable {
                table: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                purge: purge.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowTables {
            show: _,
            tables: _,
            from,
            like,
        } => {
            let database = from
                .map(|(_, name)| from_ast_object_name(name))
                .transpose()?;
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            let node = spec::CommandNode::ShowTables { database, pattern };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowTableExtended {
            show: _,
            table: _,
            extended: _,
            from,
            like,
        } => {
            let database = from
                .map(|(_, name)| from_ast_object_name(name))
                .transpose()?;
            let pattern = from_ast_string(like.1)?;
            let node = spec::CommandNode::ShowTableExtended { database, pattern };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowCreateTable { .. } => Err(SqlError::todo("SHOW CREATE TABLE")),
        Statement::ShowColumns {
            show: _,
            columns: _,
            table: (_, table),
            database,
        } => {
            let table = from_ast_object_name(table)?;
            let table = if let Some((_, database)) = database {
                let mut table: Vec<String> = table.into();
                let table = match (table.pop(), table.is_empty()) {
                    (None, _) => {
                        return Err(SqlError::invalid("SHOW COLUMNS with no table name"));
                    }
                    (Some(_), false) => {
                        return Err(SqlError::todo(
                            "SHOW COLUMNS for qualified table name with conflicting database name",
                        ));
                    }
                    (Some(name), true) => name,
                };
                let mut database: Vec<String> = from_ast_object_name(database)?.into();
                database.push(table);
                database.into()
            } else {
                table
            };
            let node = spec::CommandNode::ListColumns { table };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateView {
            create: _,
            or_replace,
            definition,
        } => {
            let view_columns = |columns: Option<ViewColumnList>| {
                columns.map(
                    |ViewColumnList {
                         left: _,
                         columns,
                         right: _,
                     }| columns.into_items().collect::<Vec<_>>(),
                )
            };
            let temporary_view_name = |name: spec::ObjectName| {
                let mut name: Vec<String> = name.into();
                match (name.pop(), name.is_empty()) {
                    (Some(x), true) => Ok(spec::Identifier::from(x)),
                    _ => Err(SqlError::invalid(
                        "expected a single identifier for temporary view name",
                    )),
                }
            };
            let node = match definition {
                CreateViewDefinition::Query {
                    temporary,
                    view: _,
                    if_not_exists,
                    name,
                    columns,
                    clauses,
                    r#as: _,
                    query,
                } => {
                    let name = from_ast_object_name(name)?;
                    let CreateViewClauses {
                        comment,
                        properties,
                    } = clauses.try_into()?;
                    let comment = comment.map(from_ast_string).transpose()?;
                    let properties = properties
                        .map(from_ast_property_list)
                        .transpose()?
                        .unwrap_or_default();
                    if if_not_exists.is_some() && or_replace.is_some() {
                        return Err(SqlError::invalid(
                            "CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed",
                        ));
                    }
                    if temporary.is_some() && !properties.is_empty() {
                        return Err(SqlError::invalid(
                            "TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW",
                        ));
                    }
                    if temporary.is_some() && if_not_exists.is_some() {
                        return Err(SqlError::invalid(
                            "It is not allowed to define a TEMPORARY view with IF NOT EXISTS",
                        ));
                    }
                    let columns = view_columns(columns)
                        .map(from_ast_view_columns)
                        .transpose()?;
                    let query_text = query.text();
                    let query = from_ast_query(query)?;
                    if let Some(temporary) = temporary {
                        spec::CommandNode::CreateTemporaryView {
                            view: temporary_view_name(name)?,
                            is_global: temporary.global.is_some(),
                            definition: spec::TemporaryViewDefinition {
                                input: Box::new(query),
                                columns,
                                if_not_exists: if_not_exists.is_some(),
                                replace: or_replace.is_some(),
                                comment,
                                properties,
                            },
                        }
                    } else {
                        spec::CommandNode::CreateView {
                            view: name,
                            definition: spec::ViewDefinition {
                                definition: query_text,
                                input: Box::new(query),
                                columns,
                                if_not_exists: if_not_exists.is_some(),
                                replace: or_replace.is_some(),
                                comment,
                                properties,
                            },
                        }
                    }
                }
                CreateViewDefinition::Using {
                    temporary,
                    view: _,
                    name,
                    columns,
                    using:
                        ViewUsingClause {
                            using: _,
                            format,
                            options,
                        },
                } => {
                    let name = from_ast_object_name(name)?;
                    let (schema, columns) = view_columns(columns)
                        .map(from_ast_view_using_columns)
                        .transpose()?
                        .map(|(schema, columns)| (Some(schema), Some(columns)))
                        .unwrap_or((None, None));
                    let options = options
                        .map(|(_, x)| from_ast_property_list(x))
                        .transpose()?
                        .unwrap_or_default();
                    // The path is also kept in the options, since data sources such as
                    // Python data sources read the path from the options.
                    // Only the `path` option is a path source; `location` is forwarded
                    // as an ordinary option and never used as the path in Spark.
                    let paths = options
                        .iter()
                        .rfind(|(key, _)| key.eq_ignore_ascii_case("path"))
                        .map(|(_, value)| vec![value.clone()])
                        .ok_or_else(|| {
                            SqlError::invalid(
                                "the data source path must be specified for CREATE TEMPORARY VIEW ... USING",
                            )
                        })?;
                    let input = spec::QueryPlan::new(spec::QueryNode::Read {
                        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
                            format: Some(format.value),
                            schema,
                            options,
                            paths,
                            predicates: vec![],
                        })),
                        is_streaming: false,
                    });
                    spec::CommandNode::CreateTemporaryView {
                        view: temporary_view_name(name)?,
                        is_global: temporary.global.is_some(),
                        definition: spec::TemporaryViewDefinition {
                            input: Box::new(input),
                            columns,
                            if_not_exists: false,
                            replace: or_replace.is_some(),
                            comment: None,
                            properties: vec![],
                        },
                    }
                }
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AlterView {
            alter: _,
            view: _,
            name,
            operation,
        } => {
            let node = spec::CommandNode::AlterView {
                view: from_ast_object_name(name)?,
                if_exists: false,
                operation: from_ast_alter_view_operation(operation)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::DropView {
            drop: _,
            view: _,
            if_exists,
            name,
        } => {
            let node = spec::CommandNode::DropView {
                view: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowViews {
            show: _,
            views: _,
            from,
            like,
        } => {
            let database = from
                .map(|(_, name)| from_ast_object_name(name))
                .transpose()?;
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            let node = spec::CommandNode::ListViews { database, pattern };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::RefreshFunction {
            refresh: _,
            function: _,
            name,
        } => {
            let node = spec::CommandNode::RefreshFunction {
                function: from_ast_object_name(name)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::DropFunction {
            drop: _,
            temporary,
            function: _,
            if_exists,
            name,
        } => {
            let node = spec::CommandNode::DropFunction {
                function: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                is_temporary: temporary.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowFunctions {
            show: _,
            scope,
            functions: _,
            clause,
        } => {
            let (database, pattern) = from_ast_show_functions_clause(clause)?;
            let (show_user_functions, show_system_functions) = from_ast_show_function_scope(scope);
            let node = spec::CommandNode::ShowFunctions {
                database,
                pattern,
                show_user_functions,
                show_system_functions,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::Explain {
            explain: _,
            format,
            statement,
        } => {
            let mode = from_ast_explain_format(format)?;
            let statement = from_ast_statement(*statement)?;
            let node = spec::CommandNode::Explain {
                mode,
                input: Box::new(statement),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::InsertOverwriteDirectory {
            insert: _,
            overwrite: _,
            local,
            directory: _,
            destination,
            query,
        } => {
            let (location, file_format, row_format, options) = match destination {
                InsertDirectoryDestination::Spark {
                    path,
                    using: (_, format),
                    options,
                } => {
                    let options = options
                        .map(|o| {
                            let (_, x) = *o;
                            from_ast_property_list(x)
                        })
                        .transpose()?
                        .unwrap_or_default();
                    (
                        path.map(from_ast_string).transpose()?,
                        Some(spec::TableFileFormat::General {
                            format: format.value,
                        }),
                        None,
                        options,
                    )
                }
                InsertDirectoryDestination::Hive {
                    path,
                    row_format,
                    stored_as,
                } => {
                    let path = from_ast_string(path)?;
                    let file_format = stored_as
                        .map(|s| {
                            let (_, _, x) = *s;
                            from_ast_file_format(x)
                        })
                        .transpose()?;
                    let row_format = row_format
                        .map(|r| {
                            let (_, _, x) = *r;
                            from_ast_row_format(x)
                        })
                        .transpose()?;
                    (Some(path), file_format, row_format, vec![])
                }
            };
            let node = spec::CommandNode::InsertOverwriteDirectory {
                input: Box::new(from_ast_query(query)?),
                local: local.is_some(),
                location,
                file_format,
                row_format,
                options,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::InsertIntoAndReplace {
            insert: _,
            into: _,
            table: _,
            name,
            replace: _,
            r#where,
            query,
        } => {
            let query = from_ast_query(query)?;
            let WhereClause {
                r#where: _,
                condition,
            } = r#where;
            let source = condition.text();
            let node = spec::CommandNode::InsertInto {
                input: Box::new(query),
                table: from_ast_object_name(name)?,
                mode: spec::InsertMode::Replace {
                    condition: Box::new(spec::ExprWithSource {
                        expr: from_ast_expression(condition)?,
                        source: Some(source),
                    }),
                },
                partition: vec![],
                if_not_exists: false,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::InsertInto {
            insert: _,
            into_or_overwrite,
            table: _,
            name,
            partition,
            if_not_exists,
            columns,
            query,
        } => {
            let overwrite = matches!(into_or_overwrite, Either::Right(Overwrite { .. }));
            let partition = if let Some(partition) = partition {
                from_ast_partition(partition)?
            } else {
                vec![]
            };
            let mode = match columns {
                Some(Either::Left((_, _))) => spec::InsertMode::InsertByName { overwrite },
                Some(Either::Right(columns)) => spec::InsertMode::InsertByColumns {
                    columns: from_ast_identifier_list(columns)?,
                    overwrite,
                },
                None => spec::InsertMode::InsertByPosition { overwrite },
            };
            let query = from_ast_query(query)?;
            let node = spec::CommandNode::InsertInto {
                input: Box::new(query),
                table: from_ast_object_name(name)?,
                mode,
                partition,
                if_not_exists: if_not_exists.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::MergeInto {
            merge: _,
            with_schema_evolution,
            into: _,
            target,
            alias: target_alias,
            using,
            on,
            r#match,
        } => {
            let (_, source) = *using;
            let (_, on_expr) = *on;
            if target_alias
                .as_ref()
                .is_some_and(|alias| alias.columns.is_some())
            {
                return Err(SqlError::invalid(
                    "column aliases are not allowed for target table in MERGE",
                ));
            }
            if r#match.is_empty() {
                return Err(SqlError::invalid(
                    "expected at least one WHEN ... MATCHED ... clause for MERGE",
                ));
            }

            let target_alias = target_alias.map(|alias| alias.table.value.into());
            let source = match source {
                MergeSource::Table { name, alias } => {
                    if alias.as_ref().is_some_and(|alias| alias.columns.is_some()) {
                        return Err(SqlError::invalid(
                            "column aliases are not allowed for source table in MERGE",
                        ));
                    }
                    spec::MergeSource::Table {
                        name: from_ast_object_name(name)?,
                        alias: alias.map(|alias| alias.table.value.into()),
                    }
                }
                MergeSource::Query {
                    query,
                    alias,
                    left: _,
                    right: _,
                } => {
                    if alias.as_ref().is_some_and(|alias| alias.columns.is_some()) {
                        return Err(SqlError::invalid(
                            "column aliases are not allowed for source table in MERGE",
                        ));
                    }
                    spec::MergeSource::Query {
                        input: Box::new(from_ast_query(query)?),
                        alias: alias.map(|alias| alias.table.value.into()),
                    }
                }
            };
            let clauses = r#match
                .into_iter()
                .map(|clause| match clause {
                    MergeMatchClause::Matched {
                        condition, action, ..
                    } => {
                        let condition = from_ast_merge_optional_condition(condition)?;
                        let action = match action {
                            MergeMatchedAction::Delete(_) => spec::MergeMatchedAction::Delete,
                            MergeMatchedAction::UpdateAll(_, _, _) => {
                                spec::MergeMatchedAction::UpdateAll
                            }
                            MergeMatchedAction::Update(_, _, assignments) => {
                                let assignments = from_ast_merge_assignment_list(assignments)?;
                                spec::MergeMatchedAction::UpdateSet(assignments)
                            }
                        };
                        Ok(spec::MergeClause::Matched(spec::MergeMatchedClause {
                            condition,
                            action,
                        }))
                    }
                    MergeMatchClause::NotMatchedBySource {
                        condition, action, ..
                    } => {
                        let condition = from_ast_merge_optional_condition(condition)?;
                        let action = match action {
                            MergeNotMatchedBySourceAction::Delete(_) => {
                                spec::MergeNotMatchedBySourceAction::Delete
                            }
                            MergeNotMatchedBySourceAction::Update(_, _, assignments) => {
                                let assignments = from_ast_merge_assignment_list(assignments)?;
                                spec::MergeNotMatchedBySourceAction::UpdateSet(assignments)
                            }
                        };
                        Ok(spec::MergeClause::NotMatchedBySource(
                            spec::MergeNotMatchedBySourceClause { condition, action },
                        ))
                    }
                    MergeMatchClause::NotMatchedByTarget {
                        condition, action, ..
                    } => {
                        let condition = from_ast_merge_optional_condition(condition)?;
                        let action = match action {
                            MergeNotMatchedByTargetAction::InsertAll(_, _) => {
                                spec::MergeNotMatchedByTargetAction::InsertAll
                            }
                            MergeNotMatchedByTargetAction::Insert {
                                columns,
                                expressions,
                                ..
                            } => {
                                let columns = columns
                                    .into_items()
                                    .map(from_ast_object_name)
                                    .collect::<SqlResult<Vec<_>>>()?;
                                let mut values = expressions
                                    .into_items()
                                    .map(from_ast_expression)
                                    .collect::<SqlResult<Vec<_>>>()?;
                                if values.len() == 1 {
                                    let expr = values.pop().ok_or_else(|| {
                                        SqlError::invalid(
                                            "INSERT action must include at least one expression",
                                        )
                                    })?;
                                    if let spec::Expr::UnresolvedFunction(func) = expr {
                                        if func.function_name == spec::ObjectName::bare("struct")
                                            && func.named_arguments.is_empty()
                                        {
                                            values = func.arguments;
                                        } else {
                                            values = vec![spec::Expr::UnresolvedFunction(func)];
                                        }
                                    } else {
                                        values = vec![expr];
                                    }
                                }
                                if columns.len() != values.len() {
                                    return Err(SqlError::invalid(format!(
                                        "INSERT action has {} columns but {} expressions",
                                        columns.len(),
                                        values.len()
                                    )));
                                }
                                let values = values
                                    .into_iter()
                                    .map(expr_with_default_column_values)
                                    .collect();
                                spec::MergeNotMatchedByTargetAction::InsertColumns {
                                    columns,
                                    values,
                                }
                            }
                        };
                        Ok(spec::MergeClause::NotMatchedByTarget(
                            spec::MergeNotMatchedByTargetClause { condition, action },
                        ))
                    }
                })
                .collect::<SqlResult<Vec<_>>>()?;

            let on_condition_source = on_expr.text();
            let on_condition = spec::ExprWithSource {
                expr: from_ast_expression(on_expr)?,
                source: Some(on_condition_source),
            };
            let node = spec::CommandNode::MergeInto(spec::MergeInto {
                target: from_ast_object_name(target)?,
                target_alias,
                source,
                on_condition,
                clauses,
                with_schema_evolution: with_schema_evolution.is_some(),
            });
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::Update {
            update: _,
            name,
            alias,
            set: SetClause {
                set: _,
                assignments,
            },
            r#where,
        } => {
            let table_alias = alias
                .map(|x| {
                    let UpdateTableAlias {
                        r#as: _,
                        table,
                        columns,
                    } = x;
                    if columns.is_some() {
                        return Err(SqlError::invalid(
                            "column list must not appear in table alias for UPDATE",
                        ));
                    }
                    Ok(table.value.into())
                })
                .transpose()?;
            let assignments = match assignments {
                AssignmentList::Delimited {
                    left: _,
                    assignments,
                    right: _,
                } => assignments,
                AssignmentList::NotDelimited { assignments } => assignments,
            };
            let assignments = assignments
                .into_items()
                .map(|x| {
                    let Assignment {
                        target,
                        equals: _,
                        value,
                    } = x;
                    Ok((
                        from_ast_object_name(target)?,
                        expr_with_default_column_values(from_ast_expression(value)?),
                    ))
                })
                .collect::<SqlResult<_>>()?;
            let condition = r#where
                .map(|x| {
                    let WhereClause {
                        r#where: _,
                        condition,
                    } = x;
                    let source = condition.text();
                    Ok::<_, SqlError>(spec::ExprWithSource {
                        expr: expr_with_default_column_values(from_ast_expression(condition)?),
                        source: Some(source),
                    })
                })
                .transpose()?;
            let node = spec::CommandNode::Update {
                table: from_ast_object_name(name)?,
                table_alias,
                assignments,
                condition,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::Delete {
            delete: _,
            from: _,
            name,
            alias,
            r#where,
        } => {
            let table_alias = alias
                .map(|x| {
                    let DeleteTableAlias {
                        r#as: _,
                        table,
                        columns,
                    } = x;
                    if columns.is_some() {
                        return Err(SqlError::invalid(
                            "column list must not appear in table alias for DELETE",
                        ));
                    }
                    Ok(table.value.into())
                })
                .transpose()?;
            let condition = r#where
                .map(|x| {
                    let WhereClause {
                        r#where: _,
                        condition,
                    } = x;
                    let source = condition.text();
                    Ok::<_, SqlError>(spec::ExprWithSource {
                        expr: from_ast_expression(condition)?,
                        source: Some(source),
                    })
                })
                .transpose()?;
            let node = spec::CommandNode::Delete {
                table: from_ast_object_name(name)?,
                table_alias,
                condition,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::LoadData {
            load_data: _,
            local,
            path: (_, path),
            overwrite,
            into_table: _,
            name,
            partition,
        } => {
            let partition = partition
                .map(from_ast_partition)
                .transpose()?
                .unwrap_or_default();
            let node = spec::CommandNode::LoadData {
                local: local.is_some(),
                location: from_ast_string(path)?,
                table: from_ast_object_name(name)?,
                overwrite: overwrite.is_some(),
                partition,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CacheTable {
            cache: _,
            lazy,
            table: _,
            name,
            options,
            r#as,
        } => {
            let storage_level = options
                .map(|x| {
                    let (_, properties) = x;
                    let properties = from_ast_property_list(properties)?;
                    let mut output = None;
                    for (key, value) in properties {
                        if key.eq_ignore_ascii_case("storageLevel") {
                            if output.replace(value).is_some() {
                                return Err(SqlError::invalid("duplicate 'storageLevel' option"));
                            }
                        } else {
                            return Err(SqlError::invalid(format!("unknown option: {key}")));
                        }
                    }
                    Ok(output)
                })
                .transpose()?
                .flatten()
                .map(|x| x.parse())
                .transpose()?;
            let query = r#as
                .map(|x| {
                    let AsQueryClause { r#as: _, query } = x;
                    from_ast_query(query)
                })
                .transpose()?
                .map(Box::new);
            let node = spec::CommandNode::CacheTable {
                table: from_ast_object_name(name)?,
                lazy: lazy.is_some(),
                storage_level,
                query,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::UncacheTable {
            uncache: _,
            table: _,
            if_exists,
            name,
        } => {
            let node = spec::CommandNode::UncacheTable {
                table: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ClearCache { clear: _, cache: _ } => {
            let node = spec::CommandNode::ClearCache;
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::SetTimeZone { set: _, timezone } => {
            let node = spec::CommandNode::SetVariable {
                variable: SESSION_TIME_ZONE_KEY.to_string(),
                value: from_ast_time_zone(timezone)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::SetProperty { set: _, property } => {
            let Some(property) = property else {
                return Err(SqlError::todo("list all properties"));
            };
            let (variable, value) = from_ast_set_property(property)?;
            let Some(value) = value else {
                return Err(SqlError::todo("show property"));
            };
            let node = spec::CommandNode::SetVariable { variable, value };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ResetProperty {
            reset: _,
            key,
            rest,
        } => {
            // Spark takes the key as it is written, so a string literal is not a key and the parts of a
            // dotted key are not separated by spaces.
            let key_is_plain = key.as_ref().is_none_or(is_plain_config_key);
            let malformed = !rest.is_empty() || matches!(key, Some(PropertyKey::Literal(_)));
            let variable = key.map(from_ast_property_key).transpose()?;
            if malformed || (!key_is_plain && variable.as_deref() == Some(SESSION_TIME_ZONE_KEY)) {
                return Err(SqlError::invalid(INVALID_RESET_COMMAND_FORMAT));
            }
            let node = spec::CommandNode::ResetVariable { variable };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AnalyzeTable {
            analyze: _,
            name,
            partition,
            compute: _,
            modifier,
        } => {
            let partition = partition
                .map(from_ast_partition)
                .transpose()?
                .unwrap_or_default();
            let (columns, no_scan) = match modifier {
                Some(AnalyzeTableModifier::NoScan(_)) => (vec![], true),
                Some(AnalyzeTableModifier::ForAllColumns(_, _, _)) => (vec![], false),
                Some(AnalyzeTableModifier::ForColumns(_, _, x)) => {
                    let columns = x
                        .into_items()
                        .map(from_ast_object_name)
                        .collect::<SqlResult<_>>()?;
                    (columns, false)
                }
                None => (vec![], false),
            };
            let node = spec::CommandNode::AnalyzeTable {
                table: from_ast_object_name(name)?,
                partition,
                columns,
                no_scan,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AnalyzeTables {
            analyze: _,
            from,
            compute: _,
            no_scan,
        } => {
            let from = from.map(|(_, x)| from_ast_object_name(x)).transpose()?;
            let node = spec::CommandNode::AnalyzeTables {
                from,
                no_scan: no_scan.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::Describe { describe: _, item } => {
            let node = match item {
                DescribeItem::Query { query: _, item } => {
                    let query = from_ast_query(item)?;
                    spec::CommandNode::DescribeQuery {
                        query: Box::new(query),
                    }
                }
                DescribeItem::Function {
                    function: _,
                    extended,
                    item,
                } => spec::CommandNode::DescribeFunction {
                    function: from_ast_describe_function_name(item)?,
                    extended: extended.is_some(),
                },
                DescribeItem::Catalog {
                    catalog: _,
                    extended,
                    item,
                } => spec::CommandNode::DescribeCatalog {
                    catalog: from_ast_object_name(item)?,
                    extended: extended.is_some(),
                },
                DescribeItem::Database {
                    database: _,
                    extended,
                    item,
                } => spec::CommandNode::DescribeDatabase {
                    database: from_ast_object_name(item)?,
                    extended: extended.is_some(),
                },
                DescribeItem::Table {
                    table: _,
                    extended,
                    name,
                    partition,
                    column,
                } => {
                    let partition = partition
                        .map(from_ast_partition)
                        .transpose()?
                        .unwrap_or_default();
                    let column = column.map(from_ast_object_name).transpose()?;
                    spec::CommandNode::DescribeTable {
                        table: from_ast_object_name(name)?,
                        extended: extended.is_some(),
                        partition,
                        column,
                    }
                }
                DescribeItem::TableExtended {
                    extended: _,
                    name,
                    partition,
                    column,
                } => {
                    let partition = partition
                        .map(from_ast_partition)
                        .transpose()?
                        .unwrap_or_default();
                    let column = column.map(from_ast_object_name).transpose()?;
                    spec::CommandNode::DescribeTable {
                        table: from_ast_object_name(name)?,
                        extended: true,
                        partition,
                        column,
                    }
                }
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CommentOnCatalog {
            comment: _,
            name,
            is: _,
            value,
        } => {
            let node = spec::CommandNode::CommentOnCatalog {
                catalog: from_ast_object_name(name)?,
                value: from_ast_comment_value(value)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CommentOnDatabase {
            comment: _,
            name,
            is: _,
            value,
        } => {
            let node = spec::CommandNode::CommentOnDatabase {
                database: from_ast_object_name(name)?,
                value: from_ast_comment_value(value)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CommentOnTable {
            comment: _,
            name,
            is: _,
            value,
        } => {
            let node = spec::CommandNode::CommentOnTable {
                table: from_ast_object_name(name)?,
                value: from_ast_comment_value(value)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CommentOnColumn {
            comment: _,
            name,
            is: _,
            value,
        } => {
            let node = spec::CommandNode::CommentOnColumn {
                column: from_ast_object_name(name)?,
                value: from_ast_comment_value(value)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
    }
}

struct TableDefinition {
    external: bool,
    replace: bool,
    or_replace: bool,
    if_not_exists: bool,
    using: Option<Ident>,
    columns: Option<ColumnDefinitionList>,
    clauses: CreateTableClauses,
    query: Option<AsQueryClause>,
}

fn from_ast_table_definition(
    definition: TableDefinition,
) -> SqlResult<(spec::TableDefinition, Option<Box<QueryPlan>>)> {
    let TableDefinition {
        external,
        replace,
        or_replace,
        if_not_exists,
        using,
        columns,
        clauses:
            CreateTableClauses {
                partition_by,
                bucket_by,
                cluster_by,
                row_format,
                stored_as,
                location,
                comment,
                options,
                properties,
            },
        query,
    } = definition;
    let mode = if replace {
        spec::CreateTableMode::Replace
    } else if or_replace && if_not_exists {
        return Err(SqlError::invalid(
            "CREATE OR REPLACE TABLE cannot be used with IF NOT EXISTS",
        ));
    } else if or_replace {
        spec::CreateTableMode::CreateOrReplace
    } else if if_not_exists {
        spec::CreateTableMode::CreateIfNotExists
    } else {
        spec::CreateTableMode::Create
    };
    let row_format = row_format.map(from_ast_row_format).transpose()?;
    let file_format = match (using, stored_as) {
        (Some(using), None) => Some(spec::TableFileFormat::General {
            format: using.value,
        }),
        (None, Some(stored_as)) => Some(from_ast_file_format(stored_as)?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(SqlError::invalid("conflicting USING and STORED AS clauses"));
        }
    };
    let partition_by = partition_by
        .into_iter()
        .flatten()
        .map(|x| match x {
            PartitionByItem::ColumnDefinition(ColumnTypeDefinition {
                name,
                data_type,
                not_null,
                comment,
                colon: _,
            }) => {
                let name = name.value;
                let data_type = from_ast_data_type(data_type)?;
                let comment = comment.map(|(_, s)| from_ast_string(s)).transpose()?;
                Ok(spec::PartitionColumn::Definition(
                    spec::TableColumnDefinition {
                        name,
                        data_type,
                        nullable: not_null.is_none(),
                        default: None,
                        comment,
                        generated_always_as: None,
                        identity: None,
                    },
                ))
            }
            PartitionByItem::Expression(expr) => {
                from_ast_expression(expr).map(spec::PartitionColumn::Expression)
            }
        })
        .collect::<SqlResult<Vec<_>>>()?;
    let (sort_by, bucket_by) = if let Some(bucket_by) = bucket_by {
        let CreateTableBucketBy {
            columns,
            sort_columns,
            buckets,
        } = bucket_by;
        let bucket_column_names = columns.into_iter().map(|x| x.value.into()).collect();
        let sort_columns = sort_columns
            .into_iter()
            .flatten()
            .map(from_ast_sort_column)
            .collect::<SqlResult<Vec<_>>>()?;
        let num_buckets = buckets
            .value
            .try_into()
            .map_err(|e| SqlError::invalid(format!("invalid number of buckets: {e}")))?;
        (
            sort_columns,
            Some(spec::SaveBucketBy {
                bucket_column_names,
                num_buckets,
            }),
        )
    } else {
        (vec![], None)
    };
    let cluster_by = cluster_by
        .into_iter()
        .flatten()
        .map(from_ast_object_name)
        .collect::<SqlResult<Vec<_>>>()?;
    let options = options.map(from_ast_property_list).transpose()?;
    let properties = properties.map(from_ast_property_list).transpose()?;
    let columns = from_ast_table_columns(columns)?;
    let location = location.map(from_ast_string).transpose()?;
    let options = options.into_iter().flatten().collect();
    let definition = spec::TableDefinition {
        external,
        columns,
        comment: comment.map(from_ast_string).transpose()?,
        constraints: vec![],
        location,
        file_format,
        row_format,
        partition_by,
        sort_by,
        bucket_by,
        cluster_by,
        mode,
        options,
        properties: properties.into_iter().flatten().collect(),
    };
    let query = query
        .map(|AsQueryClause { r#as: _, query }| from_ast_query(query).map(Box::new))
        .transpose()?;
    Ok((definition, query))
}

fn from_ast_table_columns(
    columns: Option<ColumnDefinitionList>,
) -> SqlResult<Vec<spec::TableColumnDefinition>> {
    let columns = columns.map(
        |ColumnDefinitionList {
             left: _,
             columns,
             right: _,
         }| columns,
    );
    let mut output = Vec::with_capacity(
        columns
            .as_ref()
            .map(|x| 1 + x.tail.len())
            .unwrap_or_default(),
    );
    for column in columns.map(|x| x.into_items()).into_iter().flatten() {
        let ColumnDefinition {
            name,
            data_type,
            options,
        } = column;
        let ColumnDefinitionOptions {
            not_null,
            default,
            generated_always_as,
            identity,
            comment,
        } = options.try_into()?;
        let comment = comment.map(from_ast_string).transpose()?;
        let default = default.map(|expr| expr.text().trim().to_string());
        let generated_always_as = generated_always_as.map(|expr| expr.text().trim().to_string());
        let identity = identity
            .map(|(options, allow_explicit_insert)| {
                from_ast_identity_column(options, allow_explicit_insert)
            })
            .transpose()?;
        let column = spec::TableColumnDefinition {
            name: name.value,
            data_type: from_ast_data_type(data_type)?,
            nullable: !not_null,
            default,
            generated_always_as,
            identity,
            comment,
        };
        output.push(column);
    }
    Ok(output)
}

fn from_ast_view_columns(columns: Vec<ViewColumn>) -> SqlResult<Vec<spec::ViewColumnDefinition>> {
    columns
        .into_iter()
        .map(|column| {
            let ViewColumn {
                name,
                data_type,
                not_null,
                comment,
            } = column;
            if data_type.is_some() || not_null.is_some() {
                return Err(SqlError::invalid(
                    "a typed column list can only be used in CREATE TEMPORARY VIEW ... USING",
                ));
            }
            let comment = comment.map(|(_, s)| from_ast_string(s)).transpose()?;
            Ok(spec::ViewColumnDefinition {
                name: name.value,
                comment,
            })
        })
        .collect::<SqlResult<Vec<_>>>()
}

fn from_ast_view_using_columns(
    columns: Vec<ViewColumn>,
) -> SqlResult<(spec::Schema, Vec<spec::ViewColumnDefinition>)> {
    let columns = columns
        .into_iter()
        .map(|column| {
            let ViewColumn {
                name,
                data_type,
                not_null,
                comment,
            } = column;
            let Some(data_type) = data_type else {
                return Err(SqlError::invalid(
                    "expected a data type for each column in CREATE TEMPORARY VIEW ... USING",
                ));
            };
            let comment = comment
                .map(|(_, comment)| from_ast_string(comment))
                .transpose()?;
            let mut metadata = vec![];
            if let Some(comment) = comment.clone() {
                metadata.push(("comment".to_string(), comment));
            }
            let name = name.value;
            Ok((
                spec::Field {
                    name: name.clone(),
                    data_type: from_ast_data_type(data_type)?,
                    nullable: not_null.is_none(),
                    metadata,
                },
                spec::ViewColumnDefinition { name, comment },
            ))
        })
        .collect::<SqlResult<Vec<_>>>()?;
    let (fields, columns): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    Ok((
        spec::Schema {
            fields: fields.into(),
        },
        columns,
    ))
}

fn from_ast_row_format(format: RowFormat) -> SqlResult<spec::TableRowFormat> {
    match format {
        RowFormat::Serde {
            serde: _,
            name,
            properties,
        } => {
            let properties = properties
                .map(|(_, _, x)| from_ast_property_list(x))
                .transpose()?
                .unwrap_or_default();
            Ok(spec::TableRowFormat::Serde {
                name: from_ast_string(name)?,
                properties,
            })
        }
        RowFormat::Delimited {
            delimited: _,
            clauses,
        } => {
            let RowFormatDelimitedClauses {
                fields_terminated_by_escaped_by,
                collection_items_terminated_by,
                map_keys_terminated_by,
                lines_terminated_by,
                null_defined_as,
            } = clauses.try_into()?;
            let (fields_terminated_by, fields_escaped_by) = fields_terminated_by_escaped_by
                .map(|(t, e)| -> SqlResult<_> {
                    Ok((
                        Some(from_ast_string(t)?),
                        e.map(from_ast_string).transpose()?,
                    ))
                })
                .transpose()?
                .unwrap_or((None, None));
            let collection_items_terminated_by = collection_items_terminated_by
                .map(from_ast_string)
                .transpose()?;
            let map_keys_terminated_by = map_keys_terminated_by.map(from_ast_string).transpose()?;
            let lines_terminated_by = lines_terminated_by.map(from_ast_string).transpose()?;
            let null_defined_as = null_defined_as.map(from_ast_string).transpose()?;
            Ok(spec::TableRowFormat::Delimited {
                fields_terminated_by,
                fields_escaped_by,
                collection_items_terminated_by,
                map_keys_terminated_by,
                lines_terminated_by,
                null_defined_as,
            })
        }
    }
}

fn from_ast_file_format(format: FileFormat) -> SqlResult<spec::TableFileFormat> {
    match format {
        FileFormat::Table(_, input, _, output) => Ok(spec::TableFileFormat::Table {
            input_format: from_ast_string(input)?,
            output_format: from_ast_string(output)?,
        }),
        FileFormat::General(x) => Ok(spec::TableFileFormat::General { format: x.value }),
    }
}

#[derive(Default)]
struct RowFormatDelimitedClauses {
    fields_terminated_by_escaped_by: Option<(StringLiteral, Option<StringLiteral>)>,
    collection_items_terminated_by: Option<StringLiteral>,
    map_keys_terminated_by: Option<StringLiteral>,
    lines_terminated_by: Option<StringLiteral>,
    null_defined_as: Option<StringLiteral>,
}

impl TryFrom<Vec<RowFormatDelimitedClause>> for RowFormatDelimitedClauses {
    type Error = SqlError;

    fn try_from(value: Vec<RowFormatDelimitedClause>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for clause in value {
            match clause {
                RowFormatDelimitedClause::Fields(_, _, _, terminate, escape) => {
                    let escape = escape.map(|(_, _, x)| x);
                    if output
                        .fields_terminated_by_escaped_by
                        .replace((terminate, escape))
                        .is_some()
                    {
                        return Err(SqlError::invalid("duplicate FIELDS TERMINATED BY clause"));
                    }
                }
                RowFormatDelimitedClause::CollectionItems(_, _, _, _, x) => {
                    if output.collection_items_terminated_by.replace(x).is_some() {
                        return Err(SqlError::invalid(
                            "duplicate COLLECTION ITEMS TERMINATED BY clause",
                        ));
                    }
                }
                RowFormatDelimitedClause::MapKeys(_, _, _, _, x) => {
                    if output.map_keys_terminated_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate MAP KEYS TERMINATED BY clause"));
                    }
                }
                RowFormatDelimitedClause::Lines(_, _, _, x) => {
                    if output.lines_terminated_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate LINES TERMINATED BY clause"));
                    }
                }
                RowFormatDelimitedClause::Null(_, _, _, x) => {
                    if output.null_defined_as.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate NULL DEFINED AS clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}

#[derive(Default)]
struct ColumnDefinitionOptions {
    not_null: bool,
    default: Option<Expr>,
    generated_always_as: Option<Expr>,
    identity: Option<(Option<TableColumnIdentityOptions>, bool)>,
    comment: Option<StringLiteral>,
}

impl TryFrom<Vec<ColumnDefinitionOption>> for ColumnDefinitionOptions {
    type Error = SqlError;

    fn try_from(value: Vec<ColumnDefinitionOption>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for option in value {
            match option {
                ColumnDefinitionOption::NotNull(_, _) => {
                    if output.not_null {
                        return Err(SqlError::invalid("duplicate NOT NULL clause"));
                    }
                    output.not_null = true;
                }
                ColumnDefinitionOption::Default(_, x) => {
                    if output.default.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate DEFAULT clause"));
                    }
                }
                ColumnDefinitionOption::Generated(_, _, _, _, x, _) => {
                    if output.generated_always_as.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate GENERATED clause"));
                    }
                }
                ColumnDefinitionOption::GeneratedAlwaysIdentity(_, _, _, _, options) => {
                    if output.identity.replace((options, false)).is_some() {
                        return Err(SqlError::invalid("duplicate GENERATED clause"));
                    }
                }
                ColumnDefinitionOption::GeneratedByDefaultIdentity(_, _, _, _, _, options) => {
                    if output.identity.replace((options, true)).is_some() {
                        return Err(SqlError::invalid("duplicate GENERATED clause"));
                    }
                }
                ColumnDefinitionOption::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
            }
        }
        if output.generated_always_as.is_some() && output.identity.is_some() {
            return Err(SqlError::invalid(
                "a column cannot have both a generation expression and identity",
            ));
        }
        Ok(output)
    }
}

fn from_ast_identity_column(
    options: Option<TableColumnIdentityOptions>,
    allow_explicit_insert: bool,
) -> SqlResult<spec::TableColumnIdentity> {
    let mut start = None;
    let mut step = None;
    if let Some(TableColumnIdentityOptions {
        left: _,
        options,
        right: _,
    }) = options
    {
        for option in options {
            match option {
                TableColumnIdentityOption::StartWith(_, _, sign, value) => {
                    if start
                        .replace(from_ast_identity_i64(sign, value, "START WITH")?)
                        .is_some()
                    {
                        return Err(SqlError::invalid(
                            "duplicate START WITH clause for identity column",
                        ));
                    }
                }
                TableColumnIdentityOption::IncrementBy(_, _, sign, value) => {
                    if step
                        .replace(from_ast_identity_i64(sign, value, "INCREMENT BY")?)
                        .is_some()
                    {
                        return Err(SqlError::invalid(
                            "duplicate INCREMENT BY clause for identity column",
                        ));
                    }
                }
            }
        }
    }
    Ok(spec::TableColumnIdentity {
        start,
        step,
        allow_explicit_insert,
    })
}

fn from_ast_identity_i64(
    sign: Option<Either<Plus, Minus>>,
    value: NumberLiteral,
    clause: &str,
) -> SqlResult<i64> {
    let raw = value.value.as_str();
    let unsigned = raw.parse::<i128>().map_err(|_| {
        SqlError::invalid(format!(
            "{clause} value for identity column must be an integer literal"
        ))
    })?;
    let signed = match sign {
        Some(Either::Right(_)) => -unsigned,
        _ => unsigned,
    };
    i64::try_from(signed).map_err(|_| {
        SqlError::invalid(format!(
            "{clause} value for identity column is outside the BIGINT range"
        ))
    })
}

#[derive(Default)]
struct CreateDatabaseClauses {
    comment: Option<StringLiteral>,
    location: Option<StringLiteral>,
    properties: Option<PropertyList>,
}

impl TryFrom<Vec<CreateDatabaseClause>> for CreateDatabaseClauses {
    type Error = SqlError;

    fn try_from(value: Vec<CreateDatabaseClause>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for clause in value {
            match clause {
                CreateDatabaseClause::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
                CreateDatabaseClause::Location(_, x) => {
                    if output.location.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate LOCATION clause"));
                    }
                }
                CreateDatabaseClause::Properties(_, _, properties) => {
                    if output.properties.replace(properties).is_some() {
                        return Err(SqlError::invalid(
                            "duplicate PROPERTIES or DBPROPERTIES clause",
                        ));
                    }
                }
            }
        }
        Ok(output)
    }
}

struct CreateTableBucketBy {
    columns: Vec<Ident>,
    sort_columns: Option<Vec<SortColumn>>,
    buckets: IntegerLiteral,
}

#[derive(Default)]
struct CreateTableClauses {
    partition_by: Option<Vec<PartitionByItem>>,
    bucket_by: Option<CreateTableBucketBy>,
    cluster_by: Option<Vec<ObjectName>>,
    row_format: Option<RowFormat>,
    stored_as: Option<FileFormat>,
    location: Option<StringLiteral>,
    comment: Option<StringLiteral>,
    options: Option<PropertyList>,
    properties: Option<PropertyList>,
}

impl TryFrom<Vec<CreateTableClause>> for CreateTableClauses {
    type Error = SqlError;

    fn try_from(value: Vec<CreateTableClause>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for clause in value {
            match clause {
                CreateTableClause::PartitionedBy(
                    _,
                    _,
                    PartitionByList {
                        left: _,
                        columns,
                        right: _,
                    },
                ) => {
                    if output
                        .partition_by
                        .replace(columns.into_items().collect())
                        .is_some()
                    {
                        return Err(SqlError::invalid("duplicate PARTITIONED BY clause"));
                    }
                }
                CreateTableClause::ClusteredBy(
                    _,
                    _,
                    IdentList {
                        left: _,
                        names,
                        right: _,
                    },
                    sort,
                    _,
                    n,
                    _,
                ) => {
                    let bucket_by = CreateTableBucketBy {
                        columns: names.into_items().collect(),
                        sort_columns: sort.map(
                            |SortColumnClause {
                                 sorted: _,
                                 by: _,
                                 columns:
                                     SortColumnList {
                                         left: _,
                                         columns,
                                         right: _,
                                     },
                             }| columns.into_items().collect(),
                        ),
                        buckets: n,
                    };
                    if output.bucket_by.replace(bucket_by).is_some() {
                        return Err(SqlError::invalid("duplicate CLUSTERED BY clause"));
                    }
                }
                CreateTableClause::ClusterBy(_, _, _, cluster_by, _) => {
                    let cluster_by = cluster_by.into_items().collect();
                    if output.cluster_by.replace(cluster_by).is_some() {
                        return Err(SqlError::invalid("duplicate CLUSTER BY clause"));
                    }
                }
                CreateTableClause::RowFormat(_, _, x) => {
                    if output.row_format.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate ROW FORMAT clause"));
                    }
                }
                CreateTableClause::StoredAs(_, _, x) => {
                    if output.stored_as.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate STORED AS clause"));
                    }
                }
                CreateTableClause::Location(_, x) => {
                    if output.location.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate LOCATION clause"));
                    }
                }
                CreateTableClause::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
                CreateTableClause::Options(_, options) => {
                    if output.options.replace(options).is_some() {
                        return Err(SqlError::invalid("duplicate OPTIONS clause"));
                    }
                }
                CreateTableClause::Properties(_, properties) => {
                    if output.properties.replace(properties).is_some() {
                        return Err(SqlError::invalid("duplicate TBLPROPERTIES clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}

#[derive(Default)]
struct CreateViewClauses {
    comment: Option<StringLiteral>,
    properties: Option<PropertyList>,
}

impl TryFrom<Vec<CreateViewClause>> for CreateViewClauses {
    type Error = SqlError;

    fn try_from(value: Vec<CreateViewClause>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for clause in value {
            match clause {
                CreateViewClause::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
                CreateViewClause::Properties(_, properties) => {
                    if output.properties.replace(properties).is_some() {
                        return Err(SqlError::invalid("duplicate TBLPROPERTIES clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}

// The messages of these errors are the templates of Spark (`error-conditions.json`), with its SQLSTATE.
const INVALID_SET_SYNTAX: &str = "[INVALID_SET_SYNTAX] Expected format is 'SET', 'SET key', or \
    'SET key=value'. If you want to include special characters in key, or include semicolon in value, \
    please use backquotes, e.g., SET `key`=`value`. SQLSTATE: 42000";

const INVALID_RESET_COMMAND_FORMAT: &str = "[INVALID_RESET_COMMAND_FORMAT] Expected format is \
    'RESET' or 'RESET key'. If you want to include special characters in key, please use quotes, \
    e.g., RESET `key`. SQLSTATE: 42000";

/// The configuration key that `SET TIME ZONE` assigns to.
/// It must equal `SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE` in `sail-spark-connect`.
const SESSION_TIME_ZONE_KEY: &str = "spark.sql.session.timeZone";

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_HOUR: u64 = 3_600 * 1_000_000;
const MICROS_PER_DAY: i64 = 24 * 3_600 * 1_000_000;

/// The largest absolute offset that `SET TIME ZONE INTERVAL ...` accepts.
const MAX_TIME_ZONE_OFFSET_MICROS: u64 = 18 * MICROS_PER_HOUR;

/// `QueryParsingErrors.intervalValueOutOfRangeError`, where `input` is the part of the interval that
/// Spark rejects: its months, its days, its whole hours, or its whole seconds.
fn interval_out_of_range(input: impl std::fmt::Display) -> SqlError {
    SqlError::invalid(format!(
        "[INVALID_INTERVAL_FORMAT.TIMEZONE_INTERVAL_OUT_OF_RANGE] Error parsing '{input}' to \
        interval. Please ensure that the value provided is in a valid format for defining an \
        interval. You can reference the documentation for the correct format. The interval value \
        must be in the range of [-18, +18] hours with second precision. SQLSTATE: 22006"
    ))
}

/// Resolves the argument of `SET TIME ZONE` to the value of the session time zone,
/// following `SparkSqlParser.visitSetTimeZone`.
fn from_ast_time_zone(timezone: TimeZoneValue) -> SqlResult<String> {
    match timezone {
        // `LOCAL` is resolved when the statement is analyzed, like `TimeZone.getDefault`
        // in Spark. The result depends on the host.
        TimeZoneValue::Local(_) => Ok(get_system_timezone()?),
        TimeZoneValue::Literal(x) => from_ast_string(x),
        TimeZoneValue::Interval(_, interval) => {
            let interval = *interval;
            // Spark keeps the months, the days and the microseconds of an interval apart
            // (`SparkSqlParser.scala:379-399`). The days of an interval written with units are the
            // ones of its `DAY` and `WEEK` parts. An interval written as `FROM ... TO ...` splits
            // its whole days from the rest.
            let unit_days = match &interval {
                IntervalExpr::MultiUnit { head, tail } => {
                    Some(multi_unit_interval_days(once(head).chain(tail))?)
                }
                IntervalExpr::Standard { .. } => None,
                // Spark requires a unit, so `INTERVAL '1 hour'` is not a time zone displacement.
                IntervalExpr::Literal(_) => {
                    return Err(SqlError::invalid(
                        "[_LEGACY_ERROR_TEMP_0045] Invalid time zone displacement value.",
                    ));
                }
            };
            let (months, micros) = match from_ast_signed_interval(Signed::Positive(interval))? {
                IntervalValue::YearMonth { months, .. } => (i64::from(months), 0),
                IntervalValue::Microsecond { microseconds, .. } => (0, microseconds),
                IntervalValue::MonthDayNanosecond {
                    months,
                    days,
                    nanoseconds,
                } => (
                    i64::from(months),
                    i64::from(days) * MICROS_PER_DAY + nanoseconds / 1_000,
                ),
            };
            let (days, microseconds) = match unit_days {
                Some(days) => (days, micros),
                None => (micros / MICROS_PER_DAY, micros % MICROS_PER_DAY),
            };
            if months != 0 {
                return Err(interval_out_of_range(months));
            }
            if days != 0 {
                return Err(interval_out_of_range(days));
            }
            if microseconds.unsigned_abs() > MAX_TIME_ZONE_OFFSET_MICROS {
                return Err(interval_out_of_range(
                    microseconds.unsigned_abs() / MICROS_PER_HOUR,
                ));
            }
            if microseconds % MICROS_PER_SECOND != 0 {
                return Err(interval_out_of_range(microseconds / MICROS_PER_SECOND));
            }
            Ok(format_zone_offset(microseconds / MICROS_PER_SECOND))
        }
    }
}

/// Formats an offset like `java.time.ZoneOffset.toString`: `Z`, `+01:00` or `-08:00:30`.
fn format_zone_offset(total_seconds: i64) -> String {
    if total_seconds == 0 {
        return "Z".to_string();
    }
    let sign = if total_seconds < 0 { '-' } else { '+' };
    let abs = total_seconds.abs();
    let (hours, minutes, seconds) = (abs / 3_600, abs / 60 % 60, abs % 60);
    if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

/// Whether a key is written as one word, like Spark requires: a dotted name has no spaces and no quoted part.
/// A key quoted as a whole, or a single name, is fine.
fn is_plain_config_key(key: &PropertyKey) -> bool {
    let PropertyKey::Name(ObjectName(parts)) = key else {
        return true;
    };
    if parts.tail.is_empty() {
        return true;
    }
    let is_unquoted =
        |ident: &Ident| ident.span.end - ident.span.start == ident.value.chars().count();
    let mut end = parts.head.span.end;
    is_unquoted(&parts.head)
        && parts.tail.iter().all(|(period, ident)| {
            let adjacent = period.span.start == end && ident.span.start == period.span.end;
            end = ident.span.end;
            adjacent && is_unquoted(ident)
        })
}

fn from_ast_property_key(key: PropertyKey) -> SqlResult<String> {
    match key {
        PropertyKey::Name(ObjectName(parts)) => Ok(parts
            .into_items()
            .map(|x| x.value)
            .collect::<Vec<_>>()
            .join(".")),
        PropertyKey::Literal(x) => from_ast_string(x),
    }
}

fn from_ast_property_value(value: PropertyValue) -> SqlResult<String> {
    match value {
        PropertyValue::String(x) => from_ast_string(x),
        PropertyValue::Number(
            sign,
            NumberLiteral {
                value,
                suffix,
                span: _,
            },
        ) => {
            let sign = match sign {
                Some(Either::Left(Plus { .. })) => "+",
                Some(Either::Right(Minus { .. })) => "-",
                None => "",
            };
            let suffix = match suffix {
                None => "",
                Some(x) => x.as_str(),
            };
            Ok(format!("{sign}{value}{suffix}"))
        }
        PropertyValue::Boolean(BooleanLiteral::True(_)) => Ok("true".to_string()),
        PropertyValue::Boolean(BooleanLiteral::False(_)) => Ok("false".to_string()),
    }
}

fn from_ast_property(property: PropertyKeyValue) -> SqlResult<(String, Option<String>)> {
    let PropertyKeyValue { key, value } = property;
    let key = from_ast_property_key(key)?;
    let value = value
        .map(|(_, value)| from_ast_property_value(value))
        .transpose()?;
    Ok((key, value))
}

fn from_ast_set_property(property: SetPropertyKeyValue) -> SqlResult<(String, Option<String>)> {
    let SetPropertyKeyValue { key, value } = property;
    let key_is_string = matches!(key, PropertyKey::Literal(_));
    let key_is_plain = is_plain_config_key(&key);
    let key = from_ast_property_key(key)?;
    // Spark takes `SET key=value` with the key written as one word, and the session time zone is the key
    // this statement acts on. Every other key keeps its previous grammar.
    let malformed_zone_key = key == SESSION_TIME_ZONE_KEY && !key_is_plain;
    let value = value
        .map(|(equals, value)| match value {
            SetPropertyValue::Property(_) | SetPropertyValue::Unquoted(_) if malformed_zone_key => {
                Err(SqlError::invalid(INVALID_SET_SYNTAX))
            }
            SetPropertyValue::Property(_)
                if key == SESSION_TIME_ZONE_KEY && (equals.is_none() || key_is_string) =>
            {
                Err(SqlError::invalid(INVALID_SET_SYNTAX))
            }
            // Spark keeps the quotes of the value (`SparkSqlParser.scala:291`), so a quoted session
            // time zone is invalid there rather than the zone within the quotes.
            // TODO: Do this for every key once `SET` on the other keys goes through the Spark
            //   configuration. They are sent to DataFusion as variables, which need the plain value.
            SetPropertyValue::Property(PropertyValue::String(x))
                if key == SESSION_TIME_ZONE_KEY =>
            {
                Ok(x.text().trim().to_string())
            }
            SetPropertyValue::Property(x) => from_ast_property_value(x),
            // `SET TIME ZONE` followed by anything that is not a zone is not a property named `time`.
            SetPropertyValue::Unquoted(ConfigValue { value, span: _ })
                if equals.is_none()
                    && key.eq_ignore_ascii_case("time")
                    && value.eq_ignore_ascii_case("zone") =>
            {
                Err(SqlError::invalid(
                    "[_LEGACY_ERROR_TEMP_0045] Invalid time zone displacement value.",
                ))
            }
            // Spark takes the key as it is written, so only an identifier can be followed by a value
            // that is not quoted.
            SetPropertyValue::Unquoted(_) if equals.is_none() || key_is_string => {
                Err(SqlError::invalid(INVALID_SET_SYNTAX))
            }
            SetPropertyValue::Unquoted(ConfigValue { value, span: _ }) => Ok(value),
        })
        .transpose()?;
    Ok((key, value))
}

fn from_ast_property_list(properties: PropertyList) -> SqlResult<Vec<(String, String)>> {
    let PropertyList {
        left: _,
        properties,
        right: _,
    } = properties;
    properties
        .into_items()
        .map(|x| {
            let (key, value) = from_ast_property(x)?;
            let Some(value) = value else {
                return Err(SqlError::invalid(format!("missing property value: {key}")));
            };
            Ok((key, value))
        })
        .collect::<SqlResult<Vec<_>>>()
}

fn from_ast_property_key_list(properties: PropertyKeyList) -> SqlResult<Vec<String>> {
    let PropertyKeyList {
        left: _,
        properties,
        right: _,
    } = properties;
    properties
        .into_items()
        .map(|key| match key {
            PropertyKey::Name(ObjectName(parts)) => Ok(parts
                .into_items()
                .map(|x| x.value)
                .collect::<Vec<_>>()
                .join(".")),
            PropertyKey::Literal(x) => from_ast_string(x),
        })
        .collect::<SqlResult<Vec<_>>>()
}

fn from_ast_partition(
    partition: PartitionClause,
) -> SqlResult<Vec<(spec::Identifier, Option<spec::Expr>)>> {
    let PartitionClause {
        partition: _,
        values:
            PartitionValueList {
                left: _,
                values,
                right: _,
            },
    } = partition;
    values
        .into_items()
        .map(|x| {
            let PartitionValue { column, value } = x;
            let expr = value.map(|(_, e)| from_ast_expression(e)).transpose()?;
            Ok((column.value.into(), expr))
        })
        .collect::<SqlResult<Vec<_>>>()
}

fn from_ast_sort_column(sort: SortColumn) -> SqlResult<spec::SortOrder> {
    let SortColumn { column, direction } = sort;
    let direction = match direction {
        Some(OrderDirection::Asc(_)) => spec::SortDirection::Ascending,
        Some(OrderDirection::Desc(_)) => spec::SortDirection::Descending,
        None => spec::SortDirection::Unspecified,
    };
    Ok(spec::SortOrder {
        child: Box::new(from_ast_expression(column)?),
        direction,
        null_ordering: spec::NullOrdering::Unspecified,
    })
}

fn from_ast_explain_format(format: Option<ExplainFormat>) -> SqlResult<spec::ExplainMode> {
    // TODO(spark-compat):
    //   - COST: match Spark (logical + stats, not physical-with-stats).
    //   - FORMATTED: add outline + node-details sections to mirror Spark.
    //   - CODEGEN: keep "unsupported" notice until DataFusion adds support.
    //   - ANALYZE: align metrics formatting with Spark once available.
    //   Reference: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
    match format {
        None => Ok(spec::ExplainMode::Simple),
        Some(ExplainFormat::Extended(_)) => Ok(spec::ExplainMode::Extended),
        Some(ExplainFormat::Codegen(_)) => Ok(spec::ExplainMode::Codegen),
        Some(ExplainFormat::Cost(_)) => Ok(spec::ExplainMode::Cost),
        Some(ExplainFormat::Formatted(_)) => Ok(spec::ExplainMode::Formatted),
        Some(ExplainFormat::Analyze(_)) => Ok(spec::ExplainMode::Analyze),
        Some(ExplainFormat::Verbose(_)) => Ok(spec::ExplainMode::Verbose),
    }
}

fn from_ast_comment_value(value: CommentValue) -> SqlResult<Option<String>> {
    match value {
        CommentValue::NotNull(x) => Ok(Some(from_ast_string(x)?)),
        CommentValue::Null(_) => Ok(None),
    }
}

#[derive(Default)]
struct ColumnAlterationOptions {
    not_null: bool,
    default: Option<Expr>,
    comment: Option<StringLiteral>,
    position: Option<ColumnPosition>,
}

impl TryFrom<Vec<ColumnAlterationOption>> for ColumnAlterationOptions {
    type Error = SqlError;

    fn try_from(value: Vec<ColumnAlterationOption>) -> Result<Self, Self::Error> {
        let mut output = Self::default();
        for option in value {
            match option {
                ColumnAlterationOption::NotNull(_, _) => {
                    if output.not_null {
                        return Err(SqlError::invalid("duplicate NOT NULL clause"));
                    }
                    output.not_null = true;
                }
                ColumnAlterationOption::Default(_, x) => {
                    if output.default.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate DEFAULT clause"));
                    }
                }
                ColumnAlterationOption::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
                ColumnAlterationOption::Position(x) => {
                    if output.position.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate POSITION clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}

// TODO: implement the conversion properly for column-level ALTER TABLE operations
fn from_ast_column_alteration_list(items: ColumnAlterationList) -> SqlResult<()> {
    // TODO: implement the conversion properly
    let columns = match items {
        ColumnAlterationList::Delimited {
            left: _,
            columns,
            right: _,
        } => columns,
        ColumnAlterationList::NotDelimited { columns } => columns,
    };
    let _ = columns
        .into_items()
        .map(|x| {
            let ColumnAlteration {
                name: _,
                data_type: _,
                options,
            } = x;
            let _: ColumnAlterationOptions = options.try_into()?;
            Ok(())
        })
        .collect::<SqlResult<Vec<_>>>()?;
    Ok(())
}

fn from_ast_merge_optional_condition<T>(
    condition: Option<(T, Expr)>,
) -> SqlResult<Option<spec::ExprWithSource>> {
    condition
        .map(|(_, expr)| {
            let source = expr.text();
            let expr = from_ast_expression(expr)?;
            Ok(spec::ExprWithSource {
                expr,
                source: Some(source),
            })
        })
        .transpose()
}

fn from_ast_merge_assignment_list(
    assignments: AssignmentList,
) -> SqlResult<Vec<(spec::ObjectName, spec::Expr)>> {
    let assignments = match assignments {
        AssignmentList::Delimited { assignments, .. } => assignments,
        AssignmentList::NotDelimited { assignments } => assignments,
    };
    assignments
        .into_items()
        .map(|assignment| {
            let Assignment { target, value, .. } = assignment;
            Ok((
                from_ast_object_name(target)?,
                expr_with_default_column_values(from_ast_expression(value)?),
            ))
        })
        .collect()
}

fn from_ast_alter_table_operation(
    operation: AlterTableOperation,
) -> SqlResult<spec::AlterTableOperation> {
    match operation {
        AlterTableOperation::SetTableProperties { properties, .. } => {
            let properties = from_ast_property_list(properties)?;
            Ok(spec::AlterTableOperation::SetTableProperties { properties })
        }
        AlterTableOperation::UnsetTableProperties {
            if_exists,
            properties,
            ..
        } => {
            let keys = from_ast_property_key_list(properties)?;
            Ok(spec::AlterTableOperation::UnsetTableProperties {
                keys,
                if_exists: if_exists.is_some(),
            })
        }
        AlterTableOperation::AlterColumn {
            name,
            operation: AlterColumnOperation::Type(_, data_type),
            ..
        } => Ok(spec::AlterTableOperation::AlterColumnType {
            name: from_ast_object_name(name)?,
            data_type: from_ast_data_type(data_type)?,
        }),
        AlterTableOperation::AlterColumn {
            name,
            operation: AlterColumnOperation::SetDefault(_, _, expr),
            ..
        } => Ok(spec::AlterTableOperation::AlterColumnDefault {
            name: from_ast_object_name(name)?,
            default: Some(expr.text().trim().to_string()),
        }),
        AlterTableOperation::AlterColumn {
            name,
            operation: AlterColumnOperation::DropDefault(_, _),
            ..
        } => Ok(spec::AlterTableOperation::AlterColumnDefault {
            name: from_ast_object_name(name)?,
            default: None,
        }),
        AlterTableOperation::AddConstraint {
            name, expression, ..
        } => {
            let source = expression.text().trim().to_string();
            Ok(spec::AlterTableOperation::AddCheckConstraint {
                name: name.value.into(),
                expression: spec::ExprWithSource {
                    expr: from_ast_expression(expression)?,
                    source: Some(source),
                },
            })
        }
        AlterTableOperation::RenameTable { .. }
        | AlterTableOperation::RenamePartition { .. }
        | AlterTableOperation::DropColumns { .. }
        | AlterTableOperation::RenameColumn { .. }
        | AlterTableOperation::AddPartitions { .. }
        | AlterTableOperation::DropPartition { .. }
        | AlterTableOperation::SetFileFormat { .. }
        | AlterTableOperation::SetLocation { .. }
        | AlterTableOperation::RecoverPartitions { .. } => Ok(spec::AlterTableOperation::Unknown),
        AlterTableOperation::AlterColumn { .. } => Ok(spec::AlterTableOperation::Unknown),
        AlterTableOperation::AddColumns { items, .. }
        | AlterTableOperation::ReplaceColumns { items, .. } => {
            // Validate column descriptors (e.g. detect duplicate COMMENT/DEFAULT/NOT NULL/POSITION
            // clauses) even though we do not yet translate these operations.
            from_ast_column_alteration_list(items)?;
            Ok(spec::AlterTableOperation::Unknown)
        }
    }
}

fn from_ast_alter_view_operation(
    _operation: AlterViewOperation,
) -> SqlResult<spec::AlterViewOperation> {
    Ok(spec::AlterViewOperation::Unknown)
}
