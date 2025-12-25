use either::Either;
use sail_common::spec;
use sail_common::spec::QueryPlan;
use sail_sql_parser::ast::expression::{BooleanLiteral, Expr, OrderDirection};
use sail_sql_parser::ast::identifier::{Ident, ObjectName};
use sail_sql_parser::ast::keywords::{Cascade, Global, Overwrite, Restrict, Temp, Temporary};
use sail_sql_parser::ast::literal::{IntegerLiteral, NumberLiteral, StringLiteral};
use sail_sql_parser::ast::operator::{Minus, Plus};
use sail_sql_parser::ast::query::{IdentList, WhereClause};
use sail_sql_parser::ast::statement::{
    AlterTableOperation, AlterViewOperation, AnalyzeTableModifier, AsQueryClause, Assignment,
    AssignmentList, ColumnAlteration, ColumnAlterationList, ColumnAlterationOption,
    ColumnDefinition, ColumnDefinitionList, ColumnDefinitionOption, ColumnPosition,
    ColumnTypeDefinition, CommentValue, CreateDatabaseClause, CreateTableClause, CreateViewClause,
    DeleteTableAlias, DescribeItem, ExplainFormat, FileFormat, InsertDirectoryDestination,
    MergeMatchClause, MergeMatchedAction, MergeNotMatchedBySourceAction,
    MergeNotMatchedByTargetAction, MergeSource, PartitionClause, PartitionColumn,
    PartitionColumnList, PartitionValue, PartitionValueList, PropertyKey, PropertyKeyValue,
    PropertyList, PropertyValue, RowFormat, RowFormatDelimitedClause, SetClause, SortColumn,
    SortColumnList, Statement, UpdateTableAlias, ViewColumn,
};
use sail_sql_parser::tree::TreeText;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_identifier_list, from_ast_object_name};
use crate::query::from_ast_query;
use crate::value::from_ast_string;

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
                    return Err(SqlError::todo("RESTRICT in DROP DATABASE"))
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
        Statement::CreateTable {
            create: _,
            or_replace,
            temporary: _, // TODO: handle temporary tables
            external: _,  // TODO: handle external tables
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
            table: _,
            name,
            columns,
            using,
            clauses,
            r#as,
        } => {
            let definition = TableDefinition {
                or_replace: true,
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
            let node = spec::CommandNode::ListTables { database, pattern };
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
            global_temporary,
            view: _,
            if_not_exists,
            name,
            columns,
            clauses,
            r#as: _,
            query,
        } => {
            let columns = if let Some((_, columns, _)) = columns {
                Some(
                    columns
                        .into_items()
                        .map(|ViewColumn { name, comment }| {
                            let comment = comment.map(|(_, s)| from_ast_string(s)).transpose()?;
                            Ok(spec::ViewColumnDefinition {
                                name: name.value,
                                comment,
                            })
                        })
                        .collect::<SqlResult<Vec<_>>>()?,
                )
            } else {
                None
            };
            let query = from_ast_query(query)?;
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
            let temporary_view_name = |name: spec::ObjectName| {
                let mut name: Vec<String> = name.into();
                match (name.pop(), name.is_empty()) {
                    (Some(x), true) => Ok(spec::Identifier::from(x)),
                    _ => Err(SqlError::invalid(
                        "expected a single identifier for temporary view name",
                    )),
                }
            };
            let node = match global_temporary {
                Some((
                    Some(Global { .. }),
                    Either::Left(Temp { .. }) | Either::Right(Temporary { .. }),
                )) => spec::CommandNode::CreateTemporaryView {
                    view: temporary_view_name(name)?,
                    is_global: true,
                    definition: spec::TemporaryViewDefinition {
                        input: Box::new(query),
                        columns,
                        if_not_exists: if_not_exists.is_some(),
                        replace: or_replace.is_some(),
                        comment,
                        properties,
                    },
                },
                Some((None, Either::Left(Temp { .. }) | Either::Right(Temporary { .. }))) => {
                    spec::CommandNode::CreateTemporaryView {
                        view: temporary_view_name(name)?,
                        is_global: false,
                        definition: spec::TemporaryViewDefinition {
                            input: Box::new(query),
                            columns,
                            if_not_exists: if_not_exists.is_some(),
                            replace: or_replace.is_some(),
                            comment,
                            properties,
                        },
                    }
                }
                None => spec::CommandNode::CreateView {
                    view: name,
                    definition: spec::ViewDefinition {
                        // TODO: handle view definition
                        definition: "".to_string(),
                        columns,
                        if_not_exists: if_not_exists.is_some(),
                        replace: or_replace.is_some(),
                        comment,
                        properties,
                    },
                },
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
        Statement::ShowFunctions { .. } => Err(SqlError::todo("SHOW FUNCTIONS")),
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
                        .map(|(_, x)| from_ast_property_list(x))
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
                        .map(|(_, _, x)| from_ast_file_format(x))
                        .transpose()?;
                    let row_format = row_format
                        .map(|(_, _, x)| from_ast_row_format(x))
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
            using: (_, source),
            on: (_, on_expr),
            r#match,
        } => {
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
                    Ok((from_ast_object_name(target)?, from_ast_expression(value)?))
                })
                .collect::<SqlResult<_>>()?;
            let condition = r#where
                .map(|x| {
                    let WhereClause {
                        r#where: _,
                        condition,
                    } = x;
                    from_ast_expression(condition)
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
        Statement::SetTimeZone { .. } => Err(SqlError::todo("SET TIME ZONE")),
        Statement::SetProperty { set: _, property } => {
            let Some(property) = property else {
                return Err(SqlError::todo("list all properties"));
            };
            let (variable, value) = from_ast_property(property)?;
            let Some(value) = value else {
                return Err(SqlError::todo("show property"));
            };
            let node = spec::CommandNode::SetVariable { variable, value };
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
                } => {
                    let function = match item {
                        Either::Left(x @ ObjectName { .. }) => from_ast_object_name(x)?,
                        Either::Right(x @ StringLiteral { .. }) => {
                            spec::ObjectName::bare(from_ast_string(x)?)
                        }
                    };
                    spec::CommandNode::DescribeFunction {
                        function,
                        extended: extended.is_some(),
                    }
                }
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
    let row_format = row_format.map(from_ast_row_format).transpose()?;
    let file_format = match (using, stored_as) {
        (Some(using), None) => Some(spec::TableFileFormat::General {
            format: using.value,
        }),
        (None, Some(stored_as)) => Some(from_ast_file_format(stored_as)?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(SqlError::invalid("conflicting USING and STORED AS clauses"))
        }
    };
    let partition_by = partition_by
        .into_iter()
        .flatten()
        .map(|x| match x {
            PartitionColumn::Typed(ColumnTypeDefinition { name, .. }) => name.value.into(),
            PartitionColumn::Name(x) => x.value.into(),
        })
        .collect();
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
    let definition = spec::TableDefinition {
        columns,
        comment: comment.map(from_ast_string).transpose()?,
        constraints: vec![],
        location: location.map(from_ast_string).transpose()?,
        file_format,
        row_format,
        partition_by,
        sort_by,
        bucket_by,
        cluster_by,
        if_not_exists,
        replace: or_replace,
        options: options.into_iter().flatten().collect(),
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
        // TODO: support `default` and `generated_always_as` SQL expression strings
        let ColumnDefinitionOptions {
            not_null,
            default: _,
            generated_always_as: _,
            comment,
        } = options.try_into()?;
        let comment = comment.map(from_ast_string).transpose()?;
        let column = spec::TableColumnDefinition {
            name: name.value,
            data_type: from_ast_data_type(data_type)?,
            nullable: !not_null,
            default: None,
            generated_always_as: None,
            comment,
        };
        output.push(column);
    }
    Ok(output)
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
                ColumnDefinitionOption::Comment(_, x) => {
                    if output.comment.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicate COMMENT clause"));
                    }
                }
            }
        }
        Ok(output)
    }
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
    partition_by: Option<Vec<PartitionColumn>>,
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
                    PartitionColumnList {
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
                            |(
                                _,
                                _,
                                SortColumnList {
                                    left: _,
                                    columns,
                                    right: _,
                                },
                            )| columns.into_items().collect(),
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

fn from_ast_property(property: PropertyKeyValue) -> SqlResult<(String, Option<String>)> {
    let PropertyKeyValue { key, value } = property;
    let key = match key {
        PropertyKey::Name(ObjectName(parts)) => parts
            .into_items()
            .map(|x| x.value)
            .collect::<Vec<_>>()
            .join("."),
        PropertyKey::Literal(x) => from_ast_string(x)?,
    };
    let value = if let Some((_, value)) = value {
        let value = match value {
            PropertyValue::String(x) => from_ast_string(x)?,
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
                format!("{sign}{value}{suffix}")
            }
            PropertyValue::Boolean(BooleanLiteral::True(_)) => "true".to_string(),
            PropertyValue::Boolean(BooleanLiteral::False(_)) => "false".to_string(),
        };
        Some(value)
    } else {
        None
    };
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
        child: Box::new(spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::bare(column.value),
            plan_id: None,
            is_metadata_column: false,
        }),
        direction,
        null_ordering: spec::NullOrdering::Unspecified,
    })
}

fn from_ast_explain_format(format: Option<ExplainFormat>) -> SqlResult<spec::ExplainMode> {
    // TODO(spark-compat):
    //   - EXTENDED: emit Parsed/Analyzed/Optimized Logical Plan sections distinctly.
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
            Ok((from_ast_object_name(target)?, from_ast_expression(value)?))
        })
        .collect()
}

fn from_ast_alter_table_operation(
    operation: AlterTableOperation,
) -> SqlResult<spec::AlterTableOperation> {
    // TODO: implement the conversion properly
    match operation {
        AlterTableOperation::RenameTable { .. } => {}
        AlterTableOperation::RenamePartition { .. } => {}
        AlterTableOperation::AddColumns { items, .. } => {
            from_ast_column_alteration_list(items)?;
        }
        AlterTableOperation::DropColumns { .. } => {}
        AlterTableOperation::RenameColumn { .. } => {}
        AlterTableOperation::AlterColumn { .. } => {}
        AlterTableOperation::ReplaceColumns { items, .. } => {
            from_ast_column_alteration_list(items)?;
        }
        AlterTableOperation::AddPartitions { .. } => {}
        AlterTableOperation::DropPartition { .. } => {}
        AlterTableOperation::SetTableProperties { .. } => {}
        AlterTableOperation::UnsetTableProperties { .. } => {}
        AlterTableOperation::SetFileFormat { .. } => {}
        AlterTableOperation::SetLocation { .. } => {}
        AlterTableOperation::RecoverPartitions { .. } => {}
    }
    Ok(spec::AlterTableOperation::Unknown)
}

fn from_ast_alter_view_operation(
    _operation: AlterViewOperation,
) -> SqlResult<spec::AlterViewOperation> {
    Ok(spec::AlterViewOperation::Unknown)
}

// TODO: add the following test cases as gold tests:
//   `CREATE TABLE foo.1m(a INT)`
//   `CREATE TABLE foo.1m(a INT) USING parquet`
