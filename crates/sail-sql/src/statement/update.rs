use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_ident, from_ast_object_name_normalized};
use crate::query::from_ast_table_with_joins;

pub(crate) fn update_statement_to_plan(update: ast::Statement) -> SqlResult<spec::Plan> {
    let (table, assignments, from, filter, returning) = match update {
        ast::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        } => (table, assignments, from, selection, returning),
        _ => return Err(SqlError::invalid("Expected an UPDATE statement")),
    };

    if returning.is_some() {
        return Err(SqlError::todo("UPDATE-RETURNING not yet supported"));
    }

    let (table_name, table_alias) = match &table.relation {
        ast::TableFactor::Table { name, alias, .. } => (name, alias),
        _ => return Err(SqlError::invalid("Can only UPDATE table relation")),
    };

    let table_name = from_ast_object_name_normalized(table_name)?;
    let table_alias = table_alias
        .as_ref()
        .map(|alias| {
            if !alias.columns.is_empty() {
                return Err(SqlError::invalid(
                    "Columns aliases are not allowed in UPDATE",
                ));
            }
            from_ast_ident(&alias.name, true)
        })
        .transpose()?;

    let assignments: Vec<(spec::Identifier, spec::Expr)> = assignments
        .into_iter()
        .map(|assignment| {
            let columns = match &assignment.target {
                ast::AssignmentTarget::ColumnName(columns) => columns,
                _ => return Err(SqlError::invalid("Tuples are not supported")),
            };
            let column_name = columns
                .0
                .last()
                .ok_or_else(|| SqlError::invalid("Expected at least one column in assignment"))?;
            Ok((
                from_ast_ident(column_name, true)?,
                from_ast_expression(assignment.value)?,
            ))
        })
        .collect::<SqlResult<_>>()?;

    let mut input_tables = vec![from_ast_table_with_joins(table)?];
    let from = from.map(from_ast_table_with_joins).transpose()?;
    input_tables.extend(from);

    let filter = filter.map(from_ast_expression).transpose()?;

    let node = spec::CommandNode::Update {
        input_tables,
        table: table_name,
        table_alias,
        assignments,
        filter,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
