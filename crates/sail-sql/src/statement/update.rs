use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_ident, from_ast_object_name_normalized};
use crate::operation::filter::query_plan_with_filter;
use crate::operation::join::join_plan_from_tables;

pub(crate) fn update_statement_to_plan(update: ast::Statement) -> SqlResult<spec::Plan> {
    let (table, assignments, from, selection, returning) = match update {
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

    let mut input_tables = vec![table];
    input_tables.extend(from);

    let plan = join_plan_from_tables(input_tables)?;
    let plan = query_plan_with_filter(plan, selection)?;

    let node = spec::CommandNode::Update {
        input: Box::new(plan),
        table: table_name,
        table_alias,
        assignments,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
