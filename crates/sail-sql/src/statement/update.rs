use sail_common::spec;
use sqlparser::ast;
use sqlparser::ast::UpdateTableFromKind;

use crate::error::{SqlError, SqlResult};
use crate::expression::common::{
    from_ast_expression, from_ast_ident, from_ast_object_name_normalized,
};
use crate::query::{from_ast_tables, query_plan_with_filter};

pub(crate) fn update_statement_to_plan(update: ast::Statement) -> SqlResult<spec::Plan> {
    let (table, assignments, from, selection, returning) = match update {
        ast::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            if or.is_some() {
                return Err(SqlError::unsupported("UPDATE OR"));
            }
            (table, assignments, from, selection, returning)
        }
        _ => return Err(SqlError::invalid("Expected an UPDATE statement")),
    };

    if returning.is_some() {
        return Err(SqlError::todo("UPDATE RETURNING not yet supported"));
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

    let assignments: Vec<(spec::ObjectName, spec::Expr)> = assignments
        .into_iter()
        .map(|assignment| {
            let columns = match &assignment.target {
                ast::AssignmentTarget::ColumnName(columns) => columns,
                _ => return Err(SqlError::invalid("Tuples are not supported")),
            };
            Ok((
                from_ast_object_name_normalized(columns)?,
                from_ast_expression(assignment.value)?,
            ))
        })
        .collect::<SqlResult<_>>()?;

    let mut input_tables = vec![table];
    if let Some(from) = from {
        let from = match from {
            UpdateTableFromKind::BeforeSet(x) => x,
            UpdateTableFromKind::AfterSet(x) => x,
        };
        input_tables.push(from);
    };

    let plan = from_ast_tables(input_tables)?;
    let plan = query_plan_with_filter(plan, selection)?;

    let node = spec::CommandNode::Update {
        input: Box::new(plan),
        table: table_name,
        table_alias,
        assignments,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
