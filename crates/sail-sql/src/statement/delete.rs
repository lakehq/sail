use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::common::{from_ast_expression, from_ast_object_name_normalized};

pub(crate) fn delete_statement_to_plan(delete: ast::Delete) -> SqlResult<spec::Plan> {
    let ast::Delete {
        tables,
        from,
        using,
        selection,
        returning,
        order_by,
        limit,
    } = delete;
    if !tables.is_empty() {
        return Err(SqlError::invalid("Multi tables delete not supported"));
    }
    if using.is_some() {
        return Err(SqlError::invalid("USING clause not supported"));
    }
    if returning.is_some() {
        return Err(SqlError::todo("DELETE RETURNING clause not yet supported"));
    }
    if !order_by.is_empty() {
        return Err(SqlError::invalid(
            "DELETE ORDER BY clause not yet supported",
        ));
    }
    if limit.is_some() {
        return Err(SqlError::invalid("DELETE LIMIT clause not yet supported"));
    }

    let mut from = match from {
        ast::FromTable::WithFromKeyword(tables) => tables,
        ast::FromTable::WithoutKeyword(tables) => tables,
    };
    if from.len() != 1 {
        return Err(SqlError::invalid(format!(
            "DELETE FROM only supports single table, got: {from:?}"
        )));
    }

    let table_with_joins = from
        .pop()
        .ok_or_else(|| SqlError::invalid("Missing table in DELETE statement"))?;
    let table_name = match table_with_joins.relation {
        ast::TableFactor::Table { name, alias, .. } => {
            if let Some(alias) = alias {
                if !alias.columns.is_empty() {
                    return Err(SqlError::invalid(
                        "Columns aliases are not allowed in DELETE",
                    ));
                }
            }
            Ok(name)
        }
        _ => Err(SqlError::invalid("DELETE FROM only supports table")),
    }?;

    let node = spec::CommandNode::Delete {
        table: from_ast_object_name_normalized(&table_name)?,
        condition: selection.map(from_ast_expression).transpose()?,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
