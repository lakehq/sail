use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::common::{from_ast_expression, from_ast_object_name_normalized};
use crate::query::from_ast_query;
use crate::utils::normalize_ident;

// Spark Syntax reference:
//  https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html
//  https://spark.apache.org/docs/3.5.1/sql-ref-syntax-dml-insert-table.html#content
// TODO: Custom parsing to fully sport Spark's INSERT syntax
pub(crate) fn insert_statement_to_plan(insert: ast::Insert) -> SqlResult<spec::Plan> {
    let ast::Insert {
        or,
        ignore,
        into: _,
        table_name,
        table_alias,
        columns,
        overwrite,
        source,
        partitioned,
        after_columns,
        table: _,
        on,
        returning,
        replace_into,
        priority,
        insert_alias,
    } = insert;

    let Some(source) = source else {
        return Err(SqlError::invalid("INSERT without source is not supported."));
    };
    if or.is_some() {
        return Err(SqlError::invalid(
            "INSERT with `OR` clause is not supported.",
        ));
    }
    if ignore {
        return Err(SqlError::invalid(
            "INSERT `IGNORE` clause is not supported.",
        ));
    }
    if table_alias.is_some() {
        return Err(SqlError::invalid(format!(
            "INSERT with a table alias is not supported: {table_alias:?}.",
        )));
    }
    if on.is_some() {
        return Err(SqlError::invalid("INSERT `ON` clause is not supported."));
    }
    if returning.is_some() {
        return Err(SqlError::invalid(
            "INSERT `RETURNING` clause is not supported.",
        ));
    }
    if replace_into {
        return Err(SqlError::invalid(
            "INSERT with a `REPLACE INTO` clause is not supported.",
        ));
    }
    if priority.is_some() {
        return Err(SqlError::invalid(format!(
            "INSERT with a `PRIORITY` clause is not supported: {priority:?}.",
        )));
    }
    if insert_alias.is_some() {
        return Err(SqlError::invalid("INSERT with an alias is not supported."));
    }

    let table_name = from_ast_object_name_normalized(&table_name)?;
    let columns: Vec<spec::Identifier> = columns
        .iter()
        .map(|x| spec::Identifier::from(normalize_ident(x)))
        .collect();
    let partitioned: Vec<spec::Expr> = match partitioned {
        Some(partitioned_vec) => partitioned_vec
            .into_iter()
            .map(from_ast_expression)
            .collect::<SqlResult<Vec<_>>>()?,
        None => Vec::new(),
    };
    let after_columns: Vec<spec::Identifier> = after_columns
        .iter()
        .map(|x| spec::Identifier::from(normalize_ident(x)))
        .collect();
    let columns = if columns.is_empty() && !after_columns.is_empty() {
        // after_columns and columns are the same. SQLParser just parses this weird.
        after_columns
    } else {
        columns
    };

    let node = spec::CommandNode::InsertInto {
        input: Box::new(from_ast_query(*source)?),
        table: table_name,
        columns,
        partition_spec: partitioned,
        overwrite,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
