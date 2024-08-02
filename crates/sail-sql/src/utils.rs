use std::collections::HashMap;

use sail_common::spec;
use sqlparser::ast;

use crate::data_type::from_ast_data_type;
use crate::error::SqlResult;
use crate::expression::from_ast_expression;

/// Normalize an identifier to a lowercase string if the identifier is not quoted.
///
/// This function is necessary because SQL identifiers can be case-insensitive or case-sensitive
/// depending on whether they are quoted. Unquoted identifiers are typically case-insensitive
/// and should be normalized to lowercase for consistent handling. Quoted identifiers, however,
/// retain their case sensitivity and must be preserved exactly as written.
/// TODO: Make sure this gets called everywhere in sail-sql
///     because sail-plan spec expects the raw identifier.
pub fn normalize_ident(id: ast::Ident) -> String {
    let ast::Ident { value, quote_style } = id;
    match quote_style {
        Some(_) => value,
        None => value.to_ascii_lowercase(),
    }
}

pub fn build_column_defaults(
    columns: &Vec<ast::ColumnDef>,
) -> SqlResult<Vec<(String, spec::Expr)>> {
    let mut column_defaults = vec![];
    for column in columns {
        if let Some(default_sql_expr) = column.options.iter().find_map(|o| match &o.option {
            ast::ColumnOption::Default(expr) => Some(expr),
            _ => None,
        }) {
            let default_expr = from_ast_expression(default_sql_expr.clone())?;
            column_defaults.push((normalize_ident(column.name.clone()), default_expr));
        }
    }
    Ok(column_defaults)
}

pub fn build_schema_from_columns(columns: Vec<ast::ColumnDef>) -> SqlResult<spec::Schema> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        let not_nullable = column
            .options
            .iter()
            .any(|x| x.option == ast::ColumnOption::NotNull);
        let field = spec::Field {
            name: normalize_ident(column.name),
            data_type: from_ast_data_type(&column.data_type)?,
            nullable: !not_nullable,
            metadata: HashMap::new(),
        };
        fields.push(field);
    }

    let fields = spec::Fields::new(fields);
    Ok(spec::Schema { fields })
}
