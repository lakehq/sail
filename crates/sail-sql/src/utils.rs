use datafusion::sql::sqlparser::ast as df_ast;
use sail_common::spec;
use sqlparser::ast;

use crate::data_type::from_ast_data_type;
use crate::error::SqlResult;
use crate::expression::common::from_ast_expression;

/// Normalize an identifier to a lowercase string if the identifier is not quoted.
///
/// This function is necessary because SQL identifiers can be case-insensitive or case-sensitive
/// depending on whether they are quoted. Unquoted identifiers are typically case-insensitive
/// and should be normalized to lowercase for consistent handling. Quoted identifiers, however,
/// retain their case sensitivity and must be preserved exactly as written.
/// TODO: Make sure this gets called everywhere in sail-sql
///     because sail-plan spec expects the raw identifier.
pub fn normalize_ident(id: &ast::Ident) -> String {
    let ast::Ident { value, quote_style } = id.to_owned();
    match quote_style {
        Some(_) => value,
        None => value.to_ascii_lowercase(),
    }
}

pub fn object_name_to_string(object_name: &ast::ObjectName) -> String {
    // FIXME: handle identifiers with special characters such as `.`.
    object_name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
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
            column_defaults.push((normalize_ident(&column.name), default_expr));
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
            name: normalize_ident(&column.name),
            data_type: from_ast_data_type(&column.data_type)?,
            nullable: !not_nullable,
            metadata: vec![],
        };
        fields.push(field);
    }

    let fields = spec::Fields::new(fields);
    Ok(spec::Schema { fields })
}

pub fn value_to_string(value: &ast::Value) -> Option<String> {
    match value {
        ast::Value::SingleQuotedString(s) => Some(s.to_string()),
        ast::Value::DollarQuotedString(s) => Some(s.to_string()),
        ast::Value::Number(_, _) | ast::Value::Boolean(_) => Some(value.to_string()),
        ast::Value::DoubleQuotedString(_)
        | ast::Value::EscapedStringLiteral(_)
        | ast::Value::NationalStringLiteral(_)
        | ast::Value::SingleQuotedByteStringLiteral(_)
        | ast::Value::DoubleQuotedByteStringLiteral(_)
        | ast::Value::TripleSingleQuotedString(_)
        | ast::Value::TripleDoubleQuotedString(_)
        | ast::Value::TripleSingleQuotedByteStringLiteral(_)
        | ast::Value::TripleDoubleQuotedByteStringLiteral(_)
        | ast::Value::SingleQuotedRawStringLiteral(_)
        | ast::Value::DoubleQuotedRawStringLiteral(_)
        | ast::Value::TripleSingleQuotedRawStringLiteral(_)
        | ast::Value::TripleDoubleQuotedRawStringLiteral(_)
        | ast::Value::UnicodeStringLiteral(_)
        | ast::Value::HexStringLiteral(_)
        | ast::Value::Null
        | ast::Value::Placeholder(_) => None,
    }
}

// TODO: update the patched sqlparser version so that we do not need this.
pub fn to_datafusion_ast_object_name(object_name: &ast::ObjectName) -> df_ast::ObjectName {
    df_ast::ObjectName(
        object_name
            .0
            .iter()
            .map(|ident| df_ast::Ident {
                value: ident.value.clone(),
                quote_style: ident.quote_style,
            })
            .collect(),
    )
}
