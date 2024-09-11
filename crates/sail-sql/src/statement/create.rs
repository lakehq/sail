use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_object_name;
use crate::parse::{parse_comment, parse_file_format, parse_value_options};
use crate::query::from_ast_query;
use crate::statement::common::{from_ast_sql_options, from_ast_table_constraint, Statement};
use crate::utils::{build_column_defaults, build_schema_from_columns, normalize_ident};

pub(crate) fn is_create_table_statement(parser: &mut Parser) -> bool {
    // CREATE TABLE
    // CREATE OR REPLACE TABLE
    // CREATE EXTERNAL TABLE
    // CREATE OR REPLACE EXTERNAL TABLE
    // CREATE UNBOUNDED EXTERNAL TABLE
    // CREATE OR REPLACE UNBOUNDED EXTERNAL TABLE
    let tokens = parser.peek_tokens_with_location::<6>();
    if !matches!(&tokens[0].token, Token::Word(w) if w.keyword == Keyword::CREATE) {
        return false;
    }
    for token in tokens.iter().skip(1) {
        if matches!(&token.token, Token::Word(w) if w.keyword == Keyword::TABLE) {
            return true;
        }
    }
    false
}

pub(crate) fn from_create_table_statement(
    table: spec::ObjectName,
    definition: spec::TableDefinition,
) -> SqlResult<spec::Plan> {
    Ok(spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::CreateTable { table, definition },
    )))
}

// Spark Syntax reference:
//  https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table.html
//  https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table.html
pub(crate) fn parse_create_statement(parser: &mut Parser) -> SqlResult<Statement> {
    if !is_create_table_statement(parser) {
        parser.expect_keyword(Keyword::CREATE)?;
        return Ok(Statement::Standard(parser.parse_create()?));
    }

    parser.expect_keyword(Keyword::CREATE)?;
    let or_replace: bool = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let unbounded = if parser.parse_keyword(Keyword::UNBOUNDED) {
        parser.expect_keyword(Keyword::EXTERNAL)?;
        true
    } else {
        let _ = parser.parse_keyword(Keyword::EXTERNAL);
        false
        // FIXME: Spark does not have an "Unbounded" keyword,
        //  so we will need to figure out how to detect if a table is unbounded.
        //  See: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
    };
    parser.expect_keyword(Keyword::TABLE)?;

    let if_not_exists: bool = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let table_name: ast::ObjectName = parser.parse_object_name(true)?;
    if parser.parse_keyword(Keyword::LIKE) {
        return Err(SqlError::todo("CREATE TABLE LIKE"));
    }
    let (columns, mut constraints): (Vec<ast::ColumnDef>, Vec<ast::TableConstraint>) =
        parser.parse_columns()?;

    let mut file_format: Option<String> = None;
    let mut comment: Option<String> = None;
    let mut options: Vec<(String, String)> = vec![];
    let mut table_partition_cols: Vec<spec::Identifier> = vec![];
    let mut location: Option<String> = None;
    let mut table_properties: Vec<ast::SqlOption> = vec![];

    if parser.parse_keyword(Keyword::USING) {
        file_format = Some(parse_file_format(parser)?);
    }

    while let Some(keyword) = parser.parse_one_of_keywords(&[
        Keyword::ROW,
        Keyword::STORED,
        Keyword::LOCATION,
        Keyword::WITH,
        Keyword::PARTITIONED,
        Keyword::OPTIONS,
        Keyword::COMMENT,
        Keyword::TBLPROPERTIES,
    ]) {
        match keyword {
            Keyword::ROW => {
                parser.expect_keyword(Keyword::FORMAT)?;
                return Err(SqlError::todo("ROW FORMAT in CREATE TABLE statement"));
            }
            Keyword::STORED => {
                parser.expect_keyword(Keyword::AS)?;
                if file_format.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple file formats in CREATE TABLE statement",
                    ));
                }
                if parser.parse_keyword(Keyword::INPUTFORMAT) {
                    let input_format: ast::Expr = parser.parse_expr()?;
                    parser.expect_keyword(Keyword::OUTPUTFORMAT)?;
                    let output_format: ast::Expr = parser.parse_expr()?;
                    return Err(SqlError::todo(format!("STORED AS INPUTFORMAT: {input_format} OUTPUTFORMAT: {output_format} in CREATE TABLE statement")));
                }
                file_format = Some(parse_file_format(parser)?);
            }
            Keyword::LOCATION => {
                if location.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple locations in CREATE TABLE statement",
                    ));
                }
                location = Some(parser.parse_literal_string()?);
            }
            Keyword::WITH => {
                parser.prev_token();
                let properties: Vec<ast::SqlOption> = parser
                    .parse_options_with_keywords(&[Keyword::WITH, Keyword::SERDEPROPERTIES])?;
                if !properties.is_empty() {
                    return Err(SqlError::todo(
                        "WITH SERDEPROPERTIES in CREATE TABLE statement",
                    ));
                }
            }
            Keyword::PARTITIONED => {
                parser.expect_keyword(Keyword::BY)?;
                if !table_partition_cols.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple PARTITIONED BY clauses in CREATE TABLE statement",
                    ));
                }
                parser.expect_token(&Token::LParen)?;
                table_partition_cols = parser
                    .parse_comma_separated(Parser::parse_column_def)?
                    .iter()
                    .map(|x| spec::Identifier::from(normalize_ident(&x.name).to_string()))
                    .collect();
                parser.expect_token(&Token::RParen)?;
            }
            Keyword::OPTIONS => {
                if !options.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple OPTIONS clauses in CREATE TABLE statement",
                    ));
                }
                options = parse_value_options(parser)?;
            }
            Keyword::COMMENT => {
                if comment.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple comments in CREATE TABLE statement",
                    ));
                }
                comment = parse_comment(parser)?;
            }
            Keyword::TBLPROPERTIES => {
                if !table_properties.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple TBLPROPERTIES clauses in CREATE TABLE statement",
                    ));
                }
                parser.expect_token(&Token::LParen)?;
                table_properties = parser.parse_comma_separated(Parser::parse_sql_option)?;
                parser.expect_token(&Token::RParen)?;
            }
            _ => {
                unreachable!()
            }
        }
    }

    let query: Option<Box<ast::Query>> = if parser.parse_keyword(Keyword::AS) {
        Some(parser.parse_boxed_query()?)
    } else {
        None
    };

    options.extend(from_ast_sql_options(table_properties)?);
    constraints.extend(calc_inline_constraints_from_columns(&columns));
    let constraints: Vec<spec::TableConstraint> = constraints
        .into_iter()
        .map(from_ast_table_constraint)
        .collect::<SqlResult<Vec<_>>>()?;
    let column_defaults: Vec<(String, spec::Expr)> = build_column_defaults(&columns)?;
    let schema: spec::Schema = build_schema_from_columns(columns)?;
    let query: Option<Box<spec::QueryPlan>> =
        query.map(|q| from_ast_query(*q)).transpose()?.map(Box::new);

    Ok(Statement::CreateExternalTable {
        table: from_ast_object_name(table_name)?,
        definition: spec::TableDefinition {
            schema,
            comment,
            column_defaults,
            constraints,
            location,
            file_format,
            table_partition_cols,
            file_sort_order: vec![], //TODO: file_sort_order
            if_not_exists,
            or_replace,
            unbounded,
            options,
            query,
            definition: None,
        },
    })
}

/// [Credit]: <https://github.com/apache/datafusion/blob/5bdc7454d92aaaba8d147883a3f81f026e096761/datafusion/sql/src/statement.rs#L115>
pub(crate) fn calc_inline_constraints_from_columns(
    columns: &[ast::ColumnDef],
) -> Vec<ast::TableConstraint> {
    let mut constraints = vec![];
    for column in columns {
        for ast::ColumnOptionDef { name, option } in &column.options {
            match option {
                ast::ColumnOption::Unique {
                    is_primary: false,
                    characteristics,
                } => constraints.push(ast::TableConstraint::Unique {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type_display: ast::KeyOrIndexDisplay::None,
                    index_type: None,
                    index_options: vec![],
                }),
                ast::ColumnOption::Unique {
                    is_primary: true,
                    characteristics,
                } => constraints.push(ast::TableConstraint::PrimaryKey {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type: None,
                    index_options: vec![],
                }),
                ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                } => constraints.push(ast::TableConstraint::ForeignKey {
                    name: name.clone(),
                    columns: vec![],
                    foreign_table: foreign_table.clone(),
                    referred_columns: referred_columns.to_vec(),
                    on_delete: *on_delete,
                    on_update: *on_update,
                    characteristics: *characteristics,
                }),
                ast::ColumnOption::Check(expr) => constraints.push(ast::TableConstraint::Check {
                    name: name.clone(),
                    expr: Box::new(expr.clone()),
                }),
                // Other options are not constraint related.
                ast::ColumnOption::Default(_)
                | ast::ColumnOption::Null
                | ast::ColumnOption::NotNull
                | ast::ColumnOption::DialectSpecific(_)
                | ast::ColumnOption::CharacterSet(_)
                | ast::ColumnOption::Generated { .. }
                | ast::ColumnOption::Comment(_)
                | ast::ColumnOption::Options(_)
                | ast::ColumnOption::OnUpdate(_) => {}
            }
        }
    }
    constraints
}
