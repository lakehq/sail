use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::parse::{
    parse_comma_separated, parse_comment, parse_file_format, parse_normalized_object_name,
    parse_partition_column_name, parse_value_options,
};
use crate::query::from_ast_query;
use crate::statement::common::{
    from_ast_row_format, from_ast_sql_options, from_ast_table_constraint, Statement,
};
use crate::utils::{build_column_defaults, build_schema_from_columns, normalize_ident};

pub(crate) fn is_create_table_statement(parser: &mut Parser) -> bool {
    // CREATE TABLE
    // CREATE OR REPLACE TABLE
    // CREATE EXTERNAL TABLE
    // CREATE OR REPLACE EXTERNAL TABLE
    // CREATE UNBOUNDED EXTERNAL TABLE
    // CREATE OR REPLACE UNBOUNDED EXTERNAL TABLE
    // REPLACE TABLE
    let tokens = parser.peek_tokens_with_location::<6>();
    if !matches!(&tokens[0].token, Token::Word(w) if w.keyword == Keyword::CREATE || w.keyword == Keyword::REPLACE)
    {
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

    let or_replace: bool =
        match parser.expect_one_of_keywords(&[Keyword::CREATE, Keyword::REPLACE])? {
            Keyword::CREATE => parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]),
            Keyword::REPLACE => true,
            _ => unreachable!(),
        };
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
    let table_name: spec::ObjectName = parse_normalized_object_name(parser)?;
    if parser.parse_keyword(Keyword::LIKE) {
        return Err(SqlError::todo("CREATE TABLE LIKE"));
    }
    let (mut columns, mut constraints): (Vec<ast::ColumnDef>, Vec<ast::TableConstraint>) =
        parser.parse_columns()?; // TODO: Parse comments in columns

    let mut row_format: Option<ast::HiveRowFormat> = None;
    let mut file_format: Option<spec::TableFileFormat> = None;
    let mut comment: Option<String> = None;
    let mut options: Vec<(String, String)> = vec![];
    let mut table_partition_cols: Vec<spec::Identifier> = vec![];
    let mut location: Option<String> = None;
    let mut table_properties: Vec<ast::SqlOption> = vec![];
    let mut serde_properties: Vec<(String, String)> = vec![];

    if parser.parse_keyword(Keyword::USING) {
        let format = parse_file_format(parser)?;
        file_format = Some(spec::TableFileFormat {
            input_format: format,
            output_format: None,
        });
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
                if row_format.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple ROW FORMAT clauses in CREATE TABLE statement",
                    ));
                }
                row_format = Some(parser.parse_row_format()?);
            }
            Keyword::STORED => {
                parser.expect_keyword(Keyword::AS)?;
                if file_format.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple file formats in CREATE TABLE statement",
                    ));
                }
                if parser.parse_keyword(Keyword::INPUTFORMAT) {
                    let input_format = parse_file_format(parser)?;
                    parser.expect_keyword(Keyword::OUTPUTFORMAT)?;
                    let output_format = parse_file_format(parser)?;
                    file_format = Some(spec::TableFileFormat {
                        input_format,
                        output_format: Some(output_format),
                    });
                } else {
                    let format = parse_file_format(parser)?;
                    file_format = Some(spec::TableFileFormat {
                        input_format: format,
                        output_format: None,
                    });
                }
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
                if !properties.is_empty() && !serde_properties.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple WITH SERDEPROPERTIES clauses in CREATE TABLE statement",
                    ));
                }
                serde_properties = from_ast_sql_options(properties)?;
            }
            Keyword::PARTITIONED => {
                parser.expect_keyword(Keyword::BY)?;
                if !table_partition_cols.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple PARTITIONED BY clauses in CREATE TABLE statement",
                    ));
                }
                let peekaboo = parser.peek_nth_token(2);
                if peekaboo == Token::Comma || peekaboo == Token::RParen {
                    parser.expect_token(&Token::LParen)?;
                    table_partition_cols =
                        parse_comma_separated(parser, parse_partition_column_name)?;
                    parser.expect_token(&Token::RParen)?;
                } else {
                    let (partition_cols, partition_constraints): (
                        Vec<ast::ColumnDef>,
                        Vec<ast::TableConstraint>,
                    ) = parser.parse_columns()?;
                    if !partition_constraints.is_empty() {
                        return Err(SqlError::invalid(
                            "Constraints in PARTITIONED BY clause in CREATE TABLE statement",
                        ));
                    }
                    table_partition_cols = partition_cols
                        .iter()
                        .map(|col| spec::Identifier::from(normalize_ident(&col.name)))
                        .collect();
                    columns.extend(partition_cols);
                }
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
    let row_format = row_format
        .map(|row_format| from_ast_row_format(row_format, &file_format))
        .transpose()?;

    Ok(Statement::CreateExternalTable {
        table: table_name,
        definition: spec::TableDefinition {
            schema,
            comment,
            column_defaults,
            constraints,
            location,
            serde_properties,
            file_format,
            row_format,
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
                | ast::ColumnOption::Materialized(_)
                | ast::ColumnOption::Identity(_)
                | ast::ColumnOption::Ephemeral(_)
                | ast::ColumnOption::Alias(_)
                | ast::ColumnOption::OnUpdate(_) => {}
            }
        }
    }
    constraints
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SparkDialect;

    #[test]
    fn test_parse_create_statement() -> SqlResult<()> {
        // Gold tests thoroughly test the parser. Adding a test for coverage of number identifiers
        let parse = |sql: &str| -> SqlResult<Statement> {
            let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
            parse_create_statement(&mut parser)
        };

        let sql = "CREATE TABLE foo.1m(a INT)";
        let statement = parse(sql)?;
        assert_eq!(
            statement,
            Statement::CreateExternalTable {
                table: spec::ObjectName::from(vec!["foo".to_string(), "1m".to_string()]),
                definition: spec::TableDefinition {
                    schema: spec::Schema {
                        fields: spec::Fields(vec![spec::Field {
                            name: "a".to_string(),
                            data_type: spec::DataType::Integer,
                            nullable: true,
                            metadata: vec![],
                        }]),
                    },
                    comment: None,
                    column_defaults: vec![],
                    constraints: vec![],
                    location: None,
                    serde_properties: vec![],
                    file_format: None,
                    row_format: None,
                    table_partition_cols: vec![],
                    file_sort_order: vec![],
                    if_not_exists: false,
                    or_replace: false,
                    unbounded: false,
                    options: vec![],
                    query: None,
                    definition: None,
                }
            }
        );

        let sql = "CREATE TABLE foo.1m(a INT) USING parquet";
        let statement = parse(sql)?;
        assert_eq!(
            statement,
            Statement::CreateExternalTable {
                table: spec::ObjectName::from(vec!["foo".to_string(), "1m".to_string()]),
                definition: spec::TableDefinition {
                    schema: spec::Schema {
                        fields: spec::Fields(vec![spec::Field {
                            name: "a".to_string(),
                            data_type: spec::DataType::Integer,
                            nullable: true,
                            metadata: vec![],
                        }]),
                    },
                    comment: None,
                    column_defaults: vec![],
                    constraints: vec![],
                    location: None,
                    serde_properties: vec![],
                    file_format: Some(spec::TableFileFormat {
                        input_format: "PARQUET".to_string(),
                        output_format: None,
                    }),
                    row_format: None,
                    table_partition_cols: vec![],
                    file_sort_order: vec![],
                    if_not_exists: false,
                    or_replace: false,
                    unbounded: false,
                    options: vec![],
                    query: None,
                    definition: None,
                }
            }
        );

        Ok(())
    }
}
