use chumsky::Parser;
use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use sail_common::spec;
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::{Expr, IntervalLiteral};
use sail_sql_parser::ast::identifier::{ObjectName, QualifiedWildcard};
use sail_sql_parser::ast::query::NamedExpr;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_expression_parser, create_interval_literal_parser,
    create_named_expression_parser, create_object_name_parser, create_parser,
    create_qualified_wildcard_parser,
};
use sail_sql_parser::token::{Punctuation, Token};

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_expression;
use crate::literal::datetime::{
    DateValue, TimeValue, TimestampValue, create_date_parser, create_time_parser,
    create_timestamp_parser,
};
use crate::literal::interval::{IntervalValue, parse_unqualified_interval_string};

fn map_parser_input<'a, C>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize, C>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize, C>) {
    (t, s)
}

fn tokenize<'a>(
    input: &'a str,
    options: &ParserOptions,
) -> SqlResult<Vec<(Token<'a>, SimpleSpan<usize>)>> {
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options);
    lexer.parse(input).into_result().map_err(SqlError::parser)
}

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{ parse!($input, $parser, &ParserOptions::default()) }};
    ($input:ident, $parser:ident, $options:expr $(,)?) => {{
        let options: &ParserOptions = $options;
        let length = $input.len();
        let tokens = tokenize($input, options)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

macro_rules! parse_simple {
    ($input:ident, $parser:ident $(,)?) => {{
        let parser = $parser::<chumsky::extra::Err<chumsky::error::Rich<_, _>>>();
        parser.parse($input).into_result().map_err(SqlError::parser)
    }};
}

pub fn rewrite_positional_parameter_markers(s: &str) -> SqlResult<(String, usize)> {
    let tokens = tokenize(s, &ParserOptions::default())?;

    let mut output = String::with_capacity(s.len());
    let mut last = 0;
    let mut count = 0;
    for (token, span) in tokens {
        if matches!(token, Token::Punctuation(Punctuation::QuestionMark)) {
            count += 1;
            output.push_str(&s[last..span.start]);
            output.push('$');
            output.push_str(&count.to_string());
            last = span.end;
        }
    }
    output.push_str(&s[last..]);
    Ok((output, count))
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, create_expression_parser)
}

/// Ignores SQL comments and decodes string literals for native configuration settings.
pub fn parse_native_config_value(s: &str) -> SqlResult<String> {
    let tokens = tokenize(s, &ParserOptions::default())?;
    let mut value = String::with_capacity(s.len());
    let mut last = 0;
    for (token, span) in tokens {
        if matches!(
            token,
            Token::SingleLineComment { .. } | Token::MultiLineComment { .. }
        ) {
            value.push_str(&s[last..span.start]);
            last = span.end;
        }
    }
    value.push_str(&s[last..]);
    match parse_expression(&value).and_then(from_ast_expression) {
        Ok(spec::Expr::Literal(spec::Literal::Utf8 { value: Some(value) })) => Ok(value),
        _ => Ok(value.trim().to_string()),
    }
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
}

/// Parses a SQL string into statements with custom parser options,
/// e.g. for SQL that is not in the Spark SQL dialect.
pub fn parse_statements_with_options(
    s: &str,
    options: &ParserOptions,
) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser, options)
}

/// Parses a SQL string containing exactly one statement into an AST.
pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(s: &str) -> SqlResult<ObjectName> {
    parse!(s, create_object_name_parser)
}

pub fn parse_qualified_wildcard(s: &str) -> SqlResult<QualifiedWildcard> {
    parse!(s, create_qualified_wildcard_parser)
}

pub fn parse_named_expression(s: &str) -> SqlResult<NamedExpr> {
    parse!(s, create_named_expression_parser)
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    parse!(s, create_interval_literal_parser)
}

pub fn parse_interval(s: &str) -> SqlResult<IntervalValue> {
    parse_unqualified_interval_string(s, false)
}

pub fn parse_date(s: &str) -> SqlResult<DateValue> {
    parse_simple!(s, create_date_parser)
}

pub fn parse_timestamp(s: &str) -> SqlResult<TimestampValue<'_>> {
    parse_simple!(s, create_timestamp_parser)
}

pub fn parse_time(s: &str) -> SqlResult<TimeValue> {
    parse_simple!(s, create_time_parser)
}

#[cfg(test)]
mod tests {
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::{DescribeItem, Statement};
    use sail_sql_parser::tree::TreeText;

    use crate::error::SqlResult;
    use crate::parser::{parse_one_statement, parse_statements};

    #[test]
    fn test_parse() -> SqlResult<()> {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let tree = parse_statements(sql)?;
        assert!(matches!(
            tree.as_slice(),
            [
                Statement::Query(Query { .. }),
                Statement::Query(Query { .. }),
            ]
        ));
        Ok(())
    }

    #[test]
    fn test_hint_requires_parameters_inside_parentheses() {
        for hint in ["REPARTITION", "COALESCE", "UNKNOWN_HINT"] {
            let sql = format!("SELECT /*+ {hint}() */ 1");
            assert!(parse_one_statement(&sql).is_err(), "{sql}");
        }
        assert!(parse_one_statement("SELECT /*+ REPARTITION */ 1").is_ok());
    }

    #[test]
    fn test_unparse() -> SqlResult<()> {
        let hint_sql = "SELECT /*+ REPARTITION ( 3 , id ) , COALESCE ( 2 ) */ id FROM t ";
        assert_eq!(parse_one_statement(hint_sql)?.text(), hint_sql);
        assert_eq!(
            parse_one_statement("/* */ SELECT 1+1")?.text(),
            "SELECT 1 + 1 "
        );
        assert_eq!(
            parse_one_statement("Select  2*3 +(4*5)AS a, b '\\x01', $1,? -- comment")?.text(),
            "SELECT 2 * 3 + ( 4 * 5 ) AS a , b '\\x01' , $1 , ? "
        );
        assert_eq!(
            parse_one_statement("SELECT foo(0), cast(1L as decimal(10, -1)) FROM a.b")?.text(),
            "SELECT foo ( 0 ) , CAST ( 1L AS DECIMAL ( 10 , -1 ) ) FROM a . b "
        );
        assert_eq!(
            parse_one_statement("SELECT U&\"a#2014b#+002014c\"   UESCAPE '#'")?.text(),
            "SELECT U&\"a#2014b#+002014c\" UESCAPE '#' "
        );
        assert_eq!(
            parse_one_statement("SET k=` a;b``c `")?.text(),
            "SET k = ` a;b``c ` "
        );
        Ok(())
    }

    #[test]
    fn test_unparse_quoted_identifiers() -> SqlResult<()> {
        use crate::statement::from_ast_statement;

        for (sql, expected) in [
            (
                "select `source rows`.`a.b` as `out-put`, 'it\\'s a string' as `select` \
                 from `db name`.`table``name` as `source rows`",
                "SELECT `source rows` . `a.b` AS `out-put` , 'it\\'s a string' AS `select` \
                 FROM `db name` . `table``name` AS `source rows` ",
            ),
            (
                "SELECT `part-key`,`part``key`,`select`,`123`,`café name` FROM `weird table`",
                "SELECT `part-key` , `part``key` , `select` , `123` , `café name` FROM `weird table` ",
            ),
            (
                "CREATE VIEW `saved view` AS SELECT `part key` AS `out.name`, \
                 'it\\'s a string' AS `quoted``literal` FROM `source table` WHERE `part key`>0",
                "CREATE VIEW `saved view` AS SELECT `part key` AS `out.name` , \
                 'it\\'s a string' AS `quoted``literal` FROM `source table` WHERE `part key` > 0 ",
            ),
        ] {
            let statement = parse_one_statement(sql)?;
            let text = statement.text();
            assert_eq!(text, expected);
            assert_eq!(
                from_ast_statement(parse_one_statement(&text)?)?,
                from_ast_statement(statement)?,
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_call_procedure() -> SqlResult<()> {
        let statement = parse_one_statement(
            "CALL sail.system.rollback_to_snapshot(table => 'events', snapshot_id => 42)",
        )?;
        assert!(matches!(statement, Statement::CallProcedure { .. }));
        assert_eq!(
            statement.text(),
            "CALL sail . system . rollback_to_snapshot ( table => 'events' , snapshot_id => 42 ) "
        );
        Ok(())
    }

    #[test]
    fn test_lakehouse_command_targets() -> SqlResult<()> {
        use sail_common::spec::{CommandNode, LakehouseCommandTarget, ObjectName, Plan};

        for command in ["DESCRIBE DETAIL", "DESCRIBE HISTORY", "OPTIMIZE", "VACUUM"] {
            for (target, expected) in [
                (
                    "'relative-table'",
                    LakehouseCommandTarget::Path("relative-table".to_string()),
                ),
                (
                    "'./relative-table'",
                    LakehouseCommandTarget::Path("./relative-table".to_string()),
                ),
                (
                    "delta.`relative-table`",
                    LakehouseCommandTarget::Table(ObjectName::from(["delta", "relative-table"])),
                ),
                (
                    "delta.`/absolute-table`",
                    LakehouseCommandTarget::Table(ObjectName::from(["delta", "/absolute-table"])),
                ),
            ] {
                let sql = format!("{command} {target}");
                let plan = crate::statement::from_ast_statement(parse_one_statement(&sql)?)?;
                assert!(matches!(
                    &plan,
                    Plan::Command(plan) if matches!(
                        &plan.node,
                        CommandNode::CallTableProcedure { target, .. } if target == &expected
                    )
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn test_optimize_predicate_preserves_semantics() -> SqlResult<()> {
        use sail_common::spec;

        use crate::expression::from_ast_expression;
        use crate::parser::parse_expression;
        use crate::statement::from_ast_statement;

        for (predicate_sql, expected_sql, column) in [
            ("`part key` = 1", "`part key` = 1", "part key"),
            ("`part.key` = 1", "`part.key` = 1", "part.key"),
            ("`part-key` = 1", "`part-key` = 1", "part-key"),
            ("`part``key` = 1", "`part``key` = 1", "part`key"),
            ("`select` = 1", "`select` = 1", "select"),
            ("`123` = 1", "`123` = 1", "123"),
            ("`café name` = 1", "`café name` = 1", "café name"),
            ("`part key`\t=\r\n  1", "`part key` = 1", "part key"),
            (
                "`part key` /* WHERE ZORDER BY (ignored) */ = 1",
                "`part key` = 1",
                "part key",
            ),
            (
                "`part key` -- WHERE ZORDER BY (ignored)\n = 1",
                "`part key` = 1",
                "part key",
            ),
            (
                "`part key` = 1 /* ZORDER BY (ignored) */",
                "`part key` = 1",
                "part key",
            ),
            (
                "`WHERE ZORDER BY` = 1",
                "`WHERE ZORDER BY` = 1",
                "WHERE ZORDER BY",
            ),
            ("zorder = 1", "zorder = 1", "zorder"),
        ] {
            for (suffix, zorder_sql) in [
                ("", None),
                (" ZORDER BY id", Some("ZORDER BY id ")),
                (" ZORDER BY (id)", Some("ZORDER BY ( id ) ")),
            ] {
                let sql = format!("OPTIMIZE t WHERE {predicate_sql}{suffix}");
                let statement = parse_one_statement(&sql)?;
                let Statement::Optimize {
                    predicate: Some(predicate),
                    zorder,
                    ..
                } = &statement
                else {
                    unreachable!("expected OPTIMIZE with a predicate: {sql}");
                };
                let predicate_text = predicate.condition.text();
                assert_eq!(predicate_text.trim(), expected_sql, "{sql}");
                assert_eq!(zorder.is_some(), zorder_sql.is_some(), "{sql}");

                for condition in [
                    from_ast_expression(predicate.condition.clone())?,
                    from_ast_expression(parse_expression(&predicate_text)?)?,
                ] {
                    let spec::Expr::UnresolvedFunction(function) = condition else {
                        unreachable!("expected equality: {sql}");
                    };
                    assert_eq!(function.function_name, spec::ObjectName::bare("=="));
                    assert_eq!(
                        function.arguments,
                        vec![
                            spec::Expr::UnresolvedAttribute {
                                name: spec::ObjectName::bare(column),
                                plan_id: None,
                                is_metadata_column: false,
                            },
                            spec::Expr::Literal(spec::Literal::Int32 { value: Some(1) }),
                        ],
                        "{sql}"
                    );
                }

                let mut arguments = vec![(
                    "predicate".into(),
                    spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some(expected_sql.to_string()),
                    }),
                )];
                if let Some(zorder_sql) = zorder_sql {
                    arguments.push((
                        "zorder".into(),
                        spec::Expr::Literal(spec::Literal::Utf8 {
                            value: Some(zorder_sql.to_string()),
                        }),
                    ));
                }
                assert_eq!(
                    from_ast_statement(statement)?,
                    spec::Plan::Command(spec::CommandPlan::new(
                        spec::CommandNode::CallTableProcedure {
                            format: "delta".to_string(),
                            procedure: "optimize".to_string(),
                            target: spec::LakehouseCommandTarget::Table(spec::ObjectName::bare(
                                "t",
                            )),
                            arguments,
                        }
                    )),
                    "{sql}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_optimize_predicate_boundaries() -> SqlResult<()> {
        use sail_common::spec;

        use crate::expression::from_ast_expression;

        let predicate_sql = "`part key` = 'WHERE ZORDER BY (ignored); it\\'s a string'";
        let sql = format!(
            "OPTIMIZE t WHERE /* before */ {predicate_sql} /* after */ ZORDER BY (id); SELECT 1"
        );
        let statements = parse_statements(&sql)?;
        let [
            Statement::Optimize {
                predicate: Some(predicate),
                zorder: Some(_),
                ..
            },
            Statement::Query(_),
        ] = statements.as_slice()
        else {
            unreachable!("expected OPTIMIZE followed by SELECT");
        };
        let predicate_text = predicate.condition.text();
        assert_eq!(predicate_text.trim(), predicate_sql);
        for condition in [
            from_ast_expression(predicate.condition.clone())?,
            from_ast_expression(crate::parser::parse_expression(&predicate_text)?)?,
        ] {
            let spec::Expr::UnresolvedFunction(function) = condition else {
                unreachable!("expected equality");
            };
            assert_eq!(function.function_name, spec::ObjectName::bare("=="));
            assert_eq!(
                function.arguments,
                vec![
                    spec::Expr::UnresolvedAttribute {
                        name: spec::ObjectName::bare("part key"),
                        plan_id: None,
                        is_metadata_column: false,
                    },
                    spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some("WHERE ZORDER BY (ignored); it's a string".to_string()),
                    }),
                ]
            );
        }

        for sql in [
            "OPTIMIZE t WHERE",
            "OPTIMIZE t WHERE `part key` =",
            "OPTIMIZE t WHERE `part key` = 1 AND ZORDER BY (id)",
            "OPTIMIZE t WHERE `part key` = 1 ZORDER BY",
            "OPTIMIZE t WHERE `part key` = 1 SELECT 1",
        ] {
            assert!(parse_one_statement(sql).is_err(), "{sql}");
        }
        Ok(())
    }

    #[test]
    fn test_describe_table_and_query_disambiguation() -> SqlResult<()> {
        for sql in [
            "DESCRIBE t",
            "DESC default.t",
            "DESCRIBE TABLE t",
            "DESCRIBE table",
            "DESCRIBE table.t",
            "DESCRIBE TABLE table",
            "DESCRIBE TABLE table.t",
            "DESCRIBE EXTENDED t",
            "DESCRIBE TABLE FORMATTED t",
            "DESCRIBE FORMATTED t PARTITION (p = 1) c",
            "DESCRIBE formatted",
            "DESCRIBE TABLE formatted",
            "DESCRIBE TABLE formatted.t",
            "DESCRIBE TABLE formatted PARTITION (p = 1)",
            "DESCRIBE TABLE extended",
            "DESCRIBE TABLE extended.t",
            "DESCRIBE TABLE extended PARTITION (p = 1)",
            "DESCRIBE SELECT a",
            "DESCRIBE SELECT 1m",
            "DESCRIBE SELECT `1`",
            "DESCRIBE VALUES x",
            "DESCRIBE QUERY t",
            "DESCRIBE `query`",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::Describe {
                        item: DescribeItem::Table { .. },
                        ..
                    }
                ),
                "{sql}"
            );
        }
        for sql in [
            "DESCRIBE SELECT 1",
            "DESCRIBE SELECT 1L",
            "DESCRIBE SELECT 1.2",
            "DESCRIBE SELECT a FROM t",
            "DESCRIBE TABLE t ORDER BY a",
            "DESCRIBE VALUES (1)",
            "DESCRIBE VALUES 1",
            "DESCRIBE (SELECT 1)",
            "DESCRIBE WITH s AS (SELECT 1) SELECT * FROM s",
            "DESCRIBE QUERY SELECT a",
            "DESCRIBE QUERY SELECT a FROM t",
            "DESCRIBE QUERY TABLE t",
            "DESCRIBE QUERY WITH s AS (SELECT 1) SELECT * FROM s",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::Describe {
                        item: DescribeItem::Query { .. },
                        ..
                    }
                ),
                "{sql}"
            );
        }
        for sql in [
            "DESCRIBE SELECT a FROM",
            "DESCRIBE t c unexpected",
            "DESCRIBE QUERY SELECT 1 unexpected trailing",
        ] {
            assert!(parse_one_statement(sql).is_err(), "{sql}");
        }
        Ok(())
    }

    #[test]
    fn test_describe_statement_boundaries() -> SqlResult<()> {
        let statements = parse_statements(
            "DESCRIBE SELECT a /* table and column */; \
             DESCRIBE SELECT a FROM t /* query */; \
             EXPLAIN DESCRIBE SELECT a FROM t; SELECT 1",
        )?;
        assert!(matches!(
            statements.as_slice(),
            [
                Statement::Describe {
                    item: DescribeItem::Table { .. },
                    ..
                },
                Statement::Describe {
                    item: DescribeItem::Query { .. },
                    ..
                },
                Statement::Explain { .. },
                Statement::Query(_),
            ]
        ));
        assert!(matches!(
            parse_one_statement("EXPLAIN DESCRIBE SELECT a FROM t")?,
            Statement::Explain { statement, .. }
                if matches!(statement.as_ref(), Statement::Describe {
                    item: DescribeItem::Query { .. },
                    ..
                })
        ));
        Ok(())
    }

    #[test]
    fn test_insert_with_ctes() -> SqlResult<()> {
        let statement = parse_one_statement(
            "WITH s AS (SELECT 1 AS x) INSERT INTO t \
             WITH s AS (SELECT 2 AS x) SELECT x FROM s",
        )?;
        assert!(matches!(
            statement,
            Statement::InsertInto {
                with: Some(_),
                query: Query { with: Some(_), .. },
                ..
            }
        ));
        for sql in [
            "WITH s AS (SELECT 1 AS x) INSERT OVERWRITE TABLE t (x) SELECT x FROM s",
            "WITH s AS (SELECT 1 AS x) INSERT INTO t BY NAME SELECT x FROM s",
            "WITH s AS (SELECT 1 AS x) INSERT INTO t PARTITION (p = 1) SELECT x FROM s",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::InsertInto { with: Some(_), .. }
                ),
                "{sql}"
            );
        }
        assert!(matches!(
            parse_one_statement(
                "WITH s AS (SELECT 1 AS x) INSERT INTO t REPLACE WHERE x = 1 SELECT x FROM s"
            )?,
            Statement::InsertIntoAndReplace { with: Some(_), .. }
        ));
        assert!(matches!(
            parse_one_statement(
                "WITH s AS (SELECT 1 AS x) INSERT OVERWRITE DIRECTORY '/tmp/t' USING parquet SELECT x FROM s"
            )?,
            Statement::InsertOverwriteDirectory { with: Some(_), .. }
        ));
        Ok(())
    }
}
