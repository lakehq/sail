use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use chumsky::Parser;
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
use sail_sql_parser::token::Token;

use crate::error::{SqlError, SqlResult};
use crate::literal::datetime::{
    create_date_parser, create_time_parser, create_timestamp_parser, DateValue, TimeValue,
    TimestampValue,
};
use crate::literal::interval::{parse_unqualified_interval_string, IntervalValue};

fn map_parser_input<'a, C>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize, C>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize, C>) {
    (t, s)
}

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

macro_rules! parse_simple {
    ($input:ident, $parser:ident $(,)?) => {{
        let parser = $parser::<chumsky::extra::Err<chumsky::error::Rich<_, _>>>();
        parser.parse($input).into_result().map_err(SqlError::parser)
    }};
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, create_expression_parser)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
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
    use sail_common::spec;
    use sail_sql_parser::ast::graph::GraphQuery;
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;
    use sail_sql_parser::tree::TreeText;

    use crate::error::SqlResult;
    use crate::parser::{parse_one_statement, parse_statements};
    use crate::statement::from_ast_statement;

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
    fn test_parse_cypher_graph_query() -> SqlResult<()> {
        let statement = parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age > 30 \
             RETURN a.id, b.id, e.since \
             ORDER BY e.since \
             SKIP 5 \
             LIMIT 10",
        )?;
        assert!(matches!(
            statement,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        Ok(())
    }

    #[test]
    fn test_parse_cypher_graph_query_directions() -> SqlResult<()> {
        assert!(matches!(
            parse_one_statement("MATCH (b:Person)<-[e:KNOWS]-(a:Person) RETURN b.id")?,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        assert!(matches!(
            parse_one_statement("MATCH (a:Person)-[e:KNOWS]-(b:Person) RETURN b.id")?,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        assert!(matches!(
            parse_one_statement("MATCH (a:Person)-->(b:Person) RETURN b.id")?,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        assert!(matches!(
            parse_one_statement("MATCH (b:Person)<--(a:Person) RETURN b.id")?,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        assert!(matches!(
            parse_one_statement("MATCH (a:Person)--(b:Person) RETURN b.id")?,
            Statement::GraphQuery(GraphQuery { .. })
        ));
        Ok(())
    }

    #[test]
    fn test_analyze_cypher_graph_query_property_maps() -> SqlResult<()> {
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person {age: '42'})-[e:KNOWS {since: '2020'}]->(b {name: 'Bob'}) \
             RETURN a.id, e.id, b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };

        let pattern = &graph.patterns[0];
        assert_eq!(pattern.start.properties.len(), 1);
        assert_eq!(
            pattern.start.properties[0].key,
            spec::Identifier::from("age")
        );
        assert_eq!(pattern.steps[0].edge.properties.len(), 1);
        assert_eq!(
            pattern.steps[0].edge.properties[0].key,
            spec::Identifier::from("since")
        );
        assert_eq!(pattern.steps[0].target.properties.len(), 1);
        assert_eq!(
            pattern.steps[0].target.properties[0].key,
            spec::Identifier::from("name")
        );
        assert_eq!(graph.predicates.len(), 0);
        assert_eq!(graph.returns.len(), 3);
        Ok(())
    }

    #[test]
    fn test_analyze_cypher_graph_query() -> SqlResult<()> {
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age > 30 \
             RETURN a.id, b.id, e.since \
             ORDER BY e.since \
             SKIP 5 \
             LIMIT 10",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };

        assert_eq!(graph.patterns.len(), 1);
        assert_eq!(
            graph.patterns[0].start.variable,
            Some(spec::Identifier::from("a"))
        );
        assert_eq!(
            graph.patterns[0].start.label,
            Some(spec::Identifier::from("Person"))
        );
        assert_eq!(graph.patterns[0].steps.len(), 1);
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Outgoing
        );
        assert_eq!(
            graph.patterns[0].steps[0].edge.variable,
            Some(spec::Identifier::from("e"))
        );
        assert_eq!(
            graph.patterns[0].steps[0].edge.label,
            Some(spec::Identifier::from("KNOWS"))
        );
        assert_eq!(
            graph.patterns[0].steps[0].target.variable,
            Some(spec::Identifier::from("b"))
        );
        assert_eq!(graph.predicates.len(), 1);
        assert_eq!(graph.returns.len(), 3);
        assert_eq!(graph.order.len(), 1);
        assert!(graph.skip.is_some());
        assert!(graph.limit.is_some());
        Ok(())
    }

    #[test]
    fn test_analyze_cypher_graph_query_multiple_patterns() -> SqlResult<()> {
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person), (b)<-[f:KNOWS]-(d:Document) \
             RETURN a.id, b.id, d.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };

        assert_eq!(graph.patterns.len(), 2);
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Outgoing
        );
        assert_eq!(
            graph.patterns[1].start.variable,
            Some(spec::Identifier::from("b"))
        );
        assert_eq!(
            graph.patterns[1].steps[0].direction,
            spec::GraphDirection::Incoming
        );
        assert_eq!(
            graph.patterns[1].steps[0].edge.variable,
            Some(spec::Identifier::from("f"))
        );
        assert_eq!(
            graph.patterns[1].steps[0].target.label,
            Some(spec::Identifier::from("Document"))
        );

        Ok(())
    }

    #[test]
    fn test_analyze_cypher_graph_query_limit_all() -> SqlResult<()> {
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-->(b:Person) \
             RETURN b.id \
             ORDER BY b.id \
             SKIP 1 \
             LIMIT ALL",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };

        assert_eq!(graph.order.len(), 1);
        assert!(graph.skip.is_some());
        assert!(graph.limit.is_none());
        Ok(())
    }

    #[test]
    fn test_analyze_cypher_graph_query_directions() -> SqlResult<()> {
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (b:Person)<-[e:KNOWS]-(a:Person) RETURN b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Incoming
        );

        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]-(b:Person) RETURN b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Undirected
        );

        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-->(b:Person) RETURN b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Outgoing
        );
        assert_eq!(graph.patterns[0].steps[0].edge.variable, None);
        assert_eq!(graph.patterns[0].steps[0].edge.label, None);

        let plan = from_ast_statement(parse_one_statement(
            "MATCH (b:Person)<--(a:Person) RETURN b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Incoming
        );
        assert_eq!(graph.patterns[0].steps[0].edge.variable, None);
        assert_eq!(graph.patterns[0].steps[0].edge.label, None);

        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)--(b:Person) RETURN b.id",
        )?)?;
        let spec::Plan::Query(spec::QueryPlan {
            node: spec::QueryNode::Graph(graph),
            plan_id: None,
        }) = plan
        else {
            panic!("expected graph query plan");
        };
        assert_eq!(
            graph.patterns[0].steps[0].direction,
            spec::GraphDirection::Undirected
        );
        assert_eq!(graph.patterns[0].steps[0].edge.variable, None);
        assert_eq!(graph.patterns[0].steps[0].edge.label, None);

        Ok(())
    }

    #[test]
    fn test_unparse() -> SqlResult<()> {
        assert_eq!(
            parse_one_statement("/* */ SELECT 1+1")?.text(),
            "SELECT 1 + 1 "
        );
        assert_eq!(
            parse_one_statement("SELECT 1 -- comment")?.text(),
            "SELECT 1 "
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
            parse_one_statement("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a.id, b.id")?.text(),
            "MATCH ( a : Person ) - [ e : KNOWS ] -> ( b : Person ) RETURN a . id , b . id "
        );
        assert_eq!(
            parse_one_statement("MATCH (a:Person)-->(b:Person) RETURN b.id")?.text(),
            "MATCH ( a : Person ) - -> ( b : Person ) RETURN b . id "
        );
        Ok(())
    }
}
