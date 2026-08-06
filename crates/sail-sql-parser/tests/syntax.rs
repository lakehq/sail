use std::any::TypeId;

use sail_common::tests::test_gold_set;
use sail_sql_parser::ast::identifier::Ident;
use sail_sql_parser::ast::operator::Comma;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::common::Sequence;
use sail_sql_parser::tree::{SyntaxGraph, SyntaxNode, TreeSyntax};

#[test]
fn test_sequence_syntax_allows_single_item() {
    let sequence_syntax = Sequence::<Ident, Comma>::syntax();
    assert_eq!(
        sequence_syntax.node,
        SyntaxNode::Sequence(vec![
            SyntaxNode::NonTerminal(TypeId::of::<Ident>()),
            SyntaxNode::ZeroOrMore(Box::new(SyntaxNode::Sequence(vec![
                SyntaxNode::NonTerminal(TypeId::of::<Comma>()),
                SyntaxNode::NonTerminal(TypeId::of::<Ident>()),
            ]))),
        ])
    );
}

#[test]
#[expect(clippy::unwrap_used)]
fn test_syntax() {
    test_gold_set(
        "tests/gold_data/syntax.json",
        |()| Ok(SyntaxGraph::build::<Statement>()),
        |e| e,
    )
    .unwrap();
}
