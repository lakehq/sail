// SPDX-License-Identifier: Apache-2.0

use sail_common::tests::test_gold_set;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::tree::SyntaxGraph;

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
