use sail_sql_macro::TreeParser;

use crate::ast::keywords::{Codegen, Cost, Explain, Extended, Formatted, Logical};
use crate::ast::statement::Statement;
use crate::combinator::boxed;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Statement")]
pub struct ExplainStatement {
    pub explain: Explain,
    pub format: Option<ExplainFormat>,
    #[parser(function = |s, _| boxed(s))]
    pub statement: Box<Statement>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum ExplainFormat {
    Logical(Logical),
    Formatted(Formatted),
    Extended(Extended),
    Codegen(Codegen),
    Cost(Cost),
}
