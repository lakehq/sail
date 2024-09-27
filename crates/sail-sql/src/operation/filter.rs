use sail_common::spec;
use sqlparser::ast;

use crate::error::SqlResult;
use crate::expression::common::from_ast_expression;

pub fn query_plan_with_filter(
    plan: spec::QueryPlan,
    selection: Option<ast::Expr>,
) -> SqlResult<spec::QueryPlan> {
    let plan = if let Some(selection) = selection {
        let selection = from_ast_expression(selection)?;
        spec::QueryPlan::new(spec::QueryNode::Filter {
            input: Box::new(plan),
            condition: selection,
        })
    } else {
        plan
    };
    Ok(plan)
}
