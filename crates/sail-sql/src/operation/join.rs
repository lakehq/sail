use sail_common::spec;
use sqlparser::ast;

use crate::error::SqlResult;
use crate::query::from_ast_table_with_joins;

pub fn join_plan_from_tables(from: Vec<ast::TableWithJoins>) -> SqlResult<spec::QueryPlan> {
    let plan = from
        .into_iter()
        .try_fold(
            None,
            |r: Option<spec::QueryPlan>, table| -> SqlResult<Option<spec::QueryPlan>> {
                let right = from_ast_table_with_joins(table)?;
                match r {
                    Some(left) => Ok(Some(spec::QueryPlan::new(spec::QueryNode::Join(
                        spec::Join {
                            left: Box::new(left),
                            right: Box::new(right),
                            join_condition: None,
                            join_type: spec::JoinType::Cross,
                            using_columns: vec![],
                            join_data_type: None,
                        },
                    )))),
                    None => Ok(Some(right)),
                }
            },
        )?
        .unwrap_or_else(|| {
            spec::QueryPlan::new(spec::QueryNode::Empty {
                produce_one_row: true,
            })
        });
    Ok(plan)
}
