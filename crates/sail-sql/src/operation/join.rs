use sail_common::spec;
use sqlparser::ast;

use crate::error::SqlResult;
use crate::expression::common::{from_ast_expression, from_ast_ident, from_ast_object_name};
use crate::query::from_ast_table_with_joins;

pub fn query_plan_from_tables(from: Vec<ast::TableWithJoins>) -> SqlResult<spec::QueryPlan> {
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

pub fn query_plan_with_lateral_views(
    plan: spec::QueryPlan,
    lateral_views: Vec<ast::LateralView>,
) -> SqlResult<spec::QueryPlan> {
    lateral_views
        .into_iter()
        .try_fold(plan, |plan, lateral_view| -> SqlResult<_> {
            let ast::LateralView {
                lateral_view,
                lateral_view_name,
                lateral_col_alias,
                outer,
            } = lateral_view;
            Ok(spec::QueryPlan::new(spec::QueryNode::LateralView {
                input: Box::new(plan),
                expression: from_ast_expression(lateral_view)?,
                table_alias: Some(from_ast_object_name(lateral_view_name)?),
                column_aliases: lateral_col_alias
                    .iter()
                    .map(|x| from_ast_ident(x, true))
                    .collect::<SqlResult<_>>()?,
                outer,
            }))
        })
}
