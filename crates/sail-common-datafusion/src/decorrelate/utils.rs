// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Private helper closure from DataFusion optimizer revision
//! `1635ed94742c1c060852bc34dc6ebb2302241c7b`.

use std::collections::{BTreeSet, HashMap};

use datafusion_common::{Column, DFSchema, Result, assert_or_internal_err};
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair, split_conjunction_owned};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, JoinType, Operator};

type EquijoinPredicate = (Expr, Expr);

// Preserved output sides, not ON-clause preservation.
// [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/push_down_filter.rs
pub(super) fn lr_is_preserved(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left => (true, false),
        JoinType::Right => (false, true),
        JoinType::Full => (false, false),
        // No columns from the right side of the join can be referenced in output
        // predicates for semi/anti joins. The right side must stay `false`:
        // `PullUpCorrelatedExpr` uses it to refuse to pull a correlated filter
        // out of a side that the join does not output.
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => (true, false),
        // No columns from the left side of the join can be referenced in output
        // predicates for semi/anti joins. The left side must stay `false`:
        // `PullUpCorrelatedExpr` uses it to refuse to pull a correlated filter
        // out of a side that the join does not output.
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => (false, true),
    }
}

// [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/utils.rs
pub(super) fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| Column::new(Some(subquery_alias), &col.name))
        .collect();
    let replace_map: HashMap<&Column, &Column> = cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

// The following two functions split equijoin predicates.
// [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/extract_equijoin_predicate.rs
pub(super) fn split_eq_and_noneq_join_predicate(
    filter: Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    split_op_and_other_join_predicates(filter, left_schema, right_schema, Operator::Eq)
}

fn split_op_and_other_join_predicates(
    filter: Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
    operator: Operator,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    assert_or_internal_err!(
        matches!(operator, Operator::Eq | Operator::IsNotDistinctFrom),
        "split_op_and_other_join_predicates only supports 'Eq' or 'IsNotDistinctFrom' operators, \
        but received: {:?}",
        operator
    );

    let exprs = split_conjunction_owned(filter);

    // Treat 'is not distinct from' comparison as join key in equal joins
    let mut accum_join_keys: Vec<(Expr, Expr)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    for expr in exprs {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref op,
                ref right,
            }) if *op == operator => {
                let join_key_pair =
                    find_valid_equijoin_key_pair(left, right, left_schema, right_schema)?;

                if let Some((left_expr, right_expr)) = join_key_pair {
                    let left_expr_type = left_expr.get_type(left_schema)?;
                    let right_expr_type = right_expr.get_type(right_schema)?;

                    if can_hash(&left_expr_type) && can_hash(&right_expr_type) {
                        accum_join_keys.push((left_expr, right_expr));
                    } else {
                        accum_filters.push(expr);
                    }
                } else {
                    accum_filters.push(expr);
                }
            }
            _ => accum_filters.push(expr),
        }
    }

    let result_filter = accum_filters.into_iter().reduce(Expr::and);
    Ok((accum_join_keys, result_filter))
}
