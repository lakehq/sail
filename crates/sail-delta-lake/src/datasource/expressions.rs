// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column as PhysicalColumn;

use crate::kernel::DeltaResult;

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

/// Simplify a logical expression and convert it to a physical expression.
pub fn simplify_expr(
    session: &dyn Session,
    df_schema: &DFSchema,
    expr: Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    let simplified = simplifier.simplify(expr)?;

    session.create_physical_expr(simplified, df_schema)
}

/// Determine which filters can be pushed down to the table provider.
pub fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .cloned()
        .map(|expr| {
            let applicable = expr_is_exact_predicate_for_cols(partition_cols, expr);
            if !expr.column_refs().is_empty() && applicable {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

/// Check if an expression is an exact predicate for the given columns.
fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    let _ = expr.apply(|expr| match expr {
        Expr::Column(Column { ref name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { ref op, .. }) => {
            is_applicable &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_applicable {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(..)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    is_applicable
}

/// Extract column names referenced by a `PhysicalExpr`.
pub fn collect_physical_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<String> {
    let mut columns = HashSet::<String>::new();
    let _ = expr.apply(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<PhysicalColumn>() {
            columns.insert(column.name().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    columns
}

/// Analyze predicate properties for file pruning.
#[derive(Debug)]
pub struct PredicateProperties {
    pub partition_columns: Vec<String>,
    pub partition_only: bool,
    pub referenced_columns: HashSet<String>,
}

impl PredicateProperties {
    pub fn new(partition_columns: Vec<String>) -> Self {
        Self {
            partition_columns,
            partition_only: true,
            referenced_columns: HashSet::new(),
        }
    }

    pub fn analyze_predicate(&mut self, expr: &Arc<dyn PhysicalExpr>) -> DeltaResult<()> {
        self.referenced_columns = collect_physical_columns(expr);

        self.partition_only = self
            .referenced_columns
            .iter()
            .all(|col| self.partition_columns.contains(col));

        Ok(())
    }
}
