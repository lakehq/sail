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

use datafusion::arrow::datatypes::{DataType as ArrowDataType, FieldRef, Schema as ArrowSchema};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::simplify::SimplifyContextBuilder;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{
    Column as PhysicalColumn, Literal as PhysicalLiteral,
};

use crate::schema::arrow_field_physical_name;
use crate::spec::{ColumnMappingMode, DeltaResult};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

/// Simplify a logical expression and convert it to a physical expression.
pub fn simplify_expr(
    session: &dyn Session,
    df_schema: &DFSchema,
    expr: Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    let simplify_context = SimplifyContextBuilder::default()
        .with_schema(df_schema.clone().into())
        .build();
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
        Expr::Column(Column { name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { op, .. }) => {
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

/// Rewrite column references in a parquet pushdown predicate from logical names to the
/// physical names used in the data files of column-mapped tables. Nested struct field
/// accesses (`get_field`) are rewritten as well, using the column mapping metadata carried
/// by the logical schema. Partition columns are exposed to the file scan as virtual columns
/// under their logical names and are left untouched.
pub fn rewrite_predicate_for_column_mapping(
    expr: Arc<dyn PhysicalExpr>,
    logical_schema: &ArrowSchema,
    mode: ColumnMappingMode,
    partition_cols: &[String],
) -> Result<Arc<dyn PhysicalExpr>> {
    if mode == ColumnMappingMode::None {
        return Ok(expr);
    }
    Ok(rewrite_expr_for_column_mapping(expr, logical_schema, mode, partition_cols)?.0)
}

/// Recursively rewrite an expression, returning the rewritten expression along with the
/// logical schema field it resolves to (when the expression is a column or a chain of
/// struct field accesses rooted at a column).
fn rewrite_expr_for_column_mapping(
    expr: Arc<dyn PhysicalExpr>,
    logical_schema: &ArrowSchema,
    mode: ColumnMappingMode,
    partition_cols: &[String],
) -> Result<(Arc<dyn PhysicalExpr>, Option<FieldRef>)> {
    if let Some(column) = expr.downcast_ref::<PhysicalColumn>() {
        if partition_cols.iter().any(|col| col == column.name()) {
            return Ok((expr, None));
        }
        let Some((_, field)) = logical_schema.fields().find(column.name()) else {
            return Ok((expr, None));
        };
        let physical_name = arrow_field_physical_name(field, mode);
        let rewritten: Arc<dyn PhysicalExpr> = if physical_name == column.name() {
            Arc::clone(&expr)
        } else {
            Arc::new(PhysicalColumn::new(physical_name, column.index()))
        };
        return Ok((rewritten, Some(Arc::clone(field))));
    }

    if ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref()).is_some()
        && let [source_expr, name_expr] = expr.children().as_slice()
    {
        let (new_source, source_field) = rewrite_expr_for_column_mapping(
            Arc::clone(source_expr),
            logical_schema,
            mode,
            partition_cols,
        )?;
        let field_name = name_expr
            .downcast_ref::<PhysicalLiteral>()
            .and_then(|lit| lit.value().try_as_str().flatten());
        let child_field = match (&source_field, field_name) {
            (Some(source_field), Some(name)) => match source_field.data_type() {
                ArrowDataType::Struct(children) => {
                    children.iter().find(|f| f.name() == name).cloned()
                }
                _ => None,
            },
            _ => None,
        };
        let new_name_expr = child_field.as_ref().and_then(|child| {
            let physical_name = arrow_field_physical_name(child, mode);
            (physical_name != child.name().as_str()).then(|| {
                Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some(
                    physical_name.to_string(),
                )))) as Arc<dyn PhysicalExpr>
            })
        });
        if new_name_expr.is_some() || !Arc::ptr_eq(&new_source, source_expr) {
            let new_name = new_name_expr.unwrap_or_else(|| Arc::clone(name_expr));
            let rewritten = Arc::clone(&expr).with_new_children(vec![new_source, new_name])?;
            return Ok((rewritten, child_field));
        }
        return Ok((expr, child_field));
    }

    let children = expr.children();
    if children.is_empty() {
        return Ok((expr, None));
    }
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let (new_child, _) = rewrite_expr_for_column_mapping(
            Arc::clone(child),
            logical_schema,
            mode,
            partition_cols,
        )?;
        changed |= !Arc::ptr_eq(&new_child, child);
        new_children.push(new_child);
    }
    if changed {
        Ok((Arc::clone(&expr).with_new_children(new_children)?, None))
    } else {
        Ok((expr, None))
    }
}

/// Extract column names referenced by a `PhysicalExpr`.
pub fn collect_physical_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<String> {
    let mut columns = HashSet::<String>::new();
    let _ = expr.apply(|expr| {
        if let Some(column) = expr.downcast_ref::<PhysicalColumn>() {
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
