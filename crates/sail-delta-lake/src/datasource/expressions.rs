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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/expr.rs>

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, ToDFSchema};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    AggregateUDF, BinaryExpr, Expr, Operator, ScalarUDF, TableProviderFilterPushDown, TableSource,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;

use crate::kernel::snapshot::LogDataHandler;
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::schema::arrow_schema_from_struct_type;

/// Simplify a logical expression and convert it to a physical expression
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

/// Determine which filters can be pushed down to the table provider
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

/// Check if an expression is an exact predicate for the given columns
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

/// Extract column names referenced by a PhysicalExpr
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

/// Simple context provider for Delta Lake expression parsing
pub struct DeltaContextProvider<'a> {
    session: &'a dyn Session,
}

impl<'a> DeltaContextProvider<'a> {
    pub fn new(session: &'a dyn Session) -> Self {
        DeltaContextProvider { session }
    }
}

impl ContextProvider for DeltaContextProvider<'_> {
    fn get_table_source(
        &self,
        _name: datafusion::common::TableReference,
    ) -> Result<Arc<dyn TableSource>> {
        unimplemented!("DeltaContextProvider does not support table sources")
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.session.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.session.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        self.session.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, _var: &[String]) -> Option<ArrowDataType> {
        unimplemented!("DeltaContextProvider does not support variables")
    }

    fn options(&self) -> &ConfigOptions {
        self.session.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Parse a string predicate into a DataFusion `Expr`
pub fn parse_predicate_expression(
    schema: &DFSchema,
    expr: impl AsRef<str>,
    session: &dyn Session,
) -> DeltaResult<Expr> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
    let tokens = tokenizer
        .tokenize()
        .map_err(|err| DeltaTableError::generic(format!("Failed to tokenize expression: {err}")))?;

    let sql = Parser::new(dialect)
        .with_tokens(tokens)
        .parse_expr()
        .map_err(|err| DeltaTableError::generic(format!("Failed to parse expression: {err}")))?;

    let context_provider = DeltaContextProvider::new(session);
    let sql_to_rel = SqlToRel::new(&context_provider);

    sql_to_rel
        .sql_to_expr(sql, schema, &mut Default::default())
        .map_err(|err| {
            DeltaTableError::generic(format!("Failed to convert SQL to expression: {err}"))
        })
}

/// Parse predicate strings using the schema materialized in [`LogDataHandler`]
pub fn parse_log_data_predicate(
    read_snapshot: &LogDataHandler<'_>,
    expr: impl AsRef<str>,
    session: &dyn Session,
) -> DeltaResult<Expr> {
    let table_config = read_snapshot.table_configuration();
    let schema = table_config.schema();
    let arrow_schema = arrow_schema_from_struct_type(
        schema.as_ref(),
        table_config.metadata().partition_columns(),
        false,
    )?;
    let df_schema = arrow_schema.to_dfschema_ref()?;
    parse_predicate_expression(df_schema.as_ref(), expr, session)
}

/// Analyze predicate properties for file pruning
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
