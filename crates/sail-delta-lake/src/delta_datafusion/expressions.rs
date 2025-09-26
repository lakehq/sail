use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, SessionStateBuilder};
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
use deltalake::errors::{DeltaResult, DeltaTableError};

/// Simplify a logical expression and convert it to a physical expression
pub fn simplify_expr(
    runtime_env: Arc<RuntimeEnv>,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    #[allow(clippy::expect_used)]
    let simplified = simplifier
        .simplify(expr)
        .expect("Failed to simplify expression");

    let session_state = SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .build();

    #[allow(clippy::expect_used)]
    session_state
        .create_physical_expr(simplified, df_schema)
        .expect("Failed to create physical expression")
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
    #[allow(clippy::expect_used)]
    expr.apply(|expr| match expr {
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
    })
    .expect("Failed to apply expression transformation");
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
    state: &'a SessionState,
}

impl<'a> DeltaContextProvider<'a> {
    pub fn new(state: &'a SessionState) -> Self {
        DeltaContextProvider { state }
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
        #[allow(clippy::disallowed_methods)]
        self.state.expr_planners()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, _var: &[String]) -> Option<ArrowDataType> {
        unimplemented!("DeltaContextProvider does not support variables")
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

/// Parse a string predicate into a DataFusion `Expr`
pub fn parse_predicate_expression(
    schema: &DFSchema,
    expr: impl AsRef<str>,
    state: &SessionState,
) -> DeltaResult<Expr> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
    let tokens = tokenizer
        .tokenize()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to tokenize expression: {err}")))?;

    let sql = Parser::new(dialect)
        .with_tokens(tokens)
        .parse_expr()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to parse expression: {err}")))?;

    let context_provider = DeltaContextProvider::new(state);
    let sql_to_rel = SqlToRel::new(&context_provider);

    sql_to_rel
        .sql_to_expr(sql, schema, &mut Default::default())
        .map_err(|err| {
            DeltaTableError::Generic(format!("Failed to convert SQL to expression: {err}"))
        })
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
