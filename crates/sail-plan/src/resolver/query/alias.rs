use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};
use sail_common::spec;
use sail_sql_analyzer::query::AUTO_GENERATED_SUBQUERY_NAME;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_subquery_alias(
        &self,
        input: spec::QueryPlan,
        alias: spec::Identifier,
        qualifier: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            self.resolve_table_reference(&spec::ObjectName::from(qualifier).child(alias))?,
        )?))
    }

    pub(super) async fn resolve_query_table_alias(
        &self,
        input: spec::QueryPlan,
        name: spec::Identifier,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let input = if columns.is_empty() {
            input
        } else {
            if columns.len() != schema.fields().len() {
                return Err(PlanError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    columns.len(),
                    schema.fields().len()
                )));
            }
            let expr: Vec<Expr> = schema
                .columns()
                .into_iter()
                .zip(columns)
                .map(|(col, name)| Expr::Column(col.clone()).alias(state.register_field_name(name)))
                .collect();
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?)
        };
        let alias = TableReference::Bare {
            table: Arc::from(String::from(name)),
        };
        if alias.table() == AUTO_GENERATED_SUBQUERY_NAME {
            // Spark removes subquery aliases before decorrelation, and correlated predicates
            // cannot be pulled up through `SubqueryAlias` here. So the generated alias only
            // requalifies the output and, like Spark, stops missing-reference resolution.
            // Give the projection fresh field IDs so optimizer rewrites cannot combine
            // its qualified output with unqualified input columns of the same name.
            let names = Self::get_field_names(input.schema(), state)?;
            let expr = input
                .schema()
                .columns()
                .into_iter()
                .zip(names)
                .map(|(col, name)| NamedExpr::new(vec![name], Expr::Column(col)))
                .collect();
            let mut expr = self.rewrite_named_expressions(expr, state)?;
            for expr in &mut expr {
                if let Expr::Alias(expr) = expr {
                    expr.relation = Some(alias.clone());
                }
            }
            let plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?);
            state.register_missing_input_boundary(&plan);
            return Ok(plan);
        }
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            alias,
        )?))
    }
}
